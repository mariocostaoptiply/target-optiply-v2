"""Base sink class for Optiply streams."""

from __future__ import annotations

import json
import logging
import math
import os
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Type

from pydantic import BaseModel
from singer_sdk.exceptions import FatalAPIError
from singer_sdk.plugin_base import PluginBase

from target_optiply.client import OptiplySink

logger = logging.getLogger(__name__)

_products_id_cache: Dict[str, str] = {}

_FIELD_ALIASES = {
    "optiply_id": "id",
    "remoteId": "externalId",
    "updated_at": "updatedAt",
    "created_at": "createdAtRemote",
}


class BaseOptiplySink(OptiplySink):
    """Base sink for Optiply streams."""

    endpoint = None
    field_mappings = {}
    unified_schema: Optional[Type[BaseModel]] = None
    _job_healthy: bool = True  # class-level flag; False skips all remaining records across all sinks
    _job_unhealthy_reason: str = ""  # stores the error that caused the job to be marked unhealthy

    def __init__(self, target: PluginBase, stream_name: str, schema: Dict, key_properties: List[str]):
        super().__init__(target, stream_name, schema, key_properties)
        self.endpoint = self.stream_name.lower() if not self.endpoint else self.endpoint
        self._record_count: int = 0
        self._stashed_external_id = None
        self._last_was_fatal = False
        self._last_response_status: Optional[int] = None
        total_env = os.environ.get(f"STREAM_TOTAL_{stream_name.upper()}")
        self._record_total: Optional[int] = int(total_env) if total_env else None

    def process_record(self, record: dict, context: dict) -> None:
        self._stashed_external_id = record.get("externalId") or record.get("inputId")
        super().process_record(record, context)

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Preprocess the record before sending to API."""
        for alias, canonical in _FIELD_ALIASES.items():
            if alias in record and canonical not in record:
                record[canonical] = record.pop(alias)
        # Apply unified schema validation and type coercion
        if self.unified_schema is not None:
            try:
                validated = self.unified_schema.model_validate(record, strict=False)
                attributes = validated.model_dump(mode="json", exclude_none=True)
            except Exception as e:
                reason = f"{self.stream_name} schema validation failed: {e}"
                self.logger.error(reason)
                BaseOptiplySink._job_healthy = False
                BaseOptiplySink._job_unhealthy_reason = reason
                raise FatalAPIError(reason)
        else:
            attributes = self.build_attributes(record, self.field_mappings)

        self._add_additional_attributes(record, attributes)

        payload = {
            "data": {
                "type": self.endpoint,
                "attributes": attributes,
            }
        }

        if "id" in record and record["id"] is not None and str(record["id"]).lower() not in ("nan", "", "none"):
            payload["data"]["id"] = str(record["id"])

        deleted_at = record.get("deleted_at") or record.get("_sdc_deleted_at")
        if deleted_at:
            payload["deleted_at"] = deleted_at

        return payload

    def upsert_record(self, record: dict, context: dict) -> tuple:
        """Process the record and return (id, success, state_updates)."""
        if not BaseOptiplySink._job_healthy:
            reason = BaseOptiplySink._job_unhealthy_reason or "unknown error"
            self.logger.warning(f"{self.stream_name} record skipped — job marked unhealthy: {reason}")
            return None, False, {"error": f"Skipped — job marked unhealthy: {reason}"}

        # externalId is double-popped by the SDK; stashed in process_record override
        external_id = record.get("externalId") or self._stashed_external_id

        # Check for delete signal
        deleted_at = record.get("_sdc_deleted_at") or record.get("deleted_at")

        try:
            record_id = None
            if "data" in record and "id" in record["data"]:
                record_id = record["data"]["id"]
            elif "id" in record:
                record_id = record["id"]

            if record_id is not None and str(record_id).lower() in ("nan", "", "none"):
                record_id = None

            self._record_count += 1

            if deleted_at and record_id:
                http_method = "DELETE"
            elif record_id:
                http_method = "PATCH"
            else:
                http_method = "POST"

            count_label = f"#{self._record_count}/{self._record_total}" if self._record_total else f"#{self._record_count}"
            self.logger.info(f"{self.stream_name} [{count_label}] {http_method} | externalId: {external_id}")

            if http_method == "POST":
                mandatory_fields = self.get_mandatory_fields()
                actual_record = record
                if "data" in record and "attributes" in record["data"]:
                    actual_record = record["data"]["attributes"]

                missing_fields = [
                    f for f in mandatory_fields
                    if f not in actual_record
                    or actual_record[f] is None
                    or (isinstance(actual_record[f], str) and not actual_record[f].strip())
                ]
                if missing_fields:
                    error_msg = f"Record missing mandatory fields: {', '.join(missing_fields)}"
                    self.logger.error(error_msg)
                    return None, False, {"error": error_msg}

            endpoint = f"{self.endpoint}/{record_id}" if record_id else self.endpoint
            request_data = None if http_method == "DELETE" else record
            response = self.request_api(http_method=http_method, endpoint=endpoint, request_data=request_data)

            self._last_response_status = response.status_code

            if response.status_code == 404:
                error_details = self._get_error_message(response.text, response.status_code, response.url)
                if http_method == "DELETE":
                    self.logger.warning(f"DELETE 404 — resource already gone, treating as success: {error_details}")
                    return record_id, True, {"_action": "delete"}
                self.logger.error(f"Request failed with status 404: {error_details}")
                return None, False, {"error": error_details}
            elif response.status_code == 409 and http_method in ("POST", "PATCH"):
                # 409 Conflict — subclasses (e.g. SupplierProductSink) handle recovery; do not mark job unhealthy
                error_details = self._get_error_message(response.text, response.status_code, response.url)
                self.logger.error(f"Request failed with status 409: {error_details}")
                self._last_response_status = response.status_code
                return None, False, {"error": error_details}
            elif response.status_code >= 400 and http_method in ("POST", "PATCH"):
                error_details = self._get_error_message(response.text, response.status_code, response.url)
                self.logger.error(f"Request failed with status {response.status_code}: {error_details}")
                self._last_was_fatal = True
                BaseOptiplySink._job_healthy = False
                BaseOptiplySink._job_unhealthy_reason = f"{self.stream_name} [{count_label}] {http_method} {response.status_code}: {error_details}"
                return None, False, {"error": error_details}
            elif response.status_code >= 400:
                error_details = self._get_error_message(response.text, response.status_code, response.url)
                self.logger.error(f"Request failed with status {response.status_code}: {error_details}")
                return None, False, {"error": error_details}

            response_data = response.json() if http_method != "DELETE" else {}
            response_record_id = response_data.get("data", {}).get("id") or record_id or "unknown"

            state_updates = {"_action": "delete" if http_method == "DELETE" else "upsert"}
            if http_method == "PATCH":
                state_updates["is_updated"] = True

            return response_record_id, True, state_updates

        except FatalAPIError:
            raise
        except Exception as e:
            error_msg = f"Error processing record: {str(e)}"
            self.logger.error(error_msg)
            return None, False, {"error": error_msg}

    @staticmethod
    def _normalize_id(v):
        """Return None if v is NaN, infinite, empty, or a 'nan'/'none' string — guards raw record fallbacks."""
        if v is None:
            return None
        if isinstance(v, float) and not math.isfinite(v):
            return None
        if isinstance(v, str) and v.strip().lower() in ("nan", "none", ""):
            return None
        return v

    def build_attributes(self, record: Dict, field_mappings: Dict[str, str]) -> Dict:
        attributes = {}
        for record_field, api_field in field_mappings.items():
            if record_field in record and record[record_field] is not None:
                value = record[record_field]
                if isinstance(value, datetime):
                    value = value.isoformat()
                elif isinstance(value, Decimal):
                    value = float(value)
                attributes[api_field] = value
        return attributes

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        """Override in subclasses to add attributes not covered by field mappings."""
        pass

    def _parse_line_items(self, record: Dict, line_type: str) -> tuple:
        """Parse line_items JSON string into (total_value, order_lines) for order sinks."""
        raw = record.get("line_items")
        if not raw:
            return None, []

        total_value = 0.0
        order_lines = []
        for item in json.loads(raw):
            subtotal = float(item["subtotalValue"])
            total_value += subtotal
            raw_product_id = item.get("productId")
            # float NaN is truthy — normalise to None so cache fallback works
            if isinstance(raw_product_id, float) and not math.isfinite(raw_product_id):
                raw_product_id = None
            product_id = raw_product_id or _products_id_cache.get(str(item.get("Remote_productId", "")))
            line = {
                "quantity": int(item["quantity"]),
                "subtotalValue": round(subtotal, 2),
                "productId": product_id,
            }
            if "expectedDeliveryDate" in item:
                line["expectedDeliveryDate"] = item["expectedDeliveryDate"]
            order_lines.append(line)

        return total_value, order_lines

    def get_mandatory_fields(self) -> List[str]:
        return []
