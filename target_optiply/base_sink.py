"""Base sink class for Optiply streams."""

from __future__ import annotations

import csv
import json
import logging
import os
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Type

from pydantic import BaseModel
from singer_sdk.plugin_base import PluginBase

from target_optiply.client import OptiplySink

logger = logging.getLogger(__name__)

_root_dir = os.environ.get("ROOT_DIR", ".")
SNAPSHOT_DIR = os.environ.get("SNAPSHOT_DIR") or f"{_root_dir}/snapshots"


class BaseOptiplySink(OptiplySink):
    """Base sink for Optiply streams."""

    endpoint = None
    field_mappings = {}
    unified_schema: Optional[Type[BaseModel]] = None
    # Override per sink to match ETL snapshot filename (e.g. "supplier_products")
    snapshot_name: Optional[str] = None
    # Set to True on a sink to enable ETL snapshot writing in clean_up()
    write_etl_snapshot: bool = False
    concat_exclude_fields: tuple = ()

    def __init__(self, target: PluginBase, stream_name: str, schema: Dict, key_properties: List[str]):
        super().__init__(target, stream_name, schema, key_properties)
        self.endpoint = self.stream_name.lower() if not self.endpoint else self.endpoint
        self._etl_snapshot_cache: Optional[Dict[str, dict]] = None
        self._record_count: int = 0
        total_env = os.environ.get(f"STREAM_TOTAL_{stream_name.upper()}")
        self._record_total: Optional[int] = int(total_env) if total_env else None

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Preprocess the record before sending to API."""
        # Stash original source fields for snapshot writing in clean_up().
        # Note: externalId is popped by the SDK before this call; it will be
        # re-injected after and retrieved from upsert_record's record param.
        self._current_original = {
            k: v for k, v in record.items()
            if not k.startswith("_sdc_") and k != "id"
        }

        # Apply unified schema validation and type coercion
        if self.unified_schema is not None:
            try:
                validated = self.unified_schema.model_validate(record, strict=False)
                attributes = validated.model_dump(exclude_none=True)
            except Exception as e:
                self.logger.warning(f"Schema validation warning for {self.stream_name}: {e}")
                attributes = self.build_attributes(record, self.field_mappings)
        else:
            attributes = self.build_attributes(record, self.field_mappings)

        self._add_additional_attributes(record, attributes)

        # Compute concat_attributes from API-bound fields, excluding datetimes
        self._current_concat = self._compute_concat_attributes(attributes)

        payload = {
            "data": {
                "type": self.endpoint,
                "attributes": attributes,
            }
        }

        if "id" in record:
            payload["data"]["id"] = record["id"]

        return payload

    def upsert_record(self, record: dict, context: dict) -> tuple:
        """Process the record and return (id, success, state_updates)."""
        # externalId is re-injected by the SDK after preprocess_record
        external_id = record.get("externalId")
        original = {**getattr(self, "_current_original", {})}
        if external_id:
            original["externalId"] = external_id

        concat = getattr(self, "_current_concat", "")

        # Check for delete signal
        deleted_at = record.get("_sdc_deleted_at") or original.get("deleted_at")

        try:
            record_id = None
            if "data" in record and "id" in record["data"]:
                record_id = record["data"]["id"]
            elif "id" in record:
                record_id = record["id"]

            self._record_count += 1

            if deleted_at and record_id:
                http_method = "DELETE"
            elif record_id:
                # PATCH — check concat_attributes to skip if unchanged
                if self.write_etl_snapshot and external_id:
                    snapshot = self._load_etl_snapshot()
                    existing = snapshot.get(str(external_id))
                    if existing and existing.get("concat_attributes") == concat:
                        count_label = f"#{self._record_count}/{self._record_total}" if self._record_total else f"#{self._record_count}"
                        self.logger.info(f"{self.stream_name} [{count_label}] SKIPPED (no changes): {external_id}")
                        return record_id, True, {"_action": "skip"}
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
                    error_msg = f"Record skipped due to missing mandatory fields: {', '.join(missing_fields)}"
                    self.logger.error(error_msg)
                    return None, False, {"error": error_msg}

            endpoint = f"{self.endpoint}/{record_id}" if record_id else self.endpoint
            request_data = None if http_method == "DELETE" else record
            response = self.request_api(http_method=http_method, endpoint=endpoint, request_data=request_data)

            if response.status_code == 404:
                error_details = self._get_error_message(response.text, response.status_code, response.url)
                self.logger.warning(f"Record not found (404): {record_id} - {error_details}")
                return None, False, {"error": error_details}
            elif response.status_code >= 400:
                error_details = self._get_error_message(response.text, response.status_code, response.url)
                self.logger.error(f"Request failed with status {response.status_code}: {error_details}")
                return None, False, {"error": error_details}

            response_data = response.json() if http_method != "DELETE" else {}
            response_record_id = response_data.get("data", {}).get("id") or record_id or "unknown"

            state_updates = {
                "_snapshot_row": {**original, "concat_attributes": concat},
                "_action": "delete" if http_method == "DELETE" else "upsert",
            }
            if http_method == "PATCH":
                state_updates["is_updated"] = True

            return response_record_id, True, state_updates

        except Exception as e:
            error_msg = f"Error processing record: {str(e)}"
            self.logger.error(error_msg)
            return None, False, {"error": error_msg}

    def clean_up(self) -> None:
        """Write ETL snapshot after stream completes — only confirmed successes."""
        if not self.write_etl_snapshot:
            super().clean_up()
            return

        bookmarks = self.latest_state.get("bookmarks", {}).get(self.name, [])

        upsert_rows = []
        delete_rows = []
        for entry in bookmarks:
            if not entry.get("success") or not entry.get("_snapshot_row"):
                continue
            if entry.get("_action") == "delete":
                delete_rows.append(entry["_snapshot_row"])
            else:
                upsert_rows.append(entry["_snapshot_row"])

        if upsert_rows or delete_rows:
            self._update_etl_snapshot(upsert_rows, delete_rows)

        super().clean_up()

    def _update_etl_snapshot(self, upsert_rows: List[dict], delete_rows: List[dict]) -> None:
        """Upsert/delete rows in the ETL snapshot CSV."""
        name = self.snapshot_name or self.name.lower()
        path = os.path.join(SNAPSHOT_DIR, f"{name}.snapshot.csv")
        Path(SNAPSHOT_DIR).mkdir(parents=True, exist_ok=True)

        # Read existing snapshot keyed by remoteId
        existing: Dict[str, dict] = {}
        if os.path.isfile(path):
            with open(path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    existing[row["remoteId"]] = row

        # Apply deletes
        for row in delete_rows:
            remote_id = str(row.get("externalId") or row.get("remoteId") or "")
            existing.pop(remote_id, None)

        # Apply upserts — map externalId → remoteId
        for row in upsert_rows:
            remote_id = str(row.get("externalId") or row.get("remoteId") or "")
            if not remote_id:
                continue
            snapshot_row = {k: ("" if v is None else str(v)) for k, v in row.items()
                            if k not in ("externalId", "id")}
            snapshot_row["remoteId"] = remote_id
            existing[remote_id] = snapshot_row

        if not existing:
            return

        # Collect all fieldnames preserving remoteId first
        all_fields: List[str] = ["remoteId"]
        for row in existing.values():
            for k in row:
                if k not in all_fields:
                    all_fields.append(k)

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_fields, extrasaction="ignore", restval="")
            writer.writeheader()
            writer.writerows(existing.values())

        self.logger.info(f"ETL snapshot updated: {path} ({len(existing)} rows)")

    def _load_etl_snapshot(self) -> Dict[str, dict]:
        """Load the ETL snapshot once per stream run, keyed by remoteId."""
        if self._etl_snapshot_cache is not None:
            return self._etl_snapshot_cache

        name = self.snapshot_name or self.name.lower()
        path = os.path.join(SNAPSHOT_DIR, f"{name}.snapshot.csv")
        cache: Dict[str, dict] = {}
        if os.path.isfile(path):
            with open(path, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    cache[row["remoteId"]] = row

        self._etl_snapshot_cache = cache
        return cache

    def _compute_concat_attributes(self, attributes: dict) -> str:
        """Build a pipe-delimited string from API attributes, excluding configured fields."""
        values = []
        for k in sorted(attributes.keys()):
            if k in self.concat_exclude_fields:
                continue
            values.append(str(attributes[k]))
        return "|".join(values)

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
            line = {
                "type": line_type,
                "attributes": {
                    "quantity": item["quantity"],
                    "subtotalValue": str(subtotal),
                    "productId": item["productId"],
                },
            }
            if "expectedDeliveryDate" in item:
                line["attributes"]["expectedDeliveryDate"] = item["expectedDeliveryDate"]
            order_lines.append(line)

        return total_value, order_lines

    def get_mandatory_fields(self) -> List[str]:
        return []
