"""Optiply target sink classes."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List

from target_optiply.base_sink import BaseOptiplySink, _products_id_cache

# In-memory cache: source inputId → Optiply id, populated by SupplierSink during the run
_suppliers_id_cache: Dict[str, str] = {}

# In-memory cache: source inputId → Optiply id, populated by SupplierProductSink during the run
_supplier_products_id_cache: Dict[str, str] = {}

# In-memory cache: source inputId → Optiply id, populated by BuyOrderSink during the run
_buy_orders_id_cache: Dict[str, str] = {}

# In-memory cache: source inputId → Optiply id, populated by BuyOrderLineSink during the run
_buy_order_lines_id_cache: Dict[str, str] = {}
from target_optiply.unified_schemas import (
    BuyOrderLineSchema,
    BuyOrderSchema,
    ProductCompositionSchema,
    ProductSchema,
    ReceiptLineSchema,
    SellOrderLineSchema,
    SellOrderSchema,
    SupplierProductSchema,
    SupplierSchema,
)

_INVALID_EMAIL_PHRASE = "not a valid address"


class ProductsSink(BaseOptiplySink):
    """Products sink."""

    endpoint = "products"
    unified_schema = ProductSchema

    @property
    def name(self) -> str:
        return "Products"

    def get_mandatory_fields(self) -> List[str]:
        return ["name", "stockLevel"]

    def process_record(self, record: dict, context: dict) -> None:
        self._last_was_fatal = False
        super().process_record(record, context)
        if self._last_was_fatal:
            BaseOptiplySink._job_healthy = False

    def upsert_record(self, record: dict, context: dict) -> tuple:
        record_id, success, state_updates = super().upsert_record(record, context)
        if success and record_id and self._stashed_external_id:
            _products_id_cache[str(self._stashed_external_id)] = str(record_id)
        return record_id, success, state_updates


class SupplierSink(BaseOptiplySink):
    """Suppliers sink."""

    endpoint = "suppliers"
    snapshot_name = "suppliers"
    unified_schema = SupplierSchema

    @property
    def name(self) -> str:
        return "Suppliers"

    def get_mandatory_fields(self) -> List[str]:
        return ["name"]

    def upsert_record(self, record: dict, context: dict) -> tuple:
        record_id, success, state_updates = super().upsert_record(record, context)

        # Retry without emails if Optiply rejects due to invalid email address
        if not success and state_updates.get("error", "").lower().find(_INVALID_EMAIL_PHRASE) != -1:
            self.logger.warning("Supplier has invalid email — retrying without emails field")
            if "data" in record and "attributes" in record["data"]:
                record["data"]["attributes"].pop("emails", None)
            record_id, success, state_updates = super().upsert_record(record, context)

        if success and record_id and self._stashed_external_id:
            _suppliers_id_cache[str(self._stashed_external_id)] = str(record_id)

        return record_id, success, state_updates


class SupplierProductSink(BaseOptiplySink):
    """Supplier products sink."""

    endpoint = "supplierProducts"
    snapshot_name = "supplier_products"
    unified_schema = SupplierProductSchema

    @property
    def name(self) -> str:
        return "SupplierProducts"

    def get_mandatory_fields(self) -> List[str]:
        return ["name", "productId", "supplierId"]

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        remote_product = record.get("Remote_productId")
        remote_supplier = record.get("Remote_supplierId")

        product_id = self._normalize_id(
            (_products_id_cache.get(str(remote_product)) if remote_product else None)
            or self._normalize_id(record.get("productId"))
        )

        supplier_id = self._normalize_id(
            (_suppliers_id_cache.get(str(remote_supplier)) if remote_supplier else None)
            or self._normalize_id(record.get("supplierId"))
        )

        if not product_id or not supplier_id:
            self.logger.warning(
                f"SupplierProducts skipped: could not resolve IDs "
                f"(Remote_productId={remote_product}, Remote_supplierId={remote_supplier}) — "
                f"product or supplier may no longer exist"
            )
            attributes.pop("productId", None)
            attributes.pop("supplierId", None)
            return

        attributes["productId"] = product_id
        attributes["supplierId"] = supplier_id

    def upsert_record(self, record: dict, context: dict) -> tuple:
        record_id, success, state_updates = super().upsert_record(record, context)

        # POST 409 conflict: supplier product already exists — GET to retrieve Optiply ID
        if not success and self._last_response_status == 409:
            attributes = {}
            if "data" in record and "attributes" in record["data"]:
                attributes = record["data"]["attributes"]
            product_id = attributes.get("productId")
            supplier_id = attributes.get("supplierId")
            if product_id and supplier_id:
                self.logger.warning(
                    f"SupplierProducts POST 409 — fetching existing record "
                    f"(productId={product_id}, supplierId={supplier_id})"
                )
                get_response = self.request_api(
                    http_method="GET",
                    endpoint=self.endpoint,
                    params={"filter[productId]": product_id, "filter[supplierId]": supplier_id},
                )
                if get_response.status_code == 200:
                    items = get_response.json().get("data", [])
                    if items:
                        existing_id = items[0].get("id")
                        if existing_id and self._stashed_external_id:
                            _supplier_products_id_cache[str(self._stashed_external_id)] = str(existing_id)
                        return existing_id, True, {"_action": "upsert"}

        # PATCH 404: resource gone — re-POST
        if not success and self._last_response_status == 404:
            if "data" in record and record["data"].get("id"):
                self.logger.warning("SupplierProducts PATCH 404 — re-posting as new record")
                record["data"].pop("id", None)
                record_id, success, state_updates = super().upsert_record(record, context)

        if success and record_id and self._stashed_external_id:
            _supplier_products_id_cache[str(self._stashed_external_id)] = str(record_id)

        return record_id, success, state_updates


class BuyOrderSink(BaseOptiplySink):
    """Buy orders sink."""

    endpoint = "buyOrders"
    snapshot_name = "buy_orders"
    unified_schema = BuyOrderSchema

    @property
    def name(self) -> str:
        return "BuyOrders"

    def get_mandatory_fields(self) -> List[str]:
        return ["placed", "totalValue", "supplierId", "accountId"]

    def upsert_record(self, record: dict, context: dict) -> tuple:
        record_id, success, state_updates = super().upsert_record(record, context)
        if success and record_id and self._stashed_external_id:
            _buy_orders_id_cache[str(self._stashed_external_id)] = str(record_id)
        return record_id, success, state_updates

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        remote_supplier = record.get("Remote_supplierId")
        supplier_id = (
            _suppliers_id_cache.get(str(remote_supplier)) if remote_supplier else None
        ) or attributes.get("supplierId")
        if supplier_id is not None:
            try:
                attributes["supplierId"] = int(float(str(supplier_id)))
            except (ValueError, TypeError):
                pass

        account_id = self.config.get("account_id")
        if account_id is not None:
            try:
                attributes["accountId"] = int(account_id)
            except (ValueError, TypeError):
                pass

        total_value, order_lines = self._parse_line_items(record, "buyOrderLines")
        if order_lines:
            attributes["totalValue"] = str(total_value)
            attributes["orderLines"] = order_lines


class BuyOrderLineSink(BaseOptiplySink):
    """Buy order lines sink."""

    endpoint = "buyOrderLines"
    snapshot_name = "buy_order_lines"
    unified_schema = BuyOrderLineSchema

    @property
    def name(self) -> str:
        return "BuyOrderLines"

    def get_mandatory_fields(self) -> List[str]:
        return ["subtotalValue", "productId", "quantity", "buyOrderId"]

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        remote_product = record.get("Remote_productId")
        product_id = (
            _products_id_cache.get(str(remote_product)) if remote_product else None
        ) or attributes.get("productId")
        if product_id is not None:
            try:
                attributes["productId"] = int(float(str(product_id)))
            except (ValueError, TypeError):
                pass

        remote_buy_order = record.get("Remote_buyOrderId")
        buy_order_id = (
            _buy_orders_id_cache.get(str(remote_buy_order)) if remote_buy_order else None
        ) or attributes.get("buyOrderId")
        if buy_order_id is not None:
            try:
                attributes["buyOrderId"] = int(float(str(buy_order_id)))
            except (ValueError, TypeError):
                pass

    def upsert_record(self, record: dict, context: dict) -> tuple:
        record_id, success, state_updates = super().upsert_record(record, context)
        if success and record_id and self._stashed_external_id:
            _buy_order_lines_id_cache[str(self._stashed_external_id)] = str(record_id)
        return record_id, success, state_updates


class SellOrderSink(BaseOptiplySink):
    """Sell orders sink."""

    endpoint = "sellOrders"
    snapshot_name = "sell_orders"
    unified_schema = SellOrderSchema

    @property
    def name(self) -> str:
        return "SellOrders"

    def get_mandatory_fields(self) -> List[str]:
        return ["totalValue", "placed"]

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        total_value, order_lines = self._parse_line_items(record, "sellOrderLines")
        if order_lines:
            attributes["totalValue"] = str(total_value)
            attributes["orderLines"] = order_lines


class SellOrderLineSink(BaseOptiplySink):
    """Sell order lines sink."""

    endpoint = "sellOrderLines"
    snapshot_name = "sell_order_lines"
    unified_schema = SellOrderLineSchema

    @property
    def name(self) -> str:
        return "SellOrderLines"

    def get_mandatory_fields(self) -> List[str]:
        return ["subtotalValue", "sellOrderId", "productId", "quantity"]


class ProductCompositionSink(BaseOptiplySink):
    """Product compositions sink."""

    endpoint = "productCompositions"
    unified_schema = ProductCompositionSchema

    @property
    def name(self) -> str:
        return "ProductCompositions"

    def get_mandatory_fields(self) -> List[str]:
        return ["composedProductId", "partProductId", "partQuantity"]

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        remote_composed = record.get("Remote_composedProductId")
        remote_part = record.get("Remote_partProductId")

        composed_id = self._normalize_id(
            (_products_id_cache.get(str(remote_composed)) if remote_composed else None)
            or self._normalize_id(record.get("composedProductId"))
        )

        part_id = self._normalize_id(
            (_products_id_cache.get(str(remote_part)) if remote_part else None)
            or self._normalize_id(record.get("partProductId"))
        )

        if not composed_id or not part_id:
            self.logger.warning(
                f"ProductCompositions skipped: could not resolve IDs "
                f"(Remote_composedProductId={remote_composed}, Remote_partProductId={remote_part}) — "
                f"product may no longer exist"
            )
            attributes.pop("composedProductId", None)
            attributes.pop("partProductId", None)
            return

        attributes["composedProductId"] = composed_id
        attributes["partProductId"] = part_id


class ReceiptLineSink(BaseOptiplySink):
    """Receipt lines sink."""

    endpoint = "receiptLines"
    unified_schema = ReceiptLineSchema

    @property
    def name(self) -> str:
        return "ReceiptLines"

    def get_mandatory_fields(self) -> List[str]:
        return ["occurred", "quantity", "buyOrderLineId"]

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        remote_bol = record.get("Remote_buyOrderLineId")
        buy_order_line_id = (
            _buy_order_lines_id_cache.get(str(remote_bol)) if remote_bol else None
        ) or attributes.get("buyOrderLineId")
        if buy_order_line_id is not None:
            attributes["buyOrderLineId"] = str(buy_order_line_id)

        # remoteId is aliased to externalId by _FIELD_ALIASES; restore it for the Optiply payload
        remote_id = record.get("externalId") or self._stashed_external_id
        if remote_id:
            attributes["remoteId"] = str(remote_id)

        attributes["remoteDataSyncedToDate"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
