"""Optiply target sink classes."""

from __future__ import annotations

from typing import Dict, List

from target_optiply.base_sink import BaseOptiplySink

# In-memory cache: source inputId → Optiply id, populated by ProductsSink during the run
_products_id_cache: Dict[str, str] = {}
from target_optiply.unified_schemas import (
    BuyOrderLineSchema,
    BuyOrderSchema,
    ProductCompositionSchema,
    ProductSchema,
    SellOrderLineSchema,
    SellOrderSchema,
    SupplierProductSchema,
    SupplierSchema,
)


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
        # TEMP: simulate 400 failure on record 3 for testing
        if self._record_count == 2:
            self.logger.error("TEST: simulated 400 failure on record 3")
            self._last_was_fatal = True
            return None, False, {"error": "TEST: simulated 400 on record 3"}
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

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
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

        composed_id = (
            _products_id_cache.get(str(remote_composed)) if remote_composed else None
        ) or record.get("composedProductId")

        part_id = (
            _products_id_cache.get(str(remote_part)) if remote_part else None
        ) or record.get("partProductId")

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
