"""Optiply target sink classes."""

from __future__ import annotations

from typing import Dict, List

from target_optiply.base_sink import BaseOptiplySink
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
    relation_fields = [
        {"field": "Remote_composedProductId", "objectName": "Products"},
        {"field": "Remote_partProductId", "objectName": "Products"},
    ]

    @property
    def name(self) -> str:
        return "ProductCompositions"

    def get_mandatory_fields(self) -> List[str]:
        return ["composedProductId", "partProductId", "partQuantity"]

    def _add_additional_attributes(self, record: Dict, attributes: Dict) -> None:
        attributes["composedProductId"] = (
            record.get("composedProductId") or record.get("Remote_composedProductId")
        )
        attributes["partProductId"] = (
            record.get("partProductId") or record.get("Remote_partProductId")
        )
