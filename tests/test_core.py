"""Core target tests."""

from __future__ import annotations

import pytest
from target_optiply.unified_schemas.products import ProductSchema
from target_optiply.unified_schemas.suppliers import SupplierSchema
from target_optiply.unified_schemas.supplier_products import SupplierProductSchema
from target_optiply.unified_schemas.buy_orders import BuyOrderSchema
from target_optiply.unified_schemas.sell_orders import SellOrderSchema


# ---------------------------------------------------------------------------
# ProductSchema
# ---------------------------------------------------------------------------

class TestProductSchema:

    def _valid(self, **overrides):
        base = {"name": "Test Product", "stockLevel": 10, "unlimitedStock": False}
        base.update(overrides)
        return ProductSchema.model_validate(base, strict=False)

    def test_mandatory_fields(self):
        p = self._valid()
        assert p.name == "Test Product"
        assert p.stockLevel == 10.0
        assert p.unlimitedStock is False

    def test_name_truncated_to_255(self):
        long_name = "A" * 300
        p = self._valid(name=long_name)
        assert len(p.name) == 255

    def test_sku_ean_article_truncated_to_255(self):
        long = "X" * 300
        p = self._valid(skuCode=long, eanCode=long, articleCode=long)
        assert len(p.skuCode) == 255
        assert len(p.eanCode) == 255
        assert len(p.articleCode) == 255

    def test_stock_measurement_unit_truncated_to_10(self):
        p = self._valid(stockMeasurementUnit="TOOLONGUNIT")
        assert len(p.stockMeasurementUnit) == 10

    def test_price_rounded_to_2_decimals(self):
        p = self._valid(price="9.999")
        assert p.price == 10.0

    def test_price_too_large_returns_none(self):
        p = self._valid(price="9999999999.99")  # 10 integer digits
        assert p.price is None

    def test_price_string_coercion(self):
        p = self._valid(price="12.5")
        assert p.price == 12.5

    def test_price_invalid_returns_none(self):
        p = self._valid(price="not-a-number")
        assert p.price is None

    def test_created_at_remote_parsed(self):
        p = self._valid(createdAtRemote="2023-01-15T10:30:00")
        assert p.createdAtRemote is not None
        assert "2023-01-15" in p.createdAtRemote

    def test_created_at_remote_adds_utc_if_missing(self):
        p = self._valid(createdAtRemote="2023-01-15T10:30:00")
        assert "+00:00" in p.createdAtRemote or "Z" in p.createdAtRemote or "UTC" in p.createdAtRemote

    def test_created_at_remote_invalid_returns_none(self):
        p = self._valid(createdAtRemote="not-a-date")
        assert p.createdAtRemote is None

    def test_updated_at_parsed(self):
        p = self._valid(updatedAt="2024-06-01T00:00:00+02:00")
        assert p.updatedAt is not None
        assert "2024-06-01" in p.updatedAt

    def test_extra_fields_ignored(self):
        p = self._valid(concat_attributes="somehash", optiply_id=123, externalId="abc")
        dumped = p.model_dump(exclude_none=True, exclude_unset=True)
        assert "concat_attributes" not in dumped
        assert "optiply_id" not in dumped
        assert "externalId" not in dumped

    def test_carriage_returns_stripped(self):
        p = self._valid(name="Hello\r\nWorld")
        assert "\r" not in p.name
        assert "\n" not in p.name

    def test_none_optional_excluded_from_dump(self):
        p = self._valid()
        dumped = p.model_dump(exclude_none=True, exclude_unset=True)
        assert "skuCode" not in dumped
        assert "price" not in dumped


# ---------------------------------------------------------------------------
# SupplierSchema
# ---------------------------------------------------------------------------

class TestSupplierSchema:

    def test_emails_parsed_from_json_string(self):
        s = SupplierSchema.model_validate(
            {"name": "Supplier A", "emails": '["a@b.com", "c@d.com"]'}, strict=False
        )
        assert s.emails == ["a@b.com", "c@d.com"]

    def test_emails_plain_string_wrapped_in_list(self):
        s = SupplierSchema.model_validate(
            {"name": "Supplier A", "emails": "a@b.com"}, strict=False
        )
        assert s.emails == ["a@b.com"]

    def test_type_invalid_defaults_to_vendor(self):
        s = SupplierSchema.model_validate(
            {"name": "Supplier A", "type": "unknown"}, strict=False
        )
        assert s.type == "vendor"

    def test_type_valid_values(self):
        for t in ["vendor", "producer"]:
            s = SupplierSchema.model_validate({"name": "X", "type": t}, strict=False)
            assert s.type == t

    def test_gln_wrong_length_returns_none(self):
        s = SupplierSchema.model_validate(
            {"name": "X", "globalLocationNumber": "123"}, strict=False
        )
        assert s.globalLocationNumber is None

    def test_gln_correct_length(self):
        s = SupplierSchema.model_validate(
            {"name": "X", "globalLocationNumber": "1234567890123"}, strict=False
        )
        assert s.globalLocationNumber == "1234567890123"

    def test_int_fields_coerced(self):
        s = SupplierSchema.model_validate(
            {"name": "X", "deliveryTime": "7.9"}, strict=False
        )
        assert s.deliveryTime == 7


# ---------------------------------------------------------------------------
# SupplierProductSchema
# ---------------------------------------------------------------------------

class TestSupplierProductSchema:

    def test_mandatory_fields(self):
        sp = SupplierProductSchema.model_validate(
            {"name": "SP", "productId": "10", "supplierId": "20"}, strict=False
        )
        assert sp.productId == "10"
        assert sp.supplierId == "20"

    def test_price_rounded(self):
        sp = SupplierProductSchema.model_validate(
            {"name": "SP", "productId": 1, "supplierId": 1, "price": "5.678"}, strict=False
        )
        assert sp.price == 5.68

    def test_minimum_purchase_quantity_below_1_returns_none(self):
        sp = SupplierProductSchema.model_validate(
            {"name": "SP", "productId": 1, "supplierId": 1, "minimumPurchaseQuantity": "0"}, strict=False
        )
        assert sp.minimumPurchaseQuantity is None

    def test_status_normalized(self):
        sp = SupplierProductSchema.model_validate(
            {"name": "SP", "productId": 1, "supplierId": 1, "status": "active"}, strict=False
        )
        assert sp.status == "enabled"


# ---------------------------------------------------------------------------
# BuyOrderSchema
# ---------------------------------------------------------------------------

class TestBuyOrderSchema:

    def test_line_items_excluded_from_dump(self):
        bo = BuyOrderSchema.model_validate(
            {"placed": "2024-01-01", "supplierId": 1, "accountId": 2,
             "line_items": '[{"subtotalValue": "10.0", "quantity": 1, "productId": 5}]'},
            strict=False
        )
        dumped = bo.model_dump(exclude_none=True, exclude_unset=True)
        assert "line_items" not in dumped

    def test_supplier_account_id_coerced(self):
        bo = BuyOrderSchema.model_validate(
            {"placed": "2024-01-01", "supplierId": "5.0", "accountId": "3.0"}, strict=False
        )
        assert bo.supplierId == 5
        assert bo.accountId == 3


# ---------------------------------------------------------------------------
# SellOrderSchema
# ---------------------------------------------------------------------------

class TestSellOrderSchema:

    def test_line_items_excluded_from_dump(self):
        so = SellOrderSchema.model_validate(
            {"placed": "2024-01-01",
             "line_items": '[{"subtotalValue": "5.0", "quantity": 2, "productId": 3}]'},
            strict=False
        )
        dumped = so.model_dump(exclude_none=True, exclude_unset=True)
        assert "line_items" not in dumped
