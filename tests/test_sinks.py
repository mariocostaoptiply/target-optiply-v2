"""Sink behaviour tests — no HTTP calls, no SDK init."""

from __future__ import annotations

import json
import logging
from unittest.mock import patch

import pytest
from singer_sdk.exceptions import FatalAPIError

import target_optiply.base_sink as base_sink_module
import target_optiply.sinks as sinks_module
from target_optiply.base_sink import BaseOptiplySink, _products_id_cache
from target_optiply.sinks import BuyOrderLineSink, BuyOrderSink, ReceiptLineSink, SellOrderSink


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _TestSellOrderSink(SellOrderSink):
    """SellOrderSink subclass that replaces the SDK config property for testing."""
    config = {}  # type: ignore[assignment]


def _make_sink() -> _TestSellOrderSink:
    """Create a sink instance without running the SDK/HotGlue __init__ chain."""
    sink = _TestSellOrderSink.__new__(_TestSellOrderSink)
    sink.__dict__.update({
        "logger": logging.getLogger("test"),
        "stream_name": "SellOrders",
        "endpoint": "sellOrders",
        "_stashed_external_id": None,
        "_record_count": 0,
        "_record_total": None,
        "_last_was_fatal": False,
        "_last_response_status": None,
    })
    return sink


# ---------------------------------------------------------------------------
# _parse_line_items
# ---------------------------------------------------------------------------

class TestParseLineItems:

    def test_product_id_already_resolved(self):
        sink = _make_sink()
        raw = json.dumps([
            {"quantity": 2, "subtotalValue": "10.00", "productId": "999", "Remote_productId": "abc"},
        ])
        total, lines = sink._parse_line_items({"line_items": raw}, "sellOrderLines")
        assert total == 10.0
        assert lines[0]["productId"] == "999"

    def test_product_id_resolved_from_cache(self):
        sink = _make_sink()
        base_sink_module._products_id_cache["src-42"] = "optiply-999"
        raw = json.dumps([
            {"quantity": 1, "subtotalValue": "5.00", "productId": None, "Remote_productId": "src-42"},
        ])
        try:
            total, lines = sink._parse_line_items({"line_items": raw}, "sellOrderLines")
            assert lines[0]["productId"] == "optiply-999"
        finally:
            base_sink_module._products_id_cache.pop("src-42", None)

    def test_product_id_missing_and_not_in_cache(self):
        sink = _make_sink()
        # ensure key absent from cache
        base_sink_module._products_id_cache.pop("unknown-id", None)
        raw = json.dumps([
            {"quantity": 1, "subtotalValue": "5.00", "productId": None, "Remote_productId": "unknown-id"},
        ])
        total, lines = sink._parse_line_items({"line_items": raw}, "sellOrderLines")
        # productId ends up None — Optiply will reject, but we don't silently drop the line
        assert lines[0]["productId"] is None

    def test_total_value_summed_correctly(self):
        sink = _make_sink()
        raw = json.dumps([
            {"quantity": 1, "subtotalValue": "10.00", "productId": "1", "Remote_productId": "r1"},
            {"quantity": 2, "subtotalValue": "20.00", "productId": "2", "Remote_productId": "r2"},
        ])
        total, lines = sink._parse_line_items({"line_items": raw}, "sellOrderLines")
        assert total == 30.0
        assert len(lines) == 2

    def test_no_line_items_returns_empty(self):
        sink = _make_sink()
        total, lines = sink._parse_line_items({}, "sellOrderLines")
        assert total is None
        assert lines == []


# ---------------------------------------------------------------------------
# preprocess_record — SellOrderSink
# ---------------------------------------------------------------------------

class TestSellOrderSinkPreprocess:

    def test_valid_record_builds_payload(self):
        sink = _make_sink()
        line_items = json.dumps([
            {"quantity": 2, "subtotalValue": "50.00", "productId": "123", "Remote_productId": "r1"},
        ])
        record = {
            "externalId": "SO-1",
            "placed": "2024-01-01T00:00:00Z",
            "totalValue": 100.0,
            "line_items": line_items,
        }
        payload = sink.preprocess_record(record, {})
        attrs = payload["data"]["attributes"]
        assert attrs["placed"] == "2024-01-01T00:00:00Z"
        assert "orderLines" in attrs
        assert attrs["totalValue"] == "50.0"  # overridden by line items sum

    def test_totalvalue_float_coerced_to_string(self):
        sink = _make_sink()
        record = {
            "placed": "2024-01-01T00:00:00Z",
            "totalValue": 786.49,
        }
        payload = sink.preprocess_record(record, {})
        assert payload["data"]["attributes"]["totalValue"] == "786.49"

    def test_optiply_id_becomes_payload_id(self):
        sink = _make_sink()
        record = {
            "optiply_id": "555",
            "placed": "2024-01-01T00:00:00Z",
            "totalValue": "10.00",
        }
        payload = sink.preprocess_record(record, {})
        assert payload["data"]["id"] == "555"

    def test_deleted_at_set_on_payload(self):
        sink = _make_sink()
        record = {
            "optiply_id": "555",
            "placed": "2024-01-01T00:00:00Z",
            "totalValue": "10.00",
            "deleted_at": "2024-06-01T00:00:00Z",
        }
        payload = sink.preprocess_record(record, {})
        assert payload["deleted_at"] == "2024-06-01T00:00:00Z"

    def test_schema_validation_failure_raises_fatal_and_marks_unhealthy(self):
        sink = _make_sink()
        BaseOptiplySink._job_healthy = True
        # placed is mandatory and missing — SellOrderSchema will raise
        record = {"totalValue": "10.00"}
        with pytest.raises(FatalAPIError):
            sink.preprocess_record(record, {})
        assert BaseOptiplySink._job_healthy is False
        # reset for other tests
        BaseOptiplySink._job_healthy = True

    def test_line_items_with_cache_fallback_in_full_flow(self):
        sink = _make_sink()
        base_sink_module._products_id_cache["remote-p1"] = "optiply-777"
        line_items = json.dumps([
            {"quantity": 1, "subtotalValue": "25.00", "productId": None, "Remote_productId": "remote-p1"},
        ])
        record = {
            "placed": "2024-03-01T00:00:00Z",
            "totalValue": 25.0,
            "line_items": line_items,
        }
        try:
            payload = sink.preprocess_record(record, {})
            order_lines = payload["data"]["attributes"]["orderLines"]
            assert order_lines[0]["productId"] == "optiply-777"
        finally:
            base_sink_module._products_id_cache.pop("remote-p1", None)


# ---------------------------------------------------------------------------
# Helpers — BuyOrders
# ---------------------------------------------------------------------------

class _TestBuyOrderSink(BuyOrderSink):
    config = {"account_id": "9999"}  # type: ignore[assignment]


class _TestBuyOrderLineSink(BuyOrderLineSink):
    config = {}  # type: ignore[assignment]


def _make_buy_order_sink() -> _TestBuyOrderSink:
    sink = _TestBuyOrderSink.__new__(_TestBuyOrderSink)
    sink.__dict__.update({
        "logger": logging.getLogger("test"),
        "stream_name": "BuyOrders",
        "endpoint": "buyOrders",
        "_stashed_external_id": None,
        "_record_count": 0,
        "_record_total": None,
        "_last_was_fatal": False,
        "_last_response_status": None,
    })
    return sink


def _make_buy_order_line_sink() -> _TestBuyOrderLineSink:
    sink = _TestBuyOrderLineSink.__new__(_TestBuyOrderLineSink)
    sink.__dict__.update({
        "logger": logging.getLogger("test"),
        "stream_name": "BuyOrderLines",
        "endpoint": "buyOrderLines",
        "_stashed_external_id": None,
        "_record_count": 0,
        "_record_total": None,
        "_last_was_fatal": False,
        "_last_response_status": None,
    })
    return sink


# ---------------------------------------------------------------------------
# preprocess_record — BuyOrderSink
# ---------------------------------------------------------------------------

class TestBuyOrderSinkPreprocess:

    def test_supplier_id_from_record(self):
        """supplierId resolved from snapshot and sent directly — no cache needed."""
        sink = _make_buy_order_sink()
        sinks_module._suppliers_id_cache.pop("4071", None)
        record = {
            "placed": "2024-01-01T00:00:00Z",
            "supplierId": "723287",
            "Remote_supplierId": "4071",
            "totalValue": 0.0,
        }
        payload = sink.preprocess_record(record, {})
        assert payload["data"]["attributes"]["supplierId"] == 723287

    def test_supplier_id_from_cache_only(self):
        """Only Remote_supplierId provided — new supplier posted this run, resolved from cache."""
        sink = _make_buy_order_sink()
        sinks_module._suppliers_id_cache["4071"] = "723287"
        record = {
            "placed": "2024-01-01T00:00:00Z",
            "Remote_supplierId": "4071",
            "totalValue": 0.0,
        }
        try:
            payload = sink.preprocess_record(record, {})
            assert payload["data"]["attributes"]["supplierId"] == 723287
        finally:
            sinks_module._suppliers_id_cache.pop("4071", None)

    def test_supplier_id_cache_takes_priority_over_record(self):
        """Both supplierId in record and Remote_supplierId in cache — cache wins."""
        sink = _make_buy_order_sink()
        sinks_module._suppliers_id_cache["4071"] = "999999"
        record = {
            "placed": "2024-01-01T00:00:00Z",
            "supplierId": "111111",
            "Remote_supplierId": "4071",
            "totalValue": 0.0,
        }
        try:
            payload = sink.preprocess_record(record, {})
            assert payload["data"]["attributes"]["supplierId"] == 999999
        finally:
            sinks_module._suppliers_id_cache.pop("4071", None)

    def test_supplier_id_missing_not_in_cache(self):
        """Neither supplierId nor Remote_supplierId in cache — supplierId absent from payload."""
        sink = _make_buy_order_sink()
        sinks_module._suppliers_id_cache.pop("unknown-supplier", None)
        record = {
            "placed": "2024-01-01T00:00:00Z",
            "Remote_supplierId": "unknown-supplier",
            "totalValue": 0.0,
        }
        payload = sink.preprocess_record(record, {})
        assert "supplierId" not in payload["data"]["attributes"]

    def test_account_id_injected_from_config(self):
        """accountId is always injected from config, not from the incoming record."""
        sink = _make_buy_order_sink()
        record = {
            "placed": "2024-01-01T00:00:00Z",
            "supplierId": "723287",
            "totalValue": 0.0,
        }
        payload = sink.preprocess_record(record, {})
        assert payload["data"]["attributes"]["accountId"] == 9999

    def test_account_id_not_overridden_by_record(self):
        """accountId in the record is ignored — config value is always used."""
        sink = _make_buy_order_sink()
        record = {
            "placed": "2024-01-01T00:00:00Z",
            "supplierId": "723287",
            "accountId": 1111,  # should be ignored — config has 9999
            "totalValue": 0.0,
        }
        payload = sink.preprocess_record(record, {})
        assert payload["data"]["attributes"]["accountId"] == 9999

    def test_total_value_float_coerced_to_string(self):
        sink = _make_buy_order_sink()
        record = {
            "placed": "2024-01-01T00:00:00Z",
            "supplierId": "723287",
            "totalValue": 1234.56,
        }
        payload = sink.preprocess_record(record, {})
        assert payload["data"]["attributes"]["totalValue"] == "1234.56"

    def test_total_value_zero_coerced_to_string(self):
        sink = _make_buy_order_sink()
        record = {
            "placed": "2024-01-01T00:00:00Z",
            "supplierId": "723287",
            "totalValue": 0,
        }
        payload = sink.preprocess_record(record, {})
        assert payload["data"]["attributes"]["totalValue"] == "0.0"

    def test_optiply_id_becomes_payload_id(self):
        sink = _make_buy_order_sink()
        record = {
            "optiply_id": "888",
            "placed": "2024-01-01T00:00:00Z",
            "supplierId": "723287",
            "totalValue": "100.00",
        }
        payload = sink.preprocess_record(record, {})
        assert payload["data"]["id"] == "888"

    def test_deleted_at_set_on_payload(self):
        sink = _make_buy_order_sink()
        record = {
            "optiply_id": "888",
            "placed": "2024-01-01T00:00:00Z",
            "supplierId": "723287",
            "totalValue": "100.00",
            "deleted_at": "2024-06-01T00:00:00Z",
        }
        payload = sink.preprocess_record(record, {})
        assert payload["deleted_at"] == "2024-06-01T00:00:00Z"

    def test_schema_validation_failure_raises_fatal_and_marks_unhealthy(self):
        """Missing placed (mandatory) → FatalAPIError, _job_healthy = False."""
        sink = _make_buy_order_sink()
        BaseOptiplySink._job_healthy = True
        record = {"supplierId": "723287", "totalValue": "100.00"}
        with pytest.raises(FatalAPIError):
            sink.preprocess_record(record, {})
        assert BaseOptiplySink._job_healthy is False
        BaseOptiplySink._job_healthy = True

    def test_job_unhealthy_skips_upsert(self):
        """After _job_healthy = False, upsert_record returns skip without HTTP call."""
        sink = _make_buy_order_sink()
        BaseOptiplySink._job_healthy = False
        preprocessed = {"data": {"type": "buyOrders", "attributes": {"placed": "2024-01-01T00:00:00Z"}}}
        record_id, success, state = sink.upsert_record(preprocessed, {})
        assert success is False
        assert "Skipped" in state["error"]
        BaseOptiplySink._job_healthy = True


# ---------------------------------------------------------------------------
# preprocess_record — BuyOrderLineSink
# ---------------------------------------------------------------------------

class TestBuyOrderLineSinkPreprocess:

    def test_product_id_from_record(self):
        """productId provided directly in record."""
        sink = _make_buy_order_line_sink()
        base_sink_module._products_id_cache.pop("remote-p1", None)
        record = {
            "productId": "23761425",
            "buyOrderId": "555",
            "quantity": 3,
            "subtotalValue": "75.00",
        }
        payload = sink.preprocess_record(record, {})
        assert payload["data"]["attributes"]["productId"] == 23761425

    def test_product_id_from_cache(self):
        """Remote_productId resolves from _products_id_cache — new product posted this run."""
        sink = _make_buy_order_line_sink()
        base_sink_module._products_id_cache["remote-p1"] = "23761425"
        record = {
            "Remote_productId": "remote-p1",
            "buyOrderId": "555",
            "quantity": 3,
            "subtotalValue": "75.00",
        }
        try:
            payload = sink.preprocess_record(record, {})
            assert payload["data"]["attributes"]["productId"] == 23761425
        finally:
            base_sink_module._products_id_cache.pop("remote-p1", None)

    def test_product_id_cache_takes_priority_over_record(self):
        """Cache value wins over productId already in record."""
        sink = _make_buy_order_line_sink()
        base_sink_module._products_id_cache["remote-p1"] = "99999"
        record = {
            "productId": "11111",
            "Remote_productId": "remote-p1",
            "buyOrderId": "555",
            "quantity": 3,
            "subtotalValue": "75.00",
        }
        try:
            payload = sink.preprocess_record(record, {})
            assert payload["data"]["attributes"]["productId"] == 99999
        finally:
            base_sink_module._products_id_cache.pop("remote-p1", None)

    def test_product_id_missing_not_in_cache(self):
        """Neither productId nor Remote_productId in cache — productId absent from payload."""
        sink = _make_buy_order_line_sink()
        base_sink_module._products_id_cache.pop("unknown-p", None)
        record = {
            "Remote_productId": "unknown-p",
            "buyOrderId": "555",
            "quantity": 3,
            "subtotalValue": "75.00",
        }
        payload = sink.preprocess_record(record, {})
        assert "productId" not in payload["data"]["attributes"]

    def test_buy_order_id_from_record(self):
        """buyOrderId provided directly in record."""
        sink = _make_buy_order_line_sink()
        sinks_module._buy_orders_id_cache.pop("remote-bo1", None)
        record = {
            "productId": "23761425",
            "buyOrderId": "555",
            "quantity": 3,
            "subtotalValue": "75.00",
        }
        payload = sink.preprocess_record(record, {})
        assert payload["data"]["attributes"]["buyOrderId"] == 555

    def test_buy_order_id_from_cache(self):
        """Remote_buyOrderId resolves from _buy_orders_id_cache — new buy order posted this run."""
        sink = _make_buy_order_line_sink()
        sinks_module._buy_orders_id_cache["remote-bo1"] = "555"
        record = {
            "productId": "23761425",
            "Remote_buyOrderId": "remote-bo1",
            "quantity": 3,
            "subtotalValue": "75.00",
        }
        try:
            payload = sink.preprocess_record(record, {})
            assert payload["data"]["attributes"]["buyOrderId"] == 555
        finally:
            sinks_module._buy_orders_id_cache.pop("remote-bo1", None)

    def test_buy_order_id_cache_takes_priority_over_record(self):
        """Cache value wins over buyOrderId already in record."""
        sink = _make_buy_order_line_sink()
        sinks_module._buy_orders_id_cache["remote-bo1"] = "99999"
        record = {
            "productId": "23761425",
            "buyOrderId": "11111",
            "Remote_buyOrderId": "remote-bo1",
            "quantity": 3,
            "subtotalValue": "75.00",
        }
        try:
            payload = sink.preprocess_record(record, {})
            assert payload["data"]["attributes"]["buyOrderId"] == 99999
        finally:
            sinks_module._buy_orders_id_cache.pop("remote-bo1", None)

    def test_buy_order_id_missing_not_in_cache(self):
        """Neither buyOrderId nor Remote_buyOrderId in cache — buyOrderId absent from payload."""
        sink = _make_buy_order_line_sink()
        sinks_module._buy_orders_id_cache.pop("unknown-bo", None)
        record = {
            "productId": "23761425",
            "Remote_buyOrderId": "unknown-bo",
            "quantity": 3,
            "subtotalValue": "75.00",
        }
        payload = sink.preprocess_record(record, {})
        assert "buyOrderId" not in payload["data"]["attributes"]

    def test_subtotal_value_float_coerced_to_string(self):
        sink = _make_buy_order_line_sink()
        record = {
            "productId": "23761425",
            "buyOrderId": "555",
            "quantity": 3,
            "subtotalValue": 75.995,
        }
        payload = sink.preprocess_record(record, {})
        assert payload["data"]["attributes"]["subtotalValue"] == "76.0"

    def test_quantity_string_coerced_to_float(self):
        sink = _make_buy_order_line_sink()
        record = {
            "productId": "23761425",
            "buyOrderId": "555",
            "quantity": "3",
            "subtotalValue": "75.00",
        }
        payload = sink.preprocess_record(record, {})
        assert payload["data"]["attributes"]["quantity"] == 3.0

    def test_expected_delivery_date_optional(self):
        sink = _make_buy_order_line_sink()
        record = {
            "productId": "23761425",
            "buyOrderId": "555",
            "quantity": 1,
            "subtotalValue": "50.00",
            "expectedDeliveryDate": "2024-06-01T00:00:00Z",
        }
        payload = sink.preprocess_record(record, {})
        assert payload["data"]["attributes"]["expectedDeliveryDate"] == "2024-06-01T00:00:00Z"

    def test_job_unhealthy_skips_bol_upsert(self):
        """After _job_healthy = False (e.g. from a BuyOrders failure), BOL records are skipped."""
        sink = _make_buy_order_line_sink()
        BaseOptiplySink._job_healthy = False
        preprocessed = {"data": {"type": "buyOrderLines", "attributes": {}}}
        record_id, success, state = sink.upsert_record(preprocessed, {})
        assert success is False
        assert "Skipped" in state["error"]
        BaseOptiplySink._job_healthy = True


# ---------------------------------------------------------------------------
# Cross-stream: BuyOrders failure propagates to BuyOrderLines
# ---------------------------------------------------------------------------

class TestJobHealthyPropagation:

    def test_buy_order_schema_failure_skips_buy_order_lines(self):
        """
        A fatal schema failure on BuyOrders sets _job_healthy = False.
        Subsequent BuyOrderLines upsert_record calls are skipped without HTTP.
        """
        bo_sink = _make_buy_order_sink()
        bol_sink = _make_buy_order_line_sink()
        BaseOptiplySink._job_healthy = True

        # Trigger fatal failure on BuyOrders (missing placed)
        with pytest.raises(FatalAPIError):
            bo_sink.preprocess_record({"supplierId": "723287", "totalValue": "100.00"}, {})

        assert BaseOptiplySink._job_healthy is False

        # BuyOrderLines upsert should be skipped
        preprocessed = {"data": {"type": "buyOrderLines", "attributes": {}}}
        record_id, success, state = bol_sink.upsert_record(preprocessed, {})
        assert success is False
        assert "Skipped" in state["error"]

        BaseOptiplySink._job_healthy = True


# ---------------------------------------------------------------------------
# Helpers — ReceiptLines
# ---------------------------------------------------------------------------

class _TestReceiptLineSink(ReceiptLineSink):
    config = {}  # type: ignore[assignment]


def _make_receipt_line_sink() -> _TestReceiptLineSink:
    sink = _TestReceiptLineSink.__new__(_TestReceiptLineSink)
    sink.__dict__.update({
        "logger": logging.getLogger("test"),
        "stream_name": "ReceiptLines",
        "endpoint": "receiptLines",
        "_stashed_external_id": None,
        "_record_count": 0,
        "_record_total": None,
        "_last_was_fatal": False,
        "_last_response_status": None,
    })
    return sink


# ---------------------------------------------------------------------------
# preprocess_record — ReceiptLineSink
# ---------------------------------------------------------------------------

class TestReceiptLineSinkPreprocess:

    def test_buy_order_line_id_from_record(self):
        """buyOrderLineId provided directly — no cache needed."""
        sink = _make_receipt_line_sink()
        sinks_module._buy_order_lines_id_cache.pop("remote-bol1", None)
        record = {
            "occurred": "2024-01-15T10:00:00Z",
            "quantity": 5,
            "buyOrderLineId": "987654",
        }
        payload = sink.preprocess_record(record, {})
        assert payload["data"]["attributes"]["buyOrderLineId"] == "987654"

    def test_buy_order_line_id_from_cache(self):
        """Remote_buyOrderLineId resolves from _buy_order_lines_id_cache — new BOL posted this run."""
        sink = _make_receipt_line_sink()
        sinks_module._buy_order_lines_id_cache["remote-bol1"] = "987654"
        record = {
            "occurred": "2024-01-15T10:00:00Z",
            "quantity": 5,
            "Remote_buyOrderLineId": "remote-bol1",
        }
        try:
            payload = sink.preprocess_record(record, {})
            assert payload["data"]["attributes"]["buyOrderLineId"] == "987654"
        finally:
            sinks_module._buy_order_lines_id_cache.pop("remote-bol1", None)

    def test_buy_order_line_id_cache_takes_priority_over_record(self):
        """Cache value wins over buyOrderLineId already in record."""
        sink = _make_receipt_line_sink()
        sinks_module._buy_order_lines_id_cache["remote-bol1"] = "999999"
        record = {
            "occurred": "2024-01-15T10:00:00Z",
            "quantity": 5,
            "buyOrderLineId": "111111",
            "Remote_buyOrderLineId": "remote-bol1",
        }
        try:
            payload = sink.preprocess_record(record, {})
            assert payload["data"]["attributes"]["buyOrderLineId"] == "999999"
        finally:
            sinks_module._buy_order_lines_id_cache.pop("remote-bol1", None)

    def test_buy_order_line_id_missing_not_in_cache(self):
        """Neither buyOrderLineId nor Remote_buyOrderLineId in cache — field absent from payload."""
        sink = _make_receipt_line_sink()
        sinks_module._buy_order_lines_id_cache.pop("unknown-bol", None)
        record = {
            "occurred": "2024-01-15T10:00:00Z",
            "quantity": 5,
            "Remote_buyOrderLineId": "unknown-bol",
        }
        payload = sink.preprocess_record(record, {})
        assert "buyOrderLineId" not in payload["data"]["attributes"]

    def test_remote_data_synced_to_date_always_injected(self):
        """remoteDataSyncedToDate is always set to current UTC time by the target."""
        sink = _make_receipt_line_sink()
        record = {
            "occurred": "2024-01-15T10:00:00Z",
            "quantity": 5,
            "buyOrderLineId": "987654",
        }
        payload = sink.preprocess_record(record, {})
        assert "remoteDataSyncedToDate" in payload["data"]["attributes"]
        ts = payload["data"]["attributes"]["remoteDataSyncedToDate"]
        assert ts.endswith("Z")
        assert "T" in ts

    def test_remote_data_synced_to_date_overrides_record_value(self):
        """Even if remoteDataSyncedToDate is in the record, target always injects current time."""
        sink = _make_receipt_line_sink()
        record = {
            "occurred": "2024-01-15T10:00:00Z",
            "quantity": 5,
            "buyOrderLineId": "987654",
            "remoteDataSyncedToDate": "2020-01-01T00:00:00Z",
        }
        payload = sink.preprocess_record(record, {})
        assert payload["data"]["attributes"]["remoteDataSyncedToDate"] != "2020-01-01T00:00:00Z"

    def test_occurred_string_passthrough(self):
        sink = _make_receipt_line_sink()
        record = {
            "occurred": "2024-06-15T08:30:00Z",
            "quantity": 3,
            "buyOrderLineId": "987654",
        }
        payload = sink.preprocess_record(record, {})
        assert payload["data"]["attributes"]["occurred"] == "2024-06-15T08:30:00Z"

    def test_quantity_string_coerced_to_int(self):
        sink = _make_receipt_line_sink()
        record = {
            "occurred": "2024-01-15T10:00:00Z",
            "quantity": "7",
            "buyOrderLineId": "987654",
        }
        payload = sink.preprocess_record(record, {})
        assert payload["data"]["attributes"]["quantity"] == 7

    def test_remote_id_optional(self):
        sink = _make_receipt_line_sink()
        record = {
            "occurred": "2024-01-15T10:00:00Z",
            "quantity": 5,
            "buyOrderLineId": "987654",
            "remoteId": "RL-001",
        }
        payload = sink.preprocess_record(record, {})
        assert payload["data"]["attributes"]["remoteId"] == "RL-001"

    def test_optiply_id_becomes_payload_id(self):
        sink = _make_receipt_line_sink()
        record = {
            "optiply_id": "777",
            "occurred": "2024-01-15T10:00:00Z",
            "quantity": 5,
            "buyOrderLineId": "987654",
        }
        payload = sink.preprocess_record(record, {})
        assert payload["data"]["id"] == "777"

    def test_schema_failure_raises_fatal_and_marks_unhealthy(self):
        """Schema failure (forced) → FatalAPIError, _job_healthy = False."""
        sink = _make_receipt_line_sink()
        BaseOptiplySink._job_healthy = True
        with patch.object(
            type(sink).unified_schema,
            "model_validate",
            side_effect=Exception("forced failure"),
        ):
            with pytest.raises(FatalAPIError):
                sink.preprocess_record({"occurred": "2024-01-15T10:00:00Z", "quantity": 5, "buyOrderLineId": "987654"}, {})
        assert BaseOptiplySink._job_healthy is False
        BaseOptiplySink._job_healthy = True

    def test_job_unhealthy_skips_upsert(self):
        """After _job_healthy = False, receipt line upsert is skipped without HTTP call."""
        sink = _make_receipt_line_sink()
        BaseOptiplySink._job_healthy = False
        preprocessed = {"data": {"type": "receiptLines", "attributes": {}}}
        record_id, success, state = sink.upsert_record(preprocessed, {})
        assert success is False
        assert "Skipped" in state["error"]
        BaseOptiplySink._job_healthy = True
