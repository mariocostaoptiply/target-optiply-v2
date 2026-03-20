"""Sink behaviour tests — no HTTP calls, no SDK init."""

from __future__ import annotations

import json
import logging
from unittest.mock import patch

import pytest
from singer_sdk.exceptions import FatalAPIError

import target_optiply.base_sink as base_sink_module
from target_optiply.base_sink import BaseOptiplySink, _products_id_cache
from target_optiply.sinks import SellOrderSink


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
        assert lines[0]["attributes"]["productId"] == "999"

    def test_product_id_resolved_from_cache(self):
        sink = _make_sink()
        base_sink_module._products_id_cache["src-42"] = "optiply-999"
        raw = json.dumps([
            {"quantity": 1, "subtotalValue": "5.00", "productId": None, "Remote_productId": "src-42"},
        ])
        try:
            total, lines = sink._parse_line_items({"line_items": raw}, "sellOrderLines")
            assert lines[0]["attributes"]["productId"] == "optiply-999"
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
        assert lines[0]["attributes"]["productId"] is None

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
            assert order_lines[0]["attributes"]["productId"] == "optiply-777"
        finally:
            base_sink_module._products_id_cache.pop("remote-p1", None)
