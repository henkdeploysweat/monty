"""Tests for dynamo_writer.write() — the DynamoDB path for warning/info metrics.

boto3 is stubbed in conftest, so this asserts the call contract (key shape,
item attributes, TTL, null handling, return value) rather than a real DynamoDB
round-trip. The key-shape and routing logic is the part that can silently break;
the wire protocol is boto3's problem in the built image.
"""

import re
from datetime import datetime, timezone
from decimal import Decimal

import boto3
import pytest

from lambdas.shared import dynamo_writer, metric_writer

# sk = "<ISO-8601 UTC timestamp>#<uuid4>"
_SK_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?\+00:00#[0-9a-f-]{36}$"
)


def _metric(severity="info", **overrides):
    raw = {
        "pipeline_name": "test-pipe",
        "metric_name": "rows_loaded",
        "metric_value": 42,
        "severity": severity,
        "is_alert": overrides.pop("is_alert", False),
        "payload": overrides.pop("payload", {"rows": 100}),
        "environment": "dev",
    }
    raw.update(overrides)
    return metric_writer.Metric.from_dict(raw)


@pytest.fixture(autouse=True)
def _reset_dynamo():
    # Clear the cached Table handle and recorded calls so each test is isolated.
    dynamo_writer._get_table.cache_clear()
    boto3._StubDynamoTable.put_calls.clear()
    yield
    boto3._StubDynamoTable.put_calls.clear()


def _last_call():
    return boto3._StubDynamoTable.put_calls[-1]


def test_write_puts_one_item_and_returns_one():
    assert dynamo_writer.write(_metric()) == 1
    assert len(boto3._StubDynamoTable.put_calls) == 1


def test_key_shape_and_attributes():
    dynamo_writer.write(_metric())
    call = _last_call()
    assert call["TableName"] == "monty-test-metrics-ddb"
    item = call["Item"]
    assert item["pk"] == "dev#test-pipe"
    assert _SK_RE.match(item["sk"]), item["sk"]
    # sk's timestamp half must be exactly the stored occurred_at attribute.
    assert item["sk"].split("#")[0] == item["occurred_at"]
    assert item["pipeline_name"] == "test-pipe"
    assert item["metric_name"] == "rows_loaded"
    assert item["severity"] == "info"
    assert item["is_alert"] is False
    assert item["environment"] == "dev"
    assert item["payload"] == '{"rows": 100}'


def test_metric_value_stored_as_decimal():
    dynamo_writer.write(_metric(metric_value=42.5))
    value = _last_call()["Item"]["metric_value"]
    assert isinstance(value, Decimal)
    assert value == Decimal("42.5")


def test_table_comes_from_env(monkeypatch):
    monkeypatch.setenv("MONTY_METRICS_TABLE", "other-table")
    dynamo_writer.write(_metric())
    assert _last_call()["TableName"] == "other-table"


def test_null_fields_are_omitted():
    # metric_value/run_id/payload = None must not raise and must be absent from
    # the item (omitted, not stored as DynamoDB NULL).
    assert dynamo_writer.write(_metric(metric_value=None, payload=None)) == 1
    item = _last_call()["Item"]
    assert "metric_value" not in item
    assert "run_id" not in item
    assert "payload" not in item


def test_ttl_is_occurred_at_plus_retention():
    fixed = datetime(2026, 7, 17, 12, 0, 0, tzinfo=timezone.utc)
    dynamo_writer.write(_metric(), occurred_at=fixed)
    item = _last_call()["Item"]
    assert item["ttl"] == int(fixed.timestamp()) + dynamo_writer.DEFAULT_TTL_DAYS * 86400
    assert item["occurred_at"] == fixed.isoformat()


def test_ttl_retention_override(monkeypatch):
    monkeypatch.setenv("MONTY_METRICS_TTL_DAYS", "7")
    fixed = datetime(2026, 7, 17, 12, 0, 0, tzinfo=timezone.utc)
    dynamo_writer.write(_metric(), occurred_at=fixed)
    assert _last_call()["Item"]["ttl"] == int(fixed.timestamp()) + 7 * 86400


def test_occurred_at_defaults_to_utc_now():
    # The live path stamps now (UTC). We can't freeze the clock without a dep,
    # so assert the stored ISO string parses tz-aware UTC and is ~now.
    dynamo_writer.write(_metric())
    stored = datetime.fromisoformat(_last_call()["Item"]["occurred_at"])
    assert stored.tzinfo is not None
    assert abs((datetime.now(timezone.utc) - stored).total_seconds()) < 60
