"""Tests for the log-scanner Lambda — CloudWatch Logs parsing and metric building."""

import base64
import gzip
import json

import pytest

from lambdas.log_scanner import handler
from lambdas.log_scanner.handler import _build_metric, _pipeline_from_log_group


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cw_event(log_group: str, messages: list[str]) -> dict:
    """Wrap log messages in a CloudWatch Logs subscription event."""
    payload = {
        "logGroup": log_group,
        "logEvents": [{"id": str(i), "message": m} for i, m in enumerate(messages)],
    }
    compressed = gzip.compress(json.dumps(payload).encode())
    return {"awslogs": {"data": base64.b64encode(compressed).decode()}}


def _metric_line(**kwargs) -> str:
    """Build a JSON log line as emit_metric() would produce it."""
    base = {
        "MONITORING_METRIC": "rows_processed",
        "value": 42.0,
        "severity": "info",
        "pipeline": "ingest.iterate",
        "is_alert": False,
        "payload": {},
        "environment": "dev",
    }
    base.update(kwargs)
    return json.dumps(base)


# ---------------------------------------------------------------------------
# _build_metric — lowercase keys (emit_metric format)
# ---------------------------------------------------------------------------

def test_build_metric_lowercase_keys():
    """emit_metric() outputs lowercase keys — all fields must be picked up."""
    metric = _build_metric(_metric_line(), log_group="/aws/lambda/ingest.iterate")

    assert metric is not None
    assert metric.metric_name == "rows_processed"
    assert metric.metric_value == 42.0
    assert metric.severity == "info"
    assert metric.pipeline_name == "ingest.iterate"
    assert metric.is_alert is False
    assert metric.environment == "dev"


def test_build_metric_uppercase_keys_fallback():
    """Manual emitters that use uppercase keys must still work."""
    line = json.dumps({
        "MONITORING_METRIC": "latency_p99",
        "VALUE": 1.5,
        "SEVERITY": "warning",
        "PIPELINE": "etl.orders",
        "IS_ALERT": True,
        "ENVIRONMENT": "prod",
    })
    metric = _build_metric(line, log_group="/aws/lambda/etl.orders")

    assert metric is not None
    assert metric.metric_name == "latency_p99"
    assert metric.metric_value == 1.5
    assert metric.severity == "warning"
    assert metric.pipeline_name == "etl.orders"
    assert metric.is_alert is True
    assert metric.environment == "prod"


def test_build_metric_environment_falls_back_to_deploy_env(monkeypatch):
    """Missing environment key → fall back to the Lambda's MONTY_ENV, so the
    NOT NULL column is always populated and non-prod routing still works."""
    monkeypatch.setenv("MONTY_ENV", "dev")
    line = json.dumps({
        "MONITORING_METRIC": "rows_processed",
        "value": 10,
        "severity": "info",
        "pipeline": "p",
    })
    metric = _build_metric(line, log_group="/aws/lambda/p")
    assert metric is not None
    assert metric.environment == "dev"


def test_build_metric_environment_none_when_no_deploy_env(monkeypatch):
    """With no producer value and no MONTY_ENV, environment is None (not a
    KeyError). In production MONTY_ENV is always set by the stack."""
    monkeypatch.delenv("MONTY_ENV", raising=False)
    line = json.dumps({
        "MONITORING_METRIC": "rows_processed",
        "value": 10,
        "severity": "info",
        "pipeline": "p",
    })
    metric = _build_metric(line, log_group="/aws/lambda/p")
    assert metric is not None
    assert metric.environment is None


def test_build_metric_pipeline_falls_back_to_log_group():
    """No pipeline key in log line → derive from log group."""
    line = json.dumps({"MONITORING_METRIC": "rows", "value": 1, "severity": "info"})
    metric = _build_metric(line, log_group="/aws/lambda/my-function")
    assert metric.pipeline_name == "my-function"


def test_build_metric_returns_none_for_non_json():
    assert _build_metric("not json at all", "/aws/lambda/x") is None


def test_build_metric_returns_none_without_monitoring_metric_key():
    assert _build_metric(json.dumps({"foo": "bar"}), "/aws/lambda/x") is None


def test_build_metric_known_keys_excluded_from_raw_payload():
    """Known keys like 'pipeline', 'value' must not leak into raw."""
    metric = _build_metric(_metric_line(extra_field="keep_me"), "/aws/lambda/x")
    assert metric is not None
    raw = metric.payload["raw"]
    assert "pipeline" not in raw
    assert "value" not in raw
    assert "severity" not in raw
    assert "environment" not in raw
    assert raw.get("extra_field") == "keep_me"


def test_build_metric_slack_webhook_lifted_to_payload():
    """slack_webhook must land on payload top-level, not buried inside raw."""
    url = "https://hooks.slack.com/services/AAA/BBB/CCC"
    metric = _build_metric(
        _metric_line(slack_webhook=url),
        "/aws/lambda/x",
    )
    assert metric is not None
    assert metric.payload["slack_webhook"] == url
    assert "slack_webhook" not in metric.payload["raw"]


# ---------------------------------------------------------------------------
# lambda_handler — end-to-end with stubbed metric_writer
# ---------------------------------------------------------------------------

@pytest.fixture
def stub_writer(monkeypatch):
    written = []
    monkeypatch.setattr(handler.metric_writer, "write", lambda m: written.append(m) or 1)
    return written


def test_handler_writes_one_metric(stub_writer):
    event = _make_cw_event(
        "/aws/lambda/ingest.iterate",
        [_metric_line()],
    )
    result = handler.lambda_handler(event, context=None)

    assert result["written"] == 1
    assert result["skipped"] == 0
    assert stub_writer[0].environment == "dev"
    assert stub_writer[0].metric_name == "rows_processed"


def test_handler_skips_non_metric_lines(stub_writer):
    event = _make_cw_event(
        "/aws/lambda/ingest.iterate",
        ["plain log line", json.dumps({"other": "key"}), _metric_line()],
    )
    result = handler.lambda_handler(event, context=None)

    assert result["written"] == 1
    assert result["skipped"] == 2


def test_handler_isolates_bad_events(stub_writer, monkeypatch):
    """A write failure on one event must not prevent the others from landing."""
    calls = []

    def _sometimes_fail(metric):
        calls.append(metric)
        if len(calls) == 1:
            raise RuntimeError("transient error")
        return 1

    monkeypatch.setattr(handler.metric_writer, "write", _sometimes_fail)

    event = _make_cw_event(
        "/aws/lambda/ingest.iterate",
        [_metric_line(), _metric_line(value=99)],
    )
    result = handler.lambda_handler(event, context=None)

    assert result["written"] == 1
    assert result["skipped"] == 1


# ---------------------------------------------------------------------------
# _pipeline_from_log_group
# ---------------------------------------------------------------------------

def test_pipeline_from_lambda_log_group():
    assert _pipeline_from_log_group("/aws/lambda/my-function") == "my-function"


def test_pipeline_from_non_lambda_log_group():
    assert _pipeline_from_log_group("/ecs/my-service") == "/ecs/my-service"
