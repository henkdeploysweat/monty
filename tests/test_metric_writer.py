"""Tests for severity-based storage routing in metric_writer.write().

critical/error must INSERT into Snowflake (never touch DynamoDB); warning/info
must be handed to dynamo_writer (never open a Snowflake connection). Both
third-party deps are stubbed in conftest, so these run in a plain venv.
"""

import pytest

from lambdas.shared import metric_writer


def _metric(severity, **overrides):
    raw = {
        "pipeline_name": "test-pipe",
        "metric_name": "rows_loaded",
        "metric_value": 42,
        "severity": severity,
        "is_alert": overrides.pop("is_alert", True),
        "payload": overrides.pop("payload", {"k": "v"}),
        "environment": "dev",
    }
    raw.update(overrides)
    return metric_writer.Metric.from_dict(raw)


@pytest.fixture(autouse=True)
def _spies(monkeypatch):
    """Record which path each write() took without hitting real infra."""
    calls = {"dynamo": [], "snowflake": 0}

    def fake_dynamo_write(metric):
        calls["dynamo"].append(metric)
        return 1

    class _Conn:
        def __enter__(self):
            calls["snowflake"] += 1
            return self

        def __exit__(self, *a):
            return False

        def cursor(self):
            return _Cur()

    class _Cur:
        rowcount = 1

        def execute(self, *a, **k):
            pass

        def close(self):
            pass

    monkeypatch.setattr(metric_writer.dynamo_writer, "write", fake_dynamo_write)
    monkeypatch.setattr(metric_writer, "get_connection", lambda **k: _Conn())
    return calls


@pytest.mark.parametrize("severity", ["warning", "info"])
def test_low_severity_routes_to_dynamo_only(severity, _spies):
    result = _metric(severity)
    assert metric_writer.write(result) == 1
    assert len(_spies["dynamo"]) == 1
    assert _spies["dynamo"][0].severity == severity
    # The Snowflake path must not be opened for DynamoDB-bound severities.
    assert _spies["snowflake"] == 0


@pytest.mark.parametrize("severity", ["critical", "error"])
def test_high_severity_routes_to_snowflake_only(severity, _spies):
    result = _metric(severity)
    assert metric_writer.write(result) == 1
    assert _spies["snowflake"] == 1
    # DynamoDB must not be touched for Snowflake-bound severities.
    assert _spies["dynamo"] == []


def test_routing_sets_are_disjoint_and_cover_allowed():
    # Guard the invariant the write() branch relies on: every allowed severity
    # has exactly one storage route.
    ddb = set(metric_writer.DDB_SEVERITIES)
    sf = set(metric_writer.SNOWFLAKE_SEVERITIES)
    assert ddb.isdisjoint(sf)
    assert ddb | sf == set(metric_writer.ALLOWED_SEVERITIES)


def test_is_alert_info_still_goes_to_dynamo(_spies):
    # An info row with is_alert=True used to reach Slack via the observer; it now
    # goes to DynamoDB like any other info row (intentional behaviour change).
    metric_writer.write(_metric("info", is_alert=True))
    assert len(_spies["dynamo"]) == 1
    assert _spies["snowflake"] == 0
