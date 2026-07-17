"""Observer Lambda integration tests — focuses on the idempotency contract.

The Observer must:
  - Mark a metric as sent ONLY after Slack accepts it.
  - On Slack failure, leave sent_to_slack=FALSE so the next minute retries.
  - Always record an ALERT_OUTBOX row (sent OR failed) so we have an audit
    trail of every delivery attempt.
"""

from unittest.mock import patch

import pytest

from lambdas.observer import handler


@pytest.fixture
def secrets(monkeypatch):
    """Patch get_secret to hand back the full secret dict (including webhook URLs)."""
    monkeypatch.setattr(handler, "get_secret", lambda: {
        "SLACK_WEBHOOK_INCIDENTS": "https://hooks.invalid/incidents",
        "SLACK_WEBHOOK_ALERTS": "https://hooks.invalid/alerts",
    })


@pytest.fixture
def fake_conn(monkeypatch):
    """Inject a fake Snowflake connection that pre-loads one unsent row."""

    captured = {"executed": [], "committed": 0}

    class _Cursor:
        description = [
            ("ID",), ("PIPELINE_NAME",), ("METRIC_NAME",), ("METRIC_VALUE",),
            ("SEVERITY",), ("RUN_ID",), ("PAYLOAD",),
        ]
        def execute(self, sql, params=None):
            captured["executed"].append((sql.strip().split()[0], sql, params))
            self._last_sql = sql
        def fetchall(self):
            # Return one critical-severity row.
            return [(
                42, "ingest.iterate", "pipeline_failure", None,
                "critical", "run-abc",
                '{"error_message": "boom"}',  # JSON string from VARIANT
            )]
        def close(self):
            pass

    class _Conn:
        def cursor(self):
            return _Cursor()
        def commit(self):
            captured["committed"] += 1
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False

    monkeypatch.setattr(handler, "get_connection", lambda **kw: _Conn())
    return captured


def test_observer_marks_metric_sent_on_successful_slack_post(secrets, fake_conn):
    with patch.object(handler.slack, "post",
                      return_value=handler.slack.SlackResult(
                          delivered=True, channel="", error_message=None,
                      )) as mock_post:
        result = handler.lambda_handler({}, context=None)

    # Slack was called once, on the incidents webhook (severity=critical).
    assert mock_post.call_count == 1
    webhook_url = mock_post.call_args[0][0]
    assert "incidents" in webhook_url

    # Two DML statements after the SELECT: INSERT outbox, UPDATE custom_metrics.
    statements = [verb for verb, _, _ in fake_conn["executed"]]
    assert statements.count("INSERT") == 1
    assert statements.count("UPDATE") == 1
    assert fake_conn["committed"] == 1
    assert result == {"lane": "all", "polled": 1, "sent": 1, "failed": 0}


def test_observer_lane_applies_disjoint_severity_filters(secrets, fake_conn):
    """The fast lane queries only critical/error; the batch lane queries only
    the complement. Disjoint filters guarantee the two schedules never claim
    (and double-post) the same row. An empty event keeps the legacy behaviour
    of scanning every severity."""
    with patch.object(handler.slack, "post",
                      return_value=handler.slack.SlackResult(
                          delivered=True, channel="", error_message=None,
                      )):
        handler.lambda_handler({"lane": "fast"}, context=None)
        fast_select = next(sql for verb, sql, _ in fake_conn["executed"] if verb == "SELECT")

        fake_conn["executed"].clear()
        handler.lambda_handler({"lane": "batch"}, context=None)
        batch_select = next(sql for verb, sql, _ in fake_conn["executed"] if verb == "SELECT")

        fake_conn["executed"].clear()
        handler.lambda_handler({}, context=None)
        all_select = next(sql for verb, sql, _ in fake_conn["executed"] if verb == "SELECT")

    assert "SEVERITY IN ('critical', 'error')" in fast_select
    assert "SEVERITY NOT IN ('critical', 'error')" in batch_select
    # Legacy/manual invoke (no lane) must not constrain severity at all.
    assert "SEVERITY IN" not in all_select and "SEVERITY NOT IN" not in all_select


def test_observer_keeps_sent_flag_false_on_slack_failure(secrets, fake_conn):
    """If Slack returns 5xx, write outbox 'failed' but DON'T mark sent —
    next minute's invocation retries."""
    with patch.object(handler.slack, "post",
                      return_value=handler.slack.SlackResult(
                          delivered=False, channel="",
                          error_message="slack 500",
                      )):
        result = handler.lambda_handler({}, context=None)

    statements = [verb for verb, _, _ in fake_conn["executed"]]
    # Outbox INSERT happens; UPDATE on CUSTOM_METRICS does NOT.
    assert statements.count("INSERT") == 1
    assert statements.count("UPDATE") == 0
    assert fake_conn["committed"] == 1
    assert result == {"lane": "all", "polled": 1, "sent": 0, "failed": 1}


def test_observer_routes_critical_to_incidents_webhook(secrets, fake_conn):
    """Confirms the secret-key → webhook mapping the handler uses."""
    with patch.object(handler.slack, "post",
                      return_value=handler.slack.SlackResult(
                          delivered=True, channel="", error_message=None,
                      )) as mock_post:
        handler.lambda_handler({}, context=None)

    url_used = mock_post.call_args.args[0]
    assert url_used == "https://hooks.invalid/incidents"


def test_observer_normalizes_payload_string_to_dict(secrets, fake_conn):
    """Snowflake VARIANT comes back as a JSON string; the handler's
    `_normalize_payload` should parse it before passing to the formatter."""
    captured_metric = {}

    def _capture(url, body):
        captured_metric["body"] = body
        return handler.slack.SlackResult(
            delivered=True, channel="", error_message=None,
        )

    with patch.object(handler.slack, "post", side_effect=_capture):
        handler.lambda_handler({}, context=None)

    # The Slack body should mention the parsed error_message — proving the
    # JSON string was decoded before format_message saw it.
    assert "boom" in str(captured_metric["body"])
