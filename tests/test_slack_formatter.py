"""Tests for the Slack formatter and severity-based webhook routing.

Pure-Python: no IO, no mocks. Exercises `route()` and `format_message()`
plus the urllib HTTP path via a stubbed `urllib.request.urlopen`.
"""

import io
import urllib.error
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from lambdas.observer import slack


# ---------------------------------------------------------------------------
# route() — severity → (channel label, secret key)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("severity,expected_secret", [
    ("critical", "SLACK_WEBHOOK_INCIDENTS"),
    ("error",    "SLACK_WEBHOOK_INCIDENTS"),
    ("warning",  "SLACK_WEBHOOK_ALERTS"),
    ("info",     "SLACK_WEBHOOK_ALERTS"),
])
def test_route_maps_severity_to_correct_webhook(severity, expected_secret):
    _, secret_key = slack.route(severity, channel_override=None)
    assert secret_key == expected_secret


def test_route_unknown_severity_falls_back_to_alerts():
    """Defensive: a metric with severity='gibberish' should still get routed
    somewhere rather than crashing the Observer."""
    label, secret_key = slack.route("gibberish", None)
    assert secret_key == "SLACK_WEBHOOK_ALERTS"
    assert label == slack.ALERTS_CHANNEL


def test_route_severity_is_case_insensitive():
    label, secret_key = slack.route("CRITICAL", None)
    assert secret_key == "SLACK_WEBHOOK_INCIDENTS"
    assert label == slack.INCIDENTS_CHANNEL


def test_route_channel_override_changes_label_only_not_webhook():
    """v1 routes by severity for the WEBHOOK URL, but lets a per-rule
    override change the *display* label that's recorded in ALERT_OUTBOX."""
    label, secret_key = slack.route("warning", channel_override="#my-team")
    assert label == "#my-team"
    assert secret_key == "SLACK_WEBHOOK_ALERTS"  # still routed by severity


# ---------------------------------------------------------------------------
# resolve_destination() — slack_webhook URL wins; otherwise severity routing
# ---------------------------------------------------------------------------

_SEVERITY_WEBHOOKS = {
    "SLACK_WEBHOOK_INCIDENTS": "https://hooks.slack.test/incidents",
    "SLACK_WEBHOOK_ALERTS":    "https://hooks.slack.test/alerts",
    "SLACK_WEBHOOK_DEV":       "https://hooks.slack.test/dev",
}


def test_resolve_uses_payload_webhook_when_provided():
    label, url = slack.resolve_destination(
        severity="info",
        slack_webhook="https://hooks.slack.com/services/AAA/BBB/CCC",
        severity_webhooks=_SEVERITY_WEBHOOKS,
    )
    assert url == "https://hooks.slack.com/services/AAA/BBB/CCC"
    assert label == "custom-webhook"


def test_resolve_falls_back_to_severity_when_webhook_absent():
    label, url = slack.resolve_destination(
        severity="critical",
        slack_webhook=None,
        severity_webhooks=_SEVERITY_WEBHOOKS,
    )
    assert url == _SEVERITY_WEBHOOKS["SLACK_WEBHOOK_INCIDENTS"]
    assert label == slack.INCIDENTS_CHANNEL


def test_resolve_rejects_non_slack_url_for_ssrf_protection():
    """A non-hooks.slack.com URL must NOT be used as the post target."""
    label, url = slack.resolve_destination(
        severity="info",
        slack_webhook="http://169.254.169.254/latest/meta-data/",
        severity_webhooks=_SEVERITY_WEBHOOKS,
    )
    assert url == _SEVERITY_WEBHOOKS["SLACK_WEBHOOK_ALERTS"]
    assert label == slack.ALERTS_CHANNEL


@pytest.mark.parametrize("environment", ["dev", "staging", "DEV", " test "])
def test_resolve_routes_non_prod_to_dev_channel(environment):
    """Any non-prod environment goes to the dev channel, ignoring severity."""
    label, url = slack.resolve_destination(
        severity="critical",
        slack_webhook=None,
        severity_webhooks=_SEVERITY_WEBHOOKS,
        environment=environment,
    )
    assert url == _SEVERITY_WEBHOOKS["SLACK_WEBHOOK_DEV"]
    assert label == slack.DEV_CHANNEL


def test_resolve_prod_uses_severity_routing():
    """environment='prod' must NOT be diverted to the dev channel."""
    label, url = slack.resolve_destination(
        severity="critical",
        slack_webhook=None,
        severity_webhooks=_SEVERITY_WEBHOOKS,
        environment="prod",
    )
    assert url == _SEVERITY_WEBHOOKS["SLACK_WEBHOOK_INCIDENTS"]
    assert label == slack.INCIDENTS_CHANNEL


def test_resolve_explicit_webhook_beats_dev_routing():
    """A per-metric slack_webhook wins even for non-prod metrics."""
    label, url = slack.resolve_destination(
        severity="info",
        slack_webhook="https://hooks.slack.com/services/AAA/BBB/CCC",
        severity_webhooks=_SEVERITY_WEBHOOKS,
        environment="dev",
    )
    assert url == "https://hooks.slack.com/services/AAA/BBB/CCC"
    assert label == "custom-webhook"


@pytest.mark.parametrize("severity", ["warning", "info", "error", "critical"])
def test_resolve_non_prod_falls_back_to_alerts_not_incidents(severity):
    """If SLACK_WEBHOOK_DEV is unset, non-prod alerts fall back to the ALERTS
    channel for EVERY severity — including error/critical, which must never
    escalate a dev metric to the prod incident channel."""
    webhooks_without_dev = {
        "SLACK_WEBHOOK_INCIDENTS": "https://hooks.slack.test/incidents",
        "SLACK_WEBHOOK_ALERTS":    "https://hooks.slack.test/alerts",
    }
    label, url = slack.resolve_destination(
        severity=severity,
        slack_webhook=None,
        severity_webhooks=webhooks_without_dev,
        environment="dev",
    )
    assert url == webhooks_without_dev["SLACK_WEBHOOK_ALERTS"]
    assert label == slack.ALERTS_CHANNEL
    assert url != webhooks_without_dev["SLACK_WEBHOOK_INCIDENTS"]


# ---------------------------------------------------------------------------
# format_message() — block-kit shape and content
#
# format_message() returns the legacy `attachments` wrapper (for the coloured
# left-border stripe); the rich Block Kit content lives in
# out["attachments"][0]["blocks"]. This helper unwraps it.
# ---------------------------------------------------------------------------

def _blocks(out):
    """Return the Block Kit blocks from a format_message() body."""
    assert "attachments" in out
    assert len(out["attachments"]) == 1
    return out["attachments"][0]["blocks"]


def test_format_includes_pipeline_and_metric_name():
    # 'error' is a severity whose title template interpolates both names.
    out = slack.format_message({
        "pipeline_name": "ingest.iterate",
        "metric_name": "row_count_drop",
        "severity": "error",
    })
    text_blob = str(out)
    assert "ingest.iterate" in text_blob
    assert "row_count_drop" in text_blob


def test_format_color_matches_severity():
    """The attachment's left-border colour is keyed off severity."""
    out = slack.format_message({
        "pipeline_name": "p", "metric_name": "m", "severity": "critical",
    })
    assert out["attachments"][0]["color"] == slack._COLORS["critical"]


def test_format_includes_threshold_when_payload_has_one():
    """Auditor rules write `threshold` and `comparator` into payload — the
    Slack message should surface those so on-call sees the rule that tripped."""
    out = slack.format_message({
        "pipeline_name": "p", "metric_name": "m", "severity": "warning",
        "metric_value": 12.5,
        "payload": {"threshold": 10.0, "comparator": ">"},
    })
    fields_text = str(_blocks(out))
    assert "10.0" in fields_text
    assert ">" in fields_text


def test_format_truncates_huge_error_message():
    """Long error messages are trimmed to the last few lines and capped so a
    single block never approaches Slack's 3000-char section limit."""
    long_error = "BOOM" * 2000  # 8000 chars
    out = slack.format_message({
        "pipeline_name": "p", "metric_name": "m", "severity": "error",
        "payload": {"error_message": long_error},
    })
    err_block = next(
        b for b in _blocks(out)
        if b.get("type") == "section"
        and isinstance(b.get("text"), dict)
        and "BOOM" in b["text"]["text"]
    )
    assert len(err_block["text"]["text"]) <= 2510


def test_format_handles_missing_payload_gracefully():
    """metric_value=None, payload=None — should still produce a valid block-kit
    body (no exceptions, no None values inserted into Slack fields)."""
    out = slack.format_message({
        "pipeline_name": "p", "metric_name": "m", "severity": "info",
    })
    blocks = _blocks(out)
    assert blocks  # non-empty
    assert "None" not in str(blocks)


def test_format_omits_value_field_when_no_metric_value():
    out = slack.format_message({
        "pipeline_name": "p", "metric_name": "m", "severity": "info",
    })
    fields_blob = str(_blocks(out))
    # The label '*Value*' should be absent because we have no value.
    assert "*Value*" not in fields_blob


# ---------------------------------------------------------------------------
# format_message() — dbt-run failures headline the actual model, not the
# generic `dbt_run_failures` pipeline_name.
# ---------------------------------------------------------------------------

# A real dbt-run failure row: pipeline_name is the generic collector name and
# the failed object lives in payload.failures[].pipeline_name.
_DBT_METRIC = {
    "pipeline_name": "dbt_run_failures",
    "metric_name": "pipeline_failure",
    "severity": "error",
    "payload": {
        "failure_count": 1,
        "failures": [{
            "error_message": "Database Error in model braze_cdi_attribute_sync\n  syntax error line 27",
            "pipeline_name": "braze_cdi_attribute_sync",
            "resource_type": "model",
        }],
        "summary": "1 model(s) failed: braze_cdi_attribute_sync",
    },
}


def test_format_dbt_headline_uses_model_name_not_collector():
    """The headline names the failed model, not the generic 'dbt_run_failures'."""
    headline = _blocks(slack.format_message(_DBT_METRIC))[0]["text"]["text"]
    assert "braze_cdi_attribute_sync" in headline
    assert "dbt_run_failures" not in headline


def test_format_dbt_surfaces_nested_error_message():
    """dbt nests the error inside failures[]; the callout must still show it
    (top-level error_message/reason are absent on these rows)."""
    blob = str(_blocks(slack.format_message(_DBT_METRIC)))
    assert "syntax error line 27" in blob


def test_format_dbt_multiple_models_collapse_to_first_plus_count():
    metric = {
        **_DBT_METRIC,
        "payload": {
            "failures": [
                {"pipeline_name": "model_a", "error_message": "boom a"},
                {"pipeline_name": "model_b", "error_message": "boom b"},
                {"pipeline_name": "model_c", "error_message": "boom c"},
            ],
        },
    }
    headline = _blocks(slack.format_message(metric))[0]["text"]["text"]
    assert "model_a" in headline
    assert "+2 more" in headline


def test_format_non_dbt_headline_still_uses_pipeline_name():
    """Regression guard: a normal metric (no failures[]) keeps its pipeline_name
    as the headline subject."""
    headline = _blocks(slack.format_message({
        "pipeline_name": "orders_etl", "metric_name": "row_count",
        "severity": "critical", "payload": {"reason": "boom"},
    }))[0]["text"]["text"]
    assert "orders_etl" in headline


def test_dbt_failed_models_ignores_non_dbt_payload():
    """The helper returns [] for payloads without a failures list, so callers
    fall back to pipeline_name."""
    assert slack._dbt_failed_models({"reason": "boom"}) == []
    assert slack._dbt_failed_models({}) == []


# ---------------------------------------------------------------------------
# format_message() — action buttons must be valid, or Slack rejects the whole
# attachment with HTTP 400 `invalid_attachments`. Regression guard for the
# 2026-07-05 outage: placeholder buttons (no url, no action_id) are illegal.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("severity", ["critical", "error", "warning", "info"])
def test_format_every_button_is_a_valid_link_button(severity):
    """Every emitted button must carry a `url` (link button). A button with
    neither `url` nor `action_id` makes Slack reject the attachment."""
    out = slack.format_message({
        "pipeline_name": "p", "metric_name": "m", "severity": severity,
        "run_id": "run-123",
    })
    for block in _blocks(out):
        if block.get("type") != "actions":
            continue
        assert block["elements"], "actions block must not be empty"
        for element in block["elements"]:
            if element.get("type") == "button":
                assert "url" in element, (
                    f"{severity} button {element['text']['text']!r} has no url"
                )


@pytest.mark.parametrize("severity", ["critical", "error", "warning", "info"])
def test_format_drops_placeholder_buttons(severity):
    """The no-op placeholder buttons must not appear in the rendered message."""
    out = slack.format_message({
        "pipeline_name": "p", "metric_name": "m", "severity": severity,
    })
    blob = str(_blocks(out))
    for placeholder in ("Silence alarm", "Runbook", "Re-run", "Create ticket", "Investigate"):
        assert placeholder not in blob


def test_format_never_emits_empty_actions_block():
    """An `actions` block with no elements is itself invalid; it must be
    omitted entirely when no link buttons survive."""
    out = slack.format_message({
        "pipeline_name": "p", "metric_name": "m", "severity": "info",
    })
    for block in _blocks(out):
        if block.get("type") == "actions":
            assert block["elements"]


# ---------------------------------------------------------------------------
# format_message() — headline, Adelaide timestamp, payload table, CW button
# ---------------------------------------------------------------------------

def test_format_headline_says_pipeline_is_down_for_errors():
    out = slack.format_message({
        "pipeline_name": "orders_pipeline", "metric_name": "m", "severity": "error",
    })
    head = _blocks(out)[0]["text"]["text"]
    assert "orders_pipeline" in head
    assert "IS DOWN" in head


@pytest.mark.skipif(
    slack._ADELAIDE is None,
    reason="Australia/Adelaide tz data unavailable (install `tzdata`)",
)
def test_format_timestamp_is_adelaide_time():
    """occurred_at in UTC should render in Australia/Adelaide (ACST/ACDT),
    which is +9:30 / +10:30 ahead of UTC."""
    out = slack.format_message({
        "pipeline_name": "p", "metric_name": "m", "severity": "error",
        # 2026-07-06 19:44 UTC -> 2026-07-07 05:14 ACST (winter, +9:30)
        "occurred_at": datetime(2026, 7, 6, 19, 44, tzinfo=timezone.utc),
    })
    head = _blocks(out)[0]["text"]["text"]
    assert "2026-07-07 05:14 ACST" in head


@pytest.mark.skipif(
    slack._ADELAIDE is None or slack._SOURCE_TZ is None,
    reason="tz data unavailable (install `tzdata`)",
)
def test_format_naive_timestamp_is_treated_as_la_not_utc():
    """CUSTOM_METRICS.OCCURRED_AT is TIMESTAMP_NTZ under the LA account tz, so
    the connector returns a NAIVE datetime holding LA wall-clock. It must be
    read as America/Los_Angeles, not UTC.

    Naive 2026-07-14 10:09 LA (PDT, UTC-7) == 2026-07-14 17:09 UTC
    -> Adelaide (ACST, +9:30) == 2026-07-15 02:39. The old UTC assumption gave
    the wrong 2026-07-14 19:39 (7h early)."""
    out = slack.format_message({
        "pipeline_name": "dbt_run_failures", "metric_name": "pipeline_failure",
        "severity": "critical",
        "occurred_at": datetime(2026, 7, 14, 10, 9),  # naive == LA wall-clock
    })
    head = _blocks(out)[0]["text"]["text"]
    assert "2026-07-15 02:39 ACST" in head
    assert "19:39" not in head


def test_format_renders_payload_as_table():
    out = slack.format_message({
        "pipeline_name": "p", "metric_name": "m", "severity": "error",
        "metric_value": 0,
        "payload": {"rows_loaded": 42, "watermark": "2026-07-05"},
    })
    blob = str(_blocks(out))
    assert "*Payload*" in blob
    assert "rows_loaded" in blob and "42" in blob
    assert "watermark" in blob


def test_format_cloudwatch_button_deep_links_to_log_group():
    out = slack.format_message({
        "pipeline_name": "p", "metric_name": "m", "severity": "error",
        "payload": {"log_group": "/aws/lambda/monty-prod-logscanner"},
    })
    btn = next(
        el
        for b in _blocks(out) if b.get("type") == "actions"
        for el in b["elements"]
    )
    assert "View CloudWatch logs" in btn["text"]["text"]
    # log-group path is encoded with $252F for each slash
    assert "$252Faws$252Flambda$252Fmonty-prod-logscanner" in btn["url"]


def test_format_alert_callout_between_headline_and_payload():
    """Second block is an alert callout: severity emoji + *bold pipeline* +
    the first line of the error message."""
    out = slack.format_message({
        "pipeline_name": "orders_pipeline", "metric_name": "m", "severity": "error",
        "payload": {"error_message": "Deploy to staging failed — exit code 1.\nmore detail"},
    })
    blocks = _blocks(out)
    callout = blocks[1]["text"]["text"]
    assert callout.startswith(":alert:")
    assert "*orders_pipeline*" in callout           # pipeline name bold
    assert "Deploy to staging failed — exit code 1." in callout
    assert "more detail" not in callout             # only the first line


def test_format_alert_callout_falls_back_without_error():
    out = slack.format_message({
        "pipeline_name": "p", "metric_name": "row_count", "severity": "error",
    })
    callout = _blocks(out)[1]["text"]["text"]
    assert "*p*" in callout and "row_count" in callout


def test_format_escapes_slack_special_chars_in_headline():
    """`<`, `>`, `&` must be entity-escaped or Slack mis-parses the text."""
    out = slack.format_message({
        "pipeline_name": "a<b>&c", "metric_name": "m", "severity": "error",
    })
    head = _blocks(out)[0]["text"]["text"]
    assert "&lt;" in head and "&gt;" in head and "&amp;" in head
    assert "a<b>" not in head


# ---------------------------------------------------------------------------
# post() — urllib path; stub urlopen so we never hit the network
# ---------------------------------------------------------------------------

class _FakeResp:
    def __init__(self, status=200, body=b"ok"):
        self.status = status
        self._body = body
    def read(self):
        return self._body
    def __enter__(self):
        return self
    def __exit__(self, *a):
        return False


def test_post_returns_delivered_on_2xx():
    with patch("urllib.request.urlopen", return_value=_FakeResp(status=200)):
        result = slack.post("https://example.invalid/webhook", {"text": "hi"})
    assert result.delivered is True
    assert result.error_message is None


def test_post_returns_failure_on_4xx_text():
    """Slack returns 400 with a body like 'invalid_blocks'; we should capture
    that into error_message for the ALERT_OUTBOX row."""
    fake = _FakeResp(status=400, body=b"invalid_blocks")
    with patch("urllib.request.urlopen", return_value=fake):
        result = slack.post("https://example.invalid/webhook", {"text": "hi"})
    assert result.delivered is False
    assert "400" in result.error_message
    assert "invalid_blocks" in result.error_message


def test_post_returns_failure_on_http_error_exception():
    """urlopen raises HTTPError for 4xx/5xx by default; we catch and return
    a SlackResult instead of letting it propagate (so the Observer can record
    'failed' and move on to the next metric)."""
    err = urllib.error.HTTPError(
        "https://example.invalid/webhook",
        500, "server error",
        hdrs=None, fp=io.BytesIO(b"slack down"),
    )
    with patch("urllib.request.urlopen", side_effect=err):
        result = slack.post("https://example.invalid/webhook", {"text": "hi"})
    assert result.delivered is False
    assert "500" in result.error_message


def test_post_returns_failure_on_url_error():
    """Network-level failure (DNS, refused, etc.) → SlackResult with reason."""
    err = urllib.error.URLError("connection refused")
    with patch("urllib.request.urlopen", side_effect=err):
        result = slack.post("https://example.invalid/webhook", {"text": "hi"})
    assert result.delivered is False
    assert "connection refused" in result.error_message
