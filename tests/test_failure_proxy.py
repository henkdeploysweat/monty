"""Tests for the failure-proxy Lambda — schema validation + HMAC auth."""

import hashlib
import hmac
import json

import pytest

from lambdas.failure_proxy import handler, schema


# ---------------------------------------------------------------------------
# schema.validate — pure function, no IO
# ---------------------------------------------------------------------------

def test_validate_minimal_required_fields():
    """Happy path: required fields present, no extras."""
    out = schema.validate({
        "pipeline_name": "ingest.iterate",
        "run_id": "abc-123",
        "error_message": "boom",
        "severity": "error",
    })
    # error_message is folded into payload so the Slack formatter can render it.
    assert out["payload"]["error_message"] == "boom"
    assert out["metric_name"] == "pipeline_failure"  # default
    assert out["is_alert"] is True
    assert out["severity"] == "error"


def test_validate_uppercases_severity_normalized_to_lower():
    out = schema.validate({
        "pipeline_name": "p", "run_id": "r",
        "error_message": "e", "severity": "CRITICAL",
    })
    assert out["severity"] == "critical"


def test_validate_rejects_missing_required_field():
    with pytest.raises(schema.PayloadError, match="run_id"):
        schema.validate({
            "pipeline_name": "p",
            "error_message": "e",
            "severity": "error",
        })


def test_validate_rejects_empty_string_required_field():
    """Empty string is just as bad as missing."""
    with pytest.raises(schema.PayloadError, match="pipeline_name"):
        schema.validate({
            "pipeline_name": "   ",  # whitespace-only
            "run_id": "r", "error_message": "e", "severity": "error",
        })


def test_validate_rejects_bad_severity():
    with pytest.raises(schema.PayloadError, match="severity must be"):
        schema.validate({
            "pipeline_name": "p", "run_id": "r",
            "error_message": "e", "severity": "panic",
        })


def test_validate_rejects_non_numeric_metric_value():
    with pytest.raises(schema.PayloadError, match="numeric"):
        schema.validate({
            "pipeline_name": "p", "run_id": "r",
            "error_message": "e", "severity": "error",
            "metric_value": "not a number",
        })


def test_validate_truncates_very_long_error_message():
    """4000 chars is the cap. Snowflake STRING tolerates much more, but Slack
    block-text caps around 3000 — the 4000 limit gives the Slack formatter
    headroom to add its own framing."""
    long = "x" * 5000
    out = schema.validate({
        "pipeline_name": "p", "run_id": "r",
        "error_message": long, "severity": "error",
    })
    assert len(out["payload"]["error_message"]) == 4000


def test_validate_extra_payload_dict_is_merged():
    out = schema.validate({
        "pipeline_name": "p", "run_id": "r",
        "error_message": "e", "severity": "error",
        "payload": {"region": "us-east-1", "retry_count": 3},
    })
    assert out["payload"]["region"] == "us-east-1"
    assert out["payload"]["retry_count"] == 3
    # error_message still injected.
    assert out["payload"]["error_message"] == "e"


def test_validate_rejects_non_dict_payload():
    with pytest.raises(schema.PayloadError, match="payload must be"):
        schema.validate({
            "pipeline_name": "p", "run_id": "r",
            "error_message": "e", "severity": "error",
            "payload": "string instead of object",
        })


def test_validate_slack_webhook_lands_in_payload():
    """Top-level slack_webhook is folded into payload so the observer sees it."""
    url = "https://hooks.slack.com/services/AAA/BBB/CCC"
    out = schema.validate({
        "pipeline_name": "p", "run_id": "r",
        "error_message": "e", "severity": "error",
        "slack_webhook": url,
    })
    assert out["payload"]["slack_webhook"] == url


def test_validate_rejects_non_string_slack_webhook():
    with pytest.raises(schema.PayloadError, match="slack_webhook must be"):
        schema.validate({
            "pipeline_name": "p", "run_id": "r",
            "error_message": "e", "severity": "error",
            "slack_webhook": ["a", "b"],
        })


# ---------------------------------------------------------------------------
# handler.lambda_handler — HMAC + integration with metric_writer
# ---------------------------------------------------------------------------

HMAC_SECRET = "shared-secret-for-tests"


def _signed_request(body_dict):
    """Build an API Gateway HTTP API event with a valid HMAC signature."""
    body = json.dumps(body_dict)
    sig = hmac.new(
        HMAC_SECRET.encode(), body.encode(), hashlib.sha256
    ).hexdigest()
    return {
        "body": body,
        "headers": {"X-Monty-Signature": sig},
    }


@pytest.fixture
def stub_secret_and_writer(monkeypatch):
  
    monkeypatch.setattr(handler, "get_secret_value",
                        lambda key: HMAC_SECRET if key == "MONTY_HMAC_SECRET" else "")
    written: list = []
    monkeypatch.setattr(handler.metric_writer, "write",
                        lambda metric: written.append(metric) or 1)
    return written


def test_handler_returns_202_on_valid_signed_request(stub_secret_and_writer):
    event = _signed_request({
        "pipeline_name": "ingest.iterate",
        "run_id": "abc-123",
        "error_message": "boom",
        "severity": "critical",
    })

    response = handler.lambda_handler(event, context=None)

    assert response["statusCode"] == 202
    assert len(stub_secret_and_writer) == 1
    written_metric = stub_secret_and_writer[0]
    assert written_metric.pipeline_name == "ingest.iterate"
    assert written_metric.is_alert is True
    assert written_metric.severity == "critical"


def test_handler_rejects_missing_signature(stub_secret_and_writer):
    """No X-Monty-Signature header → 401, no Snowflake write."""
    body = json.dumps({
        "pipeline_name": "p", "run_id": "r",
        "error_message": "e", "severity": "error",
    })
    response = handler.lambda_handler(
        {"body": body, "headers": {}}, context=None
    )
    assert response["statusCode"] == 401
    assert stub_secret_and_writer == []


def test_handler_rejects_bad_signature(stub_secret_and_writer):
    body = json.dumps({
        "pipeline_name": "p", "run_id": "r",
        "error_message": "e", "severity": "error",
    })
    response = handler.lambda_handler(
        {"body": body, "headers": {"X-Monty-Signature": "deadbeef"}},
        context=None,
    )
    assert response["statusCode"] == 401
    assert stub_secret_and_writer == []


def test_handler_returns_400_on_invalid_json(stub_secret_and_writer):
    raw = "{not json"
    sig = hmac.new(HMAC_SECRET.encode(), raw.encode(), hashlib.sha256).hexdigest()
    response = handler.lambda_handler(
        {"body": raw, "headers": {"X-Monty-Signature": sig}},
        context=None,
    )
    assert response["statusCode"] == 400


def test_handler_returns_400_on_validation_error(stub_secret_and_writer):
    """Signed but missing fields → 400; nothing written."""
    event = _signed_request({"pipeline_name": "p"})  # missing the rest
    response = handler.lambda_handler(event, context=None)
    assert response["statusCode"] == 400
    assert stub_secret_and_writer == []


def test_handler_propagates_snowflake_errors(monkeypatch):
    """If Snowflake write fails, we re-raise so API Gateway returns 5xx and
    the caller's retry kicks in. Better duplicates than lost alerts."""
    monkeypatch.setattr(handler, "get_secret_value", lambda k: HMAC_SECRET)

    def _fail(_metric):
        raise RuntimeError("snowflake exploded")
    monkeypatch.setattr(handler.metric_writer, "write", _fail)

    event = _signed_request({
        "pipeline_name": "p", "run_id": "r",
        "error_message": "e", "severity": "error",
    })

    with pytest.raises(RuntimeError, match="snowflake exploded"):
        handler.lambda_handler(event, context=None)
