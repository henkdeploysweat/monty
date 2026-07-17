
import hashlib
import hmac
import json
import logging
from typing import Any

from lambdas.failure_proxy.schema import PayloadError, validate
from lambdas.shared import metric_writer
from lambdas.shared.snowflake_client import get_secret_value

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SIGNATURE_HEADER = "x-monty-signature"
HMAC_SECRET_KEY = "MONTY_HMAC_SECRET"  # key inside Secrets Manager


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """API Gateway v2 HTTP API event handler."""
    raw_body = event.get("body") or ""
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}

    if not _signature_valid(raw_body, headers.get(SIGNATURE_HEADER)):
        logger.warning("rejecting request: bad or missing signature")
        return _response(401, {"error": "invalid signature"})

    try:
        body = json.loads(raw_body) if raw_body else {}
    except json.JSONDecodeError as exc:
        return _response(400, {"error": f"invalid JSON: {exc}"})

    try:
        normalized = validate(body)
    except PayloadError as exc:
        return _response(400, {"error": str(exc)})

    try:
        metric = metric_writer.Metric.from_dict(normalized)
        metric_writer.write(metric)
    except Exception:
        # Re-raise so API Gateway returns 5xx and the caller's retry logic
        # kicks in. Logging happens via Lambda's default uncaught-exception
        # handler. We don't want to swallow Snowflake errors silently — that
        # would mean lost alerts.
        logger.exception("snowflake write failed")
        raise

    return _response(202, {"status": "accepted"})


def _signature_valid(raw_body: str, provided: str | None) -> bool:

    if not provided:
        return False
    secret = get_secret_value(HMAC_SECRET_KEY).encode("utf-8")
    expected = hmac.new(secret, raw_body.encode("utf-8"), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, provided)


def _response(status: int, body: dict[str, Any]) -> dict[str, Any]:
    """Format a JSON response for API Gateway HTTP API."""
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(body),
    }
