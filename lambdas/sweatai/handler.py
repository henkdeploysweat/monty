"""SweatAI endpoint Lambda.

Fronted by the Monty HTTP API at `POST /sweatai`. Accepts an HMAC-signed JSON
POST with three caller-supplied fields:

    {"prompt": "...", "system_prompt": "...", "service": "..."}

Only `prompt` is required. `system_prompt` defaults to DEFAULT_SYSTEM_PROMPT (a
DataOps failure-analysis persona); `service` defaults to "unknown". The handler
calls Claude, returns the reply to the caller, and logs the full exchange +
token accounting to the DynamoDB table named in MONTY_PROMPT_LOGS_TABLE.

No `anthropic` SDK — the Lambda image ships only stdlib + snowflake/boto3
(see CLAUDE.md gotcha #6), so the Anthropic calls use urllib, mirroring
`lambdas/observer/slack.py:ai_summary`.
"""

import hashlib
import hmac
import json
import logging
import os
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import boto3

from lambdas.shared.snowflake_client import get_secret, get_secret_value

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SIGNATURE_HEADER = "x-monty-signature"
HMAC_SECRET_KEY = "MONTY_HMAC_SECRET"  # key inside Secrets Manager
ANTHROPIC_API_KEY_KEY = "ANTHROPIC_API_KEY"  # key inside Secrets Manager
PROMPT_LOGS_TABLE_ENV = "MONTY_PROMPT_LOGS_TABLE"

DEFAULT_SYSTEM_PROMPTMODEL = "claude-haiku-4-5"
MODEL=DEFAULT_SYSTEM_PROMPTMODEL
MAX_TOKENS = 2048
TEMPERATURE = 0.2
DEFAULT_SERVICE = "sweat"

MESSAGES_URL = "https://api.anthropic.com/v1/messages"
COUNT_TOKENS_URL = "https://api.anthropic.com/v1/messages/count_tokens"
# Kept under the API Gateway HTTP API 30s integration timeout and the Lambda's
# own 30s cap so a slow upstream fails cleanly rather than being killed.
ANTHROPIC_TIMEOUT_SECONDS = 25

DEFAULT_SYSTEM_PROMPT = """You are an expert Data Operations (DataOps) Cloud Engineer and Analytics Engineer specializing in AWS data infrastructure and dbt (data build tool) Core/Cloud. Your primary directive is to analyze pipeline failure logs, error messages, and system metadata to pinpoint the exact root cause of a failure and provide actionable remediation steps.

When analyzing a failure, systematically evaluate the following domains:
1. AWS Infrastructure: Check for IAM permission issues, VPC/networking bottlenecks, OOM (Out of Memory) errors in AWS Glue/EMR/ECS, Lambda timeouts, or S3 access/storage issues.
2. dbt & Data Warehousing: Check for SQL compilation errors, schema mismatches, failing dbt tests (unique, not_null, relationships, accepted_values), upstream materialization failures, snapshot errors, or warehouse query timeouts (e.g., Snowflake, BigQuery, Redshift).
3. Data/Dependency Issues: Identify late-arriving data, broken DAG dependencies, or unexpected nulls/duplicates.

Structure your response using the following framework:
- 🚨 **Error Summary:** A concise, 1-2 sentence explanation of what failed and where (AWS vs. dbt).
- 🔍 **Root Cause Analysis:** A technical breakdown of *why* it failed, citing specific lines from the log or error message.
- 🛠️ **Remediation Steps:** Step-by-step, actionable instructions to fix the immediate issue.
- 🛡️ **Preventative Recommendations:** 1-2 long-term suggestions to prevent this specific failure from happening again (e.g., adding a dbt test, adjusting an AWS IAM policy, or scaling resources).

Maintain a professional, highly analytical, and direct tone. Avoid generic advice; rely strictly on the provided log context. If the log is truncated or missing critical information to make a definitive conclusion, explicitly state what specific logs or metadata are needed."""


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """API Gateway v2 HTTP API entry point for `POST /sweatai`.

    Verifies the HMAC signature over the raw body, parses JSON, generates a
    Claude reply, logs the exchange to DynamoDB, and returns the reply.
    """
    raw_body = event.get("body") or ""
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}

    if not _signature_valid(raw_body, headers.get(SIGNATURE_HEADER)):
        logger.warning("rejecting sweatai request: bad or missing signature")
        return _response(401, {"error": "invalid signature"})

    try:
        body = json.loads(raw_body) if raw_body else {}
    except json.JSONDecodeError as exc:
        return _response(400, {"error": f"invalid JSON: {exc}"})

    prompt = str(body.get("prompt") or "").strip()
    if not prompt:
        return _response(400, {"error": "missing required field: prompt"})

    system_prompt = str(body.get("system_prompt") or "").strip() or DEFAULT_SYSTEM_PROMPT
    MODEL = str(body.get("model") or "").strip() or DEFAULT_SYSTEM_PROMPTMODEL
    service = str(body.get("service") or "").strip() or DEFAULT_SERVICE
    logger.info(f"!!!!@system_prompt {system_prompt}")
    logger.info(f"!!!!@model  {MODEL}")
    api_key = get_secret().get(ANTHROPIC_API_KEY_KEY, "")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY missing from secret; cannot call Claude")
        return _response(503, {"error": "ANTHROPIC_API_KEY not configured"})

    try:
        result = _generate_reply(prompt, system_prompt, service, api_key)
    except Exception:
        # Re-raise so API Gateway returns 5xx and the caller can retry. The
        # uncaught exception is logged by Lambda's default handler.
        logger.exception("sweatai request handler failed")
        raise

    return _response(200, result)


def _generate_reply(
    prompt: str, system_prompt: str, service: str, api_key: str
) -> dict[str, Any]:
    """Call Claude, log the exchange to DynamoDB, and return the response body."""
    received_at = datetime.now(timezone.utc)
    messages = [{"role": "user", "content": prompt}]

    request_body = {
        "model": MODEL,
        "system": system_prompt,
        "messages": messages,
        "max_tokens": MAX_TOKENS,
        "temperature": TEMPERATURE,
    }

    logger.info("sweatai calling Claude: service=%s prompt_chars=%d", service, len(prompt))
    logger.info("sweatai system_prompt=%s", system_prompt)
    start = time.monotonic()
    api_result = _call_anthropic(MESSAGES_URL, request_body, api_key)
    duration_sec = time.monotonic() - start

    reply_text = _extract_text(api_result)
    usage = api_result.get("usage") or {}
    total_input_tokens = usage.get("input_tokens")
    completion_tokens = usage.get("output_tokens")

    # Break the input into user-prompt vs system-prompt tokens. The Messages
    # response only reports a combined input_tokens, so count the user prompt
    # alone via count_tokens and attribute the remainder to the system prompt.
    prompt_tokens = _count_user_tokens(messages, api_key)
    if prompt_tokens is not None and total_input_tokens is not None:
        system_prompt_tokens = max(0, total_input_tokens - prompt_tokens)
    else:
        system_prompt_tokens = None

    record = {
        "id": str(uuid.uuid4()),
        "model": MODEL,
        "timestamp": received_at.isoformat(),
        "system_prompt": system_prompt,
        "user_prompt": prompt,
        "response": reply_text,
        "prompt_tokens": prompt_tokens,
        "system_prompt_tokens": system_prompt_tokens,
        "completion_tokens": completion_tokens,
        "duration_sec": round(duration_sec, 3),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "service": service,
    }
    _log_prompt(record)

    return {
        "id": record["id"],
        "reply": reply_text,
        "model": MODEL,
        "service": service,
        "usage": {
            "prompt_tokens": prompt_tokens,
            "system_prompt_tokens": system_prompt_tokens,
            "completion_tokens": completion_tokens,
        },
        "duration_sec": record["duration_sec"],
    }


def _call_anthropic(url: str, body: dict[str, Any], api_key: str) -> dict[str, Any]:
    """POST to the Anthropic API with retry on 429. Returns the parsed JSON.

    Mirrors the retry/backoff shape of observer/slack.py:ai_summary — stdlib
    urllib only, no SDK.
    """
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        method="POST",
    )
    logger.info(f"BODY {json.dumps(body)}")

    max_attempts = 4
    for attempt in range(max_attempts):
        try:
            with urllib.request.urlopen(req, timeout=ANTHROPIC_TIMEOUT_SECONDS) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="replace")
            if exc.code == 429 and attempt < max_attempts - 1:
                sleep_seconds = (2 ** attempt) + 0.3
                logger.warning(
                    "Claude 429 (rate limit); retrying in %.2fs (attempt %d/%d)",
                    sleep_seconds, attempt + 1, max_attempts,
                )
                time.sleep(sleep_seconds)
                continue
            logger.error("Claude HTTPError %s: %s", exc.code, err_body)
            raise
        except Exception:
            logger.exception("Claude request failed")
            raise

    # Unreachable: the loop either returns or raises on the final attempt.
    raise RuntimeError("Claude request exhausted retries without a response")


def _count_user_tokens(messages: list[dict[str, Any]], api_key: str) -> int | None:
    """Count tokens for the user messages alone (no system prompt).

    Best-effort: token accounting must never fail the caller's request, so any
    error here is logged and yields None (the log row simply omits the split).
    """
    try:
        result = _call_anthropic(
            COUNT_TOKENS_URL, {"model": MODEL, "messages": messages}, api_key
        )
        return result.get("input_tokens")
    except Exception:
        logger.warning("count_tokens failed; prompt-token split unavailable", exc_info=True)
        return None


def _extract_text(api_result: dict[str, Any]) -> str:
    """Join all text blocks from a Messages API response into one string."""
    blocks = api_result.get("content") or []
    parts = [b.get("text", "") for b in blocks if b.get("type") == "text"]
    return "".join(parts).strip()


def _log_prompt(record: dict[str, Any]) -> None:
    """Write one prompt/response row to DynamoDB. Best-effort — a logging
    failure must not fail the caller's request, so errors are swallowed after
    logging.
    """
    table_name = os.environ.get(PROMPT_LOGS_TABLE_ENV)
    if not table_name:
        logger.warning("%s unset; skipping prompt log", PROMPT_LOGS_TABLE_ENV)
        return

    # Everything below — including building the item — sits inside the try, so a
    # bug in this function can never fail the caller's request. The reply is the
    # product; the log is bookkeeping.
    try:
        # id + created_at are the table's partition/sort keys (see infra/monty_stack.py).
        item: dict[str, Any] = {
            "id": record["id"],
            "created_at": record["created_at"],
            "model": record["model"],
            "timestamp": record["timestamp"],
            "system_prompt": record["system_prompt"],
            "user_prompt": record["user_prompt"],
            "response": record["response"],
            "service": record["service"],
            # DynamoDB rejects float; store the duration as a Decimal.
            "duration_sec": Decimal(str(record["duration_sec"])),
        }
        # Token counts are ints (DynamoDB Number). Only write them when known so a
        # count_tokens miss doesn't store a misleading zero.
        for key in ("prompt_tokens", "system_prompt_tokens", "completion_tokens"):
            value = record.get(key)
            if value is not None:
                item[key] = int(value)

        table = boto3.resource("dynamodb").Table(table_name)
        table.put_item(Item=item)
        logger.info("logged sweatai prompt id=%s to %s", record["id"], table_name)
    except Exception:
        logger.exception("failed to log sweatai prompt id=%s", record.get("id"))


def _signature_valid(raw_body: str, provided: str | None) -> bool:
    """Constant-time HMAC-SHA256 check over the raw body (mirrors failure_proxy)."""
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
