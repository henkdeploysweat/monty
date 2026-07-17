"""In-Lambda client for the SweatAI endpoint (POST /sweatai).

Lets other Monty Lambdas route their AI calls THROUGH the SweatAI endpoint
instead of calling Anthropic directly — so every call is captured in the
`monty-<env>-sweatai-prompt-logs` DynamoDB table (prompt, response, tokens,
duration, service).

This is the in-image sibling of `clients/sweatai.py`: same request shape and
HMAC signing, but it reads config the way a Lambda has it rather than from
`clients/`-style env vars:
    SWEATAI_URL        env var — the POST /sweatai URL (set by the CDK stack)
    MONTY_HMAC_SECRET  read from the Monty secret in Secrets Manager (same value
                       failure_proxy / sweatai verify against), NOT an env var.

Stdlib only — no `anthropic` SDK (Monty gotcha #6).
"""

import hashlib
import hmac
import json
import os
import urllib.request
from typing import Any

from lambdas.shared.snowflake_client import get_secret_value

SWEATAI_URL_ENV = "SWEATAI_URL"
HMAC_SECRET_KEY = "MONTY_HMAC_SECRET"  # key inside Secrets Manager


def sweatai(
    prompt: str,
    service: str = "unknown",
    system_prompt: str | None = None,
    timeout: int = 30,
) -> str:
    """POST a prompt to the SweatAI endpoint and return Claude's reply string.

    Raises on any failure (missing SWEATAI_URL, HTTP error, malformed response)
    so callers can fall back. The endpoint logs the exchange to DynamoDB, which
    is the reason to route through it rather than calling Anthropic directly.
    """
    url = os.environ[SWEATAI_URL_ENV]
    secret = get_secret_value(HMAC_SECRET_KEY)

    payload: dict[str, Any] = {"prompt": prompt, "service": service}
    if system_prompt is not None:
        payload["system_prompt"] = system_prompt

    body = json.dumps(payload).encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()

    request = urllib.request.Request(
        url,
        data=body,
        headers={"content-type": "application/json", "X-Monty-Signature": signature},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))["reply"]
