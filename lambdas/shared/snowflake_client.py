"""Snowflake connection helper for Monty Lambdas.

Mirrors the pattern in analytics-ingest-iterate-repo's
`IngestionCode/snowflake_load.py` (Secrets Manager -> JSON dict ->
`snowflake.connector.connect(**secret)`). Centralised here so all four
Lambdas share one piece of credential plumbing.

Env vars expected at Lambda runtime:
    MONTY_SECRET_NAME   Name of the Secrets Manager secret holding Snowflake
                        credentials. Provided by the CDK stack.
    AWS_REGION          Standard Lambda env var; falls back to us-east-1.
"""

import json
import logging
import os
from functools import lru_cache
from typing import Any

import boto3
import snowflake.connector

logger = logging.getLogger(__name__)

DEFAULT_REGION = "us-east-1"

# Keys we expect inside the secret JSON. Anything else is ignored — Snowflake
# accepts extra kwargs gracefully but we filter to keep the interface tight.
_SNOWFLAKE_KEYS = (
    "user",
    "password",
    "account",
    "warehouse",
    "database",
    "schema",
    "role",
    "host",
)


@lru_cache(maxsize=1)
def _get_secret() -> dict[str, Any]:
    """Fetch and parse the Monty secret. Cached for the Lambda's warm lifetime.

    The cache is what makes this safe to call from every Lambda invocation — a
    cold start hits Secrets Manager once, warm invocations reuse the parsed
    dict. Lambda freezes globals between invocations, so the cache survives.
    """
    secret_name = os.environ["MONTY_SECRET_NAME"]
    region = os.environ.get("AWS_REGION", DEFAULT_REGION)
    client = boto3.client("secretsmanager", region_name=region)
    raw = client.get_secret_value(SecretId=secret_name)["SecretString"]
    return json.loads(raw)


def get_connection(**overrides: Any) -> snowflake.connector.SnowflakeConnection:
    """Return a fresh Snowflake connection.

    Callers should wrap usage in `with get_connection() as conn:` so the
    connection is closed even on exception. Connections are NOT cached because
    Snowflake idle timeouts are unpredictable and re-authenticating is cheap
    relative to query latency.

    `overrides` lets a caller pin warehouse/role/schema for a single call
    without touching the secret.
    """
    secret = _get_secret()
    params = {k: secret[k] for k in _SNOWFLAKE_KEYS if k in secret}
    params.update(overrides)
    logger.debug(
        "snowflake.connect account=%s warehouse=%s role=%s",
        params.get("account"),
        params.get("warehouse"),
        params.get("role"),
    )
    return snowflake.connector.connect(**params)


def get_secret_value(key: str) -> str:
    """Fetch a non-Snowflake field from the same secret (HMAC, Slack URLs).

    Keeps all sensitive config in one Secrets Manager entry instead of sprawl
    across env vars and multiple secrets.
    """
    secret = _get_secret()
    if key not in secret:
        raise KeyError(f"Secret {os.environ['MONTY_SECRET_NAME']!r} missing key {key!r}")
    return secret[key]


def get_secret() -> dict[str, Any]:
    """Return a copy of the full secret dict.

    For callers that need to scan multiple keys at once (e.g. the observer
    pulling every SLACK_WEBHOOK_* entry to build its channel map).
    """
    return dict(_get_secret())
