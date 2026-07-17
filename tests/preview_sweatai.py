"""Manual preview / tester for the SweatAI endpoint (POST /sweatai).

Not a pytest file (no `test_` prefix, so pytest skips it). It's the sibling of
`preview_dbt_slack.py`: a script you run by hand to exercise the endpoint and
eyeball the request/response — including the HMAC signing every real caller
must do.

Two modes:

  LOCAL (default) — invoke `handler.lambda_handler` in-process. No AWS, no
  deploy: the Snowflake/boto3 imports are stubbed and the HMAC secret is faked
  locally, so you see the full signed-request -> handler -> response round-trip
  offline. Great for testing your `handle_request` code as you write it.

      python3 tests/preview_sweatai.py
      python3 tests/preview_sweatai.py --message "hello sweatai"
      python3 tests/preview_sweatai.py --body '{"message":"hi","foo":1}'
      python3 tests/preview_sweatai.py --bad-signature   # prove the 401 path

  REMOTE — actually POST to the deployed endpoint. Signs the body with the real
  HMAC secret and prints the HTTP status + response. Use after `make cdk-deploy`.

      export MONTY_HMAC_SECRET=...            # same value as in monty-<env>-secrets
      python3 tests/preview_sweatai.py --url https://abc123.execute-api.us-east-1.amazonaws.com/sweatai \\
          --message "hello from my laptop"

Run from anywhere — it bootstraps the repo root onto sys.path itself.
"""

import argparse
import hashlib
import hmac
import json
import sys
import types
import urllib.error
import urllib.request
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# A throwaway HMAC secret used ONLY in local mode. In remote mode the real
# secret comes from the MONTY_HMAC_SECRET env var — never hardcode the real one.
_LOCAL_HMAC_SECRET = "local-preview-secret"


def _sign(raw_body: str, secret: str) -> str:
    """Return the hex HMAC-SHA256 the endpoint expects in X-Monty-Signature."""
    return hmac.new(
        secret.encode("utf-8"), raw_body.encode("utf-8"), hashlib.sha256
    ).hexdigest()


def _stub_runtime_deps() -> None:
    """Register minimal boto3 / snowflake stubs so importing the handler works
    in a plain venv (the handler imports snowflake_client, which imports both at
    module top). Mirrors what tests/conftest.py does under pytest. No-op if the
    real packages are already importable.
    """
    if "boto3" not in sys.modules:
        try:
            import boto3  # noqa: F401
        except ImportError:
            boto3_stub = types.ModuleType("boto3")
            boto3_stub.client = lambda *a, **k: None  # type: ignore[attr-defined]
            sys.modules["boto3"] = boto3_stub

    if "snowflake.connector" not in sys.modules:
        try:
            import snowflake.connector  # noqa: F401
        except ImportError:
            sf_root = types.ModuleType("snowflake")
            sf_connector = types.ModuleType("snowflake.connector")
            sf_connector.connect = lambda **k: None  # type: ignore[attr-defined]
            sf_connector.SnowflakeConnection = object  # type: ignore[attr-defined]
            sf_root.connector = sf_connector  # type: ignore[attr-defined]
            sys.modules["snowflake"] = sf_root
            sys.modules["snowflake.connector"] = sf_connector


def _run_local(raw_body: str, sign_secret: str) -> None:
    """Invoke the handler in-process with a signed API Gateway event."""
    _stub_runtime_deps()
    from lambdas.sweatai import handler  # noqa: E402 — after stub bootstrap

    # Fake the secret lookups so no AWS call happens. The handler verifies the
    # signature against MONTY_HMAC_SECRET; get_secret() has no ANTHROPIC_API_KEY
    # locally, so a signed request cleanly returns 503 (proves the signing +
    # routing without calling Anthropic or DynamoDB).
    handler.get_secret_value = (  # type: ignore[assignment]
        lambda key: _LOCAL_HMAC_SECRET if key == "MONTY_HMAC_SECRET" else ""
    )
    handler.get_secret = lambda: {"MONTY_HMAC_SECRET": _LOCAL_HMAC_SECRET}  # type: ignore[assignment]

    signature = _sign(raw_body, sign_secret)
    event = {
        "body": raw_body,
        "headers": {"X-Monty-Signature": signature},
    }

    print("=" * 70)
    print("MODE:      local (in-process handler, no AWS)")
    print(f"signature: {signature}")
    print("request body:")
    print(raw_body)
    print("-" * 70)

    response = handler.lambda_handler(event, context=None)

    print(f"statusCode: {response['statusCode']}")
    print("response body:")
    parsed = json.loads(response["body"])
    print(json.dumps(parsed, indent=2))
    print("=" * 70)
    if response["statusCode"] == 401:
        print("(401 = signature rejected — expected with --bad-signature)")
    else:
        print("(handle_request currently echoes the payload — replace it with real logic)")


def _run_remote(url: str, raw_body: str, secret: str) -> None:
    """POST to the deployed endpoint with a real HMAC signature."""
    signature = _sign(raw_body, secret)
    req = urllib.request.Request(
        url,
        data=raw_body.encode("utf-8"),
        headers={
            "content-type": "application/json",
            "X-Monty-Signature": signature,
        },
        method="POST",
    )

    print("=" * 70)
    print(f"MODE:      remote POST -> {url}")
    print(f"signature: {signature}")
    print("request body:")
    print(raw_body)
    print("-" * 70)

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            status = resp.status
            payload = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        status = exc.code
        payload = exc.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as exc:
        print(f"request failed: {exc}")
        return

    print(f"HTTP {status}")
    print("response body:")
    try:
        print(json.dumps(json.loads(payload), indent=2))
    except json.JSONDecodeError:
        print(payload)
    print("=" * 70)


def main() -> None:
    parser = argparse.ArgumentParser(description="Preview / test the SweatAI endpoint.")
    parser.add_argument(
        "--url",
        help="Deployed endpoint URL. If given, does a real HTTP POST (remote mode) "
        "and signs with $MONTY_HMAC_SECRET. Omit for local in-process mode.",
    )
    parser.add_argument(
        "--prompt",
        default="""DBT job failed: 1 model(s) failed out of 12 total (total time: 7.17s) First error in model 'braze_cdi_delete_sync': Database Error in model braze_cdi_delete_sync (models/marts/application/braze_cdi_delete_sync.sql) 002023 (22000): SQL compilation error: Expression type does not match column data type, expecting TIMESTAMP_NTZ(9) but got TIMESTAMP_TZ(9) for column UPDATED_AT compiled code at /tmp/dbt_output/target/run/dbt_snowflake_transformation/models/marts/application/braze_cdi_delete_sync.sql. Context: DBT 1.9.4, Command: dbt build. Run select system$get_dbt_log('01c5b863-3206-5f34-0000-081154fc77e6') for more details.""",
        help="The user prompt sent as {\"prompt\": ...} in the request body.",
    )
    parser.add_argument(
        "--system-prompt",
        dest="system_prompt",
        help="Optional system prompt. Omit to use the handler's DEFAULT_SYSTEM_PROMPT.",
    )
    parser.add_argument(
        "--service",
        help="Optional service label logged with the request (defaults to 'unknown').",
    )
    parser.add_argument(
        "--body",
        help="Raw JSON body to send. Overrides --prompt/--system-prompt/--service.",
    )
    parser.add_argument(
        "--bad-signature",
        action="store_true",
        help="Local mode only: sign with the wrong secret to exercise the 401 path.",
    )
    args = parser.parse_args()

    if args.body is not None:
        raw_body = args.body
    else:
        payload = {"prompt": args.prompt}
        if args.system_prompt is not None:
            payload["system_prompt"] = args.system_prompt
        if args.service is not None:
            payload["service"] = args.service
        raw_body = json.dumps(payload)
    # Fail fast on malformed --body so the preview reflects a real request.
    try:
        json.loads(raw_body)
    except json.JSONDecodeError as exc:
        parser.error(f"--body is not valid JSON: {exc}")

    if args.url:
        import os
        secret = os.environ.get("MONTY_HMAC_SECRET")
        if not secret:
            parser.error(
                "remote mode needs the real secret: export MONTY_HMAC_SECRET=... "
                "(same value as in monty-<env>-secrets)."
            )
        _run_remote(args.url, raw_body, secret)
    else:
        sign_secret = "wrong-secret" if args.bad_signature else _LOCAL_HMAC_SECRET
        _run_local(raw_body, sign_secret)


if __name__ == "__main__":
    main()
