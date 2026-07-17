"""pytest configuration: stub heavy third-party deps so the suite runs in a
plain Python venv without `boto3` or `snowflake-connector-python`.

The Lambda code imports these at module top — that's correct for the runtime
image, but we don't want the test harness to require them. Replacing them in
`sys.modules` before the test files import the Lambda modules makes both
production and tests happy with no test-flag branches in the real code.
"""

import os
import sys
import types
from pathlib import Path

# Make the repo root importable so `from lambdas.shared import metric_writer`
# resolves when pytest is invoked from anywhere.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Lambda runtime expects these env vars; provide harmless defaults so module
# imports don't fail before tests even run.
os.environ.setdefault("MONTY_SECRET_NAME", "monty-test-secrets")
os.environ.setdefault("AWS_REGION", "us-east-1")
os.environ.setdefault("MONTY_METRICS_TABLE", "monty-test-metrics-ddb")


# ---------------------------------------------------------------------------
# boto3 stub — only needs `client(...).get_secret_value(SecretId=...)` and
# returns a JSON string. Per-test code patches what the secret contains.
#
# Always overrides any locally-installed boto3, even if importable, so the
# tests stay deterministic regardless of what's in the user's pip cache.
# ---------------------------------------------------------------------------
boto3_stub = types.ModuleType("boto3")

class _StubSecretsClient:
    """Default stub. Tests override `_secret_string` via fixture."""
    _secret_string = '{"user":"u","password":"p","account":"a"}'

    def get_secret_value(self, *, SecretId):  # noqa: N803 — boto3 API name
        return {"SecretString": self._secret_string}

class _StubDynamoTable:
    """DynamoDB Table stub for the dynamo_writer path. Records every put_item
    call (class-level so tests can inspect them regardless of resource caching
    in dynamo_writer). Each recorded call is {"TableName": ..., "Item": ...}."""
    put_calls: list[dict] = []

    def __init__(self, name):
        self.name = name

    def put_item(self, *, Item):  # noqa: N803 — boto3 API name
        _StubDynamoTable.put_calls.append({"TableName": self.name, "Item": Item})
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

class _StubDynamoResource:
    def Table(self, name):  # noqa: N802 — boto3 API name
        return _StubDynamoTable(name)

def _stub_client(name, region_name=None):  # noqa: ARG001
    return _StubSecretsClient()

def _stub_resource(name, region_name=None):  # noqa: ARG001
    return _StubDynamoResource()

boto3_stub.client = _stub_client  # type: ignore[attr-defined]
boto3_stub.resource = _stub_resource  # type: ignore[attr-defined]
boto3_stub._StubSecretsClient = _StubSecretsClient  # exposed for tests
boto3_stub._StubDynamoTable = _StubDynamoTable  # exposed for tests
sys.modules["boto3"] = boto3_stub


# ---------------------------------------------------------------------------
# snowflake.connector stub — `connect(**kwargs)` returns a context-manager
# object whose `cursor()` is overridable. Tests that exercise SQL flow
# replace this via fixture; tests that don't touch Snowflake never trigger it.
#
# Forced override (any locally-installed snowflake-connector-python is
# replaced) so a broken local install doesn't blow up the test run.
# ---------------------------------------------------------------------------
if True:
    sf_root = types.ModuleType("snowflake")
    sf_connector = types.ModuleType("snowflake.connector")

    class _StubCursor:
        def __init__(self):
            self.executed = []
            self.rowcount = 1
            self.description = []
            self._rows: list[tuple] = []

        def execute(self, sql, params=None):
            self.executed.append((sql, params))

        def fetchall(self):
            return list(self._rows)

        def close(self):
            pass

    class _StubConnection:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.cursors: list[_StubCursor] = []
            self.committed = 0

        def cursor(self):
            c = _StubCursor()
            self.cursors.append(c)
            return c

        def commit(self):
            self.committed += 1

        def close(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            self.close()

    def _connect(**kwargs):
        return _StubConnection(**kwargs)

    # Build a stub class so isinstance checks (none today, but cheap to add) work.
    sf_connector.connect = _connect  # type: ignore[attr-defined]
    sf_connector.SnowflakeConnection = _StubConnection  # type: ignore[attr-defined]
    sf_connector._StubConnection = _StubConnection  # exposed for tests
    sf_connector._StubCursor = _StubCursor

    sf_root.connector = sf_connector  # type: ignore[attr-defined]
    sys.modules["snowflake"] = sf_root
    sys.modules["snowflake.connector"] = sf_connector
