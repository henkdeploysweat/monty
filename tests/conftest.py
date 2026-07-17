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

# The dashboard is a flat package (`import db`, `import transform`), so its
# directory has to be importable too — see tests/test_dash_dynamo_reader.py.
_DASH_DIR = _REPO_ROOT / "dash"
if str(_DASH_DIR) not in sys.path:
    sys.path.insert(0, str(_DASH_DIR))

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

class _StubSession:
    """boto3.Session(profile_name=...) — the dashboard reader resolves a
    per-environment profile (db._s3_profile_for) before opening a resource."""

    def __init__(self, profile_name=None):  # noqa: N803 — boto3 API name
        self.profile_name = profile_name

    def client(self, name, region_name=None):  # noqa: ARG002
        return _StubSecretsClient()

    def resource(self, name, region_name=None):  # noqa: ARG002
        return _StubDynamoResource()

boto3_stub.client = _stub_client  # type: ignore[attr-defined]
boto3_stub.resource = _stub_resource  # type: ignore[attr-defined]
boto3_stub.Session = _StubSession  # type: ignore[attr-defined]
boto3_stub._StubSecretsClient = _StubSecretsClient  # exposed for tests
boto3_stub._StubDynamoTable = _StubDynamoTable  # exposed for tests
sys.modules["boto3"] = boto3_stub


# ---------------------------------------------------------------------------
# boto3.dynamodb.conditions — Key/Attr build the KeyConditionExpression the
# dashboard reader Queries with (dash/db.py). The real classes render to
# DynamoDB wire syntax; these just record (name, op, *values) so a test can
# assert on the sk range bounds without a live table.
# ---------------------------------------------------------------------------
_ddb_mod = types.ModuleType("boto3.dynamodb")
_cond_mod = types.ModuleType("boto3.dynamodb.conditions")

class _StubCondition:
    """One or more recorded predicates. `&` concatenates, like the real API."""

    def __init__(self, terms):
        self.terms = list(terms)

    def __and__(self, other):
        return _StubCondition(self.terms + other.terms)

    def term(self, name):
        """The recorded predicate for `name`, or None. Test convenience."""
        for t in self.terms:
            if t[0] == name:
                return t
        return None

class _StubKeyAttr:
    """Stands in for BOTH Key and Attr — identical surface for our purposes."""

    def __init__(self, name):
        self.name = name

    def eq(self, value):
        return _StubCondition([(self.name, "eq", value)])

    def between(self, low, high):
        return _StubCondition([(self.name, "between", low, high)])

    def not_exists(self):
        return _StubCondition([(self.name, "not_exists")])

_cond_mod.Key = _StubKeyAttr  # type: ignore[attr-defined]
_cond_mod.Attr = _StubKeyAttr  # type: ignore[attr-defined]
_cond_mod._StubCondition = _StubCondition
_ddb_mod.conditions = _cond_mod  # type: ignore[attr-defined]
boto3_stub.dynamodb = _ddb_mod  # type: ignore[attr-defined]
sys.modules["boto3.dynamodb"] = _ddb_mod
sys.modules["boto3.dynamodb.conditions"] = _cond_mod


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
