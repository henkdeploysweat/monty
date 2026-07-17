"""Tests for dash/backfill_dynamo.py.

This script had NO coverage and silently lost 78,912 rows in production.

The bug: it deduped rows on `row["ID"]`, but S3 Parquet rows have no ID column
at all (their keys are ENVIRONMENT, IS_ALERT, METRIC_NAME, METRIC_VALUE,
OCCURRED_AT, PAYLOAD, PIPELINE_NAME, RUN_ID, SENT_AT, SENT_TO_SLACK). Every row
therefore returned None, the first one claimed the key, and the entire leg
collapsed to ONE row — while the script logged "done" and reported success.

That class of failure is what these tests exist to prevent: a leg that reads
thousands of rows and contributes one must never look like success.
"""
import sys
import types
from datetime import datetime

import pytest

import backfill_dynamo as bf


NOW = datetime(2026, 7, 16, 12, 0, 0)


def s3_row(pipeline="alpha", metric="rows_loaded", ts=NOW, value=1.0,
           severity="info", **extra):
    """A row shaped EXACTLY as db._fetch_s3 returns it — note: no ID key."""
    row = {"PIPELINE_NAME": pipeline, "METRIC_NAME": metric, "OCCURRED_AT": ts,
           "METRIC_VALUE": value, "SEVERITY": severity, "IS_ALERT": False,
           "ENVIRONMENT": "prod", "PAYLOAD": None, "RUN_ID": None,
           "SENT_AT": None, "SENT_TO_SLACK": False}
    row.update(extra)
    return row


def sqlite_row(row_id, **kw):
    """A SQLite/Snowflake row — same contract, but it carries the source ID."""
    row = s3_row(**kw)
    row["ID"] = row_id
    return row


# ---------------------------------------------------------------------------
# _row_identity — the sk suffix
# ---------------------------------------------------------------------------
def test_identity_uses_the_source_id_when_there_is_one():
    # Must keep using ID, or every item an earlier run wrote as
    # "<occurred_at>#<ID>" would be duplicated under a new scheme.
    assert bf._row_identity(sqlite_row("evt-123")) == "evt-123"


def test_identity_falls_back_to_a_hash_when_there_is_no_id():
    ident = bf._row_identity(s3_row())
    assert ident and ident != "None"


def test_identity_is_deterministic():
    # A uuid4 here would duplicate every row on every re-run.
    assert bf._row_identity(s3_row()) == bf._row_identity(s3_row())


def test_identity_separates_metrics_from_the_same_pipeline_at_one_instant():
    # THE regression: the busiest pipelines emit ~253 metrics per run, so an
    # identity that ignores metric_name collapses a whole run into one row.
    a = bf._row_identity(s3_row(metric="rows_loaded"))
    b = bf._row_identity(s3_row(metric="rows_rejected"))
    assert a != b


def test_identity_separates_different_values_of_the_same_metric():
    assert bf._row_identity(s3_row(value=1.0)) != bf._row_identity(s3_row(value=2.0))


def test_identity_never_collapses_a_whole_leg():
    # The exact shape of the original bug, stated as a property: N distinct
    # events must yield N distinct identities, not 1.
    rows = [s3_row(metric="m%d" % i) for i in range(50)]
    assert len({bf._row_identity(r) for r in rows}) == 50


# ---------------------------------------------------------------------------
# _to_item — what actually lands in DynamoDB
# ---------------------------------------------------------------------------
def test_item_sk_starts_with_occurred_at_so_it_is_a_time_index():
    item = bf._to_item(sqlite_row("evt-1"))
    assert item["sk"].startswith("2026-07-16T12:00:00+00:00#")


def test_item_sk_never_ends_in_none():
    # "<occurred_at>#None" is what the broken version wrote for every S3 row.
    item = bf._to_item(s3_row())
    assert not item["sk"].endswith("#None")


def test_item_pk_matches_the_live_writer():
    assert bf._to_item(s3_row())["pk"] == "prod#alpha"


def test_item_ttl_counts_from_the_original_event_time():
    # A replayed 3-week-old row must expire on its REAL schedule, not get a
    # fresh 90 days.
    item = bf._to_item(s3_row())
    expected = int(NOW.timestamp()) + bf.DEFAULT_TTL_DAYS * 86400
    assert abs(item["ttl"] - expected) < 86400


def test_item_omits_optional_fields_that_are_null():
    # Mirrors dynamo_writer.write — absent, not null.
    item = bf._to_item(s3_row(value=None))
    assert "metric_value" not in item
    assert "run_id" not in item
    assert "payload" not in item


# ---------------------------------------------------------------------------
# _fetch_rows — the union. This is where the rows were lost.
# ---------------------------------------------------------------------------
@pytest.fixture
def fake_db(monkeypatch):
    """Stand in for dash/db.py with programmable legs."""
    mod = types.ModuleType("db")
    mod.s3_rows, mod.sqlite_rows = [], []
    mod._fetch_s3 = lambda *a, **k: list(mod.s3_rows)
    mod._fetch_sqlite = lambda *a, **k: list(mod.sqlite_rows)
    mod._row_key = lambda r: (r.get("PIPELINE_NAME"), r.get("METRIC_NAME"),
                              r.get("OCCURRED_AT"), r.get("METRIC_VALUE"))
    monkeypatch.setitem(sys.modules, "db", mod)
    return mod


def test_s3_leg_is_not_collapsed_by_its_missing_id(fake_db):
    """THE regression test. 500 ID-less rows in, 500 out — not 1."""
    fake_db.s3_rows = [s3_row(metric="m%d" % i) for i in range(500)]
    kept = bf._fetch_rows("prod", datetime(2026, 7, 1), datetime(2026, 8, 1), "s3")
    assert len(kept) == 500


def test_union_still_drops_a_genuine_duplicate(fake_db):
    # Same event from both stores: SQLite has an ID, S3 does not, so an
    # id-based union could never match them. Dedup is on the natural key.
    fake_db.sqlite_rows = [sqlite_row("evt-1")]
    fake_db.s3_rows = [s3_row()]
    kept = bf._fetch_rows("prod", datetime(2026, 7, 1), datetime(2026, 8, 1), "both")
    assert len(kept) == 1


def test_only_ddb_severities_are_backfilled(fake_db):
    # critical/error belong to Snowflake and are not part of the DDB contract.
    fake_db.s3_rows = [s3_row(metric="m1", severity="info"),
                       s3_row(metric="m2", severity="warning"),
                       s3_row(metric="m3", severity="error"),
                       s3_row(metric="m4", severity="critical")]
    kept = bf._fetch_rows("prod", datetime(2026, 7, 1), datetime(2026, 8, 1), "s3")
    assert {r["SEVERITY"] for r in kept} == {"info", "warning"}


def test_rows_outside_the_window_are_excluded(fake_db):
    fake_db.s3_rows = [s3_row(metric="in", ts=datetime(2026, 7, 16)),
                       s3_row(metric="out", ts=datetime(2026, 6, 1))]
    kept = bf._fetch_rows("prod", datetime(2026, 7, 1), datetime(2026, 8, 1), "s3")
    assert [r["METRIC_NAME"] for r in kept] == ["in"]


def test_a_dead_leg_does_not_abort_the_backfill(fake_db):
    def boom(*a, **k):
        raise RuntimeError("expired creds")
    fake_db._fetch_sqlite = boom
    fake_db.s3_rows = [s3_row()]
    kept = bf._fetch_rows("prod", datetime(2026, 7, 1), datetime(2026, 8, 1), "both")
    assert len(kept) == 1


def test_source_s3_does_not_read_sqlite(fake_db):
    def boom(*a, **k):
        raise AssertionError("--source s3 must not touch the sqlite leg")
    fake_db._fetch_sqlite = boom
    fake_db.s3_rows = [s3_row()]
    assert len(bf._fetch_rows("prod", datetime(2026, 7, 1),
                              datetime(2026, 8, 1), "s3")) == 1
