"""
test_ingest.py
--------------
Tests for the S3 -> SQLite ingest + archive pipeline. No AWS or Snowflake:
S3 is a small in-memory fake, and Parquet objects are real pyarrow bytes so the
reader path is exercised for real.

Run:  python -m pytest test_ingest.py -v
"""
from __future__ import annotations

import io
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import sqlite_store
import loadS3


# --------------------------------------------------------------------------
# In-memory fake S3 client (only the methods the ingest path uses)
# --------------------------------------------------------------------------
class _FakePaginator:
    def __init__(self, store):
        self._store = store

    def paginate(self, Bucket, Prefix):
        contents = [{"Key": k} for k in sorted(self._store)
                    if k.startswith(Prefix)]
        yield {"Contents": contents}


class FakeS3:
    """Minimal S3 stand-in: keys -> bytes, with copy/head/delete/list."""
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def get_paginator(self, _name):
        return _FakePaginator(self.objects)

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[Key])}

    def copy_object(self, Bucket, Key, CopySource):
        self.objects[Key] = self.objects[CopySource["Key"]]

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        return {"ContentLength": len(self.objects[Key])}

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)


def _parquet_bytes(rows: list[dict]) -> bytes:
    """Serialise event dicts to a Parquet object (what the S3 writer emits)."""
    cols = {k: [r.get(k) for r in rows] for k in rows[0]}
    buf = io.BytesIO()
    pq.write_table(pa.table(cols), buf)
    return buf.getvalue()


def _event(eid, pipeline, sev, occurred, env="prod", value=1.0):
    return {
        "ID": eid, "PIPELINE_NAME": pipeline, "METRIC_NAME": "m",
        "METRIC_VALUE": value, "SEVERITY": sev, "RUN_ID": "run",
        "PAYLOAD": "{}", "OCCURRED_AT": occurred, "IS_ALERT": False,
        "SENT_TO_SLACK": False, "SENT_AT": None, "ENVIRONMENT": env,
    }


# --------------------------------------------------------------------------
# sqlite_store unit tests
# --------------------------------------------------------------------------
def test_insert_dedup_and_fetch(tmp_path):
    db = tmp_path / "monty.db"
    conn = sqlite_store.connect(db)
    rows = [_event("a", "p1", "warning", datetime(2026, 7, 14, 10, 0)),
            _event("b", "p1", "info", datetime(2026, 7, 14, 11, 0))]
    assert sqlite_store.insert_events(conn, rows) == 2
    # re-insert the same IDs -> 0 new rows (idempotent)
    assert sqlite_store.insert_events(conn, rows) == 0
    conn.commit()
    conn.close()

    got = sqlite_store.fetch_events(datetime(2026, 7, 14, 0, 0),
                                    datetime(2026, 7, 14, 23, 0), "prod", db)
    assert [r["ID"] for r in got] == ["a", "b"]
    assert isinstance(got[0]["OCCURRED_AT"], datetime)
    assert got[0]["IS_ALERT"] is False


def test_fetch_window_and_env_filter(tmp_path):
    db = tmp_path / "monty.db"
    conn = sqlite_store.connect(db)
    sqlite_store.insert_events(conn, [
        _event("a", "p1", "warning", datetime(2026, 7, 14, 10, 0), env="prod"),
        _event("b", "p2", "warning", datetime(2026, 7, 14, 10, 0), env="dev"),
        _event("c", "p3", "warning", datetime(2026, 7, 1, 10, 0), env="prod"),
    ])
    conn.commit(); conn.close()

    prod = sqlite_store.fetch_events(datetime(2026, 7, 14, 0, 0),
                                     datetime(2026, 7, 14, 23, 0), "prod", db)
    assert [r["ID"] for r in prod] == ["a"]   # b is dev, c is out of window


def test_is_populated(tmp_path):
    db = tmp_path / "monty.db"
    assert sqlite_store.is_populated(db) is False        # fresh DB -> empty
    conn = sqlite_store.connect(db)
    sqlite_store.insert_events(
        conn, [_event("a", "p1", "warning", datetime(2026, 7, 14, 10, 0))])
    conn.commit(); conn.close()
    assert sqlite_store.is_populated(db) is True


# --------------------------------------------------------------------------
# single-run lock (prevents overlapping cron ticks)
# --------------------------------------------------------------------------
def test_single_run_lock_blocks_overlap(tmp_path):
    db = str(tmp_path / "monty.db")
    with loadS3._single_run_lock(db) as first:
        assert first is True
        with loadS3._single_run_lock(db) as second:
            assert second is False       # a concurrent run is refused
    with loadS3._single_run_lock(db) as third:
        assert third is True             # released after the first exits


# --------------------------------------------------------------------------
# archive-key mapping (pure function)
# --------------------------------------------------------------------------
@pytest.mark.parametrize("base,key,expected", [
    ("", "run_date=20260714/x.parquet", "archive/run_date=20260714/x.parquet"),
    ("metrics/", "metrics/run_date=20260714/x.parquet",
     "metrics/archive/run_date=20260714/x.parquet"),
])
def test_archive_key_for(base, key, expected):
    assert loadS3.archive_key_for(key, base) == expected


# --------------------------------------------------------------------------
# end-to-end ingest + move against the fake S3
# --------------------------------------------------------------------------
def test_ingest_moves_objects_and_populates_db(tmp_path, monkeypatch):
    fake = FakeS3()
    # two source objects in today's partition
    k1 = "run_date=20260714/obj1.parquet"
    k2 = "run_date=20260714/obj2.parquet"
    fake.objects[k1] = _parquet_bytes(
        [_event("a", "p1", "warning", datetime(2026, 7, 14, 10, 0))])
    fake.objects[k2] = _parquet_bytes(
        [_event("b", "p1", "info", datetime(2026, 7, 14, 11, 0))])

    monkeypatch.setattr(loadS3, "_s3_client", lambda env: fake)
    monkeypatch.setattr(loadS3, "MONTY_S3_BUCKET", "monty-{env}-metrics")
    monkeypatch.setattr(loadS3, "MONTY_S3_PREFIX", "")

    db = str(tmp_path / "monty.db")
    from datetime import date
    result = loadS3.ingest_to_sqlite("prod", date(2026, 7, 14), date(2026, 7, 14),
                                     dry_run=False, db_path=db)

    assert result["ingested_keys"] == 2
    assert result["rows_inserted"] == 2
    assert result["archived"] == 2

    # originals gone, archived copies present (folder structure preserved)
    assert k1 not in fake.objects
    assert k2 not in fake.objects
    assert "archive/run_date=20260714/obj1.parquet" in fake.objects
    assert "archive/run_date=20260714/obj2.parquet" in fake.objects

    # data landed in SQLite
    got = sqlite_store.fetch_events(datetime(2026, 7, 14, 0, 0),
                                    datetime(2026, 7, 14, 23, 59), "prod", db)
    assert {r["ID"] for r in got} == {"a", "b"}

    # re-running ingests nothing new and archives nothing (idempotent)
    again = loadS3.ingest_to_sqlite("prod", date(2026, 7, 14), date(2026, 7, 14),
                                    dry_run=False, db_path=db)
    assert again["ingested_keys"] == 0
    assert again["archived"] == 0


def test_ingest_dry_run_keeps_originals(tmp_path, monkeypatch):
    fake = FakeS3()
    k1 = "run_date=20260714/obj1.parquet"
    fake.objects[k1] = _parquet_bytes(
        [_event("a", "p1", "warning", datetime(2026, 7, 14, 10, 0))])

    monkeypatch.setattr(loadS3, "_s3_client", lambda env: fake)
    monkeypatch.setattr(loadS3, "MONTY_S3_BUCKET", "monty-{env}-metrics")
    monkeypatch.setattr(loadS3, "MONTY_S3_PREFIX", "")

    db = str(tmp_path / "monty.db")
    from datetime import date
    result = loadS3.ingest_to_sqlite("prod", date(2026, 7, 14), date(2026, 7, 14),
                                     dry_run=True, db_path=db)
    # dry-run still ingests to SQLite but never touches S3
    assert result["rows_inserted"] == 1
    assert k1 in fake.objects                       # original untouched
    assert "archive/run_date=20260714/obj1.parquet" not in fake.objects
