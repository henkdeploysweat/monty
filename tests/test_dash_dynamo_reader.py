"""Tests for the dashboard's DynamoDB read path (dash/db.py).

This path had NO coverage, and rewriting it from Scan to Query surfaced three
bugs by hand that a test would have caught instantly:

  1. the representative-payload items were APPENDED, double-counting a run;
  2. the trailing projection omitted `environment`, which transform filters on,
     so every trailing row was silently dropped and cadence history vanished;
  3. the representative Query was unbounded, so for the two busiest pipelines
     the newest item fell outside the (minute-truncated) window, matched no row,
     and the payload graft silently missed.

Each of those is pinned below. No AWS: conftest stubs boto3 + the Key/Attr
condition builders, and _ddb_table is patched to a recording fake.
"""
from datetime import datetime, timedelta, timezone

import pytest

import db


# ---------------------------------------------------------------------------
# fakes
# ---------------------------------------------------------------------------
class FakeTable:
    """Records every query() and returns programmed items.

    `items_for(pk)` decides what each partition holds; the fake applies the sk
    range itself so range-bound bugs surface as wrong rows, not just wrong args.
    """

    def __init__(self, by_pk=None):
        self.by_pk = by_pk or {}
        self.queries = []
        self.scans = []

    def query(self, **kwargs):
        self.queries.append(kwargs)
        cond = kwargs["KeyConditionExpression"]
        pk = cond.term("pk")[2]
        items = list(self.by_pk.get(pk, []))
        sk_term = cond.term("sk")
        if sk_term:
            _, _, low, high = sk_term
            items = [i for i in items if low <= i["sk"] <= high]
        items.sort(key=lambda i: i["sk"],
                   reverse=not kwargs.get("ScanIndexForward", True))
        if kwargs.get("Limit"):
            items = items[:kwargs["Limit"]]
        if "ProjectionExpression" in kwargs:
            keep = set(kwargs["ExpressionAttributeNames"].values())
            items = [{k: v for k, v in i.items() if k in keep} for i in items]
        return {"Items": items}

    def scan(self, **kwargs):
        self.scans.append(kwargs)
        return {"Items": [{"pk": pk} for pk in self.by_pk]}


def _item(pipeline, ts, env="prod", **extra):
    """One writer-shaped item (mirrors lambdas/shared/dynamo_writer.write)."""
    iso = ts.replace(tzinfo=timezone.utc).isoformat()
    item = {"pk": "%s#%s" % (env, pipeline),
            "sk": "%s#%s" % (iso, "0" * 8 + "-uuid"),
            "occurred_at": iso,
            "pipeline_name": pipeline,
            "metric_name": "rows_loaded",
            "severity": "info",
            "is_alert": False,
            "environment": env,
            "ttl": 1}
    item.update(extra)
    return item


NOW = datetime(2026, 7, 17, 12, 0, 0)


@pytest.fixture(autouse=True)
def _clean_cache():
    db.clear_cache()
    yield
    db.clear_cache()


@pytest.fixture
def table(monkeypatch):
    """A fake table wired in place of the real one, with 2 pipelines."""
    fake = FakeTable({
        "prod#alpha": [_item("alpha", NOW - timedelta(hours=h)) for h in (1, 50)],
        "prod#beta": [_item("beta", NOW - timedelta(hours=h)) for h in (2, 60)],
    })
    monkeypatch.setattr(db, "_ddb_table", lambda env: fake)
    monkeypatch.setenv("MONTY_SOURCE", "dynamo")
    return fake


# ---------------------------------------------------------------------------
# projections — payload is ~3x of all other bytes, so it must stay off the wire
# ---------------------------------------------------------------------------
def test_lean_attrs_exclude_payload():
    assert "payload" not in db._DDB_LEAN_ATTRS


def test_lean_attrs_exclude_environment_because_it_is_stamped_from_the_pk():
    # Paying for `environment` per row is waste: pk is "<env>#<pipeline>", so it
    # is known from the partition. _fetch_dynamo_impl re-stamps it.
    assert "environment" not in db._DDB_LEAN_ATTRS


def test_trailing_attrs_are_only_what_cadence_needs():
    assert set(db._DDB_TRAILING_ATTRS) == {"occurred_at", "pipeline_name"}


def test_projection_aliases_every_attribute():
    # Aliasing keeps a (future) DynamoDB reserved word from breaking the read.
    proj = db._ddb_projection(("occurred_at", "severity"))
    assert proj["ProjectionExpression"] == "#a0, #a1"
    assert proj["ExpressionAttributeNames"] == {"#a0": "occurred_at",
                                                "#a1": "severity"}


def test_projection_of_nothing_is_omitted_entirely():
    assert db._ddb_projection(None) == {}


# ---------------------------------------------------------------------------
# the sk range IS the time index — this is what removes the need for a GSI
# ---------------------------------------------------------------------------
def test_sk_max_sentinel_sorts_above_any_writer_suffix():
    # sk is "<occurred_at ISO>#<uuid4>" (dynamo_writer) or "#<source ID>"
    # (backfill_dynamo). The upper bound must exceed every one of them, or the
    # newest events in the window are cut off.
    iso = "2026-07-17T12:00:00.000000+00:00"
    upper = iso + db._DDB_SK_MAX
    for suffix in ("ffffffff-ffff-4fff-bfff-ffffffffffff", "zzzz", "~", "999"):
        assert iso + "#" + suffix < upper


def test_sk_bounds_inclusive_end_covers_every_id_at_that_instant():
    lo, hi = db._sk_bounds(NOW - timedelta(days=1), NOW, True)
    assert db._iso(NOW) + "#any-uuid" <= hi
    assert lo == db._iso(NOW - timedelta(days=1))


def test_sk_bounds_exclusive_end_excludes_every_id_at_that_instant():
    # This is what keeps the detail and trailing lanes from overlapping.
    _, hi = db._sk_bounds(NOW - timedelta(days=1), NOW, False)
    assert db._iso(NOW) + "#any-uuid" > hi


def test_sk_range_bounds_the_query(table):
    db._fetch_dynamo_impl(3, "prod", NOW, detail_days=1)
    window = [q for q in table.queries if "Limit" not in q]
    assert window, "expected range Queries"
    for kwargs in window:
        sk = kwargs["KeyConditionExpression"].term("sk")
        # Without an sk bound the Query degenerates to reading the whole
        # partition, which is the cost this rewrite exists to avoid.
        assert sk is not None
        assert sk[2] < sk[3]


def test_detail_and_trailing_ranges_do_not_overlap(table):
    # The union is un-deduped, so any overlap double-counts rows.
    db._fetch_dynamo_impl(8, "prod", NOW, detail_days=1)
    ranges = {}
    for kwargs in table.queries:
        if "Limit" in kwargs:
            continue
        cond = kwargs["KeyConditionExpression"]
        pk, sk = cond.term("pk")[2], cond.term("sk")
        ranges.setdefault(pk, []).append((sk[2], sk[3]))
    for pk, spans in ranges.items():
        assert len(spans) == 2, "expected a detail + a trailing range for %s" % pk
        (lo1, hi1), (lo2, hi2) = sorted(spans)
        assert hi1 <= lo2, "detail and trailing overlap for %s" % pk


# ---------------------------------------------------------------------------
# normalisation
# ---------------------------------------------------------------------------
def test_trailing_row_normalises_to_naive_utc():
    row = db._normalise_ddb_trailing_row(_item("alpha", NOW))
    assert row["PIPELINE_NAME"] == "alpha"
    assert row["OCCURRED_AT"] == NOW
    assert row["OCCURRED_AT"].tzinfo is None


def test_trailing_row_survives_an_unparseable_timestamp():
    row = db._normalise_ddb_trailing_row({"pipeline_name": "a",
                                          "occurred_at": "not-a-date"})
    assert row["OCCURRED_AT"] is None      # dropped downstream, never raises


def test_normalise_skips_key_and_ttl_plumbing():
    row = db._normalise_ddb_row(_item("alpha", NOW))
    for plumbing in ("PK", "SK", "TTL"):
        assert plumbing not in row


# ---------------------------------------------------------------------------
# the payload graft — bug (1) and bug (3)
# ---------------------------------------------------------------------------
def test_graft_attaches_payload_without_adding_a_row():
    rows = [{"PIPELINE_NAME": "alpha", "OCCURRED_AT": NOW, "PAYLOAD": None}]
    reps = [_item("alpha", NOW, payload='{"unique_id":"model.x"}')]
    db._merge_representative_payloads(rows, reps)
    assert len(rows) == 1, "representatives must be grafted, never appended"
    assert rows[0]["PAYLOAD"] == '{"unique_id":"model.x"}'


def test_graft_ignores_a_representative_outside_the_window():
    # A row that isn't in the fetched window must not be invented — the old
    # full-window Scan never reported it either.
    rows = [{"PIPELINE_NAME": "alpha", "OCCURRED_AT": NOW, "PAYLOAD": None}]
    reps = [_item("alpha", NOW - timedelta(days=99), payload="{}")]
    db._merge_representative_payloads(rows, reps)
    assert len(rows) == 1
    assert rows[0]["PAYLOAD"] is None


def test_graft_does_not_clobber_a_payload_the_detail_lane_already_has():
    rows = [{"PIPELINE_NAME": "alpha", "OCCURRED_AT": NOW, "PAYLOAD": "real"}]
    db._merge_representative_payloads(rows, [_item("alpha", NOW, payload="rep")])
    assert rows[0]["PAYLOAD"] == "real"


def test_representative_query_is_bounded_to_the_window(table):
    # Unbounded, the newest item lands after the minute-truncated `now`, matches
    # no fetched row, and the graft silently misses.
    db._fetch_dynamo_impl(3, "prod", NOW, detail_days=1)
    reps = [q for q in table.queries if q.get("Limit") == 1]
    assert reps, "expected one representative Query per pipeline"
    for kwargs in reps:
        assert kwargs["ScanIndexForward"] is False      # newest first
        assert kwargs["KeyConditionExpression"].term("sk") is not None


# ---------------------------------------------------------------------------
# bug (2): transform filters on ENVIRONMENT, so every row must carry it
# ---------------------------------------------------------------------------
def test_every_row_is_stamped_with_the_environment(table):
    rows = db._fetch_dynamo_impl(8, "prod", NOW, detail_days=1)
    assert rows, "fixture should return rows"
    assert all(r["ENVIRONMENT"] == "prod" for r in rows)


def test_rows_come_back_sorted_by_occurred_at(table):
    rows = db._fetch_dynamo_impl(8, "prod", NOW, detail_days=1)
    assert rows == sorted(rows, key=lambda r: r["OCCURRED_AT"])


def test_split_returns_the_same_rows_as_a_full_fetch(table):
    """The golden property: the lane split changes cost, never content."""
    full = db._fetch_dynamo_impl(8, "prod", NOW, detail_days=None)
    db.clear_cache()
    split = db._fetch_dynamo_impl(8, "prod", NOW, detail_days=1)
    key = lambda rows: sorted(  # noqa: E731
        (r["PIPELINE_NAME"], r["OCCURRED_AT"]) for r in rows)
    assert key(full) == key(split)


# ---------------------------------------------------------------------------
# transform._pick_pay — lives here because its upgrade rule exists ONLY to make
# the payload-less trailing projection safe.
# ---------------------------------------------------------------------------
def test_pick_pay_upgrades_a_payload_less_entry():
    # The trailing lane hands over payload-less rows first; the graft lands on
    # one row per pipeline, which is rarely the first one seen. Without the
    # upgrade the entry pins to "" and the family identity is lost.
    import transform
    pay = {}
    transform._pick_pay(pay, "alpha", None, "rows_loaded")
    transform._pick_pay(pay, "alpha", '{"unique_id":"model.x"}', "rows_loaded")
    assert pay["alpha"].startswith('{"unique_id":"model.x"}')


def test_pick_pay_keeps_the_first_real_payload():
    import transform
    pay = {}
    transform._pick_pay(pay, "alpha", "first", "rows_loaded")
    transform._pick_pay(pay, "alpha", "second", "rows_loaded")
    assert pay["alpha"].startswith("first")


def test_pick_pay_still_prefers_dbt_identity():
    import transform
    pay = {}
    transform._pick_pay(pay, "alpha", "plain", "rows_loaded")
    transform._pick_pay(pay, "alpha", "dbt", "dbt_model_run")
    assert pay["alpha"].startswith("dbt")


# ---------------------------------------------------------------------------
# pipeline discovery
# ---------------------------------------------------------------------------
def test_configured_pipeline_list_skips_the_discovery_scan(table, monkeypatch):
    monkeypatch.setattr(db, "MONTY_DDB_PIPELINES", "alpha, beta")
    assert db._ddb_pipelines("prod") == ["prod#alpha", "prod#beta"]
    assert table.scans == [], "MONTY_DDB_PIPELINES must avoid the ~30s Scan"


def test_discovery_scan_projects_only_the_key(table, monkeypatch):
    monkeypatch.setattr(db, "MONTY_DDB_PIPELINES", "")
    assert db._ddb_pipelines("prod") == ["prod#alpha", "prod#beta"]
    assert table.scans[0]["ProjectionExpression"] == "pk"


def test_discovery_is_cached(table, monkeypatch):
    monkeypatch.setattr(db, "MONTY_DDB_PIPELINES", "")
    db._ddb_pipelines("prod")
    db._ddb_pipelines("prod")
    assert len(table.scans) == 1, "discovery costs ~30s; it must not repeat"
