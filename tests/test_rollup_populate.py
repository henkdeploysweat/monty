"""Tests for dash/rollup.py — the hourly rollup populate.

The load-bearing property is idempotency-by-recompute: the item key is derived
purely from (env, pipeline, metric, hour) and aggregates are recomputed from raw,
never incremented and never keyed on an event ID. That is what makes the
"dedup on row['ID'] collapsed 78,912 rows to 1" class of bug impossible here.
"""
from datetime import datetime, timezone
from decimal import Decimal

import db
import rollup


def _raw(metric, ts, value, severity="info", is_alert=False):
    """A raw DynamoDB item as _ddb_query_pk returns it (pre-normalise)."""
    iso = ts.replace(tzinfo=timezone.utc).isoformat()
    item = {
        "pk": "prod#pipeA",
        "sk": "%s#uuid" % iso,
        "occurred_at": iso,
        "metric_name": metric,
        "severity": severity,
        "is_alert": is_alert,
    }
    if value is not None:
        item["metric_value"] = Decimal(str(value))
    return item


class FakeTable:
    """Returns programmed raw items for a pk over an sk range (honours bounds)."""
    def __init__(self, items):
        self.items = items

    def query(self, **kwargs):
        cond = kwargs["KeyConditionExpression"]
        pk = cond.term("pk")[2]
        _, _, lo, hi = cond.term("sk")
        out = [i for i in self.items
               if i["pk"] == pk and lo <= i["sk"] <= hi]
        return {"Items": out}


H = datetime(2026, 7, 17, 12, 0, 0)      # a whole hour
H1 = datetime(2026, 7, 17, 13, 0, 0)


def _run(items, start=datetime(2026, 7, 17, 0, 0, 0),
         end=datetime(2026, 7, 18, 0, 0, 0)):
    return rollup.recompute_pipeline_span(db, FakeTable(items), "prod", "pipeA",
                                          start, end)


# ---------------------------------------------------------------------------
# aggregation
# ---------------------------------------------------------------------------
def test_one_item_per_metric_hour():
    items = [_raw("ingest.x.rows", H.replace(minute=m), 100) for m in (0, 15, 59)]
    out = _run(items)
    assert len(out) == 1
    assert out[0]["n"] == 3
    assert out[0]["pk"] == "rollup#prod#pipeA"
    assert out[0]["sk"] == "%s#ingest.x.rows" % H.replace(tzinfo=timezone.utc).isoformat()


def test_mean_sum_and_last():
    items = [_raw("m.rows", H.replace(minute=0), 10),
             _raw("m.rows", H.replace(minute=30), 20),
             _raw("m.rows", H.replace(minute=59), 30)]
    out = _run(items)[0]
    assert out["sum_val"] == Decimal("60")
    assert out["mean_val"] == Decimal("20")
    assert out["last_val"] == Decimal("30")           # last by event time
    assert out["last_ts"].startswith("2026-07-17T12:59")


def test_separate_hours_are_separate_items():
    out = _run([_raw("m.rows", H, 1), _raw("m.rows", H1, 2)])
    assert len(out) == 2
    assert {i["hour"][:13] for i in out} == {"2026-07-17T12", "2026-07-17T13"}


def test_worst_sev_and_any_alert():
    items = [_raw("m.rows", H.replace(minute=0), 1, severity="info"),
             _raw("m.rows", H.replace(minute=1), 1, severity="warning", is_alert=True),
             _raw("m.rows", H.replace(minute=2), 1, severity="info")]
    out = _run(items)[0]
    assert out["worst_sev"] == "warning"
    assert out["any_alert"] is True


def test_metric_hour_with_no_numeric_value_omits_aggregates():
    out = _run([_raw("m.rows", H, None)])[0]
    assert out["n"] == 1
    assert "mean_val" not in out and "sum_val" not in out and "last_val" not in out


def test_ttl_counts_from_the_hour():
    out = _run([_raw("m.rows", H, 1)])[0]
    expected = int(H.replace(tzinfo=timezone.utc).timestamp()) + rollup.ROLLUP_TTL_DAYS * 86400
    assert out["ttl"] == expected


# ---------------------------------------------------------------------------
# idempotency — the whole point
# ---------------------------------------------------------------------------
def test_recompute_is_byte_identical():
    items = [_raw("m.rows", H.replace(minute=m), 10 + m) for m in (0, 20, 40)]
    a = _run(items)
    b = _run(items)
    assert a == b


def test_key_is_stable_without_any_event_id():
    # Raw items carry no stable ID; the rollup key must not depend on one.
    items = [_raw("m.rows", H, 5), _raw("m.rows", H, 7)]  # same metric-hour
    out = _run(items)
    assert len(out) == 1                                  # collapses to ONE item
    assert out[0]["n"] == 2
    # re-running yields the same single key, never a duplicate
    assert {i["sk"] for i in _run(items)} == {out[0]["sk"]}


def test_distinct_metrics_never_collapse_together():
    items = [_raw("a.rows", H, 1), _raw("b.rows", H, 2), _raw("c.rows", H, 3)]
    out = _run(items)
    assert len({i["sk"] for i in out}) == 3
