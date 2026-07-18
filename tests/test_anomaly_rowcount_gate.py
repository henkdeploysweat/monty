"""Only row-load metrics are scored for anomalies (*.rows + dbt_model_run).

Everything else — watermarks, max_date, batch totals, fetch counts — is listed
on the anomaly page but never flagged as an anomaly. A watermark advancing is
not an anomaly; a row count dropping is.
"""
from datetime import datetime, timedelta

import transform
from transform import _is_rowcount_metric, build_anomaly_context


# ---------------------------------------------------------------------------
# the gate predicate
# ---------------------------------------------------------------------------
def test_dot_rows_is_scored():
    assert _is_rowcount_metric("ingest.postgresql.users.rows")
    assert _is_rowcount_metric("ingest.postgresql.workout_groups.rows")


def test_dbt_model_run_is_scored():
    assert _is_rowcount_metric("dbt_model_run")


def test_strict_excludes_total_rows_and_rows_fetched():
    # Product call: strict '.rows' suffix only, not the batch/fetch variants.
    assert not _is_rowcount_metric("ingest.postgresql.batch.total_rows")
    assert not _is_rowcount_metric("ingest.plausible.visits_day.rows_fetched")


def test_watermarks_and_dates_are_not_scored():
    assert not _is_rowcount_metric("ingest.postgresql.users.max_date")
    assert not _is_rowcount_metric("ingest.postgresql.users.watermark_used")


def test_none_and_empty_are_safe():
    assert not _is_rowcount_metric(None)
    assert not _is_rowcount_metric("")


# ---------------------------------------------------------------------------
# end-to-end through build_anomaly_context
# ---------------------------------------------------------------------------
def _events(metric, pipeline="ai-ingest-postgressql-ai-ingest", n=20, value=1000.0):
    """n hourly events for one metric, enough history to clear min_points."""
    base = datetime(2026, 7, 1, 0, 0, 0)
    return [{
        "PIPELINE_NAME": pipeline, "METRIC_NAME": metric, "METRIC_VALUE": value,
        "SEVERITY": "info", "IS_ALERT": False, "ENVIRONMENT": "prod",
        "OCCURRED_AT": base + timedelta(hours=i), "PAYLOAD": None,
    } for i in range(n)]


def test_only_rowcount_and_dbt_are_scored_end_to_end():
    now = datetime(2026, 7, 2, 0, 0, 0)
    # `widget_count` is a neutral non-row metric: not .rows, not dbt, not a
    # freshness metric, and not on the dashboard hide-list (unlike total_rows,
    # which is hidden by the *total_row* rule and never reaches the table).
    rows = (_events("ingest.postgresql.users.rows")
            + _events("dbt_model_run", pipeline="some_model")
            + _events("widget_count"))
    ctx = build_anomaly_context(rows, now, baseline_days=7, env="prod",
                                row_limit=0)  # disable the volume gate to isolate

    scored = {r["metric_name"] for r in ctx["table"] if r.get("scored")}
    assert scored == {"ingest.postgresql.users.rows", "dbt_model_run"}

    # the non-row metric is present but not scored — nothing silently vanishes
    listed = {r["metric_name"] for r in ctx["table"]}
    assert "widget_count" in listed


def test_non_rowcount_gets_the_right_reason():
    now = datetime(2026, 7, 2, 0, 0, 0)
    rows = _events("widget_count")
    ctx = build_anomaly_context(rows, now, baseline_days=7, env="prod", row_limit=0)
    row = next(r for r in ctx["table"] if r["metric_name"] == "widget_count")
    assert not row["scored"]
    assert "row-count" in row["reason"]
    assert ctx["skipped_non_rowcount"] >= 1
