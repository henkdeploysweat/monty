"""
monty/app.py
------------
Two dashboards over the Monty events table.

    /            -> pipeline timeline (last 24h)
    /anomalies   -> anomaly detection (7d baseline)
    /api/*       -> same data as JSON, if you want to poll from JS

Drop these routes into your existing Flask app, or run this file directly.
Local dev:  MONTY_SOURCE=csv  MONTY_CSV=/path/to/sample.csv  flask --app app run
Prod:       set SNOWFLAKE_* env vars and MONTY_TABLE (see db.py).
"""
import math
import os
import time
from pathlib import Path


def _load_dotenv():
    """Load KEY=VALUE pairs from the .env sitting next to this file into the
    process environment, so the app behaves identically however it is launched
    (VS Code run button, bare `python3 app.py`, or a shell that sourced .env).

    This file is AUTHORITATIVE: its values OVERRIDE whatever is already in the
    environment. That is deliberate — a stale value left in the shell (e.g. an
    old `export SNOWFLAKE_WAREHOUSE=MONITORING_WH` from a previous `.env`) must
    never shadow the current config, which is exactly what caused recurring
    `000606 No active warehouse` errors. Lines may start with `export ` and
    values may be quoted."""
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export "):]
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ[key] = value  # .env is the source of truth; it wins


_load_dotenv()

from flask import Flask, render_template, jsonify, request
from datetime import datetime, timedelta

import db
from transform import (build_timeline_context, build_timeline_range_context,
                       build_anomaly_context)
from formatting import prettify as _prettify

app = Flask(__name__)
# `pretty` cleans a raw pipeline/family name for display (Title Case, no _/-).
# Group labels are already display-ready, so the template never filters those.
app.jinja_env.filters["pretty"] = _prettify

# detector knobs — tune here or override per-request via query string
DEFAULTS = dict(window_hours=24, baseline_days=7, z_threshold=3.5, min_pct=10.0,
                min_points=8, row_limit=500)

# detector knobs the anomalies page exposes: (lo, hi, step) for the UI sliders.
# Bounds also clamp the query string — a bad ?z= must not 500 or make the
# baseline window pull 10 years of events.
DETECTOR_LIMITS = {
    "z":          (0.5, 10.0, 0.1),
    "min_pct":    (0.0, 200.0, 1.0),
    "days":       (1, 30, 1),
    "min_points": (2, 50, 1),
    "row_limit":  (0, 100000, 100),
}


def _num_arg(request, name, default, cast=float):
    """A numeric query param, clamped to its declared bounds. Bad input falls
    back to the default rather than raising (the old float() would 500)."""
    lo, hi, _ = DETECTOR_LIMITS[name]
    try:
        val = cast(request.args.get(name, default))
    except (TypeError, ValueError):
        return default
    return max(lo, min(hi, val))


ENVIRONMENTS = ("prod", "dev")
# selectable timeline spans (hours). One grid column per hour, so keep these
# bounded — 168h (7d) is already 168 columns.
WINDOW_CHOICES = (6, 12, 24, 48, 72, 168)
# Where the NOW line sits across the live timeline. 0.75 = three-quarters in,
# so the panel is mostly recent history with a short future headroom.
NOW_POSITION = 0.75


def _window_arg(request):
    """Timeline span in hours from ?hours=. Clamped to a known choice so a bad
    value can't blow up the grid (one column per hour) or the fetch window."""
    raw = request.args.get("hours")
    try:
        hours = int(raw)
    except (TypeError, ValueError):
        return DEFAULTS["window_hours"]
    return hours if hours in WINDOW_CHOICES else DEFAULTS["window_hours"]


def _env_arg(request):
    """Validated environment from ?env=. `env` selects the S3 bucket AND the AWS
    profile, so a typo ('dev.') must not silently fall through to a wrong bucket
    or the default credential chain — clamp to a known value instead."""
    raw = (request.args.get("env") or "prod").strip().lower()
    return raw if raw in ENVIRONMENTS else "prod"


def _is_csv():
    """True only in local CSV-dev mode. Gates the future-dated-sample anchor
    snap so it never fires against live Snowflake data."""
    return os.environ.get("MONTY_SOURCE", "snowflake").lower() == "csv"


def _fetch_credits(start, end):
    """Warehouse credit rows for [start, end) plus the all-time peak benchmark;
    never fatal — a missing grant on ACCOUNT_USAGE just yields an empty chart
    with a note. Returns (rows, peaks, error).

    Skipped entirely while the credits chart is paused (db.ENABLE_CREDITS),
    so a page load makes zero ACCOUNT_USAGE queries."""
    if not db.ENABLE_CREDITS:
        return [], None, None
    try:
        rows = db.fetch_warehouse_credits(start, end)
        peaks = db.fetch_warehouse_credit_peaks()
        return rows, peaks, None
    except Exception as exc:
        return [], None, str(exc)


def _source_warnings():
    """Human-readable warnings when a data source dropped out mid-request.
    `both` mode is resilient by design; that must never mean invisible."""
    return [
        {"source": src,
         "lost": db.SOURCE_PROVIDES.get(src, "some rows"),
         "error": msg}
        for src, msg in db.LAST_SOURCE_ERRORS.items()
    ]


def _range_args(request):
    """Return (from_date, to_date) as datetimes if both present, else (None, None)."""
    f = request.args.get("from")
    t = request.args.get("to")
    if f and t:
        start = datetime.strptime(f, "%Y-%m-%d")
        end = datetime.strptime(t, "%Y-%m-%d") + timedelta(days=1)  # inclusive of 'to'
        if end > start:
            return start, end
    return None, None


def _timeline_ctx(env, day=None, rng=(None, None), tz="sydney", hours=None):
    r_start, r_end = rng
    if r_start and r_end:
        # range mode: pull the whole span (plus a small margin) and aggregate
        span_days = (r_end - r_start).days + 1
        rows = db.fetch_events(lookback_days=span_days + 1, env=env, now=r_end)
        credit_rows, credit_peaks, credit_err = _fetch_credits(r_start, r_end)
        ctx = build_timeline_range_context(rows, r_start, r_end, env=env,
                                           credit_rows=credit_rows,
                                           credit_peaks=credit_peaks, tzname=tz)
        ctx["warehouse_name"] = db.MONTY_WAREHOUSE
        ctx["credit_error"] = credit_err
        ctx["credits_enabled"] = db.ENABLE_CREDITS
        ctx["source_warnings"] = _source_warnings()
        ctx["is_range"] = True
        ctx["is_live"] = False
        ctx["range_from"] = r_start.strftime("%Y-%m-%d")
        ctx["range_to"] = (r_end - timedelta(days=1)).strftime("%Y-%m-%d")
        ctx["day"] = ctx["range_from"]
        ctx["prev_day"] = (r_start - timedelta(days=1)).strftime("%Y-%m-%d")
        ctx["next_day"] = (r_start + timedelta(days=1)).strftime("%Y-%m-%d")
        ctx["window_choices"] = WINDOW_CHOICES
        ctx["window_hours"] = hours or DEFAULTS["window_hours"]
        return ctx

    live_now = datetime.utcnow()
    if day:
        anchor = datetime.strptime(day, "%Y-%m-%d") + timedelta(days=1)
    else:
        anchor = live_now

    W = hours or DEFAULTS["window_hours"]
    # In CSV-dev live view, let the (future-dated) sample anchor on its own
    # newest row (now=None) so it isn't clamped out; live/Snowflake mode keeps
    # the real-UTC anchor so the range predicate is real.
    fetch_now = None if (_is_csv() and not day) else anchor
    # Pull enough history to cover the window itself plus 7d of cadence context.
    # Only the window needs full rows: the 7d tail feeds cadence + last-seen
    # only, so `detail_days` lets a source fetch it lean (see db.fetch_events).
    # Rendering 24h used to drag back 8 days of payloads for nothing.
    window_days = math.ceil(W / 24)
    # MONTY_TIMELINE_GRAIN=hour reads the pre-aggregated hourly rollup instead of
    # raw events — ≤24 marks/lane, far fewer rows. Default "event" keeps the raw
    # path, so this is opt-in and instantly reversible.
    _grain = os.environ.get("MONTY_TIMELINE_GRAIN", "event").lower()
    _hourly = _grain == "hour"
    _t_start = time.perf_counter()
    rows = db.fetch_events(lookback_days=window_days + 7, env=env, now=fetch_now,
                           detail_days=window_days,
                           grain="hour" if _hourly else "event",
                           collapse_metrics=_hourly)
    _t_fetch = time.perf_counter() - _t_start
    # CSV-dev-only: snap the window to the sample's newest row for a sensible
    # demo. In live/Snowflake mode this must NOT run — OCCURRED_AT is real UTC
    # and the anchor must stay at current UTC, or a future-looking row would
    # shove the NOW line off the right edge.
    if _is_csv() and not day and rows:
        latest = max(r["OCCURRED_AT"] for r in rows)
        if live_now < latest:
            anchor = live_now = latest

    # Credit window must mirror the visible timeline window EXACTLY, or the
    # credits chart drifts out of alignment with the lanes and the NOW line.
    if day is None:
        c_start = anchor - timedelta(hours=W * NOW_POSITION)
        c_end = anchor + timedelta(hours=W * (1 - NOW_POSITION))
    else:
        c_start, c_end = anchor - timedelta(hours=W), anchor
    credit_rows, credit_peaks, credit_err = _fetch_credits(c_start, c_end)

    # Pipelines that ran at some point in the retention horizon but emitted
    # nothing in this window still get a (stale) lane — a dead pipeline must
    # not silently vanish. Never fatal: no history just means no ghost lanes.
    try:
        last_seen_all = db.fetch_pipeline_last_seen(env=env, now=anchor)
    except Exception as exc:
        app.logger.warning("last_seen lookup failed, no ghost lanes: %s", exc)
        last_seen_all = {}

    # live view (no specific day) centres NOW in the middle of the page; an
    # archived day keeps the trailing window ending at that day.
    _t_build = time.perf_counter()
    ctx = build_timeline_context(rows, anchor,
                                 window_hours=W,
                                 env=env, live_now=live_now,
                                 center=(day is None),
                                 now_pos=NOW_POSITION,
                                 credit_rows=credit_rows,
                                 credit_peaks=credit_peaks,
                                 last_seen_all=last_seen_all,
                                 retention_weeks=db.PIPELINE_RETENTION_WEEKS,
                                 tzname=tz,
                                 bucket_to_hour=_hourly)
    _transform_s = time.perf_counter() - _t_build
    _total_s = time.perf_counter() - _t_start
    # One greppable line per load: fetch (DynamoDB/Snowflake) vs transform
    # (Python) vs total. The gap (credits + last_seen) is total - fetch - transform.
    app.logger.info("[timing] timeline env=%s window=%dh grain=%s  fetch=%.1fs  "
                    "transform=%.1fs  total=%.1fs  rows=%d",
                    env, W, _grain, _t_fetch, _transform_s, _total_s, len(rows))
    ctx["load_s"] = round(_total_s, 1)
    ctx["load_fetch_s"] = round(_t_fetch, 1)
    ctx["load_transform_s"] = round(_transform_s, 1)
    ctx["warehouse_name"] = db.MONTY_WAREHOUSE
    ctx["credit_error"] = credit_err
    ctx["credits_enabled"] = db.ENABLE_CREDITS
    ctx["source_warnings"] = _source_warnings()
    ctx["mode"] = "trailing"
    ctx["is_range"] = False
    ctx["range_from"] = ctx["day"]
    ctx["range_to"] = ctx["day"]
    ctx["window_choices"] = WINDOW_CHOICES
    return ctx


def _picker_meta(day=None, is_range=False, r_start=None, r_end=None):
    """Shared header-picker metadata for both dashboards."""
    m = {"is_range": is_range}
    if is_range:
        m["range_from"] = r_start.strftime("%Y-%m-%d")
        m["range_to"] = (r_end - timedelta(days=1)).strftime("%Y-%m-%d")
    return m


def _anomaly_ctx(env, z, min_pct, baseline_days, day=None, rng=(None, None),
                 tz="sydney", min_points=8, row_limit=500, filters=None):
    r_start, r_end = rng
    live_now = datetime.utcnow()
    f = filters or {}
    fkw = dict(group_filter=f.get("group", ""),
               family_filter=f.get("family", ""),
               pipeline_filter=f.get("pipeline", ""))

    _agrain = "hour" if os.environ.get("MONTY_ANOMALY_GRAIN", "").lower() == "hour" else "event"
    if r_start and r_end:
        rows = db.fetch_events(lookback_days=baseline_days + (r_end - r_start).days + 1,
                               env=env, now=r_end, grain=_agrain, collapse_metrics=False)
        ctx = build_anomaly_context(rows, r_end, baseline_days=baseline_days,
                                    env=env, z_threshold=z, min_pct=min_pct,
                                    min_points=min_points, row_limit=row_limit,
                                    agg_start=r_start, agg_end=r_end, tzname=tz, **fkw)
        ctx.update(_picker_meta(is_range=True, r_start=r_start, r_end=r_end))
        ctx["is_live"] = False
        ctx["day"] = r_start.strftime("%Y-%m-%d")
        ctx["prev_day"] = (r_start - timedelta(days=1)).strftime("%Y-%m-%d")
        ctx["next_day"] = (r_start + timedelta(days=1)).strftime("%Y-%m-%d")
        return ctx

    if day:
        anchor = datetime.strptime(day, "%Y-%m-%d") + timedelta(days=1)
        viewed = datetime.strptime(day, "%Y-%m-%d")
    else:
        anchor = viewed = live_now

    fetch_now = None if (_is_csv() and not day) else anchor
    # _agrain (MONTY_ANOMALY_GRAIN) set above: "hour" reads the hourly rollup
    # instead of raw events, so the detector scores on hourly means.
    _t_start = time.perf_counter()
    rows = db.fetch_events(lookback_days=baseline_days, env=env, now=fetch_now,
                           grain=_agrain, collapse_metrics=False)
    _t_fetch = time.perf_counter() - _t_start
    # CSV-dev-only snap to the (future-dated) sample's newest row; never in
    # live mode — see the matching guard in _timeline_ctx.
    if _is_csv() and not day and rows:
        latest = max(r["OCCURRED_AT"] for r in rows)
        if anchor < latest:
            anchor = viewed = latest

    _t_build = time.perf_counter()
    ctx = build_anomaly_context(rows, anchor, baseline_days=baseline_days,
                                env=env, z_threshold=z, min_pct=min_pct,
                                min_points=min_points, row_limit=row_limit,
                                tzname=tz, **fkw)
    _transform_s = time.perf_counter() - _t_build
    _total_s = time.perf_counter() - _t_start
    app.logger.info("[timing] anomaly  env=%s baseline=%dd grain=%s  fetch=%.1fs  "
                    "transform=%.1fs  total=%.1fs  rows=%d",
                    env, baseline_days, _agrain, _t_fetch, _transform_s,
                    _total_s, len(rows))
    ctx["load_s"] = round(_total_s, 1)
    ctx["load_fetch_s"] = round(_t_fetch, 1)
    ctx["load_transform_s"] = round(_transform_s, 1)
    ctx["is_live"] = day is None
    ctx["is_range"] = False
    ctx["day"] = viewed.strftime("%Y-%m-%d")
    ctx["prev_day"] = (viewed - timedelta(days=1)).strftime("%Y-%m-%d")
    ctx["next_day"] = (viewed + timedelta(days=1)).strftime("%Y-%m-%d")
    return ctx


@app.route("/")
def timeline():
    env = _env_arg(request)
    day = request.args.get("day")
    tz = request.args.get("tz", "sydney")
    return render_template("timeline.html",
                           **_timeline_ctx(env, day, _range_args(request), tz,
                                           hours=_window_arg(request)))


@app.route("/anomalies")
def anomalies():
    env = _env_arg(request)
    z = _num_arg(request, "z", DEFAULTS["z_threshold"])
    min_pct = _num_arg(request, "min_pct", DEFAULTS["min_pct"])
    days = _num_arg(request, "days", DEFAULTS["baseline_days"], cast=int)
    min_points = _num_arg(request, "min_points", DEFAULTS["min_points"], cast=int)
    row_limit = _num_arg(request, "row_limit", DEFAULTS["row_limit"], cast=int)
    day = request.args.get("day")
    tz = request.args.get("tz", "sydney")
    filters = {k: request.args.get(k, "").strip()
               for k in ("group", "family", "pipeline")}
    ctx = _anomaly_ctx(env, z, min_pct, days, day, _range_args(request), tz,
                       min_points=min_points, row_limit=row_limit, filters=filters)
    ctx["detector_limits"] = DETECTOR_LIMITS
    ctx["defaults"] = DEFAULTS
    # the events table, so the page can suggest a runnable Snowflake query
    ctx["monty_table"] = db.MONTY_TABLE
    return render_template("anomaly.html", **ctx)




@app.route("/api/timeline")
def api_timeline():
    ctx = _timeline_ctx(_env_arg(request),
                        request.args.get("day"), _range_args(request),
                        hours=_window_arg(request))
    for k, v in list(ctx.items()):
        if isinstance(v, datetime):
            ctx[k] = v.isoformat()
    return jsonify(ctx)


@app.route("/api/anomalies")
def api_anomalies():
    ctx = _anomaly_ctx(_env_arg(request),
                       float(request.args.get("z", DEFAULTS["z_threshold"])),
                       float(request.args.get("min_pct", DEFAULTS["min_pct"])),
                       int(request.args.get("days", DEFAULTS["baseline_days"])),
                       request.args.get("day"), _range_args(request),
                       row_limit=int(request.args.get("row_limit", DEFAULTS["row_limit"])))
    # strip the raw series (large) and non-serialisable bits for the API
    for key in ("focus", "chart", "charts"):
        ctx.pop(key, None)
    # `series` (raw points) and `curve` (dense render series w/ nested datetimes)
    # are chart-only + non-trivially serialisable — drop them from the API.
    _skip = ("series", "curve")
    out = []
    for a in ctx["anomalies"]:
        out.append({k: (v.isoformat() if isinstance(v, datetime) else v)
                    for k, v in a.items() if k not in _skip})
    ctx["anomalies"] = out
    ctx["table"] = [{k: (v.isoformat() if isinstance(v, datetime) else v)
                     for k, v in a.items() if k not in _skip} for a in ctx["table"]]
    ctx["drift"] = [{k: (v.isoformat() if isinstance(v, datetime) else v)
                     for k, v in a.items() if k not in _skip} for a in ctx["drift"]]
    for k, v in list(ctx.items()):
        if isinstance(v, datetime):
            ctx[k] = v.isoformat()
    return jsonify(ctx)


if __name__ == "__main__":
    app.run(debug=True, port=5011)
