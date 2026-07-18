"""
monty/transform.py
------------------
Turns raw Monty event rows (list of dicts) into template-ready context
for the two dashboards. Deliberately data-source agnostic: feed it rows
from a Snowflake cursor in prod, or from the sample CSV in tests.

Each row is expected to have (upper- or lower-case keys both work):
    ID, PIPELINE_NAME, METRIC_NAME, METRIC_VALUE, SEVERITY, RUN_ID,
    PAYLOAD, OCCURRED_AT (datetime), IS_ALERT, SENT_TO_SLACK, ENVIRONMENT
"""
from __future__ import annotations
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median, pstdev, mean
import logging
import math

# pipeline name/family formatting lives in its own module
from formatting import (pipeline_family as _pipeline_family, kind as _kind,
                        pipeline_name as _pipeline_name, remove_from_dashboard,
                        pipeline_group as _pipeline_group)

# Cyclical baseline (STL seasonal decomposition). numpy/pandas already ship
# with the dashboard; statsmodels is the only heavier add. If ANY of them is
# missing the anomaly detector silently falls back to the flat median/MAD
# baseline — a broken/absent install degrades gracefully instead of 500-ing
# the anomalies page.
try:
    import numpy as _np
    import pandas as _pd
    from statsmodels.tsa.seasonal import STL as _STL
    _SEASONAL_OK = True
except Exception as _seasonal_exc:          # pragma: no cover
    _np = _pd = _STL = None
    _SEASONAL_OK = False
    logging.getLogger("monty.transform").info(
        "seasonal detector disabled (%s); using flat baseline", _seasonal_exc)

# --- baseline / seasonal-detector knobs -------------------------------------
# Every scored metric with >= TREND_MIN_POINTS gets a SMOOTH baseline (a trend
# line that hugs the data); the wavy SEASONAL treatment is layered on ONLY when
# the data is dense enough over enough full cycles to trust a period — otherwise
# a thin series would over-fit a pretty-but-wrong cycle (the 29-point trap).
TREND_MIN_POINTS = 16        # >= this -> smooth trend baseline (else flat median)
SEASONAL_MIN_POINTS = 42     # >= this raw points before a cycle is even considered
SEASONAL_MIN_PERIODS = 3     # need >= this many full cycles in the data
SEASONAL_MIN_PTS_PER_CYCLE = 6   # and >= this many RAW points per cycle (density)
SEASONAL_ACF_MIN = 0.35      # min autocorrelation for a period to be accepted
SEASONAL_SIGMA_FLOOR = 0.5   # per-phase σ can't drop below this × overall σ
SEASONAL_MAX_GRID = 720      # cap resampled points per metric (bounds STL cost)
CURVE_RENDER_POINTS = 120    # dense grid points kept for the smooth band

# --- chart confidence bands -------------------------------------------------
# Nested "fan" bands drawn around the expected value on the anomaly chart, for
# graduated context (how far off is this point?). Each entry is
# (label, sigma-multiplier) — the multiplier is the two-sided-normal z for that
# confidence level. Edit/extend this list to change the bands; the detection
# threshold (z_threshold) is always drawn on top of these as the ALERT boundary.
#   80% -> 1.282   90% -> 1.645   95% -> 1.960   99% -> 2.576
CONFIDENCE_BANDS = [("90%", 1.645), ("95%", 1.960)]

try:
    from zoneinfo import ZoneInfo
except ImportError:                     # pragma: no cover
    ZoneInfo = None

# display timezones offered by the toggle (DB stores UTC)
TZ_MAP = {"utc": "America/Los_Angeles", "sydney": "Australia/Sydney", "adelaide": "Australia/Adelaide"}
# TZ_MAP = { "la": "America/Los_Angeles", "sydney": "Australia/Sydney", "adelaide": "Australia/Adelaide"}

def _tzinfo(tzname):
    z = TZ_MAP.get((tzname or "utc").lower())
    return ZoneInfo(z) if (z and ZoneInfo) else None


def _local(dt, tz):
    """Reinterpret a naive-UTC datetime in the display tz (returns aware dt);
    UTC (tz=None) passes through unchanged."""
    if dt is None:
        return None
    if tz is None:
        return dt
    return dt.replace(tzinfo=timezone.utc).astimezone(tz)


def _tz_abbr(tz, ref):
    """DST-correct abbreviation (AEST/AEDT/ACST/ACDT/UTC) for the given instant."""
    if tz is None or ref is None:
        return "UTC"
    return _local(ref, tz).tzname() or "LOCAL"


def _payload_snippet(raw, limit=240):
    """A short, single-line snippet of a payload for the detail drawer."""
    if not raw:
        return ""
    s = str(raw).replace("\n", " ").replace("\r", " ")
    s = " ".join(s.split())          # collapse whitespace
    return s[:limit] + ("…" if len(s) > limit else "")


def _event_detail(row, sev, is_alert, LT):
    """Compact per-event record for the click-to-expand drawer.
    Payload snippet only for alerts/failures (where the context matters)."""
    val = _g(row, "METRIC_VALUE")
    d = {
        "m": _g(row, "METRIC_NAME"),
        "s": sev,
        "v": fmt(val) if isinstance(val, (int, float)) else None,
        "a": is_alert,
        "t": LT(_as_dt(_g(row, "OCCURRED_AT")), "%H:%M:%S"),
    }
    if is_alert or sev in ("warning", "error", "critical"):
        d["p"] = _payload_snippet(_g(row, "PAYLOAD"))
    return d


# Dashboard-only, non-problem states used by the Braze CDI feed:
#   'noload'  — sync completed with zero rows (healthy no-op) -> light green
#   'running' — sync still in flight, not yet terminal        -> neutral blue
# Both rank above plain 'info' so a mixed bucket surfaces them, but stay below
# 'warning' — neither is ever a problem. Their ranks must be > the run-bucket
# rank floor of 1 (else worst-wins would never pick them over the default).
SEV_RANK = {"info": 1, "noload": 1.5, "running": 1.7,
            "warning": 2, "error": 3, "critical": 4}
RANK_SEV = {v: k for k, v in SEV_RANK.items()}
# severity -> css class used by the timeline bars. 'noload' -> light green;
# 'running' reuses the blue 'info' bar class (in-progress, non-alerting).
SEV_CLASS = {"info": "success", "noload": "noload", "running": "info",
             "warning": "warn", "error": "fail", "critical": "fail"}


def _g(row, key, default=None):
    """Case-insensitive row getter."""
    if key in row:
        return row[key]
    if key.upper() in row:
        return row[key.upper()]
    if key.lower() in row:
        return row[key.lower()]
    return default


def _as_dt(v):
    if isinstance(v, datetime):
        return v
    if v is None or v == "":
        return None
    return datetime.fromisoformat(str(v).replace("Z", "+00:00").split("+")[0])


def _pick_pay(pay_by_pipe, pipeline, payload, metric):
    """Store one representative "<payload>||<metric>" per pipeline, PREFERRING a
    payload that carries the dbt model identity (`dbt_model_run` rows, or the
    `dbt_run_failures` collector). Without this preference the last row wins,
    and a pipeline whose final row isn't the dbt one loses the unique_id — so
    its family/name silently fall back to the raw pipeline name.

    An entry holding NO payload is also always upgraded to one that has a
    payload. Sources may legitimately hand us payload-less rows — the DynamoDB
    leg projects `payload` off its trailing lane because it is ~3x of all other
    bytes (db.py) and grafts a representative back onto one row per pipeline.
    That row is rarely the first one seen, so without this an entry would pin to
    the empty payload and the identity would be lost anyway."""
    current = pay_by_pipe.get(pipeline)
    has_payload = bool(payload)
    current_empty = current is not None and not current.split("||")[0]
    if (current is None
            or (current_empty and has_payload)
            or metric == "dbt_model_run" or pipeline == "dbt_run_failures"):
        pay_by_pipe[pipeline] = str(payload or "") + "||" + str(metric or "")


# ----------------------------------------------------------------------------
# TIMELINE DASHBOARD
# ----------------------------------------------------------------------------
def build_timeline_context(rows, now: datetime, window_hours: int = 24,
                           env: str = "prod", stale_factor: float = 2.0,
                           stale_floor_s: int = 2 * 3600,
                           session_gap_s: int = 30 * 60,
                           unknown_stale_s: int = 48 * 3600,
                           last_seen_all: dict | None = None,
                           retention_weeks: int = 14,
                           live_now: datetime | None = None,
                           center: bool = False,
                           now_pos: float = 0.75,
                           credit_rows=None, credit_peaks=None,
                           tzname: str = "utc",
                           bucket_to_hour: bool = False):
    """
    Returns a dict consumed by templates/timeline.html.

    `now` is the window anchor. By default the window trails it
    ([now - window_hours, now]) so the NOW line sits at the right edge — used
    for archived days. When `center=True` (the live view), the window is
    re-centred on `now` ([now - window_hours/2, now + window_hours/2]) so the
    NOW line lands in the MIDDLE of the page: the left half is recent history,
    the right half is empty future headroom. Either way the visible span (and
    therefore the column count) stays `window_hours`.

    Pass a past `now` to view a previous day. `live_now` is the real clock; the
    "NOW" line is drawn only when it falls inside the window, and staleness is
    always measured from it (not the window bound), so the future headroom in
    centred mode never makes a fresh pipeline look stale. `tzname` selects the
    DISPLAY timezone (utc | sydney | adelaide); all math stays in UTC.

    A "run" = a cluster of events a pipeline emitted at the same minute
    (one Lambda/dbt invocation fans out many metrics at once). We colour
    each run by its worst severity and mark it if any event alerted.
    """
    tz = _tzinfo(tzname)

    def LT(dt, f):
        return _local(dt, tz).strftime(f)

    if center:
        # Live view: NOW sits `now_pos` of the way across (default 3/4), so most
        # of the panel is recent history and only the tail is future headroom.
        win_start = now - timedelta(hours=window_hours * now_pos)
    else:
        win_start = now - timedelta(hours=window_hours)
    win_end = win_start + timedelta(hours=window_hours)
    span_s = (win_end - win_start).total_seconds()

    # position of the live clock within this window (None = not in view)
    now_left_pct = None
    if live_now is not None and win_start <= live_now <= win_end:
        now_left_pct = round((live_now - win_start).total_seconds() / span_s * 100, 2)

    # Mark/cadence bucket granularity. Default is per-minute; bucket_to_hour
    # snaps to the hour so every lane shows ≤24 marks/day (the hourly-rollup
    # look), applied AFTER names are resolved so dbt identity survives. Cadence
    # then floors at 1h, the accepted hourly consequence.
    if bucket_to_hour:
        def _bkt(t):
            return t.replace(minute=0, second=0, microsecond=0)
    else:
        def _bkt(t):
            return t.replace(second=0, microsecond=0)

    # bucket events -> runs keyed by (pipeline, minute-or-hour)
    runs = defaultdict(lambda: {"rank": 1, "alert": False, "n": 0, "events": []})
    # everything (7d) for cadence + staleness
    last_seen = {}
    all_buckets = defaultdict(set)          # pipeline -> set(minute buckets) for cadence
    counts = defaultdict(lambda: {"events": 0, "warning": 0, "error": 0,
                                  "critical": 0, "noload": 0, "running": 0,
                                  "alerts": 0})
    # one representative "<payload>||<metric>" per pipeline, so the name/family
    # formatters can read the dbt model out of the payload for THIS pipeline
    # (not whatever row happened to be processed last).
    pay_by_pipe = {}

    for r in rows:
        if _g(r, "ENVIRONMENT") != env:
            continue
        ts = _as_dt(_g(r, "OCCURRED_AT"))
        if ts is None:
            continue
        p = _g(r, "PIPELINE_NAME")
        if remove_from_dashboard(p):        # hide-list (formatting.REMOVE_FROM_DASHBOARD)
            continue
        _pick_pay(pay_by_pipe, p, _g(r, "PAYLOAD"), _g(r, "METRIC_NAME"))
        sev = (_g(r, "SEVERITY") or "info").lower()
        is_alert = bool(_g(r, "IS_ALERT"))

        # track last-seen + cadence buckets over full input range
        if p not in last_seen or ts > last_seen[p]:
            last_seen[p] = ts
        all_buckets[p].add(ts.replace(second=0, microsecond=0))

        # An hourly-rollup row is a pre-aggregated bucket, not a single event:
        # its OCCURRED_AT is the hour, and _N is how many raw events it stands
        # for. Count by _N (default 1 for a real event) so a rollup mark shows
        # the true volume, and skip per-event detail — the drawer lazy-loads raw
        # events for a rollup hour instead (see app._run_detail).
        n_events = int(_g(r, "_N", 1) or 1)
        is_rollup = bool(_g(r, "_ROLLUP"))

        # track last-seen + cadence buckets over full input range
        if p not in last_seen or ts > last_seen[p]:
            last_seen[p] = ts
        all_buckets[p].add(_bkt(ts))

        # window-scoped aggregates
        if win_start <= ts < win_end:
            key = (p, _bkt(ts))
            run = runs[key]
            run["rank"] = max(run["rank"], SEV_RANK.get(sev, 1))
            run["alert"] = run["alert"] or is_alert
            run["n"] += n_events
            if not is_rollup and len(run["events"]) < 40:   # cap detail per run
                run["events"].append(_event_detail(r, sev, is_alert, LT))
            c = counts[p]
            c["events"] += n_events
            if sev in c:
                c[sev] += n_events
            if is_alert:
                c["alerts"] += 1

    # Cadence = median gap between run SESSIONS, not between individual run
    # buckets. A pipeline that fires hundreds of metrics over a few minutes
    # (e.g. ai-ingest-kaylalogs: 613 buckets, median gap 0.0h) would otherwise
    # look like it runs continuously, collapsing the stale threshold onto the
    # floor and flagging every once-a-day pipeline as STALE within 2h.
    # Buckets closer together than `session_gap_s` are one session.
    cadence = {}
    prediction = {}
    for p, buckets in all_buckets.items():
        starts = _session_starts(buckets, session_gap_s)
        gaps = [(starts[i] - starts[i - 1]).total_seconds()
                for i in range(1, len(starts))]
        gaps = [g for g in gaps if g > 0]
        cadence[p] = median(gaps) if gaps else None
        prediction[p] = _predict_next(last_seen.get(p), starts)

    # assemble one lane per pipeline that had activity in the window
    lanes = []
    run_detail = {}          # run_id -> full detail for the click drawer
    rid = 0
    for p, c in counts.items():
        lane_runs = []
        for (pp, bucket), run in runs.items():
            if pp != p:
                continue
            left = (bucket - win_start).total_seconds() / span_s * 100
            sev = RANK_SEV[run["rank"]]
            rid += 1
            run_id = "r%d" % rid
            lane_runs.append({
                "id": run_id,
                "left": round(left, 2),
                "sev": sev,
                "cls": SEV_CLASS[sev],
                "alert": run["alert"],
                "n": run["n"],
            })
            run_detail[run_id] = {
                "pipeline": p,
                "time": LT(bucket, "%Y-%m-%d %H:%M"),
                "sev": sev,
                "alert": run["alert"],
                "n": run["n"],
                "events": run["events"],
            }
        lane_runs.sort(key=lambda x: x["left"])

        # staleness is measured from the real clock, never the (possibly
        # future) window bound, so centred-live headroom never fakes staleness.
        ref_now = live_now if live_now is not None else now
        since_last = (ref_now - last_seen[p]).total_seconds() if p in last_seen else None
        cad = cadence.get(p)
        stale, stale_after = _stale_check(since_last, cad, stale_factor,
                                          stale_floor_s, unknown_stale_s)

        # Where the next run is expected to land, as a % across this window.
        # Confident predictions draw a line; jittery ones draw a ±1σ band, so
        # an ad-hoc pipeline never presents a fake-precise ETA. Overdue = the
        # predicted time has already passed (an early warning that fires
        # before the STALE threshold does).
        pred = prediction.get(p)
        pred_ctx = None
        if pred:
            pred_at, pred_cad, jitter, confident = pred
            left = (pred_at - win_start).total_seconds() / span_s * 100
            if -5 <= left <= 105:                 # only if it lands in view
                sigma_pct = jitter * pred_cad / span_s * 100
                pred_ctx = {
                    "left": round(left, 2),
                    "at": LT(pred_at, "%H:%M"),
                    "at_full": LT(pred_at, "%d %b %H:%M"),
                    "cadence": _human_duration(pred_cad),
                    "jitter_pct": round(jitter * 100),
                    "confident": confident,
                    "overdue": pred_at < ref_now,
                    # ±1σ band for the uncertain case, clamped to the panel
                    "band_left": round(max(0.0, left - sigma_pct), 2),
                    "band_width": round(min(100.0, left + sigma_pct)
                                        - max(0.0, left - sigma_pct), 2),
                }

        # RECENCY rule: the pill reflects the MOST RECENT run (the far right of
        # the row), NOT the whole window. A pipeline that failed earlier but is
        # green again now reads OK — the red markers still show the history.
        # "Anything to the right of now is what matters."
        last_sev = lane_runs[-1]["sev"] if lane_runs else None
        if last_sev == "critical":
            status = ("crit", "DOWN")
        elif last_sev == "error":
            status = ("crit", "DEGRADED")
        elif stale:
            status = ("stale", "STALE")
        elif last_sev == "warning":
            status = ("warn", "WARN")
        elif last_sev == "noload":       # latest run synced fine but 0 rows
            status = ("noload", "EMPTY")
        elif last_sev == "running":      # latest run still in flight
            status = ("info", "SYNCING")
        else:
            status = ("ok", "OK")

        lanes.append({
            "name": _pipeline_name(p, pay_by_pipe.get(p, "")),
            "family": _pipeline_family(p, pay_by_pipe.get(p, "")),
            "kind": _kind(p),
            "cadence": _human_cadence(cad),
            "status_cls": status[0],
            "status_label": status[1],
            "runs": lane_runs,
            "predict": pred_ctx,
            "stale": stale,
            "ghost": False,
            "stale_after": _human_duration(stale_after),
            "stale_after_h": round(stale_after / 3600, 1) if stale_after else None,
            "since_last_h": round(since_last / 3600, 1) if since_last else None,
            "last_seen": last_seen.get(p),
            "counts": c,
            "sort_rank": max((SEV_RANK[s] for s in ("critical", "error", "warning")
                              if c[s] > 0), default=(0 if not stale else 1.5)),
        })

    # "Ghost" lanes: pipelines that emitted nothing in this window but ran at
    # some point within the retention horizon. Without these a pipeline that
    # DIED simply vanishes from the timeline — the most dangerous failure mode,
    # since absence reads as "nothing to see". They render as empty STALE rows
    # until `retention_weeks` after their last event, then drop off for good.
    retention_s = retention_weeks * 7 * 86400
    clock = live_now or now               # staleness always measured from the real clock
    for p, seen in (last_seen_all or {}).items():
        if p in counts or seen is None:
            continue
        if remove_from_dashboard(p):
            continue
        quiet_s = (clock - seen).total_seconds()
        if quiet_s < 0 or quiet_s > retention_s:
            continue                      # future row, or older than the horizon
        cad = cadence.get(p)              # usually None (no history in this pull)
        is_stale, stale_after = _stale_check(quiet_s, cad, stale_factor,
                                             stale_floor_s, unknown_stale_s)
        lanes.append({
            "name": _pipeline_name(p, pay_by_pipe.get(p, "")),
            "family": _pipeline_family(p, pay_by_pipe.get(p, "")),
            "kind": _kind(p),
            "cadence": _human_cadence(cad),
            "status_cls": "stale" if is_stale else "ok",
            "status_label": "STALE" if is_stale else "IDLE",
            "runs": [],
            "predict": None,            # no runs in window -> nothing to project
            "stale": is_stale,
            "ghost": True,               # no events in the visible window
            "stale_after": _human_duration(stale_after),
            "stale_after_h": round(stale_after / 3600, 1) if stale_after else None,
            "since_last_h": round(quiet_s / 3600, 1),
            "last_seen": seen,
            "counts": {"events": 0, "warning": 0, "error": 0, "critical": 0, "alerts": 0},
            "sort_rank": 1.5 if is_stale else 0,
        })

    # worst pipelines first, then busiest
    lanes.sort(key=lambda l: (-l["sort_rank"], -l["counts"]["events"]))
    # hide-list: also drop lanes whose FORMATTED name or FAMILY is hidden
    lanes = [l for l in lanes
             if not remove_from_dashboard(l.get("name"))
             and not remove_from_dashboard(l.get("family"))]
    lane_groups = _lane_groups(lanes)
    supergroups = _supergroups(lane_groups)

    # alerts-per-hour strip (24 buckets ending at now)
    hours = []
    for h in range(window_hours):
        b_start = win_start + timedelta(hours=h)
        b_end = b_start + timedelta(hours=1)
        fail = warn = info = 0
        for r in rows:
            if _g(r, "ENVIRONMENT") != env:
                continue
            ts = _as_dt(_g(r, "OCCURRED_AT"))
            if ts is None or not (b_start <= ts < b_end):
                continue
            sev = (_g(r, "SEVERITY") or "info").lower()
            if sev in ("error", "critical"):
                fail += 1
            elif sev == "warning":
                warn += 1
            else:
                info += 1
        hours.append({"hour": LT(b_start, "%H:%M"), "fail": fail,
                      "warn": warn, "info": info})

    maxbar = max((h["fail"] + h["warn"] + h["info"] for h in hours), default=1) or 1

    # summary cards
    fails_by = sorted(((p, c["error"] + c["critical"]) for p, c in counts.items()
                       if c["error"] + c["critical"] > 0), key=lambda x: -x[1])
    warns_by = sorted(((p, c["warning"]) for p, c in counts.items()
                       if c["warning"] > 0), key=lambda x: -x[1])
    stale_lanes = [l for l in lanes if l["stale"]]

    # warehouse credits, bucketed to the same hourly grid as the alerts strip
    credits = _credit_context(credit_rows, win_start, 3600, window_hours,
                              peaks=credit_peaks)

    is_live = now_left_pct is not None
    return {
        **credits,
        "now": _local(now, tz),
        "win_start": _local(win_start, tz),
        "tz_abbr": _tz_abbr(tz, now),
        "tzname": tzname,
        "day": win_start.strftime("%Y-%m-%d"),
        "prev_day": (win_start - timedelta(days=1)).strftime("%Y-%m-%d"),
        "next_day": (win_start + timedelta(days=1)).strftime("%Y-%m-%d"),
        "is_live": is_live,
        "centered": center,
        # how the live window splits around NOW (used by the header/card labels)
        "history_hours": round(window_hours * now_pos) if center else window_hours,
        "future_hours": round(window_hours * (1 - now_pos)) if center else 0,
        "now_left_pct": now_left_pct,
        "env": env,
        "window_hours": window_hours,
        # One column per hour, but only label every `axis_step`-th tick so a
        # multi-day window doesn't render 168 unreadable labels. Windows longer
        # than a day also need the date, not just the clock time.
        "axis": _hour_axis(win_start, window_hours, LT),
        "lanes": lanes,
        "lane_groups": lane_groups,
        "supergroups": supergroups,
        "hours": hours,
        "run_detail": run_detail,
        "maxbar": maxbar,
        "total_fail": sum(n for _, n in fails_by),
        "total_warn": sum(n for _, n in warns_by),
        "fails_by": fails_by[:6],
        "warns_by": warns_by[:6],
        "stale": [(l["name"], l["since_last_h"], l["cadence"],
                   LT(l["last_seen"], "%H:%M") if l["last_seen"] else "—",
                   l["stale_after"])
                  for l in stale_lanes][:6],
    }


def _credit_context(credit_rows, win_start, bucket_s, n_buckets, peaks=None):
    """Bucket hourly warehouse-credit rows onto the timeline's own buckets and
    emit SVG polyline point-strings (one per series) plus summed totals, so the
    template can draw a 3-line chart aligned under the alerts strip.

    `peaks` = all-time hourly peak per series ({'used','compute','cloud'}). The
    y-axis is scaled to that worst-ever load and each peak is emitted as a
    reference-line position, so the current lines read as "how far below the
    worst load we're running". In multi-hour range buckets the hourly peak is
    scaled by the bucket's hour-count for an apples-to-apples ceiling.

    credit_rows: dicts with HOUR (naive UTC), CREDITS_USED/_COMPUTE/_CLOUD.
    Coordinates use a fixed 0..1000 x 0..100 viewBox (y inverted)."""
    buckets = [{"total": 0.0, "compute": 0.0, "cloud": 0.0} for _ in range(n_buckets)]
    for cr in (credit_rows or []):
        hour = _as_dt(_g(cr, "HOUR"))
        if hour is None:
            continue
        idx = int((hour - win_start).total_seconds() // bucket_s)
        if 0 <= idx < n_buckets:
            b = buckets[idx]
            b["total"] += float(_g(cr, "CREDITS_USED") or 0.0)
            b["compute"] += float(_g(cr, "CREDITS_COMPUTE") or 0.0)
            b["cloud"] += float(_g(cr, "CREDITS_CLOUD_SERVICES") or 0.0)

    peaks = peaks or {}
    bucket_hours = bucket_s / 3600.0
    peak_used = float(peaks.get("used") or 0.0)
    peak_compute = float(peaks.get("compute") or 0.0)
    peak_cloud = float(peaks.get("cloud") or 0.0)
    # per-bucket ceilings (hourly peak scaled to the bucket width)
    ref_used = peak_used * bucket_hours
    ref_compute = peak_compute * bucket_hours
    ref_cloud = peak_cloud * bucket_hours

    cmax_window = max([b["total"] for b in buckets], default=0.0)
    # scale the axis to the worst-ever load (or the current window if it somehow
    # exceeds it), so current usage is drawn relative to its historical peak.
    yscale = max(cmax_window, ref_used, ref_compute, ref_cloud)
    W, H, PAD = 1000.0, 100.0, 6.0

    def y_of(value):
        if yscale <= 0:
            return H - PAD
        return round(H - PAD - (value / yscale) * (H - 2 * PAD), 1)

    def points(key):
        if n_buckets == 0 or yscale <= 0:
            return ""
        return " ".join("%.1f,%.1f" % ((i + 0.5) / n_buckets * W, y_of(b[key]))
                        for i, b in enumerate(buckets))

    has_peak = (peak_used + peak_compute + peak_cloud) > 0
    return {
        "has_credits": yscale > 0,
        "credit_max": round(cmax_window, 3),
        "credit_totals": {
            "used": round(sum(b["total"] for b in buckets), 2),
            "compute": round(sum(b["compute"] for b in buckets), 2),
            "cloud": round(sum(b["cloud"] for b in buckets), 2),
        },
        "credit_pts": {"total": points("total"),
                       "compute": points("compute"),
                       "cloud": points("cloud")},
        "credit_ref": {
            "has_peak": has_peak,
            # displayed values are the all-time HOURLY peaks (bucket-agnostic)
            "used": round(peak_used, 3),
            "compute": round(peak_compute, 3),
            "cloud": round(peak_cloud, 3),
            # y positions use the bucket-scaled ceilings
            "used_y": y_of(ref_used),
            "compute_y": y_of(ref_compute),
            "cloud_y": y_of(ref_cloud),
        },
    }


def _pick_bucket(span_s):
    """Choose a bucket size that tiles the span into <= ~40 columns."""
    units = [3600, 3 * 3600, 6 * 3600, 12 * 3600, 86400, 2 * 86400, 7 * 86400]
    for u in units:
        if span_s / u <= 40:
            return u
    return units[-1]


def build_timeline_range_context(rows, start: datetime, end: datetime,
                                 env: str = "prod", credit_rows=None,
                                 credit_peaks=None, tzname: str = "utc"):
    """
    Timeline aggregated over an arbitrary [start, end] range. The span is
    tiled into adaptive buckets (hourly for short ranges, up to weekly for
    long ones); each pipeline lane shows one bar per bucket coloured by the
    worst severity seen in it, with bar opacity scaled by activity volume
    (the "average intensity" over the bucket). Summary totals cover the range.
    `tzname` selects the DISPLAY timezone (utc | sydney | adelaide).
    """
    tz = _tzinfo(tzname)
    span_s = (end - start).total_seconds()
    bucket_s = _pick_bucket(span_s)
    n_buckets = max(1, int(math.ceil(span_s / bucket_s)))
    multiday = bucket_s >= 86400

    def bucket_index(ts):
        return int((ts - start).total_seconds() // bucket_s)

    tz2_LT = lambda dt, f: _local(dt, tz).strftime(f)
    # per pipeline -> per bucket aggregate
    lanes_agg = defaultdict(lambda: defaultdict(
        lambda: {"rank": 1, "alert": False, "n": 0, "warning": 0, "error": 0,
                 "critical": 0, "noload": 0, "running": 0, "info": 0,
                 "events": []}))
    counts = defaultdict(lambda: {"events": 0, "warning": 0, "error": 0,
                                  "critical": 0, "noload": 0, "running": 0,
                                  "alerts": 0})
    per_bucket = [{"fail": 0, "warn": 0, "info": 0} for _ in range(n_buckets)]
    pay_by_pipe = {}          # pipeline -> representative "<payload>||<metric>"

    for r in rows:
        if _g(r, "ENVIRONMENT") != env:
            continue
        ts = _as_dt(_g(r, "OCCURRED_AT"))
        if ts is None or not (start <= ts < end):
            continue
        bi = bucket_index(ts)
        if bi < 0 or bi >= n_buckets:
            continue
        p = _g(r, "PIPELINE_NAME")
        if remove_from_dashboard(p):        # hide-list (formatting.REMOVE_FROM_DASHBOARD)
            continue
        _pick_pay(pay_by_pipe, p, _g(r, "PAYLOAD"), _g(r, "METRIC_NAME"))
        sev = (_g(r, "SEVERITY") or "info").lower()
        is_alert = bool(_g(r, "IS_ALERT"))
        cell = lanes_agg[p][bi]
        cell["rank"] = max(cell["rank"], SEV_RANK.get(sev, 1))
        cell["alert"] = cell["alert"] or is_alert
        cell["n"] += 1
        cell[sev] = cell.get(sev, 0) + 1
        # keep a sample: prioritise alerts/failures, cap volume
        if is_alert or sev in ("error", "critical"):
            if sum(1 for e in cell["events"] if e.get("p")) < 15:
                cell["events"].append(_event_detail(r, sev, is_alert, tz2_LT))
        elif len(cell["events"]) < 25:
            cell["events"].append(_event_detail(r, sev, is_alert, tz2_LT))
        c = counts[p]
        c["events"] += 1
        if sev in c:
            c[sev] += 1
        if is_alert:
            c["alerts"] += 1
        pb = per_bucket[bi]
        if sev in ("error", "critical"):
            pb["fail"] += 1
        elif sev == "warning":
            pb["warn"] += 1
        else:
            pb["info"] += 1

    # max events in any cell (for opacity scaling)
    max_cell = max((cell["n"] for buckets in lanes_agg.values()
                    for cell in buckets.values()), default=1) or 1
    bucket_w = 100.0 / n_buckets

    lanes = []
    run_detail = {}
    rid = 0
    for p, c in counts.items():
        bars = []
        for bi, cell in lanes_agg[p].items():
            sev = RANK_SEV[cell["rank"]]
            rid += 1
            run_id = "b%d" % rid
            bstart = start + timedelta(seconds=bi * bucket_s)
            bend = bstart + timedelta(seconds=bucket_s)
            bars.append({
                "id": run_id,
                "left": round(bi * bucket_w, 3),
                "width": round(bucket_w * 0.86, 3),
                "cls": SEV_CLASS[sev],
                "alert": cell["alert"],
                "n": cell["n"],
                "opacity": round(0.35 + 0.65 * (cell["n"] / max_cell), 2),
            })
            run_detail[run_id] = {
                "pipeline": p,
                "time": tz2_LT(bstart, "%Y-%m-%d %H:%M") + " – " + tz2_LT(bend, "%H:%M"),
                "sev": sev,
                "alert": cell["alert"],
                "n": cell["n"],
                "breakdown": {"critical": cell["critical"], "error": cell["error"],
                              "warning": cell["warning"], "noload": cell["noload"],
                              "running": cell["running"], "info": cell["info"]},
                "events": cell["events"],
            }
        bars.sort(key=lambda x: x["left"])
        loaded = (c["events"] - c["warning"] - c["error"] - c["critical"]
                  - c["noload"] - c["running"])
        if c["critical"]:
            status = ("crit", "DOWN")
        elif c["error"]:
            status = ("crit", "ERRORS")
        elif c["warning"]:
            status = ("warn", "WARN")
        elif c["noload"] and loaded == 0 and not c["running"]:
            # synced fine, zero rows -> light green (not a warning)
            status = ("noload", "EMPTY")
        elif c["running"] and loaded == 0 and not c["noload"]:
            # only in-flight syncs -> neutral blue
            status = ("info", "SYNCING")
        else:
            status = ("ok", "OK")
        lanes.append({
            "name": _pipeline_name(p, pay_by_pipe.get(p, "")),
            "family": _pipeline_family(p, pay_by_pipe.get(p, "")), "kind": _kind(p),
            "cadence": "range",
            "status_cls": status[0], "status_label": status[1],
            "bars": bars, "counts": c, "stale": False,
            "sort_rank": max((SEV_RANK[s] for s in ("critical", "error", "warning")
                              if c[s] > 0), default=0),
        })
    lanes.sort(key=lambda l: (-l["sort_rank"], -l["counts"]["events"]))
    # hide-list: also drop lanes whose FORMATTED name or FAMILY is hidden
    lanes = [l for l in lanes
             if not remove_from_dashboard(l.get("name"))
             and not remove_from_dashboard(l.get("family"))]
    lane_groups = _lane_groups(lanes)
    supergroups = _supergroups(lane_groups)

    # axis: one label per bucket (thinned if crowded)
    axis = []
    step = max(1, n_buckets // 24)
    for bi in range(n_buckets):
        bstart = _local(start + timedelta(seconds=bi * bucket_s), tz)
        if bi % step == 0:
            axis.append(bstart.strftime("%m-%d") if multiday else bstart.strftime("%m-%d %H:%M"))
        else:
            axis.append("")

    maxbar = max((b["fail"] + b["warn"] + b["info"] for b in per_bucket), default=1) or 1

    fails_by = sorted(((p, c["error"] + c["critical"]) for p, c in counts.items()
                       if c["error"] + c["critical"] > 0), key=lambda x: -x[1])
    warns_by = sorted(((p, c["warning"]) for p, c in counts.items()
                       if c["warning"] > 0), key=lambda x: -x[1])

    # warehouse credits, bucketed onto the same adaptive range buckets
    credits = _credit_context(credit_rows, start, bucket_s, n_buckets,
                              peaks=credit_peaks)

    return {
        **credits,
        "mode": "range",
        "centered": False,
        "now_left_pct": None,
        "env": env,
        "tz_abbr": _tz_abbr(tz, start),
        "tzname": tzname,
        "range_start": _local(start, tz),
        "range_end": _local(end, tz),
        "bucket_label": _human_cadence(bucket_s),
        "n_buckets": n_buckets,
        "axis": axis,
        "lanes": lanes,
        "lane_groups": lane_groups,
        "supergroups": supergroups,
        "run_detail": run_detail,
        "per_bucket": per_bucket,
        "maxbar": maxbar,
        "bucket_w": bucket_w,
        "total_fail": sum(n for _, n in fails_by),
        "total_warn": sum(n for _, n in warns_by),
        "fails_by": fails_by[:6],
        "warns_by": warns_by[:6],
        "days": round(span_s / 86400, 1),
    }


def _hour_axis(win_start, window_hours, LT):
    """Hourly tick labels for the timeline axis, thinned to stay readable.

    The grid always has one column per hour (bars line up with it), but we only
    print a label every Nth tick — otherwise a 7-day window would try to render
    168 of them. Beyond a day the label carries the date too, since '03:00'
    alone is ambiguous across several days."""
    step = max(1, math.ceil(window_hours / 12))     # <= ~12 labels
    multiday = window_hours > 24
    out = []
    for i in range(window_hours):
        if i % step:
            out.append("")
            continue
        t = win_start + timedelta(hours=i)
        out.append(LT(t, "%d %b %H:%M" if multiday else "%H:%M"))
    return out


def _human_duration(seconds):
    """A plain elapsed-time label ('90m', '4h', '2d'). Distinct from
    _human_cadence, which describes a schedule ('hourly', 'daily')."""
    if not seconds:
        return "—"
    m = seconds / 60
    if m < 90:
        return f"{round(m)}m"
    h = m / 60
    if h < 48:
        return f"{round(h)}h"
    return f"{round(h / 24, 1)}d"


def _session_starts(buckets, session_gap_s: int):
    """Collapse run buckets into sessions: consecutive buckets separated by less
    than `session_gap_s` belong to the same invocation (a pipeline fans a burst
    of metrics out over several minutes). Returns each session's start time."""
    bs = sorted(buckets)
    if not bs:
        return []
    starts = [bs[0]]
    for prev, cur in zip(bs, bs[1:]):
        if (cur - prev).total_seconds() > session_gap_s:
            starts.append(cur)
    return starts


def _predict_next(last_seen, session_starts, jitter_limit=0.35):
    """Predict a pipeline's next run from its learned schedule.

    Uses the median gap between run SESSIONS over the full fetched history (7d),
    not raw event spacing — bursty pipelines fan out hundreds of events per run.

    Returns (predicted_at, cadence_s, jitter, confident) or None when there is
    too little history (<2 gaps) to infer a schedule at all. `jitter` is the
    spread of the gaps relative to the cadence: a pipeline that runs every hour
    on the hour has ~0 jitter, an ad-hoc one can exceed 100%. Above
    `jitter_limit` the point estimate is meaningless, so callers render an
    expected *window* instead of a line."""
    if last_seen is None or len(session_starts) < 3:
        return None                       # need >= 2 gaps to have a spread
    gaps = [(session_starts[i] - session_starts[i - 1]).total_seconds()
            for i in range(1, len(session_starts))]
    gaps = [g for g in gaps if g > 0]
    if len(gaps) < 2:
        return None
    cadence_s = median(gaps)
    if cadence_s <= 0:
        return None
    # Robust spread (MAD), NOT stdev: a single 68h outage among 60m gaps sends
    # stdev to 3600% and would mark a perfectly regular pipeline as chaotic.
    mad = median([abs(g - cadence_s) for g in gaps])
    jitter = mad / cadence_s
    return (last_seen + timedelta(seconds=cadence_s), cadence_s, jitter,
            jitter <= jitter_limit)


def _stale_check(since_last_s, cadence_s, stale_factor, stale_floor_s,
                 unknown_stale_s):
    """Is a pipeline overdue? Returns (is_stale, threshold_seconds).

    A pipeline is stale once it has been quiet for materially longer than its
    own learned schedule — `cadence * stale_factor`, never less than
    `stale_floor_s`. A once-a-day pipeline therefore gets ~48h of slack, while
    a 5-minute pipeline is still caught after the 2h floor.

    With too few sessions to infer a cadence we cannot say what "normal" is, so
    we only flag after `unknown_stale_s` (48h) of total silence.
    """
    if since_last_s is None:
        return False, None
    if not cadence_s:
        return since_last_s > unknown_stale_s, unknown_stale_s
    threshold = max(cadence_s * stale_factor, stale_floor_s)
    return since_last_s > threshold, threshold


# bar css class -> colour, for blending a family's mixed severities
_CLS_COLOR = {"fail": "#f43f5e", "warn": "#fbbf24", "success": "#22c55e",
              "noload": "#86efac", "info": "#60a5fa", "stale": "#444a5b"}
# worst first, so a blended bar always leads with its most severe colour.
# 'noload' (empty sync) sits below 'success': a bucket with any real load
# leads green, one that only ever synced empty leads light green.
_CLS_ORDER = ("fail", "warn", "info", "success", "noload", "stale")


def _blend(classes):
    """Style for a bar covering several severities at the same instant.

    One severity -> no inline style (the plain .run class paints it).
    Several -> hard-stop linear-gradient so the bar literally shows the mix
    (e.g. half red / half green) rather than hiding one behind the other."""
    present = [c for c in _CLS_ORDER if c in classes]
    if len(present) <= 1:
        return None
    step = 100.0 / len(present)
    stops = []
    for i, cls in enumerate(present):
        colour = _CLS_COLOR[cls]
        stops.append("%s %.1f%% %.1f%%" % (colour, i * step, (i + 1) * step))
    return "background:linear-gradient(180deg,%s)" % ",".join(stops)


def _merge_runs(lanes, key="runs", bucket=1.0):
    """Collapse every child lane's bars onto one family track.

    Bars landing in the same horizontal bucket are merged: the bucket carries
    the union of their severities (so the template can blend them) and the sum
    of their event counts. Without this, a collapsed family shows an empty
    strip and you lose the one thing the timeline exists to show — when things
    ran, and how they went."""
    agg = {}
    for lane in lanes:
        for bar in lane.get(key) or []:
            slot = round(bar["left"] / bucket) * bucket
            cell = agg.setdefault(slot, {"left": slot, "classes": set(), "n": 0,
                                         "alert": False, "width": bar.get("width")})
            cell["classes"].add(bar["cls"])
            cell["n"] += bar.get("n", 0)
            cell["alert"] = cell["alert"] or bool(bar.get("alert"))
            if bar.get("width"):                    # range bars carry a width
                cell["width"] = max(cell["width"] or 0, bar["width"])
    out = []
    for cell in sorted(agg.values(), key=lambda c: c["left"]):
        present = [c for c in _CLS_ORDER if c in cell["classes"]]
        out.append({
            "left": round(cell["left"], 2),
            "width": cell["width"],
            "cls": present[0] if present else "info",   # worst severity leads
            "style": _blend(cell["classes"]),
            "mixed": len(present) > 1,
            "n": cell["n"],
            "alert": cell["alert"],
            "sevs": present,
        })
    return out


def _rollup_status(statuses):
    """Parent (family / group) pill = a proportion-aware rollup over its
    CHILDREN's statuses, which are already recency-aware. So a lone bad child
    among many healthy ones does NOT sink the whole parent, and a parent whose
    children have all recovered reads healthy again:
      stale  – every child is stale (nothing running)
      crit   – half or more of the RUNNING children are down/degraded
      warn   – a notable minority (>= 15%) are down/degraded/warning
      noload – all running children synced zero rows
      info   – all running children are still in flight
      ok     – healthy, including the odd isolated blip
    """
    n = len(statuses)
    if n == 0:
        return "ok"
    n_stale = statuses.count("stale")
    active = n - n_stale
    if active == 0:
        return "stale"
    n_crit = statuses.count("crit")
    n_warn = statuses.count("warn")
    if n_crit / active >= 0.5:
        return "crit"
    if (n_crit + n_warn) / active >= 0.15:
        return "warn"
    n_noload = statuses.count("noload")
    n_running = statuses.count("info")
    n_ok = statuses.count("ok")
    if n_noload and not (n_ok or n_running):
        return "noload"
    if n_running and not (n_ok or n_noload):
        return "info"
    return "ok"


def _lane_groups(lanes):
    """Group timeline lanes into pipeline families, preserving lane order
    (worst-first). Single-lane families are marked `solo` so the template can
    render them flat instead of as a pointless expandable group."""
    groups, idx = [], {}
    for lane in lanes:
        fam = lane.get("family") or lane["name"]
        if fam not in idx:
            idx[fam] = len(groups)
            groups.append({"family": fam, "lanes": [], "n_fail": 0, "n_warn": 0,
                           "n_noload": 0, "n_running": 0, "n_loaded": 0,
                           "stale": 0, "rank": 0})
        g = groups[idx[fam]]
        g["lanes"].append(lane)
        c = lane.get("counts", {})
        g["n_fail"] += c.get("error", 0) + c.get("critical", 0)
        g["n_warn"] += c.get("warning", 0)
        g["n_noload"] += c.get("noload", 0)
        g["n_running"] += c.get("running", 0)
        g["n_loaded"] += (c.get("events", 0) - c.get("warning", 0)
                          - c.get("error", 0) - c.get("critical", 0)
                          - c.get("noload", 0) - c.get("running", 0))
        g["stale"] += 1 if lane.get("stale") else 0
        g["rank"] = max(g["rank"], lane.get("sort_rank", 0))
    for g in groups:
        g["n"] = len(g["lanes"])
        g["solo"] = g["n"] == 1
        # no real loads and nothing failed: all-empty -> light green,
        # all-in-flight -> neutral blue
        g["status_cls"] = _rollup_status([l["status_cls"] for l in g["lanes"]])
        # merged track so a COLLAPSED family still shows when its pipelines ran
        g["runs"] = _merge_runs(g["lanes"], "runs", bucket=1.0)
        g["bars"] = _merge_runs(g["lanes"], "bars", bucket=0.01)
        g["ghost"] = all(l.get("ghost") for l in g["lanes"])
        # the family carries a kind too (aws/dbt/sf/task) — the dominant kind of
        # its lanes — so the group header can show the same icon/tag as a lane.
        kinds = [l.get("kind") for l in g["lanes"] if l.get("kind")]
        g["kind"] = max(set(kinds), key=kinds.count) if kinds else "task"
        # lanes within a family: alphabetical
        g["lanes"].sort(key=lambda l: (l.get("name") or "").lower())
    # families: alphabetical by the label the user actually SEES. A solo family
    # renders flat as its single lane's model name — for a lone dbt model the
    # "dbt_snowflake_transformation_" project prefix is hidden — so sorting by the
    # raw `family` would file solo dbt models under 'd' and scatter them through
    # the dbt block (e.g. funnel_/member_ landing mid-group). Use the visible
    # label: the lone lane's name for solo groups, the family for the rest.
    groups.sort(key=lambda g: ((g["lanes"][0].get("name") if g["solo"]
                                else g.get("family")) or "").lower())
    return groups


def _supergroups(lane_groups):
    """Level-1 grouping: collapse family groups (from `_lane_groups`) into named
    groups via `formatting.pipeline_group`. Returns an ordered list of
    supergroups, each either:

      * grouped=True  — a named colour group holding >= 2 families: the template
        draws a GROUP header with the families nested + collapsible under it.
      * grouped=False — a passthrough: a standalone family (or a named group that
        happens to hold a single family). The family renders exactly as before,
        just RELABELLED to the simplified/group name (`ai-ingest-iterate` ->
        `iterate`). Its single family group is exposed as `families[0]`.

    Every supergroup carries a rollup (counts + merged run/bar tracks) so a
    collapsed group still shows when its pipelines ran and how they went.
    """
    buckets, order = {}, []
    for g in lane_groups:
        label, named = _pipeline_group(g.get("family") or g["lanes"][0].get("name"))
        if label not in buckets:
            buckets[label] = {"group": label, "named": named, "families": []}
            order.append(label)
        buckets[label]["families"].append(g)

    supers = []
    for label in order:
        sg = buckets[label]
        fams = sg["families"]
        grouped = sg["named"] and len(fams) > 1

        if not grouped:
            # passthrough: relabel the lone family to the (simplified) group name.
            # Solo families still render as their single lane's name — unchanged —
            # so `display` only surfaces on multi-lane family headers.
            fams[0]["display"] = label
            sg.update(grouped=False, families=fams)
            supers.append(sg)
            continue

        # a real named group over several families: roll their stats up so the
        # collapsed group header summarises the lot.
        all_lanes = [l for f in fams for l in f["lanes"]]
        n_fail = sum(f["n_fail"] for f in fams)
        n_warn = sum(f["n_warn"] for f in fams)
        n_noload = sum(f["n_noload"] for f in fams)
        n_running = sum(f["n_running"] for f in fams)
        n_loaded = sum(f["n_loaded"] for f in fams)
        stale = sum(f["stale"] for f in fams)
        kinds = [f.get("kind") for f in fams if f.get("kind")]
        sg.update(
            grouped=True,
            n=len(all_lanes),
            n_fam=len(fams),
            n_fail=n_fail, n_warn=n_warn, stale=stale,
            status_cls=_rollup_status([f["status_cls"] for f in fams]),
            runs=_merge_runs(all_lanes, "runs", bucket=1.0),
            bars=_merge_runs(all_lanes, "bars", bucket=0.01),
            ghost=all(l.get("ghost") for l in all_lanes) if all_lanes else False,
            kind=max(set(kinds), key=kinds.count) if kinds else "dbt",
        )
        # families within a group: alphabetical by their visible label
        fams.sort(key=lambda f: ((f["lanes"][0].get("name") if f["solo"]
                                  else f.get("family")) or "").lower())
        supers.append(sg)

    # top level alphabetical by the label the user sees
    supers.sort(key=lambda sg: (sg["group"] or "").lower())
    return supers


def _target_table(metric_name: str, pipeline: str):
    """The REAL warehouse table a metric describes, when it can be derived.

    * dbt (`dbt_model_run`): the pipeline IS the model, which materialises to a
      relation of the same name (`stg_braze__email_click`).
    * ingest/load metrics: `ingest.postgresql.surveys.rows` -> `surveys`
      (second-to-last dotted segment is the table).

    Returns the bare table name (no db/schema — the payload never carries one,
    so the UI resolves it via ACCOUNT_USAGE.TABLES), or None when unknown."""
    if metric_name == "dbt_model_run" and pipeline:
        return pipeline
    parts = (metric_name or "").split(".")
    if len(parts) >= 3 and parts[0] in ("ingest", "load", "sync", "reconcile"):
        return parts[-2]
    return None


def _metric_parts(metric_name: str):
    """Split a dotted metric into (subject, leaf).
    `ingest.postgresql.user_weekly_recaps.rows`
        -> ('ingest.postgresql.user_weekly_recaps', 'rows')
    Undotted names (e.g. `dbt_model_run`) are their own subject."""
    if metric_name and "." in metric_name:
        head, leaf = metric_name.rsplit(".", 1)
        return head, leaf
    return metric_name, metric_name


def _human_cadence(seconds):
    if not seconds:
        return "irregular"
    m = seconds / 60
    if m < 1.5:
        return "~1m"
    if m < 20:
        return f"~{round(m)}m"
    if m < 90:
        return "hourly"
    h = m / 60
    if h < 20:
        return f"~{round(h)}h"
    return "daily"


# ----------------------------------------------------------------------------
# ANOMALY DASHBOARD
# ----------------------------------------------------------------------------
def _is_freshness_metric(name: str) -> bool:
    """Monotonic watermark/timestamp metrics — always climb, so value
    z-scores are meaningless. Freshness is covered by timeline STALE status."""
    n = name.lower()
    return n.endswith(".max_date") or "watermark" in n or n.endswith("_max_date")


def _period_label(seconds: float) -> str:
    """Human label for a detected cycle length (e.g. '~24h', '~7d')."""
    hours = seconds / 3600.0
    if hours < 1:
        return "~%dm" % max(1, round(seconds / 60))
    if hours < 48:
        return "~%dh" % round(hours)
    return "~%dd" % round(hours / 24)


def _resample_even(pts):
    """Resample an irregular [(ts, value), ...] series onto an EVEN time grid so
    STL (which needs equally-spaced points and no gaps) can run.

    Grid step = the median gap between observations, but never finer than
    span / SEASONAL_MAX_GRID — that caps the grid length so a per-minute metric
    over 7 days doesn't hand STL 10k points every page load. Internal gaps are
    time-interpolated; leading/trailing NaNs dropped. Returns (grid_series,
    step_seconds) or None when the series can't form a usable grid."""
    index = _pd.to_datetime([p[0] for p in pts])
    ser = _pd.Series([float(p[1]) for p in pts], index=index).sort_index()
    ser = ser.groupby(level=0).mean()               # collapse dup timestamps
    if len(ser) < 4:
        return None
    span = (ser.index[-1] - ser.index[0]).total_seconds()
    if span <= 0:
        return None
    deltas = _np.diff(ser.index.asi8) / 1e9         # gaps in seconds
    med_step = float(_np.median(deltas)) if len(deltas) else span
    step = max(med_step, span / SEASONAL_MAX_GRID)
    if step <= 0:
        return None
    grid = (ser.resample(_pd.to_timedelta(step, unit="s"))
               .mean().interpolate("time").dropna())
    if len(grid) < 4:
        return None
    return grid, step


def _detect_period(values) -> int | None:
    """Auto-detect the dominant cycle length (in grid steps) via autocorrelation.

    Returns the lag of the strongest ACF peak that (a) clears SEASONAL_ACF_MIN
    and (b) leaves room for >= SEASONAL_MIN_PERIODS full cycles in the data.
    None when no clear cycle exists — the caller then uses the flat baseline."""
    x = _np.asarray(values, dtype=float)
    n = len(x)
    if n < SEASONAL_MIN_PERIODS * 2:
        return None
    x = x - x.mean()
    denom = float(_np.dot(x, x))
    if denom == 0:                                  # perfectly constant
        return None
    acf = _np.correlate(x, x, mode="full")[n - 1:] / denom
    max_lag = n // SEASONAL_MIN_PERIODS
    best_lag, best_val = None, SEASONAL_ACF_MIN
    for lag in range(2, max_lag + 1):
        val = acf[lag]
        prev = acf[lag - 1]
        nxt = acf[lag + 1] if lag + 1 < n else -1.0
        if val > best_val and val >= prev and val >= nxt:   # strong local peak
            best_val, best_lag = val, lag
    return best_lag


def _smooth_circular(arr, win: int = 1):
    """Circular moving-average smooth of a per-phase array so band widths
    transition gently around the cycle (no hard step between adjacent phases)."""
    n = len(arr)
    k = min(int(win), n // 2)
    if k < 1 or n <= 2:
        return _np.asarray(arr, dtype=float)
    out = _np.empty(n, dtype=float)
    for i in range(n):
        idx = [(i + d) % n for d in range(-k, k + 1)]
        out[i] = float(_np.mean(_np.asarray(arr)[idx]))
    return out


def _robust_sd(values):
    """MAD → σ of an array; falls back to plain std, then 1.0, so it's never 0."""
    a = _np.asarray(values, dtype=float)
    if a.size == 0:
        return 1.0
    mad = float(_np.median(_np.abs(a - _np.median(a))))
    if mad > 0:
        return mad / 0.6745
    sd = float(_np.std(a))
    return sd if sd > 0 else 1.0


def _baseline_curve(pts):
    """A SMOOTH baseline for a metric — consistent across every chart.

    Always returns a smooth centre line that hugs the data plus a band, in one
    of two flavours:
      * SEASONAL (wavy, breathing band) — only when the data is dense over
        enough full cycles to trust a period (STL trend+seasonal, per-phase σ).
      * TREND (smooth centre, steady band) — otherwise: a robust rolling smoother
        so the centre follows the rise/fall instead of a flat slab.
    Returns None only when there's too little data (< TREND_MIN_POINTS) or the
    smoother can't run, in which case the caller uses the flat median/MAD band.

    dict keys:
      expected_pts  [float] expected value at each ORIGINAL timestamp (scoring)
      basis         'seasonal ~24h' | 'smooth trend'
      period_label  '~24h' when seasonal, else None
      sigma_latest  float σ for scoring the newest value (per-phase if seasonal)
      sigma_overall float robust σ of residuals (range mode / fallback)
      curve         dense render series (list-aligned ts/exp/sigma) for the chart
    """
    if not _SEASONAL_OK or len(pts) < TREND_MIN_POINTS:
        return None
    try:
        resampled = _resample_even(pts)
        if resampled is None:
            return None
        grid, step = resampled
        n = len(grid)
        grid_x = grid.index.asi8.astype(float)
        obs_x = _pd.to_datetime([p[0] for p in pts]).asi8.astype(float)

        # --- decide seasonal vs trend-only -----------------------------------
        # A cycle is trusted only with enough RAW density over enough cycles, so
        # a thin series (e.g. 29 points) never claims a 24h wave it can't support.
        period = _detect_period(grid.values)
        span_s = (obs_x[-1] - obs_x[0]) / 1e9
        n_cycles = span_s / (period * step) if (period and step) else 0.0
        pts_per_cycle = (len(pts) / n_cycles) if n_cycles > 0 else 0.0
        seasonal = bool(
            period and period >= 2
            and len(pts) >= SEASONAL_MIN_POINTS
            and n >= SEASONAL_MIN_PERIODS * period
            and n_cycles >= SEASONAL_MIN_PERIODS
            and pts_per_cycle >= SEASONAL_MIN_PTS_PER_CYCLE)

        if seasonal:
            fit = _STL(grid.values, period=period, robust=True).fit()
            expected_grid = _np.asarray(fit.trend + fit.seasonal, dtype=float)
            resid_grid = _np.asarray(fit.resid, dtype=float)
            overall_sd = _robust_sd(resid_grid)
            floor = SEASONAL_SIGMA_FLOOR * overall_sd    # no hairline bands
            # per-PHASE robust σ so the band breathes with the cycle; sparse
            # phases borrow the overall σ, then circular-smooth + floor.
            phase = _np.arange(n) % period
            sigma_by_phase = _np.full(period, overall_sd, dtype=float)
            for ph in range(period):
                r = resid_grid[phase == ph]
                if len(r) >= 3:
                    sigma_by_phase[ph] = _robust_sd(r)
            sigma_by_phase = _smooth_circular(sigma_by_phase, max(1, period // 12))
            sigma_by_phase = _np.maximum(sigma_by_phase, floor)
            sigma_grid = sigma_by_phase[phase]
            last_phase = int(round((obs_x[-1] - grid_x[0]) / (step * 1e9))) % period
            sigma_latest = float(sigma_by_phase[last_phase])
            basis, period_label = "seasonal %s" % _period_label(period * step), \
                _period_label(period * step)
        else:
            # trend-only: robust rolling smoother (median → mean) so the centre
            # follows the trend without chasing spikes. Constant-width band.
            win = max(5, n // 8)
            if win % 2 == 0:
                win += 1
            ser = _pd.Series(grid.values)
            trend = (ser.rolling(win, center=True, min_periods=1).median()
                        .rolling(win, center=True, min_periods=1).mean())
            expected_grid = _np.asarray(trend.values, dtype=float)
            resid_grid = grid.values - expected_grid
            overall_sd = _robust_sd(resid_grid)
            sigma_grid = _np.full(n, overall_sd, dtype=float)
            sigma_latest = overall_sd
            basis, period_label = "smooth trend", None

        expected_pts = _np.interp(obs_x, grid_x, expected_grid)

        # downsample the dense grid for the payload (keep the shape, bound size)
        keep = _np.linspace(0, n - 1, min(n, CURVE_RENDER_POINTS))
        keep = sorted(set(int(round(i)) for i in keep))
        ts_all = list(grid.index.to_pydatetime())
        curve = {
            "ts":    [ts_all[i] for i in keep],
            "exp":   [float(expected_grid[i]) for i in keep],
            "sigma": [float(sigma_grid[i]) for i in keep],
        }
        return {
            "expected_pts": [float(v) for v in expected_pts],
            "basis": basis,
            "period_label": period_label,
            "sigma_latest": float(sigma_latest),
            "sigma_overall": float(overall_sd),
            "curve": curve,
        }
    except Exception as exc:                        # pragma: no cover
        logging.getLogger("monty.transform").info(
            "baseline curve failed (%s); flat baseline", exc)
        return None


# The row-limit gate is a VOLUME gate — it applies to any count/volume metric
# (rows loaded, audiences added/removed, records synced, model-run counts, …).
# It must NOT apply to metrics where a "rows" floor is meaningless: latency /
# durations, timestamps, percentages, ratios/rates, scores. So instead of
# whitelisting count-metric names (which misses e.g. `audience_added`), we
# EXCLUDE the clearly-non-volume ones and gate everything else.
_NON_VOLUME_HINTS = ("time", "_ms", "millis", "second", "_sec", "latency",
                     "duration", "elapsed", "date", "watermark", "_ts",
                     "percent", "pct", "ratio", "_rate", "score", "age", "lag",
                     "freshness", "_avg", "average", "mean", "median",
                     "p50", "p90", "p95", "p99")


def _is_volume_metric(name: str) -> bool:
    """True for a count/volume metric (the row-limit gate applies). Latency,
    timestamps, percentages, ratios, scores etc. are NOT gated."""
    n = (name or "").lower()
    return not any(h in n for h in _NON_VOLUME_HINTS)


def _is_rowcount_metric(name: str) -> bool:
    """True ONLY for the metrics we score for anomalies: a row-load count
    (name ends '.rows') or the dbt rows-loaded metric (`dbt_model_run`).

    Everything else — watermarks, max_date timestamps, batch totals, fetch
    counts — is listed on the anomaly page but never scored: a watermark moving
    forward is not an anomaly, a row count dropping is. Deliberately strict
    (exact '.rows' suffix, not 'total_rows'/'rows_fetched') per the product call.
    """
    n = (name or "").lower()
    return n.endswith(".rows") or n == "dbt_model_run"


def build_anomaly_context(rows, now: datetime, baseline_days: int = 7,
                          env: str = "prod", z_threshold: float = 3.5,
                          min_points: int = 8, min_pct: float = 10.0,
                          row_limit: float = 500.0,
                          group_filter: str = "", family_filter: str = "",
                          pipeline_filter: str = "",
                          agg_start: datetime | None = None,
                          agg_end: datetime | None = None,
                          tzname: str = "utc"):
    """
    Returns a dict consumed by templates/anomaly.html.

    Two modes:
    - Point mode (default): score each metric's LATEST value against its
      trailing baseline with a robust z (median / MAD).
    - Range mode (agg_start/agg_end set): score each metric's MEAN over
      [agg_start, agg_end] against the baseline formed from points BEFORE
      agg_start. Answers "which metrics ran abnormally on average over the
      selected window."

    A point is an anomaly when BOTH |robust_z| >= z_threshold AND
    |percent change vs median| >= min_pct.
    """
    range_mode = agg_start is not None and agg_end is not None
    if range_mode:
        win_start = agg_start - timedelta(days=baseline_days)
        win_end = agg_end
    else:
        win_start = now - timedelta(days=baseline_days)
        win_end = now
    # Series are keyed by (METRIC_NAME, PIPELINE_NAME), NOT metric alone.
    # Some producers reuse one metric name across many pipelines — dbt emits
    # `dbt_model_run` for every model, with the model in PIPELINE_NAME. Keying
    # on metric alone merged unrelated models into one series and made the
    # median/MAD (and therefore every z-score) meaningless.
    series = defaultdict(list)          # (metric, pipeline) -> [(ts, value)]
    pay_by_pipe = {}                    # pipeline -> representative "<payload>||<metric>"
    total_obs = 0

    for r in rows:
        if _g(r, "ENVIRONMENT") != env:
            continue
        ts = _as_dt(_g(r, "OCCURRED_AT"))
        val = _g(r, "METRIC_VALUE")
        if ts is None or not (win_start <= ts <= win_end):
            continue
        total_obs += 1
        if val is None or val == "" or (isinstance(val, float) and math.isnan(val)):
            continue
        m = _g(r, "METRIC_NAME")
        p = _g(r, "PIPELINE_NAME") or ""
        # hide-list matches the pipeline AND the metric name (anomalies are keyed
        # on the metric, so a pattern like "*total_row*" hides it here too).
        if remove_from_dashboard(p) or remove_from_dashboard(m):
            continue
        _pick_pay(pay_by_pipe, p, _g(r, "PAYLOAD"), _g(r, "METRIC_NAME"))
        series[(m, p)].append((ts, float(val)))

    # A metric name used by >1 pipeline is "generic" (e.g. dbt_model_run): its
    # display label is qualified by the pipeline so each row names the real
    # thing that ran (the dbt model). Unshared names display as-is.
    pipelines_per_metric = defaultdict(set)
    for m, p in series:
        pipelines_per_metric[m].add(p)

    def _label(metric_name, pipeline):
        if pipeline and len(pipelines_per_metric[metric_name]) > 1:
            return "%s.%s" % (pipeline, metric_name)
        return metric_name

    # Group / family / pipeline of every pipeline seen. The dropdown OPTIONS are
    # built from all of them (pre-filter) so you can always switch selection.
    fam_of, grp_of = {}, {}
    for _m, _p in series:
        _fam = _pipeline_family(_p, pay_by_pipe.get(_p, ""))
        fam_of[_p] = _fam
        grp_of[_p] = _pipeline_group(_fam)[0]
    filter_options = {
        "groups":    sorted({g for g in grp_of.values() if g}, key=str.lower),
        "families":  sorted({f for f in fam_of.values() if f}, key=str.lower),
        "pipelines": sorted({p for p in fam_of if p}, key=str.lower),
    }

    scored = []
    # Metrics the detector can't score (too little history, low volume, no
    # variation, freshness). They are NOT dropped — they still appear in the
    # table marked "not scored" with the reason, so no pipeline silently
    # disappears; the status filter can hide them.
    unscored = []
    skipped_fresh = 0
    skipped_low_points = 0      # metrics with too little history to score
    skipped_low_volume = 0      # volume metrics below the row-limit gate
    skipped_non_rowcount = 0    # not a *.rows / dbt row-load metric

    def _mark_unscored(m, p, pts, reason):
        last_ts = pts[-1][0] if pts else None
        last_v = pts[-1][1] if pts else None
        unscored.append({
            "metric": _label(m, p), "metric_name": m, "pipeline": p,
            "scored": False, "reason": reason,
            "latest": last_v, "median": None, "mean": None, "center": None,
            "latest_at": last_ts, "n": len(pts), "series": pts,
            "pct": 0.0, "z": 0.0, "std": 0.0, "anom": False, "direction": "",
            "expected": None, "curve": None, "basis": "", "period_label": None,
        })
    for (m, p), pts in series.items():
        # header selectors: keep only the chosen group / family / pipeline
        if group_filter and grp_of.get(p) != group_filter:
            continue
        if family_filter and fam_of.get(p) != family_filter:
            continue
        if pipeline_filter and p != pipeline_filter:
            continue
        pts.sort(key=lambda x: x[0])
        if _is_freshness_metric(m):
            skipped_fresh += 1
            _mark_unscored(m, p, pts,
                           "freshness metric — staleness is tracked on the timeline")
            continue

        # Only row-load metrics are scored for anomalies (*.rows + dbt rows-
        # loaded). Everything else is listed but not scored — a watermark or
        # max_date advancing is not an anomaly. Placed before the history/volume
        # gates so a non-row metric never competes for a scored slot.
        if not _is_rowcount_metric(m):
            skipped_non_rowcount += 1
            _mark_unscored(m, p, pts,
                           "not a row-count metric (only *.rows + dbt are scored)")
            continue

        if range_mode:
            baseline = [v for ts, v in pts if ts < agg_start]
            observed_pts = [v for ts, v in pts if agg_start <= ts <= agg_end]
            if len(baseline) < min_points or not observed_pts:
                skipped_low_points += 1
                _mark_unscored(m, p, pts, "too little history (%d of %d points needed)"
                               % (len(baseline), min_points))
                continue
            observed = mean(observed_pts)       # AVERAGE over the range
            observed_at = agg_end
            n_pts = len(pts)
        else:
            if len(pts) < min_points:
                skipped_low_points += 1
                _mark_unscored(m, p, pts, "too little history (%d of %d points needed)"
                               % (len(pts), min_points))
                continue
            vals = [v for _, v in pts]
            baseline = vals[:-1] or vals
            observed = pts[-1][1]
            observed_at = pts[-1][0]
            n_pts = len(pts)

        med = median(baseline)

        # --- row-limit VOLUME gate --------------------------------------------
        # A count/volume metric whose TYPICAL volume is below `row_limit` isn't
        # scored at all: with too few rows a % swing is just noise, so anomalies
        # there aren't meaningful. Non-volume metrics (latency, timestamps, %s)
        # are never gated. Uses the baseline median (typical volume) so a
        # normally-high metric that DROPS is still caught — only perpetually-tiny
        # ones skip.
        if row_limit and _is_volume_metric(m) and med < row_limit:
            skipped_low_volume += 1
            _mark_unscored(m, p, pts, "low volume (typical %s < %s row limit)"
                           % (fmt(med), fmt(row_limit)))
            continue

        mad = median([abs(v - med) for v in baseline])
        robust_sd = mad / 0.6745 if mad > 0 else 0.0

        # --- smooth baseline ---------------------------------------------------
        # Score the residual (observed - smooth EXPECTED) rather than
        # (observed - flat median). Every metric with enough history gets a
        # smooth centre that hugs the data: a trend line, plus a seasonal wave
        # when the data is dense over enough cycles to trust one. So a value
        # that's off *for where the metric should be right now* trips, while a
        # natural rise/fall (or cyclical peak) no longer false-alarms. `center`
        # is the expected value at the scored point; `expected_series` the full
        # expected curve. Falls back to flat median/MAD only for very short
        # series or when statsmodels is unavailable.
        expected_series = None
        period_label = None
        curve = None
        basis_label = "flat"
        bcurve = _baseline_curve(pts)           # NB: distinct from `baseline` list
        if bcurve:
            expected_series = bcurve["expected_pts"]
            period_label = bcurve["period_label"]
            basis_label = bcurve["basis"]
            curve = bcurve["curve"]             # dense smooth render series
            if range_mode:
                obs_exp = [e for (ts, _v), e in zip(pts, expected_series)
                           if agg_start <= ts <= agg_end]
                center = mean(obs_exp) if obs_exp else expected_series[-1]
                baseline_sd = bcurve["sigma_overall"]       # window: overall spread
            else:
                center = expected_series[-1]
                # score the newest value against its own local spread
                baseline_sd = bcurve["sigma_latest"] or bcurve["sigma_overall"]
            if baseline_sd and baseline_sd > 0:
                robust_sd = baseline_sd         # residual spread now drives z
            else:                               # residuals too flat — fall back
                expected_series = period_label = curve = None
                basis_label = "flat"
                center = med
        else:
            center = med

        if robust_sd == 0:                      # constant metric: no variation to score
            _mark_unscored(m, p, pts, "no variation to score (constant value)")
            continue
        z = (observed - center) / robust_sd
        # percent change is quoted against whatever the z is measured against:
        # the seasonal expectation when cyclical, else the flat median.
        pct_ref = center if (expected_series and center) else med
        pct = (observed - pct_ref) / abs(pct_ref) * 100 if pct_ref else 0.0
        anom = abs(z) >= z_threshold and abs(pct) >= min_pct

        scored.append({
            "metric": _label(m, p),     # display label / unique chart key
            "metric_name": m,           # raw METRIC_NAME (for the SQL suggestion)
            "pipeline": p,
            "scored": True,
            "reason": "",
            "latest": observed,
            "latest_at": observed_at,
            "mean": mean(baseline),
            "median": med,
            "center": center,           # z reference (smooth expected or median)
            "std": robust_sd,
            "expected": expected_series,        # per-point expected curve or None
            "curve": curve,                     # dense smooth render series or None
            "basis": basis_label,               # 'seasonal ~24h' | 'smooth trend' | 'flat'
            "period_label": period_label,
            "z": z,
            "pct": pct,
            "n": n_pts,
            "series": pts,
            "direction": "drop" if observed < center else "spike",
            "anom": anom,
        })

    # attach display-formatted values (timestamps in the DISPLAY timezone).
    # unscored rows get them too — they still render in the table.
    _dtz = _tzinfo(tzname)
    for s in scored + unscored:
        s["latest_fmt"] = fmt(s["latest"])
        s["median_fmt"] = fmt(s["median"])
        s["latest_at_fmt"] = (_local(s["latest_at"], _dtz).strftime("%b %d %H:%M")
                              if s["latest_at"] else "—")
        s["latest_at_iso"] = (s["latest_at"].isoformat() if s["latest_at"] else "")
        s["latest_at_epoch"] = (s["latest_at"].timestamp() if s["latest_at"] else 0)

    # rank anomalies by severity; sort the rest by |z| for the drift view
    scored.sort(key=lambda s: (not s["anom"], -abs(s["z"])))
    anomalies = [s for s in scored if s["anom"]]

    # hover labels render in the DISPLAY timezone, like the rest of the page
    _tz = _tzinfo(tzname)
    _lt = lambda d, f="%b %d %H:%M": _local(d, _tz).strftime(f)

    focus = scored[0] if scored else None
    rng = (agg_start, agg_end) if range_mode else None
    chart = _svg_series(focus, z_threshold, rng=rng, lt=_lt) if focus else None

    # precompute a client-swappable chart payload for EVERY scored metric,
    # keyed by metric name. The browser just swaps rendered geometry on click;
    # all the detector math stays here in Python.
    charts = {}
    for s in scored:
        geo = _svg_series(s, z_threshold, rng=rng, lt=_lt)
        charts[s["metric"]] = {
            "line": geo["line"], "band": geo["band"], "bands": geo["bands"],
            "mid": geo["mid"],
            "anom": geo["anom"], "w": geo["w"], "h": geo["h"],
            "points": geo["points"], "band_hi": geo["band_hi"],
            "band_lo": geo["band_lo"], "center_fmt": geo["center_fmt"],
            "xticks": geo["xticks"],
            "target_table": _target_table(s["metric_name"], s["pipeline"]),
            "rangex": geo.get("rangex"), "avgline": geo.get("avgline"),
            "metric": s["metric"], "pipeline": s["pipeline"],
            "metric_name": s["metric_name"],
            "leaf": s["metric"].split(".")[-1],
            "pct": round(s["pct"], 1), "z": round(s["z"], 1),
            "basis": s.get("basis", "flat"),        # "seasonal ~24h" | "flat"
            "period_label": s.get("period_label"),
            "direction": s["direction"], "is_anom": s["anom"],
            "latest_fmt": s["latest_fmt"], "median_fmt": s["median_fmt"],
            "latest_at_fmt": s["latest_at_fmt"],
            "n": s["n"],
        }

    # Three-level grouping for the UI: family -> pipeline -> metric subject.
    # dbt emits one PIPELINE_NAME per model, so grouping on pipeline alone left
    # 81 one-row groups; the family layer collects them (stg_braze, dim, ...).
    # Any level holding a single child is marked `solo` and the template folds
    # it away, so a simple lambda pipeline still renders flat.
    # `scored` is already ordered (anomalies first, then |z|), so first-seen
    # order gives each group a sensible internal ranking.
    def _bump(node, s):
        node["n"] += 1
        node["n_anom"] += 1 if s["anom"] else 0
        node["worst_z"] = max(node["worst_z"], abs(s["z"]))

    def _new(**kw):
        return dict(n=0, n_anom=0, worst_z=0.0, **kw)

    # Two collapsible levels, same shape as the timeline: GROUP -> FAMILY ->
    # metric rows. (The old pipeline+subject sub-levels made this a flat mess —
    # each metric row already names its own pipeline in its own column.)
    # scored first (anomalies ranked), then the "not scored" rows, so every
    # pipeline is listed and nothing vanishes because of a detector gate.
    all_rows = scored + unscored
    groups = []
    gidx, famidx = {}, {}
    for s in all_rows:
        pipe = s["pipeline"] or "—"
        fam = _pipeline_family(pipe, pay_by_pipe.get(pipe, ""))
        grp_label = _pipeline_group(fam)[0]
        s["leaf_name"] = _metric_parts(s["metric_name"])[1]

        if grp_label not in gidx:
            gidx[grp_label] = len(groups)
            groups.append(_new(group=grp_label, families=[]))
        g = groups[gidx[grp_label]]

        if (grp_label, fam) not in famidx:
            famidx[(grp_label, fam)] = len(g["families"])
            g["families"].append(_new(family=fam, rows=[]))
        f = g["families"][famidx[(grp_label, fam)]]

        f["rows"].append(s)
        for node in (g, f):
            _bump(node, s)

    # alphabetical at both levels; a group holding ONE family folds that header
    # away (the group header already says it).
    for g in groups:
        g["families"].sort(key=lambda x: (x.get("family") or "").lower())
        g["worst_z"] = round(g["worst_z"], 1)
        g["solo"] = len(g["families"]) == 1
        for f in g["families"]:
            f["worst_z"] = round(f["worst_z"], 1)
    groups.sort(key=lambda g: (g.get("group") or "").lower())

    # Sensitivity curve: how many metrics would trip at other z thresholds,
    # holding min_pct fixed. Computed from the already-scored metrics, so it is
    # free — no re-fetch, no re-detect. Lets you see the cost of loosening the
    # threshold BEFORE applying it.
    z_grid = sorted({1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0, round(z_threshold, 2)})
    sensitivity = [
        {"z": zc,
         "n": sum(1 for s in scored
                  if abs(s["z"]) >= zc and abs(s["pct"]) >= min_pct),
         "current": abs(zc - z_threshold) < 1e-9}
        for zc in z_grid
    ]
    sens_max = max((p["n"] for p in sensitivity), default=0) or 1
    for p in sensitivity:
        p["pct_of_max"] = round(p["n"] / sens_max * 100, 1)

    # drift inspector: top movers (by |pct|) among reasonably-behaved metrics
    drift = sorted(scored, key=lambda s: -abs(s["pct"]))[:6]

    # overall drift score = mean |z| of tracked metrics, capped display
    drift_score = round(mean(abs(s["z"]) for s in scored), 2) if scored else 0.0

    return {
        "now": _local(now, _tzinfo(tzname)),
        "tz_abbr": _tz_abbr(_tzinfo(tzname), now),
        "tzname": tzname,
        # the exact span the detector looked at, localised for display
        "win_start": _local(win_start, _dtz),
        "win_end": _local(win_end, _dtz),
        "latest_point_at": (_local(max((s["latest_at"] for s in scored)), _dtz)
                            if scored else None),
        "env": env,
        "baseline_days": baseline_days,
        "z_threshold": z_threshold,
        "total_obs": total_obs,
        "tracked": len(scored),
        "freshness_metrics": skipped_fresh,
        "n_anom": len(anomalies),
        "drift_score": drift_score,
        "focus": focus,
        "chart": chart,
        "charts": charts,
        "min_pct": min_pct,
        "range_mode": range_mode,
        # localised for display; None outside range mode
        "range_start": _local(agg_start, _dtz) if agg_start else None,
        "range_end": _local(agg_end, _dtz) if agg_end else None,
        "anomalies": anomalies[:12],
        # EVERY metric — scored plus the "not scored" ones (with a reason), so
        # no pipeline silently disappears. Searchable/sortable client-side.
        "table": all_rows,
        "n_unscored": len(unscored),
        "groups": groups,          # pipeline -> subject -> metrics (collapsible)
        "min_points": min_points,
        "row_limit": row_limit,
        # header selectors: available options + what's currently picked
        "filter_options": filter_options,
        "group_filter": group_filter or "",
        "family_filter": family_filter or "",
        "pipeline_filter": pipeline_filter or "",
        "sensitivity": sensitivity,
        "skipped_low_points": skipped_low_points,
        "skipped_low_volume": skipped_low_volume,
        "skipped_non_rowcount": skipped_non_rowcount,
        "drift": drift,
        "coverage": round(len(scored) / max(len(series), 1) * 100),
    }


def _spline(points, *, close_to=None):
    """Catmull-Rom → cubic-Bézier smooth path through `points` [(x, y), …].
    Returns an SVG path string. With < 3 points it degrades to straight
    segments. `close_to`, if given as 'L', starts the path with an L (so a
    second spline can continue a filled shape) instead of an M."""
    if not points:
        return ""
    head = close_to or "M"
    if len(points) < 3:
        return head + f"{points[0][0]},{points[0][1]}" + \
            "".join(f" L{x},{y}" for x, y in points[1:])
    n = len(points)
    d = f"{head}{points[0][0]},{points[0][1]}"
    for i in range(n - 1):
        p0 = points[i - 1] if i > 0 else points[i]
        p1, p2 = points[i], points[i + 1]
        p3 = points[i + 2] if i + 2 < n else p2
        c1x = round(p1[0] + (p2[0] - p0[0]) / 6, 1)
        c1y = round(p1[1] + (p2[1] - p0[1]) / 6, 1)
        c2x = round(p2[0] - (p3[0] - p1[0]) / 6, 1)
        c2y = round(p2[1] - (p3[1] - p1[1]) / 6, 1)
        d += f" C{c1x},{c1y} {c2x},{c2y} {p2[0]},{p2[1]}"
    return d


def _svg_series(s, z_threshold, w=800, h=200, pad=8, rng=None, lt=None):
    """Build SVG path data for observed line + expected±kσ envelope.

    When the metric is cyclical (`s["curve"]` set), the envelope is a SMOOTH
    spline whose half-width BREATHES with the cycle (per-phase σ) — the
    "prediction envelope" look. Otherwise it's a flat band at the baseline
    median (old behaviour). The observed line stays angular either way.
    If rng=(start,end) is given, also return the x-bounds of that window
    (for shading) and a horizontal line at the range's average value.
    `lt` formats a UTC datetime into the display timezone for hover labels."""
    pts = s["series"]
    ts = [p[0] for p in pts]
    vals = [p[1] for p in pts]
    t0, t1 = ts[0].timestamp(), ts[-1].timestamp()
    tspan = (t1 - t0) or 1
    std = s["std"]

    # Per-point center at each OBSERVED timestamp (drives hover-z + the flat
    # band): the seasonal expected curve when a cycle was found, else the flat
    # baseline median.
    exp = s.get("expected")
    if exp and len(exp) == len(pts):
        centers = [float(e) for e in exp]
    else:
        centers = [s["median"]] * len(pts)
    center = centers[-1]                         # summary reference = latest expectation

    # Dense smooth render series (only when cyclical): even-grid timestamps,
    # expected value, and PER-PHASE σ so the band breathes with the rhythm.
    curve = s.get("curve")
    use_curve = bool(curve and len(curve.get("exp", [])) >= 3
                     and len(curve["exp"]) == len(curve["ts"]) == len(curve["sigma"]))
    c_ts = curve["ts"] if use_curve else []
    c_exp = [float(v) for v in curve["exp"]] if use_curve else []
    c_sig = [float(v) for v in curve["sigma"]] if use_curve else []

    # Every band we draw, widest first so narrower bands paint ON TOP: the
    # confidence "fan" (90%, 95%, …) plus the detection threshold as the outer
    # ALERT boundary. `mult` is the σ-multiplier; the y-scale must fit the widest.
    band_specs = [{"label": lbl, "mult": m, "alert": False}
                  for lbl, m in CONFIDENCE_BANDS]
    band_specs.append({"label": "±%gσ" % round(z_threshold, 2),
                       "mult": z_threshold, "alert": True})
    band_specs.sort(key=lambda b: -b["mult"])            # widest -> narrowest
    max_mult = max((b["mult"] for b in band_specs), default=z_threshold)

    # y-scale must contain the observed line AND the widest band. For the curve
    # that means the per-phase envelope extent; for the flat band, center ± kσ.
    if use_curve:
        band_lows = [e - max_mult * sg for e, sg in zip(c_exp, c_sig)]
        band_highs = [e + max_mult * sg for e, sg in zip(c_exp, c_sig)]
    else:
        band_lows = [c - max_mult * std for c in centers]
        band_highs = [c + max_mult * std for c in centers]
    vmin = min(min(vals), min(band_lows))
    vmax = max(max(vals), max(band_highs))
    vspan = (vmax - vmin) or 1

    def X(t):
        return round((t.timestamp() - t0) / tspan * (w - 2 * pad) + pad, 1)

    def Y(v):
        return round(h - pad - (v - vmin) / vspan * (h - 2 * pad), 1)

    line = " ".join(f"{'M' if i == 0 else 'L'}{X(t)},{Y(v)}"
                    for i, (t, v) in enumerate(pts))

    def _ribbon(mult):
        """±mult·σ envelope, closed. Cyclical → SMOOTH spline on the dense grid
        with per-phase width; flat → straight ribbon at the constant σ. Returns
        (path, hi_latest, lo_latest)."""
        if use_curve:
            top = [(X(t), Y(e + mult * sg)) for t, e, sg in zip(c_ts, c_exp, c_sig)]
            bot = [(X(t), Y(e - mult * sg)) for t, e, sg in zip(c_ts, c_exp, c_sig)]
            d = _spline(top) + " " + _spline(list(reversed(bot)), close_to="L") + " Z"
            return d, c_exp[-1] + mult * c_sig[-1], c_exp[-1] - mult * c_sig[-1]
        ups = [c + mult * std for c in centers]
        los = [c - mult * std for c in centers]
        top = " ".join(f"{'M' if i == 0 else 'L'}{X(t)},{Y(u)}"
                       for i, (t, u) in enumerate(zip(ts, ups)))
        bot = " ".join(f"L{X(t)},{Y(l)}"
                       for t, l in zip(reversed(ts), reversed(los)))
        return f"{top} {bot} Z", ups[-1], los[-1]

    # nested fan: opacity deepens inward (widest = faintest). The alert band gets
    # a dashed outline so the detection boundary reads distinct from the context.
    bands = []
    for i, spec in enumerate(band_specs):
        d, hi, lo = _ribbon(spec["mult"])
        bands.append({
            "d": d, "label": spec["label"], "z": round(spec["mult"], 2),
            "hi": fmt(hi), "lo": fmt(lo), "alert": spec["alert"],
            "fill": round(0.05 + 0.05 * i, 3),      # 0.05 (outer) … deeper inward
        })
    # legacy single-band key = the detection (alert) ribbon, for any old caller
    band = next((b["d"] for b in bands if b["alert"]), bands[-1]["d"] if bands else "")
    # centre line: smooth spline on the dense grid when cyclical, else straight
    if use_curve:
        mid = _spline([(X(t), Y(e)) for t, e in zip(c_ts, c_exp)])
    else:
        mid = " ".join(f"{'M' if i == 0 else 'L'}{X(t)},{Y(c)}"
                       for i, (t, c) in enumerate(zip(ts, centers)))

    # Highlight ONLY the scored (latest) point, and only when it's the anomaly —
    # peppering every historical out-of-band poke with a red dot made healthy
    # metrics look alarming and read inconsistently between charts. (Range mode
    # overrides this below with the range-average marker.)
    anom_pts = ([{"x": X(ts[-1]), "y": Y(vals[-1])}]
                if (rng is None and s.get("anom")) else [])

    # every observed point, with its REAL value + timestamp, so the chart can
    # show actual data on hover instead of just geometry. z is measured against
    # that point's own expected value (seasonal-aware).
    fmt_ts = lt or (lambda d, f="%Y-%m-%d %H:%M": d.strftime(f))
    points = [{
        "x": X(t), "y": Y(v),
        "v": v,
        "vf": fmt(v),
        "t": fmt_ts(t),
        "z": round((v - c) / std, 2) if std else 0.0,
        "out": bool(std and abs((v - c) / std) >= z_threshold),
    } for (t, v), c in zip(pts, centers)]

    # x-axis date ticks (~5, evenly spaced). Rendered as HTML under the SVG —
    # the chart stretches with preserveAspectRatio="none", which would distort
    # any <text> drawn inside it.
    # Space ticks evenly across TIME (not across point indices — observations are
    # irregularly spaced, which would bunch every label at the busy end).
    n_ticks = min(5, len(pts))
    xticks = []
    if n_ticks:
        span_days = (ts[-1] - ts[0]).total_seconds() / 86400.0
        # Multi-day views label ticks with the weekday too (e.g. "Mon Jul 15"),
        # so a reader can see the weekly cadence at a glance; intraday stays H:M.
        tick_fmt = "%a %b %d" if span_days >= 2 else "%H:%M"
        for i in range(n_ticks):
            frac = i / (n_ticks - 1) if n_ticks > 1 else 0.0
            t = ts[0] + (ts[-1] - ts[0]) * frac
            left = (pad + frac * (w - 2 * pad)) / w * 100
            xticks.append({"left": round(left, 2), "label": fmt_ts(t, tick_fmt)})

    # summary band edges = the detection (alert) band at the scored (latest) point
    alert_band = next((b for b in bands if b["alert"]), bands[-1] if bands else None)
    out = {"line": line, "band": band, "bands": bands, "mid": mid,
           "anom": anom_pts, "points": points, "w": w, "h": h, "xticks": xticks,
           "band_hi": alert_band["hi"] if alert_band else "—",
           "band_lo": alert_band["lo"] if alert_band else "—",
           "center_fmt": fmt(center)}

    if rng is not None:
        rs, re = rng
        x0 = max(pad, X(rs))
        x1 = min(w - pad, X(re))
        out["rangex"] = {"x": round(x0, 1), "w": round(max(0, x1 - x0), 1)}
        # in range mode s["latest"] is the period average
        avg_y = Y(s["latest"])
        out["avgline"] = f"M{x0},{avg_y} L{x1},{avg_y}"
        # markers: highlight the range-average point at range midpoint
        out["anom"] = [{"x": round((x0 + x1) / 2, 1), "y": avg_y}] if s["anom"] else []

    return out


def fmt(v):
    """Human number formatting for templates."""
    if v is None:
        return "—"
    av = abs(v)
    if av >= 1e9:
        return f"{v/1e9:.2f}B"
    if av >= 1e6:
        return f"{v/1e6:.2f}M"
    if av >= 1e3:
        return f"{v/1e3:,.1f}K"
    if av >= 1:
        return f"{v:,.1f}"
    return f"{v:.4f}"
