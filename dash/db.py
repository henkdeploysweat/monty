"""
monty/db.py
-----------
Fetches raw Monty events. In prod this hits Snowflake; for local dev set
MONTY_SOURCE=csv and point MONTY_CSV at the sample export.

Both paths return a list of dicts with native Python types
(OCCURRED_AT -> datetime, METRIC_VALUE -> float|None, IS_ALERT -> bool),
which is exactly what monty/transform.py expects.
"""
from __future__ import annotations
import logging
import os
import threading
import time
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("monty.db")

# ---- CONFIG: the one line most people need to change --------------------
MONTY_TABLE = os.environ.get("MONTY_TABLE", "MONITORING_DB.MONITORING.CUSTOM_METRICS")
# MONTY_TABLE must be set to MONITORING_DB.MONITORING.CUSTOM_METRICS
# OCCURRED_AT is TIMESTAMP_NTZ holding wall-clock in THIS zone (the ingestion
# writer's session tz). The SQL normalises it to UTC so everything downstream
# is UTC. Set to 'UTC' once ingestion is pinned to UTC (then it's a no-op).
MONTY_SOURCE_TZ = os.environ.get("MONTY_SOURCE_TZ", "America/Los_Angeles")
# The Segment events database whose INFORMATION_SCHEMA the event-search page
# introspects. Override with SEGMENT_DB if yours is named differently.
SEGMENT_DB = os.environ.get("SEGMENT_DB", "SEGMENT_EVENTS")
# Warehouse whose credit usage is charted under the timeline.
MONTY_WAREHOUSE = os.environ.get("MONTY_WAREHOUSE", "MONTY_WH")

# ── Snowflake ENVIRONMENT routing ───────────────────────────────────────────
# EDIT THIS to define which Snowflake CUSTOM_METRICS.ENVIRONMENT value(s) show
# up under each DASHBOARD env (the ?env= / PROD-DEV toggle). Examples:
#   "prod": ["prod"]                     -> prod shows only ENVIRONMENT='prod'
#   "prod": ["prod", "default", None]    -> also fold in 'default' + unlabelled
#                                           (None means ENVIRONMENT IS NULL) rows
#   "dev":  ["dev", "staging"]           -> dev shows dev AND staging
# Any Snowflake env not listed under some dashboard env is DROPPED from the
# dashboards. Rows returned for a dashboard env are relabelled to it, so e.g.
# folded 'default' rows display as a single 'prod' environment.
ENV_ROUTING = {
    # NOTE: NULL/None deliberately NOT in prod — dev pipelines (e.g. audiences)
    # have stray NULL-environment rows, and folding NULL into prod leaked them in
    # as stale ghost lanes. 'default' (the dbt/auditor bucket) stays.
    "prod": ["prod", "Prod", "default"],
    "dev":  ["dev"],
}


def snowflake_envs_for(dashboard_env: str) -> list:
    """The Snowflake ENVIRONMENT value(s) mapped to this dashboard env (see
    ENV_ROUTING). Unconfigured envs fall back to [dashboard_env]."""
    return ENV_ROUTING.get(dashboard_env, [dashboard_env])


def _snowflake_env_filter(dashboard_env: str, column: str = "ENVIRONMENT") -> str:
    """Build the SQL predicate selecting this dashboard env's Snowflake rows from
    ENV_ROUTING, e.g. prod=["prod","default",None] ->
    "(ENVIRONMENT IN ('prod','default') OR ENVIRONMENT IS NULL)". Values are
    trusted config; single quotes are escaped defensively. Empty list -> 1=0."""
    envs = snowflake_envs_for(dashboard_env)
    literals = [e for e in envs if e is not None]
    parts = []
    if literals:
        quoted = ", ".join("'%s'" % str(e).replace("'", "''") for e in literals)
        parts.append("%s IN (%s)" % (column, quoted))
    if any(e is None for e in envs):
        parts.append("%s IS NULL" % column)
    return "(" + " OR ".join(parts) + ")" if parts else "1=0"
# ---------------------------------------------------------------------------
# PAUSED: the warehouse-credits chart (SNOWFLAKE.ACCOUNT_USAGE.*).
# Those two queries were the slowest thing in the app (credits 1.6-11.8s, peaks
# ~1.6s) and the view lags real time by 1-3h anyway. The query code, the SQL
# files (sql/warehouse_credits.sql, sql/warehouse_credit_peaks.sql) and the
# chart are all left INTACT — this flag just short-circuits the fetch.
# Re-enable with:  MONTY_ENABLE_CREDITS=1
# ---------------------------------------------------------------------------
ENABLE_CREDITS = os.environ.get("MONTY_ENABLE_CREDITS", "0").lower() in ("1", "true", "yes")
# S3 event source (MONTY_SOURCE=s3). Events live in a per-environment bucket,
# day-partitioned at the root:  s3://monty-<env>-metrics/run_date=YYYYMMDD/*.parquet
# The bucket name is a pattern with a {env} placeholder filled from the `env`
# arg, so the dashboards' PROD/DEV toggle selects the bucket. MONTY_S3_PREFIX is
# an optional key prefix BEFORE the run_date= partition (default: none / root).
MONTY_S3_BUCKET = os.environ.get("MONTY_S3_BUCKET", "monty-{env}-metrics")
MONTY_S3_PREFIX = os.environ.get("MONTY_S3_PREFIX", "").strip("/")
# The S3 writer (lambdas/shared/s3_writer.py) stamps rows in UTC, so — unlike
# the Snowflake NTZ path (MONTY_SOURCE_TZ) — no zone shift is needed. Override
# only if a future export writes wall-clock in another zone.
MONTY_S3_TZ = os.environ.get("MONTY_S3_TZ", "UTC")
# dev and prod buckets live in SEPARATE AWS accounts, so a single credential
# can't read both. Resolution order for the AWS profile, given the dashboard's
# env:
#   1. MONTY_S3_PROFILE_<ENV>  — explicit per-env profile (names need not match
#      a pattern), e.g. MONTY_S3_PROFILE_DEV=audiences-dev
#                       MONTY_S3_PROFILE_PROD=SWEATAnalytics
#   2. MONTY_S3_PROFILE        — a {env} pattern, e.g. "monty-{env}"
#   3. unset                   — the default credential chain (single account,
#      env keys, or an assumed instance/task role)
MONTY_S3_PROFILE = os.environ.get("MONTY_S3_PROFILE", "").strip()
# DynamoDB event source (MONTY_SOURCE=dynamo). Post-cutover, warning/info
# metrics land as items in a per-environment table (lambdas/shared/
# dynamo_writer.py): pk = "<env>#<pipeline>", sk = "<occurred_at ISO UTC>#<uuid>".
# Table name is a {env} pattern like the S3 bucket; the same per-env AWS
# profile resolution (_s3_profile_for) applies — dev and prod tables live in
# separate accounts.
MONTY_DDB_TABLE = os.environ.get("MONTY_DDB_TABLE", "monty-{env}-metrics-ddb")
MONTY_DDB_REGION = os.environ.get("MONTY_DDB_REGION", "us-east-1")
# Comma-separated pipeline names, to skip the discovery Scan (_ddb_pipelines).
# Only worth setting if that Scan's cost is a problem AND the set is stable —
# a pipeline missing from this list is INVISIBLE to the dashboard.
MONTY_DDB_PIPELINES = os.environ.get("MONTY_DDB_PIPELINES", "").strip()
# Reads are parallelised per pipeline. Measured: 4 workers 2.1s, 8 workers 3.7s
# — beyond ~4 the concurrent streams contend for the same uplink and it gets
# SLOWER. Do not raise this without re-measuring.
MONTY_DDB_WORKERS = int(os.environ.get("MONTY_DDB_WORKERS", "4"))


def _s3_profile_for(env: str) -> str | None:
    """AWS profile to use for this environment's bucket (None = default chain)."""
    explicit = os.environ.get("MONTY_S3_PROFILE_%s" % str(env).upper(), "").strip()
    if explicit:
        return explicit
    if MONTY_S3_PROFILE:
        return MONTY_S3_PROFILE.format(env=env)
    return None
# -------------------------------------------------------------------------

SQL_DIR = Path(__file__).parent / "sql"


def _read_sql(name: str) -> str:
    return (SQL_DIR / name).read_text()


# Reuse an existing snowsql connection instead of exporting SNOWFLAKE_* (and
# leaking the password into shell history). Set MONTY_SNOWSQL_PROFILE=dev to
# read [connections.dev] from ~/.snowsql/config. Env vars still win per-field.
MONTY_SNOWSQL_PROFILE = os.environ.get("MONTY_SNOWSQL_PROFILE", "").strip()
SNOWSQL_CONFIG = Path(os.environ.get("SNOWSQL_CONFIG",
                                     str(Path.home() / ".snowsql" / "config")))

# snowsql config key -> snowflake.connector.connect kwarg
_SNOWSQL_KEYS = {
    "accountname": "account", "username": "user", "password": "password",
    "warehousename": "warehouse", "rolename": "role",
    "dbname": "database", "schemaname": "schema",
    "authenticator": "authenticator", "host": "host",
}


def _snowsql_profile(name: str) -> dict:
    """Read [connections.<name>] from ~/.snowsql/config into connect() kwargs.
    Values may be quoted in snowsql config, so strip surrounding quotes."""
    import configparser
    parser = configparser.ConfigParser()
    if not SNOWSQL_CONFIG.exists():
        raise FileNotFoundError("snowsql config not found: %s" % SNOWSQL_CONFIG)
    parser.read(SNOWSQL_CONFIG)
    section = "connections.%s" % name if name else "connections"
    if not parser.has_section(section):
        raise KeyError("no [%s] in %s" % (section, SNOWSQL_CONFIG))
    out = {}
    for key, value in parser[section].items():
        kwarg = _SNOWSQL_KEYS.get(key.lower())
        if kwarg:
            out[kwarg] = value.strip().strip('"').strip("'")
    return out


# --- connection reuse + result caching -----------------------------------
# A page load makes 4 Snowflake queries. Opening a connection costs ~1.6s of
# auth handshake, so a fresh connection per query burned ~6.5s doing nothing.
# Keep ONE connection per process, guarded by a lock (a connector connection is
# not safe for concurrent cursors), and reopen it if it drops.
_CONN = None
_CONN_LOCK = threading.Lock()

# ACCOUNT_USAGE views lag real time by 1-3h and are slow (the credits query
# alone measured 1.6-11.8s), so caching them costs no freshness at all.
CACHE_TTL = {
    "credits": int(os.environ.get("MONTY_TTL_CREDITS", "300")),      # 5 min
    "peaks": int(os.environ.get("MONTY_TTL_PEAKS", "1800")),         # 30 min
    "last_seen": int(os.environ.get("MONTY_TTL_LAST_SEEN", "120")),  # 2 min
    # S3 warn/info events: today's partition is thousands of tiny live-write
    # files (~9s to read). A short cache makes every refresh after the first
    # instant, at the cost of being at most this many seconds stale.
    "s3": int(os.environ.get("MONTY_TTL_S3", "60")),                 # 1 min
    # DynamoDB warn/info events: the detail lane is a per-pipeline Query over
    # the visible window (~2s) — same short cache as the S3 leg.
    "dynamo": int(os.environ.get("MONTY_TTL_DYNAMO", "60")),         # 1 min
    # The trailing lane is 7d of (occurred_at, pipeline_name) — 165k rows, ~21s,
    # and the single dominant cost of a timeline render. It feeds ONLY cadence
    # and last-seen, which move on the order of hours, so a long cache costs no
    # meaningful freshness: a pipeline's median session gap does not change
    # because 15 minutes passed. The detail lane stays on the 60s TTL, so the
    # bars the user is actually looking at are still near-live.
    "dynamo_trailing": int(os.environ.get("MONTY_TTL_DYNAMO_TRAILING", "900")),
    # The pipeline list (distinct pk) costs a full-table Scan — ~30s — because
    # DynamoDB cannot answer "distinct partition keys" any other way. It changes
    # when a pipeline is added, i.e. approximately never, so cache it hard.
    "ddb_pipelines": int(os.environ.get("MONTY_TTL_DDB_PIPELINES", "3600")),
    # The settled hourly-rollup lane (rollup.py): pre-aggregated, immutable once
    # a hour has passed, so a long cache is free — only the live tail is re-read.
    "dynamo_rollup": int(os.environ.get("MONTY_TTL_DYNAMO_ROLLUP", "900")),
}
_CACHE: dict = {}
_CACHE_LOCK = threading.Lock()


def _cached(bucket: str, key, producer):
    """Memoize `producer()` under (bucket, key) for CACHE_TTL[bucket] seconds."""
    ttl = CACHE_TTL.get(bucket, 0)
    if ttl <= 0:
        return producer()
    now = time.monotonic()
    ck = (bucket, key)
    with _CACHE_LOCK:
        hit = _CACHE.get(ck)
        if hit and now - hit[0] < ttl:
            logger.info("cache hit: %s %s (age %.0fs)", bucket, key, now - hit[0])
            return hit[1]
    value = producer()                    # produced outside the lock
    with _CACHE_LOCK:
        _CACHE[ck] = (now, value)
    return value


def clear_cache():
    """Drop every memoized query result (used by tests)."""
    with _CACHE_LOCK:
        _CACHE.clear()


def _shared_conn():
    """The process-wide Snowflake connection, opened on first use and
    transparently reopened if it has been closed or expired."""
    global _CONN
    with _CONN_LOCK:
        if _CONN is not None:
            try:
                if not _CONN.is_closed():
                    return _CONN
            except Exception:               # connector object is unusable
                pass
        _CONN = _connect()
        logger.info("snowflake: opened shared connection")
        return _CONN


def _query(sql: str, params: dict | None = None):
    """Run a query on the shared connection; retry once on a dropped session."""
    logger.info(f"query : {sql}")
    global _CONN
    for attempt in (1, 2):
        try:
            conn = _shared_conn()
            with _CONN_LOCK:               # one cursor at a time on this conn
                cur = conn.cursor()
                cur.execute(sql, params or {})
                cols = [c[0] for c in cur.description]
                return cols, cur.fetchall()
        except Exception as exc:
            if attempt == 2:
                raise
            logger.warning("snowflake: query failed (%s); reconnecting", exc)
            with _CONN_LOCK:
                try:
                    if _CONN:
                        _CONN.close()
                except Exception:
                    pass
                _CONN = None


def _connect():
    """Open a Snowflake connection.

    Credentials come from ~/.snowsql/config when MONTY_SNOWSQL_PROFILE is set
    (no secrets in the environment), otherwise from the SNOWFLAKE_* env vars.
    Individual SNOWFLAKE_* vars always override the profile, so you can point a
    profile at a different warehouse/role without editing the file."""
    import snowflake.connector

    params: dict = {}
    if MONTY_SNOWSQL_PROFILE:
        params.update(_snowsql_profile(MONTY_SNOWSQL_PROFILE))
        logger.info("snowflake: using snowsql profile %r", MONTY_SNOWSQL_PROFILE)

    # env vars win per-field; account/user are required if no profile supplied one
    overrides = {
        "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
        "user": os.environ.get("SNOWFLAKE_USER"),
        "password": os.environ.get("SNOWFLAKE_PASSWORD"),
        "authenticator": os.environ.get("SNOWFLAKE_AUTHENTICATOR"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
        "role": os.environ.get("SNOWFLAKE_ROLE"),
    }
    params.update({k: v for k, v in overrides.items() if v})

    params.setdefault("authenticator", "snowflake")
    # NOTE: do NOT force a warehouse. If neither the profile nor SNOWFLAKE_WAREHOUSE
    # names one, let Snowflake use the user's DEFAULT_WAREHOUSE — passing a
    # non-existent name silently yields a session with no active warehouse
    # ("No active warehouse selected", error 000606) on the first query.
    for required in ("account", "user"):
        if not params.get(required):
            raise RuntimeError(
                "Snowflake %s not configured — set SNOWFLAKE_%s or "
                "MONTY_SNOWSQL_PROFILE (e.g. =dev)" % (required, required.upper()))
    return snowflake.connector.connect(**params)


def fetch_events(lookback_days: int = 7, env: str = "prod",
                 now: datetime | None = None,
                 detail_days: int | None = None) -> list[dict]:
    """Return event rows for the given environment over the lookback window.
    OCCURRED_AT / SENT_AT come back normalised to UTC.

    `detail_days` is a HINT, not a contract: "I only need full rows for the last
    N days; older rows may carry occurred_at + pipeline_name alone." The
    timeline uses it because its trailing history feeds only cadence and
    last-seen (transform.py:246-248) — on DynamoDB that turns a 155s read into
    ~23s cold / ~2s warm, because payload is ~3x of all other bytes.

    A source is always free to IGNORE the hint and return full rows: every
    caller must treat the extra fields as possibly-present, never
    possibly-absent. Only the DynamoDB leg honours it today. Pass None (the
    default) to require full rows throughout — the anomaly page does, since it
    reads METRIC_VALUE across the whole window.
    """
    source = os.environ.get("MONTY_SOURCE", "snowflake").lower()
    if source == "csv":
        rows = _fetch_csv(lookback_days, env, now)
    elif source == "s3":
        rows = _fetch_s3(lookback_days, env, now)
    elif source in ("dynamo", "ddb", "dynamodb"):
        rows = _fetch_dynamo(lookback_days, env, now, detail_days)
    elif source == "sqlite":
        rows = _fetch_sqlite(lookback_days, env, now)
    elif source == "both":
        rows = _fetch_both(lookback_days, env, now, detail_days)
    else:
        rows = _fetch_snowflake(lookback_days, env, now)

    # Braze CDI sync status — PROD ONLY. Live-fetched + SQLite-cached, merged in
    # so the delete/attribute syncs show under the `braze-cdisync` family. A no-op
    # unless BRAZE_REST_ENDPOINT + BRAZE_API_KEY are set; never breaks the page.
    if env == "prod":
        try:
            import braze_cdi
            cdi = braze_cdi.fetch_events(lookback_days, now)
            if cdi:
                rows = rows + cdi
                rows.sort(key=lambda r: r.get("OCCURRED_AT") or datetime.min)
        except Exception as exc:
            logger.error("braze cdi: merge failed, rendering without it: %s", exc)
    return rows


def _fetch_snowflake(lookback_days, env, now):
    """Live event rows. Uses the shared connection (saves the ~1.6s handshake)
    but is deliberately NOT cached — this is the data the dashboard is for."""
    sql = (_read_sql("fetch_events.sql")
           .replace("{{table}}", MONTY_TABLE)
           .replace("{{env_filter}}", _snowflake_env_filter(env)))
    # %(now)s, when supplied, is a UTC anchor; the SQL converts it into the
    # source zone for the (pruning-friendly) range predicate.
    params = {"lookback_days": lookback_days, "src_tz": MONTY_SOURCE_TZ}
    if now is not None:
        # override the live clock (SYSDATE) for reproducible/testing windows
        sql = sql.replace("SYSDATE()", "%(now)s")
        params["now"] = now
    cols, rows = _query(sql, params)
    out = [dict(zip(cols, row)) for row in rows]
    # every returned row was routed to this dashboard env (see ENV_ROUTING), so
    # present a single consistent label — folded envs (e.g. 'default') show as env
    for r in out:
        r["ENVIRONMENT"] = env
    return out


def _fetch_csv(lookback_days, env, now):
    import csv
    from datetime import timedelta
    from email.utils import parsedate_to_datetime

    def parse_ts(s):
        if not s:
            return None
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            return parsedate_to_datetime(s).replace(tzinfo=None)

    path = os.environ.get("MONTY_CSV", "sample_events.csv")
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            mv = r.get("METRIC_VALUE")
            r["METRIC_VALUE"] = float(mv) if mv not in ("", None) else None
            r["IS_ALERT"] = str(r.get("IS_ALERT")).strip().lower() == "true"
            r["SENT_TO_SLACK"] = str(r.get("SENT_TO_SLACK")).strip().lower() == "true"
            r["OCCURRED_AT"] = parse_ts(r.get("OCCURRED_AT"))
            rows.append(r)

    if now is None:
        now = max(r["OCCURRED_AT"] for r in rows if r["OCCURRED_AT"])
    start = now - timedelta(days=lookback_days)
    return [r for r in rows if r["OCCURRED_AT"] and start <= r["OCCURRED_AT"] <= now]


# --- pipeline retention (keep lanes visible after a pipeline stops) -------
# How long a pipeline stays on the timeline after its last event before it
# drops off entirely. A silent pipeline should show as STALE, not disappear.
PIPELINE_RETENTION_WEEKS = int(os.environ.get("MONTY_RETENTION_WEEKS", "14"))


def fetch_pipeline_last_seen(env: str = "prod", weeks: int | None = None,
                             now: datetime | None = None) -> dict:
    """{pipeline_name: last_seen_utc} for every pipeline active in the last
    `weeks`. Cheap grouped MAX — no payloads. Empty dict when the source can't
    answer (S3/CSV), in which case the timeline just falls back to the lanes it
    can build from the fetched events."""
    weeks = weeks or PIPELINE_RETENTION_WEEKS
    source = os.environ.get("MONTY_SOURCE", "snowflake").lower()

    def _sqlite_last_seen() -> dict:
        """Cheap grouped MAX from the local cache; {} if it isn't populated."""
        from datetime import timedelta
        import sqlite_store
        ref = now or datetime.utcnow()
        start = ref - timedelta(weeks=weeks)
        try:
            return sqlite_store.fetch_pipeline_last_seen(env, start)
        except Exception as exc:
            logger.warning("last_seen: sqlite query failed, no ghost lanes: %s", exc)
            return {}

    if source == "sqlite":
        return _sqlite_last_seen()
    if source in ("csv", "s3", "dynamo", "ddb", "dynamodb"):
        # S3 would mean scanning ~98 day-partitions; DynamoDB a full-table Scan
        # over `weeks` of items; CSV has no history beyond the sample. All
        # degrade gracefully to "no ghost lanes".
        logger.info("last_seen: source=%s cannot answer cheaply; skipping", source)
        return {}

    def run():
        sql = (_read_sql("pipeline_last_seen.sql")
               .replace("{{table}}", MONTY_TABLE)
               .replace("{{env_filter}}", _snowflake_env_filter(env)))
        params = {"weeks": weeks, "src_tz": MONTY_SOURCE_TZ}
        if now is not None:
            sql = sql.replace("SYSDATE()", "%(now)s")
            params["now"] = now
        _, rows = _query(sql, params)
        out = {}
        for name, last_seen in rows:
            if last_seen is not None and getattr(last_seen, "tzinfo", None):
                last_seen = last_seen.replace(tzinfo=None)
            out[name] = last_seen
        logger.info("last_seen: %d pipeline(s) active in the last %dw", len(out), weeks)
        return out

    # minute-bucketed key so repeated loads within the TTL actually hit
    key = (env, weeks, now.replace(second=0, microsecond=0) if now else None)
    snow = _cached("last_seen", key, run)

    # `both`: merge Snowflake (critical/error + dbt pipelines) with the SQLite
    # cache (lambda warning/info pipelines), keeping the latest per pipeline.
    # Applies whenever the warn side includes sqlite (union or sqlite).
    if source == "both" and _BOTH_WARN_SOURCE in ("union", "both", "sqlite"):
        merged = dict(snow)
        for name, ts in _sqlite_last_seen().items():
            prev = merged.get(name)
            if prev is None or (ts is not None and ts > prev):
                merged[name] = ts
        return merged
    return snow


# --- S3 (Parquet) source -------------------------------------------------
_EVENT_COLS = ("ID", "PIPELINE_NAME", "METRIC_NAME", "METRIC_VALUE", "SEVERITY",
               "RUN_ID", "PAYLOAD", "OCCURRED_AT", "IS_ALERT", "SENT_TO_SLACK",
               "SENT_AT", "ENVIRONMENT")


def _to_utc_naive(value):
    """Coerce a Parquet timestamp to a naive-UTC datetime (the downstream
    contract). tz-aware -> converted to UTC; tz-naive -> treated as wall-clock
    in MONTY_S3_TZ and converted (a no-op when MONTY_S3_TZ == 'UTC', which is
    the default — the S3 writer already stamps UTC)."""
    from datetime import timezone
    if value is None:
        return None
    # pyarrow may hand back a date or a non-datetime; only handle datetimes
    if not isinstance(value, datetime):
        return value
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    if MONTY_S3_TZ.upper() == "UTC":
        return value
    from zoneinfo import ZoneInfo
    return (value.replace(tzinfo=ZoneInfo(MONTY_S3_TZ))
                 .astimezone(timezone.utc).replace(tzinfo=None))


def _as_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() == "true"


def _normalise_s3_row(raw: dict) -> dict:
    """Upper-case keys and coerce types to match the Snowflake/CSV row contract."""
    import json
    row = {str(k).upper(): v for k, v in raw.items()}
    mv = row.get("METRIC_VALUE")
    row["METRIC_VALUE"] = float(mv) if mv is not None else None
    row["IS_ALERT"] = _as_bool(row.get("IS_ALERT"))
    row["SENT_TO_SLACK"] = _as_bool(row.get("SENT_TO_SLACK"))
    row["OCCURRED_AT"] = _to_utc_naive(row.get("OCCURRED_AT"))
    row["SENT_AT"] = _to_utc_naive(row.get("SENT_AT"))
    payload = row.get("PAYLOAD")
    if payload is not None and not isinstance(payload, str):
        try:
            row["PAYLOAD"] = json.dumps(payload)
        except (TypeError, ValueError):
            row["PAYLOAD"] = str(payload)
    return row


def _fetch_s3(lookback_days, env, now):
    """Cached wrapper around the S3 read. Today's partition is thousands of tiny
    live-write files, so a cold read is ~9s; the cache (MONTY_TTL_S3, default
    60s) makes every refresh within the window instant. Key is bucketed to the
    minute so a live view (now=utcnow) actually hits it."""
    from datetime import timedelta
    ref = now or datetime.utcnow()
    key = (env, lookback_days, ref.replace(second=0, microsecond=0))
    return _cached("s3", key, lambda: _fetch_s3_impl(lookback_days, env, now))


def _fetch_s3_impl(lookback_days, env, now):
    """Read events from s3://<bucket>/[prefix/]run_date=YYYYMMDD/*.parquet.

    Bucket is per-environment (MONTY_S3_BUCKET pattern, {env} filled from `env`),
    so the PROD/DEV toggle selects it. Day-partitions are coarse, so we read
    every partition overlapping the lookback window and apply the exact
    [start, now] + ENVIRONMENT filter in Python (mirrors the Snowflake predicate)."""
    import io
    from concurrent.futures import ThreadPoolExecutor
    from datetime import timedelta
    import boto3
    from botocore.config import Config
    import pyarrow.parquet as pq

    max_workers = int(os.environ.get("MONTY_S3_WORKERS", "48"))

    if now is None:
        now = datetime.utcnow()
    start = now - timedelta(days=lookback_days)
    bucket = MONTY_S3_BUCKET.format(env=env)
    base = (MONTY_S3_PREFIX + "/") if MONTY_S3_PREFIX else ""

    # one partition prefix per UTC calendar day in [start, now]
    days = []
    d = start.date()
    while d <= now.date():
        days.append(d)
        d += timedelta(days=1)

    # Per-env AWS account: pick the matching profile so env=dev/prod reads its
    # own account's bucket. None -> default credential chain.
    profile = _s3_profile_for(env)
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    if profile:
        logger.info("s3: using AWS profile %r for env=%s", profile, env)
    # botocore clients are thread-safe; size the connection pool to the worker
    # count so parallel GETs don't thrash ("connection pool is full").
    s3 = session.client("s3", config=Config(max_pool_connections=max_workers,
                                            retries={"max_attempts": 3}))
    total = len(days)

    # 1) list keys across every partition (cheap; sequential)
    keys: list[str] = []
    for i, day in enumerate(days, 1):
        prefix = "%srun_date=%s/" % (base, day.strftime("%Y%m%d"))
        before = len(keys)
        try:
            for page in s3.get_paginator("list_objects_v2").paginate(
                    Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    if obj["Key"].lower().endswith(".parquet"):
                        keys.append(obj["Key"])
        except Exception as exc:  # bucket/permission/etc. — log and skip the day
            logger.warning("[%d/%d] %s — list failed, skipping: %s",
                           i, total, prefix, exc)
            continue
        logger.info("[%d/%d] %s — %d object(s)", i, total, prefix, len(keys) - before)

    if not keys:
        logger.info("s3: no parquet objects in window [%s, %s] (bucket=%s)",
                    start, now, bucket)
        return []

    # 2) fetch + parse objects in parallel — the writer emits one tiny file per
    #    event, so a week can be hundreds/thousands of objects; sequential GETs
    #    would take minutes. Filter to [start, now] + env inside each worker.
    workers = min(max_workers, max(4, len(keys)))
    logger.info("s3: reading %d object(s) from bucket=%s window=[%s, %s] env=%s "
                "(%d workers)", len(keys), bucket, start, now, env, workers)

    def _read_object(key):
        try:
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            table = pq.read_table(io.BytesIO(body))
        except Exception as exc:
            logger.error("s3: read failed, skipping %s: %s", key, exc)
            return []
        out = []
        for raw in table.to_pylist():
            row = _normalise_s3_row(raw)
            ts = row.get("OCCURRED_AT")
            if ts is None or not (start <= ts <= now):
                continue
            if env is not None and row.get("ENVIRONMENT") not in (None, env):
                continue
            out.append(row)
        return out

    rows: list[dict] = []
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for chunk in pool.map(_read_object, keys):
            rows.extend(chunk)
            done += 1
            if done % 250 == 0:
                logger.info("s3: %d/%d objects read", done, len(keys))

    rows.sort(key=lambda r: r["OCCURRED_AT"])
    logger.info("s3: %d event row(s) in window from %d object(s)", len(rows), len(keys))
    return rows


# --- DynamoDB source ------------------------------------------------------
# Post-cutover home of the lambda warning/info metrics (see MONTY_DDB_TABLE
# above): pk = "<env>#<pipeline>", sk = "<occurred_at ISO UTC>#<uuid>".
#
# WHY QUERY AND NOT SCAN (measured 2026-07-17, prod, 178,862 items):
#
#     24h window via Scan   28.0s   3,679 rows   92 pages
#     24h window via Query   2.1s   3,679 rows   12 pages
#
# Both return identical rows. Scan's 1MB page limit applies to SCANNED data,
# *before* FilterExpression runs, so a 24h window still pages over the entire
# table — every Scan of this table costs ~28s no matter how narrow the filter,
# and that floor grows with the table. Query reads only matching items, so it
# has no such floor. The cost model that fits every measurement is:
#
#     Scan:  ~28s + bytes/0.47MBps        Query:  bytes/0.47MBps + rows/10400ps
#
# The sk sort key already IS a time index (it starts with the ISO timestamp),
# so no GSI, no extra attribute and no backfill are needed to Query by time —
# a sk range over one pk is exactly the old occurred_at filter, per pipeline.
#
# 0.47 MB/s is the laptop's uplink to us-east-1, not a DynamoDB limit; bytes
# therefore dominate, which is why the projections below matter so much.

# Every attribute the writer emits (lambdas/shared/dynamo_writer.py) EXCEPT
# `payload`. Payload is ~3x of all other bytes combined.
# `environment` is absent deliberately — every item is read from the partition
# pk="<env>#<pipeline>", so it is known from the key and re-stamped below rather
# than paid for on the wire (see _fetch_dynamo_impl).
_DDB_LEAN_ATTRS = ("occurred_at", "pipeline_name", "metric_name", "metric_value",
                   "severity", "is_alert", "run_id")
# All the trailing/cadence lane consumes: last_seen + the per-minute bucket set
# (transform.py cadence loop). ~30B/row against ~400B for a full item.
_DDB_TRAILING_ATTRS = ("occurred_at", "pipeline_name")
# sk is "<occurred_at ISO>#<id>", so to make a sk range equal an occurred_at
# range the upper bound needs a suffix above any id. '￿' encodes to
# EF BF BF — above every byte a uuid4 (hex + '-') or a source ID can produce.
_DDB_SK_MAX = "#￿"
# The hourly rollup (rollup.py) shares this table under a separate pk namespace,
# so raw-event reads never collide with it and _ddb_pipelines can skip it.
ROLLUP_PK_PREFIX = "rollup#"


def _ddb_table(env: str):
    """boto3 Table handle for this env (dev/prod live in separate accounts)."""
    import boto3
    profile = _s3_profile_for(env)
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    if profile:
        logger.info("dynamo: using AWS profile %r for env=%s", profile, env)
    return session.resource("dynamodb", region_name=MONTY_DDB_REGION).Table(
        MONTY_DDB_TABLE.format(env=env))


def _ddb_projection(attrs) -> dict:
    """ProjectionExpression kwargs for `attrs`, aliased via ExpressionAttribute-
    Names so an attribute that is (or becomes) a DynamoDB reserved word can't
    break the read."""
    if not attrs:
        return {}
    names = {"#a%d" % i: a for i, a in enumerate(attrs)}
    return {"ProjectionExpression": ", ".join(names),
            "ExpressionAttributeNames": names}


def _ddb_pipelines(env: str) -> list[str]:
    """Every pk in the table, i.e. "<env>#<pipeline>" for each known pipeline.

    Needed because Query — unlike Scan — must be told which partition to read,
    and fetch_pipeline_last_seen deliberately returns {} for this source. There
    is no cheap "distinct partition keys" in DynamoDB, so this is a full Scan
    projecting pk alone (~30s, 11 pks in prod). Cached hard (ddb_pipelines TTL,
    default 1h): the list changes only when a pipeline is added.

    MONTY_DDB_PIPELINES short-circuits it, at the cost of new pipelines being
    invisible until someone updates the variable.
    """
    if MONTY_DDB_PIPELINES:
        pipes = [p.strip() for p in MONTY_DDB_PIPELINES.split(",") if p.strip()]
        logger.info("dynamo: pipeline list from MONTY_DDB_PIPELINES (%d)", len(pipes))
        return ["%s#%s" % (env, p) for p in pipes]

    def run():
        table = _ddb_table(env)
        started = time.monotonic()
        pks, kwargs, page = set(), {"ProjectionExpression": "pk"}, 0
        while True:
            page += 1
            resp = table.scan(**kwargs)
            for item in resp.get("Items", []):
                pk = item["pk"]
                # The hourly rollup (rollup.py) stores its items in the SAME
                # table under "rollup#<env>#<pipeline>". Those are not pipelines
                # — skip them, or a Query would target a rollup partition as if
                # it were raw event data.
                if pk.startswith(ROLLUP_PK_PREFIX):
                    continue
                pks.add(pk)
            last_key = resp.get("LastEvaluatedKey")
            if not last_key:
                break
            kwargs["ExclusiveStartKey"] = last_key
        logger.info("dynamo: pipeline discovery scan %.1fs, %d page(s) -> %d pk",
                    time.monotonic() - started, page, len(pks))
        return sorted(pks)

    return _cached("ddb_pipelines", env, run)


def _sk_bounds(start, end, include_end: bool) -> tuple[str, str]:
    """sk range for the time range [start, end] / [start, end).

    sk is "<occurred_at ISO>#<id>", so a sk range IS an occurred_at range — this
    is what makes a GSI unnecessary. The end is the fiddly part:

      include_end=True  -> "<end>#￿" so every id at `end` is INSIDE
      include_end=False -> "<end>" so every id at `end` is OUTSIDE
                           ("<end>#<id>" always sorts after "<end>")

    The exclusive form is what keeps the detail and trailing lanes disjoint. If
    both ended inclusively, an event landing exactly on the split instant would
    be returned by BOTH lanes, and the union is deliberately not de-duplicated —
    so that run would be counted twice.
    """
    return (_iso(start), _iso(end) + (_DDB_SK_MAX if include_end else ""))


def _ddb_query_pk(table, pk, sk_lo, sk_hi, attrs) -> list[dict]:
    """One pipeline's items over an sk range, via the existing sk time index."""
    from boto3.dynamodb.conditions import Key
    kwargs = {"KeyConditionExpression": (
        Key("pk").eq(pk) & Key("sk").between(sk_lo, sk_hi))}
    kwargs.update(_ddb_projection(attrs))
    items = []
    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key
    return items


def _ddb_query_window(table, pks, sk_lo, sk_hi, attrs, label) -> list[dict]:
    """Query every pipeline over one sk range, a few pipelines at a time.

    Concurrency is deliberately small (MONTY_DDB_WORKERS, default 4): the link
    is the bottleneck, so more streams contend rather than help — 8 workers
    measured SLOWER than 4. A failed pipeline is fatal: silently rendering a
    timeline that is missing a pipeline is worse than an error.
    """
    from concurrent.futures import ThreadPoolExecutor
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=max(1, MONTY_DDB_WORKERS)) as pool:
        per_pipe = list(pool.map(
            lambda pk: _ddb_query_pk(table, pk, sk_lo, sk_hi, attrs), pks))
    items = [item for chunk in per_pipe for item in chunk]
    logger.info("dynamo: [%s] %d item(s) from %d pipeline(s) in %.1fs",
                label, len(items), len(pks), time.monotonic() - started)
    return items


def _ddb_representative_items(table, pks, sk_lo, sk_hi) -> list[dict]:
    """The newest FULL item per pipeline WITHIN [start, end] — one Query each.

    The trailing lane projects `payload` away, but transform._pick_pay needs one
    representative payload per pipeline to recover dbt/family identity. Without
    this, a pipeline with no activity in the detail window (exactly the stale
    ghost lanes that matter) would lose its payload and fall back to its raw
    name. 11 single-row Queries cost ~0.5s and make that independent of recency.

    The range bound is load-bearing, not tidiness: the caller's `now` is
    truncated to the minute, so an unbounded "newest" Query returns the row
    written in the last few seconds — which is outside the window, matches no
    fetched row, and the graft silently misses. That hit exactly the two busiest
    pipelines, which always have a row in the current minute. A pipeline with
    nothing in the window returns nothing here, which is correct: the old
    full-window Scan had no payload for it either.
    """
    from boto3.dynamodb.conditions import Key
    from concurrent.futures import ThreadPoolExecutor

    def newest(pk):
        resp = table.query(
            KeyConditionExpression=(Key("pk").eq(pk)
                                    & Key("sk").between(sk_lo, sk_hi)),
            ScanIndexForward=False, Limit=1)
        return resp.get("Items", [])

    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=max(1, MONTY_DDB_WORKERS)) as pool:
        chunks = list(pool.map(newest, pks))
    items = [item for chunk in chunks for item in chunk]
    logger.info("dynamo: [payload] %d representative item(s) in %.1fs",
                len(items), time.monotonic() - started)
    return items


def _normalise_ddb_trailing_row(item: dict) -> dict:
    """Cheap normaliser for trailing-lane items (_DDB_TRAILING_ATTRS only).

    The full _normalise_ddb_row rebuilds the dict, coerces six fields and
    JSON-probes the payload — ~100µs/row, which over 165k trailing rows is ~17s
    of pure Python for fields that are all None anyway. The trailing lane only
    ever feeds last-seen and the per-minute bucket set, so it needs exactly two
    fields; everything else defaults exactly as the full normaliser would leave
    it (transform reads SEVERITY/IS_ALERT through _g with its own defaults, and
    only ever for rows inside the window, which come from the detail lane).
    """
    occurred = item.get("occurred_at")
    if isinstance(occurred, str):
        try:
            occurred = datetime.fromisoformat(occurred)
        except ValueError:
            occurred = None
    return {"PIPELINE_NAME": item.get("pipeline_name"),
            "OCCURRED_AT": _to_utc_naive(occurred)}


def _normalise_ddb_row(item: dict) -> dict:
    """Coerce one DynamoDB item to the Snowflake/CSV row contract (upper-case
    keys, float METRIC_VALUE, naive-UTC OCCURRED_AT, PAYLOAD as JSON string)."""
    from decimal import Decimal
    raw = {}
    for key, value in item.items():
        if key in ("pk", "sk", "ttl"):
            continue                       # key/expiry plumbing, not event data
        if isinstance(value, Decimal):     # boto3 returns N attributes as Decimal
            value = float(value)
        raw[key] = value
    occurred = raw.get("occurred_at")
    if isinstance(occurred, str):
        try:
            raw["occurred_at"] = datetime.fromisoformat(occurred)
        except ValueError:
            raw["occurred_at"] = None
    # The writer never sets Slack-delivery fields (observer reads Snowflake only).
    raw.setdefault("sent_to_slack", False)
    raw.setdefault("sent_at", None)
    return _normalise_s3_row(raw)          # same upper-casing + type coercion


def _iso(dt) -> str:
    """UTC ISO string matching what the writer stamps, so a lexicographic sk/
    occurred_at comparison is apples-to-apples."""
    from datetime import timezone
    return dt.replace(tzinfo=timezone.utc).isoformat()


def _fetch_dynamo(lookback_days, env, now, detail_days=None):
    """Cached wrapper around the DynamoDB read (MONTY_TTL_DYNAMO, default 60s).
    Key is bucketed to the minute so a live view (now=utcnow) actually hits it."""
    ref = now or datetime.utcnow()
    key = (env, lookback_days, detail_days, ref.replace(second=0, microsecond=0))
    return _cached("dynamo", key,
                   lambda: _fetch_dynamo_impl(lookback_days, env, now, detail_days))


def _fetch_dynamo_trailing(env, sk_range, pks, cache_key):
    """Cached (dynamo_trailing TTL, default 15min) trailing-lane read.

    Separate cache bucket from the detail lane on purpose: this is ~165k rows
    and ~21s — the dominant cost of a render — but it only feeds cadence and
    last-seen, which move on the order of hours. The detail lane keeps the 60s
    TTL, so what the user is looking at stays near-live while this doesn't get
    re-read on every refresh.

    Caches NORMALISED rows, not raw items, deliberately: coercing 165k items
    costs ~17s of pure Python, so caching the raw items would still re-pay that
    on every 60s detail-cache miss and the long TTL would buy almost nothing.
    """
    def run():
        table = _ddb_table(env)
        items = _ddb_query_window(table, pks, sk_range[0], sk_range[1],
                                  _DDB_TRAILING_ATTRS, "trailing")
        started = time.monotonic()
        rows = [_normalise_ddb_trailing_row(item) for item in items]
        logger.info("dynamo: [trailing] normalised %d row(s) in %.1fs",
                    len(rows), time.monotonic() - started)
        return rows
    return _cached("dynamo_trailing", cache_key, run)


def _fetch_dynamo_impl(lookback_days, env, now, detail_days=None):
    """Read events from the per-env DynamoDB table over [now-lookback_days, now].

    Table is per-environment (MONTY_DDB_TABLE pattern, {env} filled from `env`),
    so the PROD/DEV toggle selects it; the per-env AWS profile resolution is
    shared with the S3 leg (same two accounts).

    `detail_days` splits the read into two disjoint lanes (see fetch_events):

      * detail   [now-detail_days, now]              — full rows, incl. payload
      * trailing [now-lookback_days, now-detail_days] — occurred_at + pipeline

    The ranges do not overlap, so the union needs no dedupe. Measured on prod
    (178,862 items): 155s for the old 8d full-attribute Scan, ~23s cold and
    ~2s warm for the split Query. With detail_days=None every row comes back
    full, which is the anomaly page's contract (it needs metric values across
    the whole window, and its own gates count raw points).
    """
    from datetime import timedelta

    if now is None:
        now = datetime.utcnow()
    start = now - timedelta(days=lookback_days)
    pks = _ddb_pipelines(env)
    table = _ddb_table(env)

    trailing: list[dict] = []
    if detail_days is None:
        items = _ddb_query_window(table, pks, *_sk_bounds(start, now, True),
                                  _DDB_LEAN_ATTRS, "full")
    else:
        split = now - timedelta(days=detail_days)
        # [split, now] full ... [start, split) lean — disjoint at `split`, so
        # the two lanes can be unioned without de-duplicating (see _sk_bounds).
        items = _ddb_query_window(table, pks, *_sk_bounds(split, now, True),
                                  None, "detail")
        if split > start:
            trailing = _fetch_dynamo_trailing(
                env, _sk_bounds(start, split, False), pks,
                (env, lookback_days, detail_days,
                 start.replace(second=0, microsecond=0)))

    # `trailing` rows come from the long-lived cache — copy before touching them,
    # or the graft/env stamp below would mutate the cached objects in place.
    rows = [_normalise_ddb_row(item) for item in items] + [dict(r) for r in trailing]
    rows = [r for r in rows if r.get("OCCURRED_AT") is not None]
    _merge_representative_payloads(
        rows, _ddb_representative_items(table, pks,
                                        *_sk_bounds(start, now, True)))
    # Every item was read from pk="<env>#<pipeline>", so it belongs to this env
    # by construction — stamp it rather than fetch it (the Snowflake leg does the
    # same). transform filters rows on ENVIRONMENT, so a lean row that omitted it
    # would be silently dropped and the cadence history would vanish.
    for row in rows:
        row["ENVIRONMENT"] = env
    rows.sort(key=lambda r: r["OCCURRED_AT"])
    logger.info("dynamo: %d event row(s) in window from %s",
                len(rows), MONTY_DDB_TABLE.format(env=env))
    return rows


# --- Hourly rollup read path (rollup.py writes it) ------------------------
# Reads the materialised hourly rollup instead of raw events. Serves settled
# hours from the rollup + the last `live_tail_hours` from raw (folded to hourly
# too, so the whole window is uniform hour-grain). Same canonical row contract,
# plus _ROLLUP/_N/_LAST_TS markers. See rollup.py for the schema.
_ROLLUP_ATTRS = ("pipeline_name", "metric_name", "hour", "n", "worst_sev",
                 "any_alert", "mean_val", "last_val", "last_ts")
_ROLLUP_SEV_RANK = {"info": 1, "warning": 2, "error": 3, "critical": 4}
_ROLLUP_RANK_SEV = {v: k for k, v in _ROLLUP_SEV_RANK.items()}


def _rollup_pk(env, pipeline):
    return "%s%s#%s" % (ROLLUP_PK_PREFIX, env, pipeline)


def _num(v):
    from decimal import Decimal
    return float(v) if isinstance(v, Decimal) else v


def _rollup_row(pipeline, metric, hour, n, worst_sev, any_alert,
                mean_val, last_ts, last_val, env):
    """One canonical hourly row (the shape both lanes converge on)."""
    return {"PIPELINE_NAME": pipeline, "METRIC_NAME": metric,
            "OCCURRED_AT": hour, "SEVERITY": worst_sev or "info",
            "IS_ALERT": bool(any_alert), "METRIC_VALUE": mean_val,
            "ENVIRONMENT": env, "SENT_TO_SLACK": False, "SENT_AT": None,
            "PAYLOAD": None, "_ROLLUP": True, "_N": int(n or 0),
            "_LAST_TS": last_ts, "_LAST_VAL": last_val}


def _normalise_rollup_item(item, env):
    """A stored rollup item -> a canonical hourly row."""
    hour = item.get("hour")
    ts = None
    if isinstance(hour, str):
        try:
            ts = _to_utc_naive(datetime.fromisoformat(hour))
        except ValueError:
            ts = None
    return _rollup_row(item.get("pipeline_name"), item.get("metric_name"), ts,
                       item.get("n"), item.get("worst_sev"),
                       item.get("any_alert"), _num(item.get("mean_val")),
                       item.get("last_ts"), _num(item.get("last_val")), env)


def _fold_raw_hourly(rows, env):
    """Aggregate normalised RAW rows into per-(pipeline, metric, hour) rows,
    using the SAME aggregation the stored rollup uses — so the live tail is
    indistinguishable from a settled hour."""
    from datetime import timezone
    buckets: dict = {}
    for r in rows:
        ts = r.get("OCCURRED_AT")
        if ts is None:
            continue
        hour = ts.replace(minute=0, second=0, microsecond=0)
        key = (r.get("PIPELINE_NAME"), r.get("METRIC_NAME"), hour)
        agg = buckets.get(key)
        if agg is None:
            agg = {"n": 0, "sum": 0.0, "have": False, "last_ts": None,
                   "last_val": None, "rank": 0, "alert": False}
            buckets[key] = agg
        agg["n"] += 1
        v = r.get("METRIC_VALUE")
        if isinstance(v, (int, float)):
            agg["sum"] += v
            agg["have"] = True
        if agg["last_ts"] is None or ts > agg["last_ts"]:
            agg["last_ts"] = ts
            if isinstance(v, (int, float)):
                agg["last_val"] = v
        sev = (r.get("SEVERITY") or "info").lower()
        agg["rank"] = max(agg["rank"], _ROLLUP_SEV_RANK.get(sev, 1))
        agg["alert"] = agg["alert"] or bool(r.get("IS_ALERT"))
    out = []
    for (p, m, hour), agg in buckets.items():
        last_iso = (agg["last_ts"].replace(tzinfo=timezone.utc).isoformat()
                    if agg["last_ts"] else None)
        out.append(_rollup_row(
            p, m, hour, agg["n"], _ROLLUP_RANK_SEV.get(agg["rank"], "info"),
            agg["alert"], (agg["sum"] / agg["n"]) if agg["have"] else None,
            last_iso, agg["last_val"], env))
    return out


def _collapse_to_pipeline_hour(rows, env):
    """Fold per-metric hourly rows into one row per (pipeline, hour): worst
    severity, any-alert, summed count. This is what the TIMELINE needs — it
    aggregates across metrics anyway, and this is what makes a lane ≤24 marks."""
    buckets: dict = {}
    for r in rows:
        ts = r.get("OCCURRED_AT")
        if ts is None:
            continue
        key = (r.get("PIPELINE_NAME"), ts)
        agg = buckets.get(key)
        if agg is None:
            agg = {"n": 0, "rank": 0, "alert": False}
            buckets[key] = agg
        agg["n"] += int(r.get("_N", 1))
        sev = (r.get("SEVERITY") or "info").lower()
        agg["rank"] = max(agg["rank"], _ROLLUP_SEV_RANK.get(sev, 1))
        agg["alert"] = agg["alert"] or bool(r.get("IS_ALERT"))
    return [_rollup_row(p, None, ts, agg["n"],
                        _ROLLUP_RANK_SEV.get(agg["rank"], "info"), agg["alert"],
                        None, None, None, env)
            for (p, ts), agg in buckets.items()]


def _fetch_rollup_settled(env, start_hour, settled_end, cache_key):
    """Cached (dynamo_rollup TTL) read of the stored rollup over the settled
    span [start_hour, settled_end). Settled hours never change, so this is the
    long-lived, cheap lane; only the live tail is re-read every request."""
    def run():
        table = _ddb_table(env)
        raw_pks = [pk for pk in _ddb_pipelines(env)
                   if not pk.startswith(ROLLUP_PK_PREFIX)]
        rollup_pks = [_rollup_pk(env, pk.split("#", 1)[1]) for pk in raw_pks]
        # sk = "<hour ISO>#<metric>"; an exclusive upper at the settled_end hour
        # keeps this disjoint from the live lane (see _sk_bounds).
        sk_lo, sk_hi = _sk_bounds(start_hour, settled_end, include_end=False)
        items = _ddb_query_window(table, rollup_pks, sk_lo, sk_hi,
                                  _ROLLUP_ATTRS, "rollup")
        return [_normalise_rollup_item(it, env) for it in items]
    return _cached("dynamo_rollup", cache_key, run)


def fetch_rollup_events(lookback_days, env, now=None, live_tail_hours=2,
                        collapse_metrics=False):
    """Hourly-grain events for the window, from the rollup + a raw live tail.

    Settled hours [start, now-live_tail_hours) come from the stored rollup;
    the last `live_tail_hours` come from RAW (folded to hourly here), so the
    newest data is never stale and a missing settled hour still renders (the
    live lane widens to cover it). The two spans are disjoint at the hour
    boundary, so the union needs no dedupe.

    `collapse_metrics=True` (timeline) returns one row per (pipeline, hour);
    False (anomaly) returns one row per (pipeline, metric, hour).
    """
    from datetime import timedelta
    if now is None:
        now = datetime.utcnow()
    hour0 = now.replace(minute=0, second=0, microsecond=0)
    settled_end = hour0 - timedelta(hours=live_tail_hours)
    start_hour = (now - timedelta(days=lookback_days)).replace(
        minute=0, second=0, microsecond=0)

    settled = []
    if settled_end > start_hour:
        settled = _fetch_rollup_settled(
            env, start_hour, settled_end,
            (env, start_hour, settled_end))

    # live tail: raw events over [settled_end, now], folded to hourly.
    table = _ddb_table(env)
    raw_pks = [pk for pk in _ddb_pipelines(env)
               if not pk.startswith(ROLLUP_PK_PREFIX)]
    live_lo, live_hi = _sk_bounds(settled_end, now, include_end=True)
    live_items = _ddb_query_window(table, raw_pks, live_lo, live_hi,
                                   _DDB_LEAN_ATTRS, "live")
    live = _fold_raw_hourly([_normalise_ddb_row(it) for it in live_items], env)

    rows = [dict(r) for r in settled] + live   # copy cached settled before use
    if collapse_metrics:
        rows = _collapse_to_pipeline_hour(rows, env)
    rows = [r for r in rows if r.get("OCCURRED_AT") is not None]
    rows.sort(key=lambda r: r["OCCURRED_AT"])
    logger.info("rollup: %d hourly row(s) (%d settled + %d live, collapse=%s)",
                len(rows), len(settled), len(live), collapse_metrics)
    return rows


def _merge_representative_payloads(rows, items) -> None:
    """Graft each pipeline's representative payload ONTO its existing row.

    The representatives must not be appended as extra rows: the newest item for
    a pipeline is normally already in the detail lane (appending it would double
    -count that run), and for a long-dead pipeline it can fall outside the
    window entirely (appending it would invent activity that the old full-window
    Scan never reported either).

    Matching on (pipeline, occurred_at) rather than sk keeps the trailing lane
    lean — sk is ~68B, which over 165k rows would cost ~11MB (~23s) purely to
    carry a uuid nothing reads. Two events for one pipeline at the identical
    microsecond would both receive the payload, which is harmless: it is only
    ever read for dbt/family identity, never counted.
    """
    by_key = {}
    for row in rows:
        by_key.setdefault((row.get("PIPELINE_NAME"), row.get("OCCURRED_AT")), row)
    grafted = 0
    for rep in (_normalise_ddb_row(item) for item in items):
        row = by_key.get((rep.get("PIPELINE_NAME"), rep.get("OCCURRED_AT")))
        if row is not None and not row.get("PAYLOAD"):
            row["PAYLOAD"] = rep.get("PAYLOAD")
            row["METRIC_NAME"] = rep.get("METRIC_NAME")
            grafted += 1
    logger.info("dynamo: [payload] grafted %d representative payload(s)", grafted)


# --- SQLite (local cache) source -----------------------------------------
# Populated by `loadS3.py --ingest`, which drains the S3 warning/info Parquet
# objects into a local SQLite DB and archives them. Reading the local DB is
# near-instant, so this replaces the ~9s live-S3 leg. Same row contract as S3.
def _fetch_sqlite(lookback_days, env, now):
    """Read events from the local SQLite cache (see sqlite_store). Not cached in
    _CACHE — SQLite reads are already fast and always reflect the latest ingest."""
    from datetime import timedelta
    import sqlite_store
    if now is None:
        now = datetime.utcnow()
    start = now - timedelta(days=lookback_days)
    return sqlite_store.fetch_events(start, now, env)


# --- combined source (MONTY_SOURCE=both) ---------------------------------
# The split is by PRODUCER, not purely by severity:
#   * Lambdas (lambdas/shared/metric_writer.py) route critical/error -> Snowflake
#     and warning/info -> S3.
#   * dbt hooks and the auditor proc run INSIDE Snowflake and cannot write S3,
#     so ALL their rows (info AND error) land in Snowflake.
# So filtering Snowflake down to critical/error would silently drop every dbt
# `info` row (e.g. dbt_model_run) — they exist nowhere else. `both` therefore
# takes the UNION of the two stores. Verified disjoint against live data (0
# overlapping (pipeline, metric, occurred_at) keys), but we still dedup on that
# key so a future producer writing to both can never double-count.

# Populated by _fetch_both when a source is unreachable. The dashboard reads it
# to warn that it is showing PARTIAL data — never let half the truth look whole.
LAST_SOURCE_ERRORS: dict = {}

# what you lose when a given source drops out, for the UI banner
SOURCE_PROVIDES = {
    "snowflake": "all error/critical rows and every dbt metric",
    "s3": "lambda warning/info rows (pre-cutover Parquet history)",
    "sqlite": "lambda warning/info rows (local S3 cache)",
    "dynamo": "lambda warning/info rows (live, post-cutover)",
}

# The warning/info leg(s) of MONTY_SOURCE=both:
#   'union' (default) = DynamoDB (live post-cutover writes) PLUS the SQLite cache
#      (archived history) PLUS live S3 (pre-cutover tail not yet ingested).
#      Reading all three and deduping means neither the S3→DynamoDB cutover nor
#      the ingest job moving objects to archive/ can EVER leave a gap.
#   'dynamo' = DynamoDB only (post-cutover steady state, once S3 history aged out)
#   'sqlite' = cache only (fast, but stale between ingests; empty before first run)
#   's3'     = live S3 only (the original pre-cache behaviour)
_BOTH_WARN_SOURCE = os.environ.get("MONTY_BOTH_WARN_SOURCE", "union").lower()


def _row_key(row):
    """Natural identity of an event row, for cross-store dedup."""
    return (row.get("PIPELINE_NAME"), row.get("METRIC_NAME"),
            row.get("OCCURRED_AT"), row.get("METRIC_VALUE"))


def _fetch_both(lookback_days, env, now, detail_days=None):
    """Union of everything Snowflake has (lambda critical/error + all dbt/auditor
    rows) plus the lambda warning/info rows.

    The warning/info side is, by default, the UNION of the SQLite cache (archived
    history) and live S3 (the recent, un-ingested tail) — see _BOTH_WARN_SOURCE.
    Reading both and deduping guarantees no gap when the ingest job moves objects
    to archive/: the history is in SQLite, the fresh tail is still in the live
    partition. All sources share the identical row contract.

    Resilient by design — if a store errors (creds, network, missing bucket),
    it's logged and the others still render, so a monitoring view never 500s
    because a single source hiccuped."""
    rows: list[dict] = []
    seen: set = set()
    dupes = 0
    LAST_SOURCE_ERRORS.clear()

    # Which warning/info legs to read (deduped into the same result).
    if _BOTH_WARN_SOURCE in ("union", "both"):
        warn_legs = [("dynamo", _fetch_dynamo), ("sqlite", _fetch_sqlite),
                     ("s3", _fetch_s3)]
    elif _BOTH_WARN_SOURCE in ("dynamo", "ddb", "dynamodb"):
        warn_legs = [("dynamo", _fetch_dynamo)]
    elif _BOTH_WARN_SOURCE == "sqlite":
        warn_legs = [("sqlite", _fetch_sqlite)]
    else:
        warn_legs = [("s3", _fetch_s3)]

    def add(batch):
        nonlocal dupes
        for r in batch:
            if r.get("OCCURRED_AT") is None:
                continue
            key = _row_key(r)
            if key in seen:
                dupes += 1
                continue
            seen.add(key)
            rows.append(r)

    # The stores are independent, so run every leg concurrently — the merge waits
    # on the slowest, not the sum. Snowflake's shared connection is lock-guarded,
    # so this is safe.
    from concurrent.futures import ThreadPoolExecutor
    all_legs = [("snowflake", _fetch_snowflake)] + warn_legs

    # `detail_days` lets the DynamoDB leg fetch its trailing tail lean, which is
    # most of its speed — but ONLY when it is the sole warning/info leg.
    # _row_key dedupes on (pipeline, metric, occurred_at, value); a lean row has
    # metric/value None, so against another warn leg it would both fail to match
    # its own full twin (duplicating the event) AND collide with a sibling metric
    # at the same instant (silently dropping it). Correctness first: in a union
    # the dynamo leg reads full rows and stays slow. Set
    # MONTY_BOTH_WARN_SOURCE=dynamo to get the fast path — post-cutover the
    # sqlite/s3 legs are belt-and-braces anyway.
    lean_leg = "dynamo" if [n for n, _ in warn_legs] == ["dynamo"] else None
    if detail_days is not None and lean_leg is None and len(warn_legs) > 1:
        logger.info("both: %d warn legs — dynamo reads FULL rows so cross-store "
                    "dedup stays sound; set MONTY_BOTH_WARN_SOURCE=dynamo for "
                    "the fast path", len(warn_legs))

    def submit(pool, name, fn):
        if name == lean_leg:
            return pool.submit(fn, lookback_days, env, now, detail_days)
        return pool.submit(fn, lookback_days, env, now)

    with ThreadPoolExecutor(max_workers=len(all_legs)) as pool:
        futures = {name: submit(pool, name, fn) for name, fn in all_legs}

        # Snowflake (critical/error + every dbt metric): its own banner entry.
        # Losing it means the page would look healthy and near-empty, so shout.
        try:
            sf = futures["snowflake"].result()
            logger.info("both: %d row(s) from Snowflake (lambda critical/error + "
                        "all dbt/auditor rows)", len(sf))
            add(sf)
        except Exception as exc:
            logger.error("both: Snowflake source failed, rendering without it: %s", exc)
            LAST_SOURCE_ERRORS["snowflake"] = str(exc)

        # Warning/info legs are unioned, so they cover for each other. Only raise
        # the "missing warning/info" banner if EVERY warn leg failed — a single
        # leg erroring (e.g. sqlite empty, or expired S3 creds) is not data loss
        # when the other leg answered.
        warn_ok = 0
        warn_errs = {}
        for name, _fn in warn_legs:
            try:
                batch = futures[name].result()
                logger.info("both: %d row(s) from %s (lambda warning/info)",
                            len(batch), name)
                add(batch)
                warn_ok += 1
            except Exception as exc:
                logger.error("both: %s warn leg failed: %s", name, exc)
                warn_errs[name] = str(exc)
        if warn_legs and warn_ok == 0:
            LAST_SOURCE_ERRORS["s3"] = "; ".join(
                f"{k}: {v}" for k, v in warn_errs.items())

    if dupes:
        logger.warning("both: dropped %d duplicate row(s) present in both stores", dupes)
    rows.sort(key=lambda r: r["OCCURRED_AT"])
    logger.info("both: %d merged event row(s)", len(rows))
    return rows


# ==========================================================================
# Warehouse credit usage (for the timeline credits chart)
# --------------------------------------------------------------------------
def fetch_warehouse_credits(start: datetime, end: datetime,
                            warehouse: str | None = None) -> list[dict]:
    """Hourly credit usage for `warehouse` over the naive-UTC window
    [start, end). Rows: HOUR (naive UTC datetime), CREDITS_USED,
    CREDITS_COMPUTE, CREDITS_CLOUD_SERVICES.

    PAUSED by default — see ENABLE_CREDITS at the top of this module."""
    if not ENABLE_CREDITS:
        logger.info("credits: paused (MONTY_ENABLE_CREDITS=0), skipping ACCOUNT_USAGE")
        return []
    wh = warehouse or MONTY_WAREHOUSE
    if os.environ.get("MONTY_SOURCE", "snowflake").lower() == "csv":
        return _credits_csv(start, end, wh)

    def run():
        cols, rows = _query(_read_sql("warehouse_credits.sql"),
                            {"warehouse": wh, "start": start, "end": end})
        out = []
        for row in rows:
            rec = dict(zip(cols, row))
            # normalise HOUR to naive UTC (connector may return tz-aware)
            hour = rec.get("HOUR")
            if hour is not None and getattr(hour, "tzinfo", None) is not None:
                rec["HOUR"] = hour.replace(tzinfo=None)
            for key in ("CREDITS_USED", "CREDITS_COMPUTE", "CREDITS_CLOUD_SERVICES"):
                rec[key] = float(rec.get(key) or 0.0)
            out.append(rec)
        return out

    # bucket the cache key to the hour: ACCOUNT_USAGE has hourly grain anyway,
    # so a request 20s later must not miss the cache on a shifted `end`.
    key = (wh, start.replace(minute=0, second=0, microsecond=0),
           end.replace(minute=0, second=0, microsecond=0))
    return _cached("credits", key, run)


def fetch_warehouse_credit_peaks(warehouse: str | None = None) -> dict:
    """All-time hourly peak per series for `warehouse` — the benchmark the
    current window is measured against. Returns {'used','compute','cloud'}.

    PAUSED by default — see ENABLE_CREDITS at the top of this module."""
    if not ENABLE_CREDITS:
        return {}
    wh = warehouse or MONTY_WAREHOUSE
    if os.environ.get("MONTY_SOURCE", "snowflake").lower() == "csv":
        # demo benchmark clearly above the synthetic current usage
        return {"used": 2.4, "compute": 2.2, "cloud": 0.22}

    def run():
        _, rows = _query(_read_sql("warehouse_credit_peaks.sql"), {"warehouse": wh})
        if not rows:
            return {"used": 0.0, "compute": 0.0, "cloud": 0.0}
        row = rows[0]
        return {"used": float(row[0] or 0.0),
                "compute": float(row[1] or 0.0),
                "cloud": float(row[2] or 0.0)}

    # an all-time MAX barely moves; 30min staleness is irrelevant
    return _cached("peaks", wh, run)


def _credits_csv(start, end, warehouse):
    """Local-dev synthetic credit series so the chart renders without Snowflake.
    Deterministic (no randomness): a daytime-weighted usage curve per hour."""
    import math
    from datetime import timedelta

    rows = []
    hour = start.replace(minute=0, second=0, microsecond=0)
    while hour < end:
        # 0..1 daytime weighting (peak mid-afternoon UTC), deterministic
        weight = 0.5 * math.sin((hour.hour - 8) / 24.0 * 2 * math.pi) + 0.5
        compute = round(0.02 + 0.65 * weight, 4)
        cloud = round(compute * 0.08 + 0.001, 4)
        rows.append({"HOUR": hour,
                     "CREDITS_USED": round(compute + cloud, 4),
                     "CREDITS_COMPUTE": compute,
                     "CREDITS_CLOUD_SERVICES": cloud})
        hour += timedelta(hours=1)
    return rows


# ==========================================================================
# Segment event / property search
# --------------------------------------------------------------------------
# These introspect SEGMENT_DB.INFORMATION_SCHEMA so the "tracking plan" is
# generated live from what is actually landing, not from docs. In prod they
# hit Snowflake; for local dev set MONTY_SOURCE=csv and point SEGMENT_CSV at a
# 4-column export (source,event_name,property_name,data_type) — see
# segment_sample.csv for the shape.
# ==========================================================================

# A user-saved snapshot of the segment tracking plan. The "Save" button on the
# /segment page writes it; once it exists the page serves from it (instant, no
# Snowflake) until "Resync" refreshes it or the file is deleted.
SEGMENT_CACHE = Path(os.environ.get(
    "SEGMENT_CACHE", str(Path(__file__).parent / "segment_cache.csv")))


def _segment_csv_path() -> str | None:
    """Which CSV the segment page should read, or None to query Snowflake live.

    Precedence:
      1. SEGMENT_SOURCE=snowflake -> None (forced live; ignores the snapshot)
      2. SEGMENT_SOURCE=csv       -> the sample (forced)
      3. a saved snapshot exists  -> that snapshot  (the "Save" button)
      4. MONTY_SOURCE=csv         -> the sample (offline laptop dev)
      5. otherwise                -> None (live Snowflake)
    """
    seg = (os.environ.get("SEGMENT_SOURCE") or "").strip().lower()
    if seg == "snowflake":
        return None
    if seg == "csv":
        return os.environ.get("SEGMENT_CSV", "segment_sample.csv")
    if SEGMENT_CACHE.exists():
        return str(SEGMENT_CACHE)
    if os.environ.get("MONTY_SOURCE", "snowflake").lower() == "csv":
        return os.environ.get("SEGMENT_CSV", "segment_sample.csv")
    return None


def _segment_is_csv() -> bool:
    """True when the segment page reads a CSV (snapshot or sample) rather than
    querying Snowflake live. See _segment_csv_path for the precedence."""
    return _segment_csv_path() is not None


def segment_source_state() -> dict:
    """Describe where the segment page is currently reading from, for the UI."""
    path = _segment_csv_path()
    if path is None:
        return {"mode": "snowflake", "synced_at": None, "rows": None}
    p = Path(path)
    mode = "cache" if p == SEGMENT_CACHE else "sample"
    synced = None
    if mode == "cache" and p.exists():
        import datetime as _dt
        synced = _dt.datetime.utcfromtimestamp(p.stat().st_mtime)
    return {"mode": mode, "synced_at": synced, "rows": None}


def save_segment_snapshot() -> int:
    """Pull the full flat property list from Snowflake and write it to the
    segment snapshot CSV. Always reads Snowflake (bypasses the source toggle),
    so it works whether the page is currently live or already cached. Returns
    the number of property rows written. Both Save and Resync call this."""
    import csv as _csv
    sql = _read_sql("segment_dump.sql").replace("{{db}}", SEGMENT_DB)
    cols, rows = _query(sql)
    idx = {c.upper(): i for i, c in enumerate(cols)}
    order = ("SOURCE", "EVENT_NAME", "PROPERTY_NAME", "DATA_TYPE", "ORDINAL_POSITION")
    tmp = SEGMENT_CACHE.with_suffix(".tmp")
    with open(tmp, "w", newline="") as f:
        w = _csv.writer(f)
        w.writerow([c.lower() for c in order])
        for r in rows:
            w.writerow([r[idx[c]] for c in order])
    tmp.replace(SEGMENT_CACHE)          # atomic swap — never serve a half file
    logger.info("segment: snapshot wrote %d rows to %s", len(rows), SEGMENT_CACHE)
    return len(rows)


def clear_segment_snapshot() -> bool:
    """Delete the saved snapshot so the page goes back to live Snowflake."""
    if SEGMENT_CACHE.exists():
        SEGMENT_CACHE.unlink()
        logger.info("segment: snapshot cleared")
        return True
    return False


def fetch_segment_catalog() -> list[dict]:
    """One row per custom event: EVENT_NAME, SOURCES (list), SOURCE_COUNT,
    PROPERTY_COUNT. The 'who sends what' matrix for the whole warehouse."""
    if _segment_is_csv():
        rows = _segment_csv_rows()
        by_event: dict[str, dict] = {}
        for r in rows:
            ev = by_event.setdefault(
                r["event_name"], {"sources": set(), "props": set()})
            ev["sources"].add(r["source"])
            ev["props"].add(r["property_name"])
        out = [
            {"EVENT_NAME": name,
             "SOURCES": sorted(v["sources"]),
             "SOURCE_COUNT": len(v["sources"]),
             "PROPERTY_COUNT": len(v["props"])}
            for name, v in by_event.items()
        ]
        return sorted(out, key=lambda r: r["EVENT_NAME"])
    return _segment_query("segment_catalog.sql", {})


def fetch_segment_search(query: str) -> list[dict]:
    """Events matching `query` by event name OR by a property name. Each row:
    EVENT_NAME, SOURCES (list), SOURCE_COUNT, MATCHED_PROPS (list)."""
    term = (query or "").strip()
    if not term:
        return []
    if _segment_is_csv():
        needle = term.lower()
        rows = _segment_csv_rows()
        by_event: dict[str, dict] = {}
        for r in rows:
            name_hit = needle in r["event_name"].lower()
            prop_hit = needle in r["property_name"].lower()
            if not (name_hit or prop_hit):
                continue
            ev = by_event.setdefault(
                r["event_name"], {"sources": set(), "matched": set()})
            ev["sources"].add(r["source"])
            if prop_hit:
                ev["matched"].add(r["property_name"])
        out = [
            {"EVENT_NAME": name,
             "SOURCES": sorted(v["sources"]),
             "SOURCE_COUNT": len(v["sources"]),
             "MATCHED_PROPS": sorted(v["matched"])}
            for name, v in by_event.items()
        ]
        return sorted(out, key=lambda r: r["EVENT_NAME"])
    # ILIKE with wildcards; escape the caller's % / _ so they are literal
    safe = term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return _segment_query("segment_search.sql", {"q": f"%{safe}%"})


def fetch_segment_properties(event_name: str) -> list[dict]:
    """Full property list for one event across every source that emits it:
    SOURCE, EVENT_NAME, PROPERTY_NAME, DATA_TYPE, ORDINAL_POSITION."""
    if not event_name:
        return []
    if _segment_is_csv():
        rows = [r for r in _segment_csv_rows() if r["event_name"] == event_name]
        out = []
        for r in rows:
            out.append({
                "SOURCE": r["source"],
                "EVENT_NAME": r["event_name"],
                "PROPERTY_NAME": r["property_name"],
                "DATA_TYPE": r.get("data_type", ""),
                "ORDINAL_POSITION": r.get("ordinal_position"),
            })
        return sorted(out, key=lambda r: (r["SOURCE"], r["PROPERTY_NAME"]))
    return _segment_query("segment_properties.sql", {"event": event_name})


def _segment_query(sql_file: str, params: dict) -> list[dict]:
    """Run a segment SQL file against Snowflake and return list-of-dicts.
    ARRAY columns (SOURCES / MATCHED_PROPS) come back as JSON strings from the
    connector, so decode them into Python lists here."""
    import json
    sql = _read_sql(sql_file).replace("{{db}}", SEGMENT_DB)
    cols, rows = _query(sql, params)
    results = []
    for row in rows:
        record = dict(zip(cols, row))
        for key in ("SOURCES", "MATCHED_PROPS"):
            val = record.get(key)
            if isinstance(val, str):
                try:
                    record[key] = json.loads(val)
                except (ValueError, TypeError):
                    record[key] = [val]
        results.append(record)
    return results


# The documented tracking-plan spec (a hand-maintained CSV), shown on the
# second /segment tab and joined against the live Segment events on the third.
COMBINED_CSV = Path(os.environ.get(
    "COMBINED_CSV", str(Path(__file__).parent / "combined_tables_final.csv")))


def fetch_combined_spec() -> list[dict]:
    """The documented tracking plan from combined_tables_final.csv, grouped by
    event: [{EVENT, STATUS, PARAM_COUNT, PARAMS:[{name,type,desc,status}]}].
    The source CSV has one row per (event, parameter); we fold them per event.
    Rows whose parameter cell is a bare data type (String/Boolean/…) are the
    type of the preceding parameter, so they're skipped as their own param."""
    import csv
    if not COMBINED_CSV.exists():
        return []
    bare_types = {"string", "boolean", "integer", "number", "float", "double",
                  "object", "array", "timestamp", "date", "datetime", "uuid", "json"}
    groups: dict[str, dict] = {}
    order: list[str] = []
    with open(COMBINED_CSV) as f:
        for r in csv.DictReader(f):
            ev = (r.get("Event Name") or "").strip()
            if not ev:
                continue
            if ev not in groups:
                groups[ev] = {"EVENT": ev, "STATUS": "", "PARAMS": []}
                order.append(ev)
            g = groups[ev]
            status = (r.get("Status") or "").strip()
            if status and not g["STATUS"]:
                g["STATUS"] = status
            param = (r.get("Parameters") or "").strip()
            if not param or param.lower() in bare_types:
                continue
            g["PARAMS"].append({
                "name": param,
                "type": (r.get("Data type") or "").strip(),
                "desc": (r.get("Description") or "").strip(),
                "status": status,
            })
    out = [groups[e] for e in order]
    for g in out:
        g["PARAM_COUNT"] = len(g["PARAMS"])
    return out


def _segment_csv_rows() -> list[dict]:
    """Read the active segment CSV (a saved snapshot, or the local-dev sample).
    Columns: source,event_name,property_name,data_type (ordinal_position
    optional). Missing file -> empty list (page renders empty)."""
    import csv
    path = _segment_csv_path()
    if not path or not Path(path).exists():
        return []
    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            rows.append({
                "source": (r.get("source") or "").strip(),
                "event_name": (r.get("event_name") or "").strip(),
                "property_name": (r.get("property_name") or "").strip(),
                "data_type": (r.get("data_type") or "").strip(),
                "ordinal_position": r.get("ordinal_position"),
            })
    return rows
