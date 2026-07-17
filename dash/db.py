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
    # DynamoDB warn/info events: a lookback-window Scan is one paginated call
    # (fast, but billed per read unit) — same short cache as the S3 leg.
    "dynamo": int(os.environ.get("MONTY_TTL_DYNAMO", "60")),         # 1 min
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
                 now: datetime | None = None) -> list[dict]:
    """Return event rows for the given environment over the lookback window.
    OCCURRED_AT / SENT_AT come back normalised to UTC."""
    source = os.environ.get("MONTY_SOURCE", "snowflake").lower()
    if source == "csv":
        rows = _fetch_csv(lookback_days, env, now)
    elif source == "s3":
        rows = _fetch_s3(lookback_days, env, now)
    elif source in ("dynamo", "ddb", "dynamodb"):
        rows = _fetch_dynamo(lookback_days, env, now)
    elif source == "sqlite":
        rows = _fetch_sqlite(lookback_days, env, now)
    elif source == "both":
        rows = _fetch_both(lookback_days, env, now)
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
# above). The dashboard needs "every pipeline's events in the window", which
# crosses partition keys (pk is per-pipeline), so this is a paginated Scan with
# a server-side filter — fine at this volume on an on-demand table, and the
# short _CACHE TTL absorbs refresh bursts.
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


def _fetch_dynamo(lookback_days, env, now):
    """Cached wrapper around the DynamoDB read (MONTY_TTL_DYNAMO, default 60s).
    Key is bucketed to the minute so a live view (now=utcnow) actually hits it."""
    ref = now or datetime.utcnow()
    key = (env, lookback_days, ref.replace(second=0, microsecond=0))
    return _cached("dynamo", key, lambda: _fetch_dynamo_impl(lookback_days, env, now))


def _fetch_dynamo_impl(lookback_days, env, now):
    """Read events from the per-env DynamoDB table over [start, now].

    Table is per-environment (MONTY_DDB_TABLE pattern, {env} filled from `env`),
    so the PROD/DEV toggle selects it; the per-env AWS profile resolution is
    shared with the S3 leg (same two accounts). `occurred_at` is stored as an
    ISO-8601 UTC string, so the window filter is a lexicographic BETWEEN."""
    from datetime import timedelta, timezone
    import boto3
    from boto3.dynamodb.conditions import Attr

    if now is None:
        now = datetime.utcnow()
    start = now - timedelta(days=lookback_days)
    table_name = MONTY_DDB_TABLE.format(env=env)

    profile = _s3_profile_for(env)
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    if profile:
        logger.info("dynamo: using AWS profile %r for env=%s", profile, env)
    table = session.resource("dynamodb", region_name=MONTY_DDB_REGION).Table(table_name)

    # Writer stamps tz-aware UTC ISO strings; anchor both bounds the same way
    # so the string comparison is apples-to-apples.
    start_iso = start.replace(tzinfo=timezone.utc).isoformat()
    end_iso = now.replace(tzinfo=timezone.utc).isoformat()
    scan_filter = (Attr("environment").eq(env)
                   & Attr("occurred_at").between(start_iso, end_iso))

    rows: list[dict] = []
    kwargs = {"FilterExpression": scan_filter}
    page = 0
    while True:
        page += 1
        logger.info("dynamo: [page %d] scanning %s window=[%s, %s] env=%s ...",
                    page, table_name, start, now, env)
        resp = table.scan(**kwargs)
        items = resp.get("Items", [])
        rows.extend(_normalise_ddb_row(item) for item in items)
        logger.info("dynamo: [page %d] %d item(s), %d total", page, len(items), len(rows))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        kwargs["ExclusiveStartKey"] = last_key

    rows = [r for r in rows if r.get("OCCURRED_AT") is not None]
    rows.sort(key=lambda r: r["OCCURRED_AT"])
    logger.info("dynamo: %d event row(s) in window from %s", len(rows), table_name)
    return rows


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


def _fetch_both(lookback_days, env, now):
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
    with ThreadPoolExecutor(max_workers=len(all_legs)) as pool:
        futures = {name: pool.submit(fn, lookback_days, env, now)
                   for name, fn in all_legs}

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
