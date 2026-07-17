-- fetch_events.sql
-- One windowed pull that feeds BOTH dashboards. The Python layer
-- (monty/transform.py) does the per-pipeline / per-metric aggregation,
-- which keeps this query trivial and lets you tune detector logic without
-- redeploying SQL. Swap {{table}} for your fully-qualified events table.
--
-- TIMEZONE: OCCURRED_AT / SENT_AT are TIMESTAMP_NTZ holding wall-clock in the
-- INGESTION writer's zone (not UTC). We normalise them to UTC in the SELECT so
-- everything downstream is UTC. The source zone is bound as %(src_tz)s by db.py
-- (default 'America/Los_Angeles'); set MONTY_SOURCE_TZ='UTC' once ingestion is
-- pinned to UTC and the conversion becomes a harmless no-op.
--
-- Params (bound by db.py): %(env)s, %(lookback_days)s, %(src_tz)s
--   The timeline needs ~24h but also 7d of history to infer each pipeline's
--   cadence for staleness; the anomaly detector needs the full baseline.
--   Pulling 7d once serves both.
--
-- NOW ANCHOR: SYSDATE() is always UTC (unlike CURRENT_TIMESTAMP(), which is
-- session-local). For archive/range/testing, db.py replaces SYSDATE() with a
-- bound UTC %(now)s. The range predicate compares the RAW (source-local)
-- column against source-local bounds so micro-partition pruning still works.

SELECT
    ID,
    PIPELINE_NAME,
    METRIC_NAME,
    METRIC_VALUE,
    SEVERITY,
    RUN_ID,
    PAYLOAD,
    CONVERT_TIMEZONE(%(src_tz)s, 'UTC', OCCURRED_AT) AS OCCURRED_AT,   -- -> UTC
    IS_ALERT,
    SENT_TO_SLACK,
    CONVERT_TIMEZONE(%(src_tz)s, 'UTC', SENT_AT)     AS SENT_AT,       -- -> UTC
    ENVIRONMENT
FROM {{table}}
WHERE ENVIRONMENT = COALESCE(NULLIF(%(env)s, 'default'), 'prod')
  -- bounds are UTC (SYSDATE / %(now)s) converted INTO the source zone, then
  -- compared against the raw column so OCCURRED_AT pruning is preserved
  AND OCCURRED_AT >= CONVERT_TIMEZONE('UTC', %(src_tz)s,
                        DATEADD('day', -%(lookback_days)s, SYSDATE()))
  AND OCCURRED_AT <= CONVERT_TIMEZONE('UTC', %(src_tz)s, SYSDATE())
ORDER BY OCCURRED_AT;
