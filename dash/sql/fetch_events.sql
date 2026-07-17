-- fetch_events.sql
-- One windowed pull that feeds BOTH dashboards. The Python layer
-- (monty/transform.py) does the per-pipeline / per-metric aggregation,
-- which keeps this query trivial and lets you tune detector logic without
-- redeploying SQL. Swap {{table}} for your fully-qualified events table.
--
-- Params (bound by db.py): the-lookback-days and src-tz values; the env
-- predicate is injected as the {{env_filter}} placeholder (see db.ENV_ROUTING).
--   The timeline needs ~24h but also 7d of history to infer each pipeline's
--   cadence for staleness; the anomaly detector needs the full baseline.
--   Pulling 7d once serves both.
--
-- TIMEZONE CONTRACT
--   OCCURRED_AT / SENT_AT are TIMESTAMP_NTZ holding wall-clock in the source
--   zone %(src_tz)s (see MONTY_SOURCE_TZ in db.py). We CONVERT_TIMEZONE them
--   to UTC on the way out so EVERYTHING downstream (app.py, transform.py) is
--   UTC; display-tz localisation happens only in transform.py.
--
--   SYSDATE() is UTC. db.py rewrites SYSDATE() -> a bound "now" param for
--   reproducible windows; that anchor is likewise UTC. The range predicate stays
--   PRUNING-FRIENDLY: rather than wrap the indexed OCCURRED_AT column in a
--   function, we convert the UTC bounds back INTO the source zone and compare
--   against the raw column, so Snowflake can still prune micro-partitions.

SELECT
    ID,
    PIPELINE_NAME,
    METRIC_NAME,
    METRIC_VALUE,
    SEVERITY,
    RUN_ID,
    PAYLOAD,
    CONVERT_TIMEZONE(%(src_tz)s, 'UTC', OCCURRED_AT) AS OCCURRED_AT,
    IS_ALERT,
    SENT_TO_SLACK,
    CONVERT_TIMEZONE(%(src_tz)s, 'UTC', SENT_AT)     AS SENT_AT,
    ENVIRONMENT
FROM {{table}}
-- Which Snowflake ENVIRONMENT values feed this dashboard env is defined in
-- Python (db.ENV_ROUTING) and injected here as {{env_filter}}.
WHERE {{env_filter}}
  -- bounds are UTC (SYSDATE or the bound "now" param); convert them into the source zone so
  -- the comparison is raw-column vs raw-column and remains prunable.
  AND OCCURRED_AT >= CONVERT_TIMEZONE('UTC', %(src_tz)s,
                       DATEADD('day', -%(lookback_days)s, SYSDATE()))
  AND OCCURRED_AT <= CONVERT_TIMEZONE('UTC', %(src_tz)s, SYSDATE())
ORDER BY OCCURRED_AT;
