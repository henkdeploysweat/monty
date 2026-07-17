-- fetch_events.sql
-- One windowed pull that feeds BOTH dashboards. The Python layer
-- (monty/transform.py) does the per-pipeline / per-metric aggregation,
-- which keeps this query trivial and lets you tune detector logic without
-- redeploying SQL. Swap {{table}} for your fully-qualified events table.
--
-- Params (bound by db.py): %(env)s, %(lookback_days)s
--   The timeline needs ~24h but also 7d of history to infer each pipeline's
--   cadence for staleness; the anomaly detector needs the full baseline.
--   Pulling 7d once serves both.

SELECT
    ID,
    PIPELINE_NAME,
    METRIC_NAME,
    METRIC_VALUE,
    SEVERITY,
    RUN_ID,
    PAYLOAD,
    OCCURRED_AT,
    IS_ALERT,
    SENT_TO_SLACK,
    SENT_AT,
    ENVIRONMENT
FROM {{table}}
WHERE ENVIRONMENT = %(env)s
  AND OCCURRED_AT >= DATEADD('day', -%(lookback_days)s, CURRENT_TIMESTAMP())
  AND OCCURRED_AT <=  CURRENT_TIMESTAMP()
ORDER BY OCCURRED_AT;
