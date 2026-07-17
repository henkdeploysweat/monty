-- pipeline_last_seen.sql
--
-- One row per pipeline that has emitted ANYTHING in the retention window,
-- with the UTC timestamp of its most recent event. Used by the timeline to
-- keep a pipeline visible (as STALE) after it stops running, instead of
-- silently dropping the lane the moment it has no events in the 24h view —
-- a dead pipeline vanishing is exactly the failure you want to see.
--
-- Cheap: a single grouped MAX, no row payloads pulled.
--
-- Params (bound by db.py): the weeks and src-tz values; the env predicate is
-- injected as the {{env_filter}} placeholder (see db.ENV_ROUTING).
-- Swap {{table}} for the fully-qualified events table.
--
-- TIMEZONE: same contract as fetch_events.sql — OCCURRED_AT is TIMESTAMP_NTZ
-- holding wall-clock in %(src_tz)s, so we convert the result to UTC and keep
-- the predicate raw-column vs raw-column so it stays partition-prunable.

SELECT
    PIPELINE_NAME,
    CONVERT_TIMEZONE(%(src_tz)s, 'UTC', MAX(OCCURRED_AT)) AS LAST_SEEN
FROM {{table}}
-- env routing defined in Python (db.ENV_ROUTING), injected as {{env_filter}}
WHERE {{env_filter}}
  AND OCCURRED_AT >= CONVERT_TIMEZONE('UTC', %(src_tz)s,
                       DATEADD('week', -%(weeks)s, SYSDATE()))
  AND OCCURRED_AT <= CONVERT_TIMEZONE('UTC', %(src_tz)s, SYSDATE())
GROUP BY PIPELINE_NAME
ORDER BY LAST_SEEN DESC;
