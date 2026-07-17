-- Monty: example AUDIT_REGISTRY rules. Use as a template for real rules.
-- Edit the SQL_CHECK and table names to match your environment before running.

USE DATABASE MONITORING_DB;
USE SCHEMA MONITORING;

-- Auditor self-monitor: alert if no heartbeat in the last 2h. The heartbeat is
-- written by RUN_AUDITOR() each run; absence of recent heartbeats means the
-- task itself stopped firing.
INSERT INTO AUDIT_REGISTRY
    (PIPELINE_NAME, METRIC_NAME, SQL_CHECK,
     COMPARATOR, THRESHOLD_VALUE, SEVERITY)
VALUES (
    'monty.auditor',
    'minutes_since_last_heartbeat',
    $$
        SELECT DATEDIFF('minute',
                        MAX(OCCURRED_AT),
                        CURRENT_TIMESTAMP())
        FROM MONITORING_DB.MONITORING.CUSTOM_METRICS
        WHERE PIPELINE_NAME = 'monty.auditor'
          AND METRIC_NAME = 'auditor_heartbeat'
    $$,
    '>', 120, 'critical'
);

-- Example: marketing unsub rate above 5% is a warning.
-- Replace the FQN below with your actual mart.
INSERT INTO AUDIT_REGISTRY
    (PIPELINE_NAME, METRIC_NAME, SQL_CHECK,
     COMPARATOR, THRESHOLD_VALUE, SEVERITY)
VALUES (
    'marketing',
    'unsubscribe_rate_pct',
    $$
        SELECT COALESCE(AVG(unsub_rate) * 100, 0)
        FROM SWEAT_ANALYTICS_CORE_DBT.MARTS.MART_MARKETING_EMAIL_PERFORMANCE
        WHERE event_date >= CURRENT_DATE - 1
    $$,
    '>', 5.0, 'warning'
);

-- Example: cross-table row-count parity (raw vs processed).
INSERT INTO AUDIT_REGISTRY
    (PIPELINE_NAME, METRIC_NAME, SQL_CHECK,
     COMPARATOR, THRESHOLD_VALUE, SEVERITY)
VALUES (
    'ingest.braze_users',
    'raw_vs_staged_row_delta',
    $$
        SELECT
            (SELECT COUNT(*) FROM BRAZE_DB.DATALAKE_SHARING.USERS)
          - (SELECT COUNT(*) FROM SWEAT_ANALYTICS_CORE_DBT.STAGING.STG_BRAZE__USERS)
    $$,
    '!=', 0, 'error'
);
