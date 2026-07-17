-- Monty: the rules table. Add one row to start tracking a new SQL-based metric.
-- The Auditor stored procedure (sql/procedures/auditor_sp.sql) loops this on a
-- schedule and inserts results into CUSTOM_METRICS.
--
-- Adding a new check is a no-code operation:
--   INSERT INTO AUDIT_REGISTRY (PIPELINE_NAME, METRIC_NAME, SQL_CHECK,
--                               COMPARATOR, THRESHOLD_VALUE, SEVERITY)
--   VALUES ('marketing', 'unsub_rate',
--           'SELECT unsub_rate_pct FROM marts.marketing.fct_unsub',
--           '>', 5.0, 'warning');

USE DATABASE MONITORING_DB;
USE SCHEMA MONITORING;

CREATE TABLE IF NOT EXISTS AUDIT_REGISTRY (
    RULE_ID                 NUMBER      IDENTITY(1,1) PRIMARY KEY,
    PIPELINE_NAME           STRING      NOT NULL,
    METRIC_NAME             STRING      NOT NULL,
    -- SQL_CHECK MUST return a single scalar value (one row, one column).
    -- Auditor wraps it in a CTE; non-scalar results raise and are recorded as
    -- a 'auditor_failure' metric against this rule.
    SQL_CHECK               STRING      NOT NULL,
    -- One of: '>', '<', '>=', '<=', '==', '!='.
    -- is_alert = (metric_value <comparator> threshold_value).
    COMPARATOR              STRING      NOT NULL,
    THRESHOLD_VALUE         FLOAT       NOT NULL,
    -- Drives Slack routing when is_alert is TRUE.
    SEVERITY                STRING      NOT NULL DEFAULT 'warning',
    -- Optional: override severity-based channel routing. Leave NULL for default.
    SLACK_CHANNEL_OVERRIDE  STRING,
    ENABLED                 BOOLEAN     NOT NULL DEFAULT TRUE,
    CREATED_AT              TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT              TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT UQ_AUDIT_REGISTRY_PIPELINE_METRIC
        UNIQUE (PIPELINE_NAME, METRIC_NAME),
    CONSTRAINT CK_AUDIT_REGISTRY_COMPARATOR
        CHECK (COMPARATOR IN ('>', '<', '>=', '<=', '==', '!=')),
    CONSTRAINT CK_AUDIT_REGISTRY_SEVERITY
        CHECK (SEVERITY IN ('critical', 'error', 'warning', 'info'))
);

-- Service role (Auditor) reads the registry. No INSERT for the service role:
-- adding/removing rules is an admin operation done by hand or via Terraform/CI,
-- not by the Lambdas. This keeps SQL_CHECK strings out of reach of compromised
-- Lambdas.
GRANT SELECT ON TABLE AUDIT_REGISTRY TO ROLE MONTY_SVC_ROLE;
