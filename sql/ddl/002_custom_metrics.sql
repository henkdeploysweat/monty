USE DATABASE MONITORING_DB;
USE SCHEMA MONITORING;

CREATE TABLE IF NOT EXISTS CUSTOM_METRICS (
    ID              NUMBER          IDENTITY(1,1) PRIMARY KEY,
    PIPELINE_NAME   STRING          NOT NULL,
    ENVIRONMENT      STRING          NOT NULL,
    METRIC_NAME     STRING          NOT NULL,
    METRIC_VALUE    FLOAT,
    -- severity drives Slack channel routing in the Observer Lambda.
    -- Allowed values: 'critical', 'error', 'warning', 'info'.
    SEVERITY        STRING          NOT NULL DEFAULT 'info',
    RUN_ID          STRING,
    -- Free-form context: error_message, query, threshold, etc. Kept as VARIANT so
    -- the Observer can pull arbitrary keys without a schema migration per metric.
    PAYLOAD         VARIANT,
    OCCURRED_AT     TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    -- is_alert=TRUE means the Observer should notify Slack; FALSE means
    -- "metric recorded for trending only".
    IS_ALERT        BOOLEAN         NOT NULL DEFAULT FALSE,
    -- Observer flips this to TRUE after Slack accepts the message. Idempotency.
    SENT_TO_SLACK   BOOLEAN         NOT NULL DEFAULT FALSE,
    SENT_AT         TIMESTAMP_NTZ
);

-- Hot path for the Observer's poll query. Filters by is_alert + sent_to_slack
-- and orders by occurred_at; this index keeps the scan tiny even at millions
-- of historical rows.
ALTER TABLE CUSTOM_METRICS CLUSTER BY (IS_ALERT, SENT_TO_SLACK, OCCURRED_AT);

-- Service role: read+write+update (Observer flips sent_to_slack).
GRANT SELECT, INSERT, UPDATE ON TABLE CUSTOM_METRICS TO ROLE MONTY_SVC_ROLE;
-- Writer role: insert only (dbt + pipelines should never read or update).
GRANT INSERT ON TABLE CUSTOM_METRICS TO ROLE MONTY_WRITER_ROLE;
