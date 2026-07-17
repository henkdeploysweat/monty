-- Monty: per-delivery audit trail. One row per Slack send attempt.
-- Decouples "metric arrived" (CUSTOM_METRICS) from "Slack accepted it" so
-- transient Slack failures retry without rewriting the metric row.

USE DATABASE MONITORING_DB;
USE SCHEMA MONITORING;

CREATE TABLE IF NOT EXISTS ALERT_OUTBOX (
    DELIVERY_ID     NUMBER      IDENTITY(1,1) PRIMARY KEY,
    METRIC_ID       NUMBER      NOT NULL,
    -- e.g. '#data-incidents', '#data-alerts', or a custom override.
    CHANNEL         STRING      NOT NULL,
    -- One of: 'sent', 'failed', 'skipped'. 'skipped' is for dedup paths.
    STATUS          STRING      NOT NULL,
    SENT_AT         TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    -- Populated when STATUS='failed'. Truncated to 4096 chars by the Observer.
    ERROR_MESSAGE   STRING,
    CONSTRAINT FK_ALERT_OUTBOX_METRIC
        FOREIGN KEY (METRIC_ID) REFERENCES CUSTOM_METRICS(ID),
    CONSTRAINT CK_ALERT_OUTBOX_STATUS
        CHECK (STATUS IN ('sent', 'failed', 'skipped'))
);

CREATE INDEX IF NOT EXISTS IDX_ALERT_OUTBOX_METRIC
    ON ALERT_OUTBOX (METRIC_ID, SENT_AT);

GRANT SELECT, INSERT ON TABLE ALERT_OUTBOX TO ROLE MONTY_SVC_ROLE;
