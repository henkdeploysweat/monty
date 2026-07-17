# Monty Monitoring Dashboard Ideas

**Sample rows from the CSV (first 10 lines)**

| ID | PIPELINE_NAME | METRIC_NAME | METRIC_VALUE | SEVERITY | RUN_ID | PAYLOAD (truncated) | OCCURRED_AT | IS_ALERT | SENT_TO_SLACK | SENT_AT | ENVIRONMENT |
|----|---------------|-------------|--------------|----------|--------|----------------------|--------------|----------|----------------|---------|-------------|
| 1002816 | ai‑ingest‑kaylalogs‑ai‑load‑sn | load.postgresql.WAREHOUSE_WORKOUT_SESSION_FACTS.max_date | 1782518400 | info |  | `{"log_group":"/aws/lambda/ai‑ingest‑kaylalogs‑ai‑load‑sn","raw":{"load_type":"Append","max_date":"2026‑06‑27","rows_loaded":2659,"table_name":"WAREHOUSE_WORKOUT_SESSION_FACTS"}}` | 2026‑06‑26 23:51:40.055 | FALSE | FALSE |  | dev |
| 1002815 | ai‑ingest‑kaylalogs‑ai‑load‑sn | load.postgresql.WAREHOUSE_WORKOUT_FACTS.max_date | 1782518400 | info |  | `{"log_group":"/aws/lambda/ai‑ingest‑kaylalogs‑ai‑load‑sn","raw":{"load_type":"Append","max_date":"2026‑06‑27","rows_loaded":9,"table_name":"WAREHOUSE_WORKOUT_FACTS"}}` | 2026‑06‑26 23:51:18.609 | FALSE | FALSE |  | dev |
| 1000793 | ai‑ingest‑kaylalogs‑ai‑ingest | ingest.postgresql.batch.total_rows | 2668 | info |  | `{"log_group":"/aws/lambda/ai‑ingest‑kaylalogs‑ai‑ingest","raw":{"batch_index":0,"tables_skipped":0}}` | 2026‑06‑26 23:51:11.431 | FALSE | FALSE |  | dev |
| 1000792 | ai‑ingest‑kaylalogs‑ai‑ingest | ingest.postgresql.warehouse_workout_session_facts.max_date | 1782542700 | info |  | `{"log_group":"/aws/lambda/ai‑ingest‑kaylalogs‑ai‑ingest","raw":{"max_date":"2026‑06‑27"}}` | 2026‑06‑26 23:51:11.431 | FALSE | FALSE |  | dev |
| … | … | … | … | … | … | … | … | … | … | … | … |

*The file contains ~8 k rows; each row represents a metric emitted by a pipeline and stored in Snowflake table **MONITORING_DB.MONITORING.CUSTOM_METRICS**.*

---

## Useful monitoring dashboards / stats

Below are practical, “quick‑win” dashboards you can build directly on top of `CUSTOM_METRICS`.  All queries are written for Snowflake SQL; adapt column names if you add aliases.

| Dashboard | Core idea | Sample query (simplified) |
|----------|-----------|---------------------------|
| **Pipeline health over time** | Time‑series of the last‑N runs per `PIPELINE_NAME`, showing success‑rate and latency. | ```sql SELECT PIPELINE_NAME, DATE_TRUNC('hour', OCCURRED_AT) AS hr, COUNT(*) AS runs, SUM(CASE WHEN IS_ALERT THEN 1 ELSE 0 END) AS alerts FROM MONITORING_DB.MONITORING.CUSTOM_METRICS GROUP BY PIPELINE_NAME, hr ORDER BY hr;``` |
| **Severity heatmap** | Count of metrics by `SEVERITY` and hour/day to spot spikes. | ```sql SELECT DATE_TRUNC('day', OCCURRED_AT) AS day, SEVERITY, COUNT(*) AS cnt FROM MONITORING_DB.MONITORING.CUSTOM_METRICS GROUP BY day, SEVERITY ORDER BY day;``` |
| **Top‑N metrics by volume** | Which `METRIC_NAME`s generate the most rows (e.g., rows_loaded). | ```sql SELECT METRIC_NAME, SUM(METRIC_VALUE) AS total FROM MONITORING_DB.MONITORING.CUSTOM_METRICS WHERE METRIC_NAME ILIKE '%rows_loaded%' GROUP BY METRIC_NAME ORDER BY total DESC LIMIT 10;``` |
| **Environment comparison** | Compare dev / prod / staging metric trends side‑by‑side. | ```sql SELECT ENVIRONMENT, DATE_TRUNC('hour', OCCURRED_AT) AS hr, COUNT(*) AS cnt FROM MONITORING_DB.MONITORING.CUSTOM_METRICS GROUP BY ENVIRONMENT, hr ORDER BY hr;``` |
| **Alert latency** | Time between a metric becoming an alert (`IS_ALERT=TRUE`) and the Slack notification (`SENT_TO_SLACK=TRUE`). | ```sql SELECT ID, DATEDIFF('second', OCCURRED_AT, SENT_AT) AS latency_sec FROM MONITORING_DB.MONITORING.CUSTOM_METRICS WHERE IS_ALERT AND SENT_TO_SLACK;``` |
| **Data freshness** | Latest `max_date` values per downstream table – useful to see lag in ETL pipelines. | ```sql SELECT METRIC_NAME, MAX(METRIC_VALUE) AS latest_ts FROM MONITORING_DB.MONITORING.CUSTOM_METRICS WHERE METRIC_NAME LIKE '%.max_date' GROUP BY METRIC_NAME;``` |
| **Rows loaded per pipeline** | Sum of `rows_loaded` extracted from the JSON `PAYLOAD`. | ```sql SELECT PIPELINE_NAME, SUM(TO_NUMBER(PAYLOAD:raw:rows_loaded::string)) AS rows FROM MONITORING_DB.MONITORING.CUSTOM_METRICS WHERE METRIC_NAME ILIKE '%rows_loaded%' GROUP BY PIPELINE_NAME ORDER BY rows DESC;``` |
| **Slack delivery success** | Ratio of alerts that reached Slack. | ```sql SELECT ROUND(100.0 * SUM(CASE WHEN SENT_TO_SLACK THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_sent FROM MONITORING_DB.MONITORING.CUSTOM_METRICS WHERE IS_ALERT;``` |
| **Metric drift detection** | Detect sudden jumps in a numeric metric (e.g., `max_date` moving backwards). | ```sql WITH ordered AS ( SELECT METRIC_NAME, OCCURRED_AT, METRIC_VALUE, LAG(METRIC_VALUE) OVER (PARTITION BY METRIC_NAME ORDER BY OCCURRED_AT) AS prev FROM MONITORING_DB.MONITORING.CUSTOM_METRICS ) SELECT METRIC_NAME, OCCURRED_AT, METRIC_VALUE, prev FROM ordered WHERE prev IS NOT NULL AND METRIC_VALUE < prev;``` |
| **Daily run summary** | One‑row per day per pipeline summarising successes, alerts, and total rows loaded. | ```sql SELECT DATE_TRUNC('day', OCCURRED_AT) AS day, PIPELINE_NAME, COUNT(*) AS runs, SUM(CASE WHEN IS_ALERT THEN 1 ELSE 0 END) AS alerts, SUM(TO_NUMBER(PAYLOAD:raw:rows_loaded::string)) AS rows_loaded FROM MONITORING_DB.MONITORING.CUSTOM_METRICS GROUP BY day, PIPELINE_NAME;``` |

### Visualization suggestions
- **Time‑series line charts** for run count, alert count, and rows loaded.
- **Stacked bar charts** for severity distribution per day.
- **Heat‑map calendar** for alert frequency.
- **Gauge / KPI cards** showing “last‑run latency”, “% alerts sent to Slack”, “data freshness (hours behind)”.
- **Drill‑through tables** that link from a high‑level KPI to the underlying raw rows (including the JSON payload for detailed inspection).

### Implementation notes
1. **JSON extraction** – Snowflake’s `:` operator (`PAYLOAD:raw:rows_loaded::string`) works on the `PAYLOAD` column (stored as `VARIANT`). Cast to `NUMBER` where needed.
2. **Materialized views** – For dashboards refreshed every few minutes, create materialized views on the most‑used aggregates (e.g., per‑hour counts) to keep query latency low.
3. **Alert routing** – The `IS_ALERT` + `SENT_TO_SLACK` flags let you monitor delivery health; surface any gaps as a separate “Slack health” widget.
4. **Security** – Ensure the role used by the dashboard has only `SELECT` on `CUSTOM_METRICS` and cannot modify data.

These dashboards give you immediate visibility into pipeline performance, data freshness, and alert delivery – the core health signals Monty is designed to surface. Feel free to ask for any specific query or a more detailed visualization layout.