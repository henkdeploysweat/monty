-- warehouse_credit_peaks.sql
--
-- All-time hourly peak credit usage per series for one warehouse — the "worst
-- load before" benchmark drawn as reference lines on the timeline credits
-- chart. WAREHOUSE_METERING_HISTORY is hourly grain, so MAX per column is the
-- peak single-hour load. ACCOUNT_USAGE retains ~365 days.
--
-- Params (bound by db.py): %(warehouse)s

SELECT
    MAX(credits_used)                AS PEAK_USED,
    MAX(credits_used_compute)        AS PEAK_COMPUTE,
    MAX(credits_used_cloud_services) AS PEAK_CLOUD
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE warehouse_name = %(warehouse)s;
