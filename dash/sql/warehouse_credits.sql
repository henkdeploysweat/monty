-- warehouse_credits.sql
--
-- Hourly Snowflake credit usage for one warehouse over a UTC [start, end)
-- window, to draw under the timeline's "Alerts / hour" strip.
--
-- start_time in ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY is TIMESTAMP_LTZ, so
-- we normalise it to a naive UTC hour bucket (matching the timeline, which is
-- UTC internally and only localised for display). ACCOUNT_USAGE lags real time
-- by up to ~1-3h, so the most recent hour(s) may read 0/absent.
--
-- Params (bound by db.py): %(warehouse)s, %(start)s, %(end)s  (start/end = naive UTC)

SELECT
    DATE_TRUNC('hour', TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE('UTC', start_time))) AS HOUR,
    SUM(credits_used)                AS CREDITS_USED,
    SUM(credits_used_compute)        AS CREDITS_COMPUTE,
    SUM(credits_used_cloud_services) AS CREDITS_CLOUD_SERVICES
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE warehouse_name = %(warehouse)s
  AND TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE('UTC', start_time)) >= %(start)s
  AND TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE('UTC', start_time)) <  %(end)s
GROUP BY 1
ORDER BY HOUR;
