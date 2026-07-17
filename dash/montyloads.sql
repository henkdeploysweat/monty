SELECT
    TIME_SLICE(OCCURRED_AT, 15, 'MINUTE') AS bucket_start,
    COUNT(*)                              AS rows_loaded
FROM MONITORING_DB.MONITORING.CUSTOM_METRICS
WHERE ENVIRONMENT = 'prod'
  AND OCCURRED_AT >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY 1                    -- the TIME_SLICE expression, by position
ORDER BY 1;