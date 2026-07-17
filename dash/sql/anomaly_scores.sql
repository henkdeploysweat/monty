-- anomaly_scores.sql   (OPTIONAL / scale-up path)
--
-- Computes the same robust-z ranking as monty/transform.build_anomaly_context,
-- but entirely in Snowflake. Use this instead of the Python detector once the
-- baseline window is too large to pull into the app. Returns one row per
-- numeric, non-monotonic metric with its latest value scored against its own
-- median/MAD baseline.
--
-- Params: %(env)s, %(baseline_days)s, %(min_points)s, %(z_threshold)s, %(min_pct)s
-- Swap {{table}} for your events table.

WITH series AS (
    SELECT
        METRIC_NAME,
        PIPELINE_NAME,
        OCCURRED_AT,
        METRIC_VALUE
    FROM {{table}}
    WHERE ENVIRONMENT = %(env)s
      AND METRIC_VALUE IS NOT NULL
      AND OCCURRED_AT >= DATEADD('day', -%(baseline_days)s, CURRENT_TIMESTAMP())
      -- exclude monotonic freshness metrics; those are staleness signals,
      -- handled by the pipeline timeline, not value anomalies.
      AND NOT (LOWER(METRIC_NAME) LIKE '%%.max_date'
               OR LOWER(METRIC_NAME) LIKE '%%watermark%%')
),
latest AS (
    SELECT METRIC_NAME, PIPELINE_NAME, METRIC_VALUE AS latest_value, OCCURRED_AT AS latest_at
    FROM series
    QUALIFY ROW_NUMBER() OVER (PARTITION BY METRIC_NAME ORDER BY OCCURRED_AT DESC) = 1
),
baseline AS (
    -- baseline = every point EXCEPT the latest, per metric
    SELECT s.METRIC_NAME, s.METRIC_VALUE
    FROM series s
    JOIN latest l
      ON s.METRIC_NAME = l.METRIC_NAME
     AND NOT (s.OCCURRED_AT = l.latest_at AND s.METRIC_VALUE = l.latest_value)
),
stats AS (
    SELECT
        METRIC_NAME,
        COUNT(*)                              AS n,
        MEDIAN(METRIC_VALUE)                  AS med,
        -- MAD -> robust sigma:  MAD / 0.6745
        MEDIAN(ABS(METRIC_VALUE - MEDIAN(METRIC_VALUE) OVER (PARTITION BY METRIC_NAME)))
                                              AS mad
    FROM baseline
    GROUP BY METRIC_NAME
    HAVING COUNT(*) >= %(min_points)s
)
SELECT
    l.METRIC_NAME,
    l.PIPELINE_NAME,
    l.latest_value,
    l.latest_at,
    s.med                                                       AS median_baseline,
    s.mad / 0.6745                                              AS robust_sigma,
    (l.latest_value - s.med) / NULLIF(s.mad / 0.6745, 0)        AS z_score,
    (l.latest_value - s.med) / NULLIF(ABS(s.med), 0) * 100      AS pct_change,
    CASE WHEN l.latest_value < s.med THEN 'drop' ELSE 'spike' END AS direction,
    CASE
        WHEN ABS((l.latest_value - s.med) / NULLIF(s.mad / 0.6745, 0)) >= %(z_threshold)s
         AND ABS((l.latest_value - s.med) / NULLIF(ABS(s.med), 0) * 100) >= %(min_pct)s
        THEN TRUE ELSE FALSE
    END                                                         AS is_anomaly
FROM latest l
JOIN stats s ON l.METRIC_NAME = s.METRIC_NAME
WHERE s.mad > 0
ORDER BY ABS(z_score) DESC;
