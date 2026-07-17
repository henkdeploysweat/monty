-- anomaly_scores.sql   (OPTIONAL / scale-up path)
--
-- Computes the same robust-z ranking as monty/transform.build_anomaly_context
-- (POINT mode), but entirely in Snowflake. Use this instead of the Python
-- detector once the baseline window is too large to pull into the app.
-- Returns one row per numeric, non-monotonic metric, scoring its latest value
-- against its own median/MAD baseline.
--
-- Params: %(env)s, %(baseline_days)s, %(min_points)s, %(z_threshold)s, %(min_pct)s
-- Swap {{table}} for your fully-qualified events table.
--
-- NOTE ON MAD: median-absolute-deviation is a TWO-STAGE aggregation
-- (median of |x - median(x)|). You cannot nest a window MEDIAN() OVER inside
-- an aggregate MEDIAN() in one GROUP BY, so it is split into `med` then `mad`.

WITH series AS (
    SELECT METRIC_NAME, PIPELINE_NAME, OCCURRED_AT, METRIC_VALUE
    FROM {{table}}
    WHERE ENVIRONMENT = %(env)s
      AND METRIC_VALUE IS NOT NULL
      AND OCCURRED_AT >= DATEADD('day', -%(baseline_days)s, CURRENT_TIMESTAMP())
      -- exclude monotonic freshness metrics; those are staleness signals,
      -- handled by the pipeline timeline, not value anomalies.
      AND NOT (LOWER(METRIC_NAME) LIKE '%%.max_date'
               OR LOWER(METRIC_NAME) LIKE '%%watermark%%')
),
latest AS (                     -- the point being scored: newest per metric
    SELECT METRIC_NAME, PIPELINE_NAME,
           METRIC_VALUE AS latest_value, OCCURRED_AT AS latest_at
    FROM series
    QUALIFY ROW_NUMBER() OVER (PARTITION BY METRIC_NAME ORDER BY OCCURRED_AT DESC) = 1
),
baseline AS (                   -- every point EXCEPT the latest, per metric
    SELECT s.METRIC_NAME, s.METRIC_VALUE
    FROM series s
    JOIN latest l ON s.METRIC_NAME = l.METRIC_NAME
    WHERE NOT (s.OCCURRED_AT = l.latest_at AND s.METRIC_VALUE = l.latest_value)
),
med AS (                        -- stage 1: baseline median + point count
    SELECT METRIC_NAME, COUNT(*) AS n, MEDIAN(METRIC_VALUE) AS med
    FROM baseline
    GROUP BY METRIC_NAME
    HAVING COUNT(*) >= %(min_points)s
),
mad AS (                        -- stage 2: median absolute deviation
    SELECT b.METRIC_NAME, MEDIAN(ABS(b.METRIC_VALUE - m.med)) AS mad
    FROM baseline b
    JOIN med m ON b.METRIC_NAME = m.METRIC_NAME
    GROUP BY b.METRIC_NAME
),
stats AS (                      -- robust sigma = MAD / 0.6745 (Phi^-1(0.75))
    SELECT m.METRIC_NAME, m.n, m.med, d.mad / 0.6745 AS robust_sigma
    FROM med m
    JOIN mad d ON m.METRIC_NAME = d.METRIC_NAME
    WHERE d.mad > 0             -- constant metric: no variation to score
)
SELECT
    l.METRIC_NAME,
    l.PIPELINE_NAME,
    l.latest_value,
    l.latest_at,
    s.med                                                        AS median_baseline,
    s.robust_sigma,
    (l.latest_value - s.med) / s.robust_sigma                    AS z_score,
    (l.latest_value - s.med) / NULLIF(ABS(s.med), 0) * 100       AS pct_change,
    CASE WHEN l.latest_value < s.med THEN 'drop' ELSE 'spike' END AS direction,
    CASE
        WHEN ABS((l.latest_value - s.med) / s.robust_sigma) >= %(z_threshold)s
         AND ABS((l.latest_value - s.med) / NULLIF(ABS(s.med), 0) * 100) >= %(min_pct)s
        THEN TRUE ELSE FALSE
    END                                                          AS is_anomaly
FROM latest l
JOIN stats s ON l.METRIC_NAME = s.METRIC_NAME
ORDER BY ABS(z_score) DESC;
