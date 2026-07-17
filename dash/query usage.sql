SELECT 
    -- Clean the query text once to group similar queries together
    REGEXP_REPLACE(SUBSTR(query_text, 1, 1000), '[0-9]{4}-[0-9]{2}-[0-9]{2}', '') AS cleaned_query,
    warehouse_name,
    warehouse_size,
    COUNT(*) AS execution_count,
    
    -- Total active execution time in seconds
    SUM(execution_time / 1000) AS total_execution_seconds,
    
    -- High-accuracy estimate of compute footprint (Execution Time * WH Size Credit Rate)
    SUM((execution_time / 1000 / 3600) * CASE UPPER(warehouse_size)
            WHEN 'X-SMALL'  THEN 1
            WHEN 'SMALL'    THEN 2
            WHEN 'MEDIUM'   THEN 4
            WHEN 'LARGE'    THEN 8
            WHEN 'X-LARGE'  THEN 16
            WHEN '2X-LARGE' THEN 32
            WHEN '3X-LARGE' THEN 64
            WHEN '4X-LARGE' THEN 128
            WHEN '5X-LARGE' THEN 256
            WHEN '6X-LARGE' THEN 512
            ELSE 1 -- Fallback for serverless or safely defaulting
        END
    ) AS estimated_compute_credits,
    
    -- Exact cloud services credits billed by Snowflake for these queries
    SUM(credits_used_cloud_services) AS actual_cloud_services_credits,
    
    -- Combined estimated total credit impact
    (SUM((execution_time / 1000 / 3600) * CASE UPPER(warehouse_size)
            WHEN 'X-SMALL'  THEN 1 
            WHEN 'SMALL'    THEN 2 
            WHEN 'MEDIUM'   THEN 4 
            WHEN 'LARGE'    THEN 8 
            WHEN 'X-LARGE'  THEN 16 
            WHEN '2X-LARGE' THEN 32 
            WHEN '3X-LARGE' THEN 64 
            WHEN '4X-LARGE' THEN 128 
            WHEN '5X-LARGE' THEN 256 
            WHEN '6X-LARGE' THEN 512 
            ELSE 1 
        END)) + SUM(credits_used_cloud_services) AS total_estimated_credits,
        
    SUM(rows_produced) AS total_rows_produced
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= '2026-06-01' 
  AND start_time < '2026-07-02'  

  AND warehouse_size IS NOT NULL -- Excludes metadata-only queries that used no warehouse
GROUP BY 
    1, -- Refers to cleaned_query
    warehouse_name,
    warehouse_size
ORDER BY total_estimated_credits DESC
