-- mart_fill_latency_distribution.sql
-- PURPOSE: Fill latency histogram for "Why Across is 5-20x faster" analysis
-- GRAIN: Hour × Route × Latency Bucket
-- WHY: Enables visualization of speed distribution to prove Across's competitive advantage

{{ config(materialized='table') }}

WITH base_data AS (
    SELECT
        *,
        DATE_TRUNC('minute', deposit_timestamp) AS deposit_minute
    FROM {{ ref('int_deposit_fill_matching') }}
    WHERE is_filled = TRUE  -- Only filled deposits have latency
),

-- Chain name mapping for readability
chain_names AS (
    SELECT 
        chain_id,
        chain_name
    FROM (VALUES
        (1, 'Ethereum'),
        (42161, 'Arbitrum'),
        (137, 'Polygon'),
        (59144, 'Linea'),
        (480, 'Worldchain'),
        (130, 'Unichain'),
        (999, 'HyperEVM'),
        (143, 'Monad')
    ) AS chains(chain_id, chain_name)
),

-- Classify each fill into latency buckets
latency_classified AS (
    SELECT
        deposit_minute,
        origin_chain_id,
        destination_chain_id,
        route_id,
        fill_latency_seconds,
        deposit_amount,
        
        -- Latency bucket classification
        CASE 
            WHEN fill_latency_seconds <= 30 THEN '1_instant'
            WHEN fill_latency_seconds <= 60 THEN '2_fast'
            WHEN fill_latency_seconds <= 300 THEN '3_normal'
            WHEN fill_latency_seconds <= 900 THEN '4_slow'
            ELSE '5_very_slow'
        END AS latency_bucket,
        
        -- Human-readable bucket label
        CASE 
            WHEN fill_latency_seconds <= 30 THEN '≤30s (Instant)'
            WHEN fill_latency_seconds <= 60 THEN '31-60s (Fast)'
            WHEN fill_latency_seconds <= 300 THEN '1-5min (Normal)'
            WHEN fill_latency_seconds <= 900 THEN '5-15min (Slow)'
            ELSE '>15min (Very Slow)'
        END AS latency_bucket_label
        
    FROM base_data
),

-- Aggregate by minute, route, and latency bucket
bucket_aggregates AS (
    SELECT
        deposit_minute,
        origin_chain_id,
        destination_chain_id,
        route_id,
        latency_bucket,
        latency_bucket_label,
        
        -- Count metrics
        COUNT(*) AS fill_count,
        
        -- Volume metrics
        SUM(deposit_amount) AS fill_volume,
        
        -- Latency stats within bucket
        ROUND(AVG(fill_latency_seconds)::NUMERIC, 2) AS avg_latency_in_bucket,
        MIN(fill_latency_seconds) AS min_latency_in_bucket,
        MAX(fill_latency_seconds) AS max_latency_in_bucket
        
    FROM latency_classified
    GROUP BY 
        deposit_minute,
        origin_chain_id,
        destination_chain_id,
        route_id,
        latency_bucket,
        latency_bucket_label
),

-- Calculate percentages within each hour/route
with_percentages AS (
    SELECT
        ba.*,
        
        -- Percentage of fills in this bucket (within hour/route)
        ROUND(
            (ba.fill_count::NUMERIC / NULLIF(SUM(ba.fill_count) OVER (
                PARTITION BY ba.deposit_minute, ba.route_id
            ), 0)) * 100,
            2
        ) AS pct_of_fills,
        
        -- Cumulative percentage (for CDF charts)
        ROUND(
            (SUM(ba.fill_count) OVER (
                PARTITION BY ba.deposit_minute, ba.route_id
                ORDER BY ba.latency_bucket
            )::NUMERIC / NULLIF(SUM(ba.fill_count) OVER (
                PARTITION BY ba.deposit_minute, ba.route_id
            ), 0)) * 100,
            2
        ) AS cumulative_pct
        
    FROM bucket_aggregates ba
)

SELECT
    -- Time dimension
    wp.deposit_minute,
    
    -- Route identifiers with human-readable names
    wp.origin_chain_id,
    oc.chain_name AS origin_chain_name,
    wp.destination_chain_id,
    dc.chain_name AS destination_chain_name,
    wp.route_id,
    
    -- Latency bucket
    wp.latency_bucket,
    wp.latency_bucket_label,
    
    -- Counts and volume
    wp.fill_count,
    wp.fill_volume,
    
    -- Percentages
    wp.pct_of_fills,
    wp.cumulative_pct,
    
    -- Latency stats
    wp.avg_latency_in_bucket,
    wp.min_latency_in_bucket,
    wp.max_latency_in_bucket

FROM with_percentages wp
LEFT JOIN chain_names oc ON wp.origin_chain_id = oc.chain_id
LEFT JOIN chain_names dc ON wp.destination_chain_id = dc.chain_id

ORDER BY wp.deposit_minute DESC, wp.route_id, wp.latency_bucket