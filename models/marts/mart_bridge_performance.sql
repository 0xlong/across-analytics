-- mart_bridge_performance.sql
-- PURPOSE: Hourly bridge health dashboard with core KPIs - performance metrics for monitoring bridge efficiency

{{ config(materialized='table') }}

WITH base_data AS (
    SELECT
        *,
        -- Extract minute for aggregation
        DATE_TRUNC('minute', deposit_timestamp) AS deposit_minute
    FROM {{ ref('int_deposit_fill_matching') }}
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

-- Token metadata for symbol lookup
token_metadata AS (
    SELECT * FROM {{ ref('token_metadata') }}
),  

-- Aggregate by minute, route, and token
minute_metrics AS (
    SELECT
        deposit_minute,
        origin_chain_id,
        destination_chain_id,
        route_id,
        deposit_token,
        
        -- Volume metrics
        COUNT(*) AS total_deposits,
        SUM(CASE WHEN is_filled THEN 1 ELSE 0 END) AS total_fills,
        SUM(CASE WHEN NOT is_filled THEN 1 ELSE 0 END) AS unfilled_count,
        
        -- Fill rate (% of deposits filled)
        ROUND(
            (SUM(CASE WHEN is_filled THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(*), 0)) * 100, 
            2
        ) AS fill_rate_pct,
        
        -- Amount metrics
        SUM(deposit_amount) AS total_deposit_volume,
        SUM(CASE WHEN NOT is_filled THEN deposit_amount ELSE 0 END) AS unfilled_volume,
        
        -- Latency metrics (only for filled deposits)
        ROUND(AVG(CASE WHEN is_filled THEN fill_latency_seconds END)::NUMERIC, 2) AS avg_fill_latency_seconds,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fill_latency_seconds) FILTER (WHERE is_filled)::NUMERIC, 2) AS p50_latency_seconds,
        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fill_latency_seconds) FILTER (WHERE is_filled)::NUMERIC, 2) AS p95_latency_seconds,
        MIN(CASE WHEN is_filled THEN fill_latency_seconds END) AS min_latency_seconds,
        MAX(CASE WHEN is_filled THEN fill_latency_seconds END) AS max_latency_seconds,
        
        -- Slippage metrics
        -- slippage here is the difference between expected (what despoitor is promised to get) and actual output (what depositor actually get)
        ROUND(AVG(slippage_percent)::NUMERIC, 4) AS avg_slippage_pct, 
        
        -- Bridge fee metrics (difference between deposit and fill amounts)
        ROUND(AVG(bridge_fee_nominal)::NUMERIC, 4) AS avg_bridge_fee_nominal,
        ROUND(AVG(bridge_fee_percent)::NUMERIC, 4) AS avg_bridge_fee_pct,
        
        -- Unique participants
        COUNT(DISTINCT depositor_address) AS unique_depositors,
        COUNT(DISTINCT relayer_address) FILTER (WHERE is_filled) AS unique_relayers
        
    FROM base_data
    GROUP BY 
        deposit_minute,
        origin_chain_id,
        destination_chain_id,
        route_id,
        deposit_token
)

SELECT
    -- Time dimension
    mm.deposit_minute,
    
    -- Route identifiers with human-readable names
    mm.origin_chain_id,
    oc.chain_name AS origin_chain_name,
    mm.destination_chain_id,
    dc.chain_name AS destination_chain_name,
    mm.route_id,
    oc.chain_name || ' → ' || dc.chain_name AS route_name,
    mm.deposit_token,
    tm.token_symbol,
    
    -- Volume metrics
    mm.total_deposits,
    mm.total_fills,
    mm.unfilled_count,
    mm.fill_rate_pct,
    
    -- Amount metrics
    mm.total_deposit_volume,
    mm.unfilled_volume,
    
    -- Latency metrics
    mm.avg_fill_latency_seconds,
    mm.p50_latency_seconds,
    mm.p95_latency_seconds,
    mm.min_latency_seconds,
    mm.max_latency_seconds,
    
    -- Slippage
    mm.avg_slippage_pct,
    
    -- Bridge fees
    mm.avg_bridge_fee_nominal,
    mm.avg_bridge_fee_pct,
    
    -- Participants
    mm.unique_depositors,
    mm.unique_relayers

FROM minute_metrics mm
LEFT JOIN chain_names oc ON mm.origin_chain_id = oc.chain_id
LEFT JOIN chain_names dc ON mm.destination_chain_id = dc.chain_id
LEFT JOIN token_metadata tm ON mm.origin_chain_id = tm.chain_id 
    AND LOWER(mm.deposit_token) = LOWER(tm.token_address)

ORDER BY mm.deposit_minute DESC, mm.total_deposits DESC
