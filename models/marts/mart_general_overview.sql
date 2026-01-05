-- mart_general_overview.sql
-- PURPOSE: High-level protocol overview for the main Superset dashboard
-- GRAIN: Minute × Origin Chain × Destination Chain × Token
-- WHY: Provides general metrics with drill-down capability for flexible Superset filtering

{{ config(materialized='table') }}

WITH base_data AS (
    SELECT
        *,
        DATE_TRUNC('minute', deposit_timestamp) AS deposit_minute
    FROM {{ ref('int_deposit_fill_matching') }}
),

-- Chain name mapping
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

-- Aggregate by hour, route, and token
hourly_metrics AS (
    SELECT
        deposit_minute,
        origin_chain_id,
        destination_chain_id,
        route_id,
        deposit_token,
        
        -- === VOLUME METRICS ===
        -- Transaction counts
        COUNT(*) AS total_deposits,
        SUM(CASE WHEN is_filled THEN 1 ELSE 0 END) AS total_fills,
        
        -- Volume in native token units
        SUM(deposit_amount) AS total_deposit_volume,
        SUM(CASE WHEN is_filled THEN actual_output_amount ELSE 0 END) AS total_filled_volume,
        
        -- Volume in USD
        SUM(deposit_amount_usd) AS total_deposit_volume_usd,
        SUM(CASE WHEN is_filled THEN fill_amount_usd ELSE 0 END) AS total_filled_volume_usd,
        
        -- === ACTIVITY METRICS ===
        COUNT(DISTINCT depositor_address) AS unique_depositors,
        COUNT(DISTINCT relayer_address) FILTER (WHERE is_filled) AS unique_relayers,
        
        -- === PERFORMANCE SNAPSHOT (summary only) ===
        -- Fill rate
        ROUND((SUM(CASE WHEN is_filled THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(*), 0)) * 100, 2) AS fill_rate_pct,
        
        -- Latency summary (just avg and median for overview)
        ROUND(AVG(CASE WHEN is_filled THEN fill_latency_seconds END)::NUMERIC, 2) AS avg_fill_latency_seconds,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fill_latency_seconds) FILTER (WHERE is_filled)::NUMERIC, 2) AS median_fill_latency_seconds,
        
        -- === ECONOMIC SNAPSHOT (summary only) ===
        -- Total fees collected
        SUM(CASE WHEN is_filled THEN bridge_fee_nominal ELSE 0 END) AS total_bridge_fees,
        
        -- Average fee percentage
        ROUND(AVG(CASE WHEN is_filled THEN bridge_fee_percent END)::NUMERIC, 4) AS avg_bridge_fee_pct,
        
        -- Average slippage
        ROUND(AVG(CASE WHEN is_filled THEN slippage_percent END)::NUMERIC, 4) AS avg_slippage_pct
        
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
    hm.deposit_minute,
    
    -- Route identifiers with human-readable names
    hm.origin_chain_id,
    oc.chain_name AS origin_chain_name,
    hm.destination_chain_id,
    dc.chain_name AS destination_chain_name,
    hm.route_id,
    oc.chain_name || ' → ' || dc.chain_name AS route_name,
    
    -- Token info
    hm.deposit_token,
    tm.token_symbol,
    -- handle token symbol for base token as USDC.e and USDC.bridged are the same, etc.
    CASE 
        WHEN token_symbol LIKE 'USDC%' THEN 'USDC'
        WHEN token_symbol LIKE 'USDT%' THEN 'USDT'
        WHEN token_symbol LIKE 'DAI%' THEN 'DAI'
        WHEN token_symbol LIKE 'WETH%' THEN 'WETH'
        WHEN token_symbol LIKE 'WBTC%' THEN 'WBTC'
    ELSE token_symbol
    END AS base_token_symbol,
    
    -- Volume metrics
    hm.total_deposits,
    hm.total_fills,
    hm.total_deposit_volume,
    hm.total_filled_volume,
    hm.total_deposit_volume_usd,
    hm.total_filled_volume_usd,
    
    -- Activity metrics
    hm.unique_depositors,
    hm.unique_relayers,
    
    -- Performance snapshot
    hm.fill_rate_pct,
    hm.avg_fill_latency_seconds,
    hm.median_fill_latency_seconds,
    
    -- Economic snapshot
    hm.total_bridge_fees,
    hm.avg_bridge_fee_pct,
    hm.avg_slippage_pct

FROM hourly_metrics hm
LEFT JOIN chain_names oc ON hm.origin_chain_id = oc.chain_id
LEFT JOIN chain_names dc ON hm.destination_chain_id = dc.chain_id
LEFT JOIN token_metadata tm ON hm.origin_chain_id = tm.chain_id 
    AND LOWER(hm.deposit_token) = LOWER(tm.token_address)

ORDER BY hm.deposit_minute DESC, hm.total_deposit_volume_usd DESC