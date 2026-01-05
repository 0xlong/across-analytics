-- ============================================================================
-- mart_fill_latency_analysis.sql
-- ============================================================================
-- PURPOSE: Analyze Time-to-Fill (TTF) to identify filler hesitation and 
--          liquidity gaps across routes
-- 
-- GRAIN: Minute × Route × Token
-- 
-- KEY BUSINESS QUESTIONS:
--   1. Which routes have "Filler Hesitation"? → slow_fill_pct > 10%
--   2. Is a new chain's filler infrastructure mature? → Compare p95_ttf
--   3. Where should we prioritize filler incentives? → liquidity_gap_status = 'HIGH'
-- ============================================================================

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
hourly_latency_metrics AS (
    SELECT
        deposit_minute,
        origin_chain_id,
        destination_chain_id,
        route_id,
        deposit_token,
        
        -- === VOLUME METRICS ===
        COUNT(*) AS total_deposits,
        SUM(CASE WHEN is_filled THEN 1 ELSE 0 END) AS total_fills,
        SUM(CASE WHEN NOT is_filled THEN 1 ELSE 0 END) AS unfilled_count,
        
        -- Fill rate
        ROUND(
            (SUM(CASE WHEN is_filled THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(*), 0)) * 100, 
            2
        ) AS fill_rate_pct,
        
        -- === LATENCY DISTRIBUTION ===
        -- Core TTF metrics (only for filled deposits)
        ROUND(AVG(CASE WHEN is_filled THEN fill_latency_seconds END)::NUMERIC, 2) AS avg_ttf_seconds,
        ROUND(
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fill_latency_seconds) 
            FILTER (WHERE is_filled)::NUMERIC, 
            2
        ) AS median_ttf_seconds,
        ROUND(
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fill_latency_seconds) 
            FILTER (WHERE is_filled)::NUMERIC, 
            2
        ) AS p95_ttf_seconds,
        MIN(CASE WHEN is_filled THEN fill_latency_seconds END) AS min_ttf_seconds,
        MAX(CASE WHEN is_filled THEN fill_latency_seconds END) AS max_ttf_seconds,
        
        -- === SPEED BUCKET COUNTS ===
        -- Instant fills (≤30 seconds) - "Lightning fast"
        SUM(CASE WHEN is_filled AND fill_latency_seconds <= 30 THEN 1 ELSE 0 END) AS instant_fill_count,
        
        -- Fast fills (≤60 seconds)
        SUM(CASE WHEN is_filled AND fill_latency_seconds <= 60 THEN 1 ELSE 0 END) AS fast_fill_count,
        
        -- Slow fills (>300 seconds / 5 minutes) - "Filler Hesitation"
        SUM(CASE WHEN is_filled AND fill_latency_seconds > 300 THEN 1 ELSE 0 END) AS slow_fill_count,
        
        -- Very slow fills (>900 seconds / 15 minutes) - "Critical Delay"
        SUM(CASE WHEN is_filled AND fill_latency_seconds > 900 THEN 1 ELSE 0 END) AS very_slow_fill_count,
        
        -- === VOLUME CONTEXT ===
        SUM(deposit_amount_usd) AS total_deposit_volume_usd,
        SUM(CASE WHEN is_filled THEN fill_amount_usd ELSE 0 END) AS total_filled_volume_usd,
        
        -- === PARTICIPANT METRICS ===
        COUNT(DISTINCT relayer_address) FILTER (WHERE is_filled) AS unique_relayers
        
    FROM base_data
    GROUP BY 
        deposit_minute,
        origin_chain_id,
        destination_chain_id,
        route_id,
        deposit_token
),

-- ============================================================================
-- COMPUTED INSIGHTS CTE
-- ============================================================================
-- PURPOSE: Transform raw counts into actionable percentages and categorical 
--          labels that can be used directly in Superset filters and alerts.
-- ============================================================================
with_insights AS (
    SELECT
        *,
        
        -- ========================================================================
        -- INSTANT FILL PERCENTAGE
        -- ========================================================================
        -- WHAT: % of fills completed in ≤30 seconds
        -- WHY:  Measures "lightning fast" user experience. High % = excellent UX.
        --       Across's competitive advantage is speed - this proves it.
        -- USE:  Dashboard KPI, compare across routes to find speed champions.
        -- ========================================================================
        ROUND(
            (instant_fill_count::NUMERIC / NULLIF(total_fills, 0)) * 100, 
            2
        ) AS instant_fill_pct,
        
        -- ========================================================================
        -- FAST FILL PERCENTAGE
        -- ========================================================================
        -- WHAT: % of fills completed in ≤60 seconds (includes instant fills)
        -- WHY:  Broader "good experience" threshold. Users generally don't 
        --       notice delays under 1 minute. Target: >90% for healthy routes.
        -- USE:  Route health monitoring, alerting if drops below threshold.
        -- ========================================================================
        ROUND(
            (fast_fill_count::NUMERIC / NULLIF(total_fills, 0)) * 100, 
            2
        ) AS fast_fill_pct,
        
        -- ========================================================================
        -- SLOW FILL PERCENTAGE ("Filler Hesitation" Indicator)
        -- ========================================================================
        -- WHAT: % of fills taking >5 minutes (300 seconds)
        -- WHY:  5+ minute waits indicate fillers are HESITATING to fill this route.
        --       Causes: low liquidity, high risk perception, poor RPC, or 
        --       unfavorable economics on that destination chain.
        -- USE:  Alert trigger (>10% = investigate), filler incentive prioritization.
        -- ========================================================================
        ROUND(
            (slow_fill_count::NUMERIC / NULLIF(total_fills, 0)) * 100, 
            2
        ) AS slow_fill_pct,
        
        -- ========================================================================
        -- VERY SLOW FILL PERCENTAGE ("Critical Delay" Indicator)
        -- ========================================================================
        -- WHAT: % of fills taking >15 minutes (900 seconds)
        -- WHY:  15+ minute waits are CRITICAL failures. Users likely abandoned
        --       or contacted support. These routes need immediate attention.
        -- USE:  High-priority alert, escalation to engineering team.
        -- ========================================================================
        ROUND(
            (very_slow_fill_count::NUMERIC / NULLIF(total_fills, 0)) * 100, 
            2
        ) AS very_slow_fill_pct,
        
        -- ========================================================================
        -- LIQUIDITY GAP STATUS (Categorical Health Label)
        -- ========================================================================
        -- WHAT: Classifies each hourly observation as CRITICAL / HIGH / MODERATE / HEALTHY 
        --       based on the p95 TTF for that hour-route-token combination.
        -- WHY:  Provides a simple filter in Superset: "Show me all CRITICAL gap routes"
        --       The p95 is used (not avg) because we care about WORST-CASE experience.
        -- 
        -- DATA-DRIVEN THRESHOLDS (based on actual distribution from dataset):
        --   Raw fill latencies (all users): median=8s, p75=15s, p95=42s
        --   Route-level p95s: median=10.5s, p75=24s, p95=96s
        --
        --   - CRITICAL: > 100s  → Route's worst 5% is 2.5x slower than global worst 5%
        --   - HIGH:     30-100s → Route's worst 5% exceeds global p95 (42s), investigate
        --   - MODERATE: 15-30s  → Route's worst 5% is typical to slightly slow
        --   - HEALTHY:  < 15s   → Route's worst 5% beats global p75 (15s), excellent UX
        --
        -- USE:  Superset filter, executive summary, filler incentive targeting.
        -- ========================================================================
        CASE 
            WHEN p95_ttf_seconds > 100 THEN 'CRITICAL'
            WHEN p95_ttf_seconds > 30 THEN 'HIGH'
            WHEN p95_ttf_seconds > 15 THEN 'MODERATE'
            ELSE 'HEALTHY'
        END AS liquidity_gap_status
        
    FROM hourly_latency_metrics
)

SELECT
    -- Time dimension
    wi.deposit_minute,
    
    -- Route identifiers with human-readable names
    wi.origin_chain_id,
    oc.chain_name AS origin_chain_name,
    wi.destination_chain_id,
    dc.chain_name AS destination_chain_name,
    wi.route_id,
    oc.chain_name || ' → ' || dc.chain_name AS route_name,
    
    -- Token info
    wi.deposit_token,
    tm.token_symbol,
    CASE 
        WHEN tm.token_symbol LIKE 'USDC%' THEN 'USDC'
        WHEN tm.token_symbol LIKE 'USDT%' THEN 'USDT'
        WHEN tm.token_symbol LIKE 'DAI%' THEN 'DAI'
        WHEN tm.token_symbol LIKE 'WETH%' THEN 'WETH'
        WHEN tm.token_symbol LIKE 'WBTC%' THEN 'WBTC'
        ELSE tm.token_symbol
    END AS base_token_symbol,
    
    -- Volume metrics
    wi.total_deposits,
    wi.total_fills,
    wi.unfilled_count,
    wi.fill_rate_pct,
    wi.total_deposit_volume_usd,
    wi.total_filled_volume_usd,
    
    -- Latency distribution
    wi.avg_ttf_seconds,
    wi.median_ttf_seconds,
    wi.p95_ttf_seconds,
    wi.min_ttf_seconds,
    wi.max_ttf_seconds,
    
    -- Speed bucket counts
    wi.instant_fill_count,
    wi.fast_fill_count,
    wi.slow_fill_count,
    wi.very_slow_fill_count,
    
    -- Speed bucket percentages
    wi.instant_fill_pct,
    wi.fast_fill_pct,
    wi.slow_fill_pct,
    wi.very_slow_fill_pct,
    
    -- Computed insights
    wi.liquidity_gap_status,
    
    -- Participant metrics
    wi.unique_relayers

FROM with_insights wi
LEFT JOIN chain_names oc ON wi.origin_chain_id = oc.chain_id
LEFT JOIN chain_names dc ON wi.destination_chain_id = dc.chain_id
LEFT JOIN token_metadata tm ON wi.origin_chain_id = tm.chain_id 
    AND LOWER(wi.deposit_token) = LOWER(tm.token_address)

ORDER BY wi.deposit_minute DESC, wi.total_deposit_volume_usd DESC
