-- ============================================================================
-- mart_bridge_fee_analysis.sql
-- ============================================================================
-- PURPOSE: Analyze bridge fees to identify over-priced corridors and 
--          inform competitive pricing strategy
-- 
-- GRAIN: Hour × Route × Token
-- 
-- KEY INSIGHT: bridge_fee_nominal = deposit_amount - actual_output_amount
--              This already includes ALL user costs: gas + relayer + bridge + slippage
-- 
-- KEY BUSINESS QUESTIONS:
--   1. Which corridors are overpriced? → effective_fee_pct comparison
--   2. ETH→Unichain vs ETH→Optimism fee comparison → Filter by origin, group by destination
--   3. Where are users paying the most? → Sort by effective_fee_pct DESC
-- ============================================================================

{{ config(materialized='table') }}

WITH base_data AS (
    SELECT
        *,
        DATE_TRUNC('hour', deposit_timestamp) AS deposit_hour
    FROM {{ ref('int_deposit_fill_matching') }}
    WHERE is_filled = TRUE  -- Only filled deposits have fee data
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

-- ============================================================================
-- HOURLY FEE METRICS CTE
-- ============================================================================
-- PURPOSE: Aggregate fee-related metrics by hour, route, and token.
--          All fee calculations are in USD for cross-token comparability.
-- ============================================================================
hourly_fee_metrics AS (
    SELECT
        deposit_hour,
        origin_chain_id,
        destination_chain_id,
        route_id,
        deposit_token,
        
        -- ========================================================================
        -- VOLUME METRICS
        -- ========================================================================
        -- WHAT: Count of filled transactions and USD volume
        -- WHY:  Provides context for fee metrics. A route with $10M volume and
        --       0.3% fee is more important than $10K volume with 0.1% fee.
        -- USE:  Weight fee analysis by volume, filter out low-activity routes.
        -- ========================================================================
        COUNT(*) AS total_fills,
        SUM(deposit_amount_usd) AS total_deposit_volume_usd,
        SUM(fill_amount_usd) AS total_filled_volume_usd,
        
        -- ========================================================================
        -- TOTAL FEES (USD)
        -- ========================================================================
        -- WHAT: Sum of all fees collected on this route, converted to USD
        -- WHY:  bridge_fee_nominal = deposit_amount - actual_output_amount
        --       This is the ALL-IN user cost including gas, relayer, and protocol fees.
        -- USE:  Revenue analysis, compare fee revenue across routes.
        -- ========================================================================
        SUM(bridge_fee_nominal * COALESCE(deposit_token_price_usd, 1)) AS total_fees_usd,
        
        -- ========================================================================
        -- EFFECTIVE FEE PERCENTAGE (Core Metric)
        -- ========================================================================
        -- WHAT: Total fees / Total volume × 100 (volume-weighted average fee)
        -- WHY:  This is the TRUE cost per dollar bridged. Unlike avg_fee_pct,
        --       this weights large transactions more heavily (as they should be).
        --       Example: 10 small txns at 0.5% + 1 large txn at 0.1% → effective < 0.5%
        -- USE:  Primary metric for corridor pricing comparison. Target: <0.3%.
        -- ========================================================================
        ROUND(
            (SUM(bridge_fee_nominal * COALESCE(deposit_token_price_usd, 1)) / 
             NULLIF(SUM(deposit_amount_usd), 0)) * 100,
            4
        ) AS effective_fee_pct,
        
        -- ========================================================================
        -- AVERAGE FEE PERCENTAGE
        -- ========================================================================
        -- WHAT: Simple arithmetic mean of fee % across all transactions
        -- WHY:  Treats all transactions equally. Useful for understanding "typical"
        --       user experience regardless of transaction size.
        -- USE:  Compare with effective_fee_pct; big difference = size disparity.
        -- ========================================================================
        ROUND(AVG(bridge_fee_percent)::NUMERIC, 4) AS avg_fee_pct,
        
        -- ========================================================================
        -- MEDIAN FEE PERCENTAGE
        -- ========================================================================
        -- WHAT: 50th percentile of fee % (half of txns pay more, half pay less)
        -- WHY:  More robust than avg to outliers. If median << avg, there are
        --       some transactions with unusually high fees (investigate why).
        -- USE:  Robust "typical" fee, outlier detection when compared to avg.
        -- ========================================================================
        ROUND(
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY bridge_fee_percent)::NUMERIC, 
            4
        ) AS median_fee_pct,
        
        -- ========================================================================
        -- FEE STANDARD DEVIATION
        -- ========================================================================
        -- WHAT: How much fee % varies across transactions
        -- WHY:  High std dev = unpredictable pricing for users. Users prefer
        --       consistent fees. High volatility may indicate dynamic pricing
        --       issues or liquidity fluctuations.
        -- USE:  Stability indicator, alerting on high volatility routes.
        -- ========================================================================
        ROUND(STDDEV(bridge_fee_percent)::NUMERIC, 4) AS fee_std_dev,
        
        -- ========================================================================
        -- MIN/MAX FEE PERCENTAGE
        -- ========================================================================
        -- WHAT: Best and worst fee % observed on this route
        -- WHY:  Shows the full range of user experience. Large spread indicates
        --       inconsistent pricing that confuses users.
        -- USE:  Outlier investigation, competitive analysis ("best rate we offer").
        -- ========================================================================
        ROUND(MIN(bridge_fee_percent)::NUMERIC, 4) AS min_fee_pct,
        ROUND(MAX(bridge_fee_percent)::NUMERIC, 4) AS max_fee_pct,
        
        -- ========================================================================
        -- SLIPPAGE METRICS (Transparency, Not Cost)
        -- ========================================================================
        -- WHAT: Difference between expected_output and actual_output as %
        -- WHY:  Slippage is ALREADY INCLUDED in bridge_fee_nominal. We show it
        --       separately for transparency: how much of the fee is "predictable"
        --       vs "market movement".
        -- NOTE: Positive slippage = user got less than expected (bad)
        --       Negative slippage = user got more than expected (rare, good)
        -- USE:  Transparency reporting, not for cost calculation.
        -- ========================================================================
        ROUND(AVG(slippage_percent)::NUMERIC, 4) AS avg_slippage_pct,
        ROUND(
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY slippage_percent)::NUMERIC, 
            4
        ) AS median_slippage_pct,
        
        -- ========================================================================
        -- PARTICIPANT METRICS
        -- ========================================================================
        -- WHAT: Count of unique users and relayers on this route
        -- WHY:  High depositor count = popular route (user demand).
        --       High relayer count = competitive filling (healthy supply).
        --       Low relayer count + high volume = relayer concentration risk.
        -- USE:  Market health assessment, relayer diversity monitoring.
        -- ========================================================================
        COUNT(DISTINCT depositor_address) AS unique_depositors,
        COUNT(DISTINCT relayer_address) AS unique_relayers
        
    FROM base_data
    GROUP BY 
        deposit_hour,
        origin_chain_id,
        destination_chain_id,
        route_id,
        deposit_token
),

-- ============================================================================
-- COMPUTED INSIGHTS CTE
-- ============================================================================
-- PURPOSE: Transform numeric metrics into categorical labels for easy
--          Superset filtering, alerting, and executive reporting.
-- ============================================================================
with_insights AS (
    SELECT
        *,
        
        -- ========================================================================
        -- PRICING TIER (Categorical Competitive Position)
        -- ========================================================================
        -- WHAT: Classifies route as OVERPRICED / COMPETITIVE / AGGRESSIVE / VERY_LOW
        -- WHY:  Enables quick identification of routes where Across may be
        --       losing users to competitors (OVERPRICED) or leaving money on
        --       the table (VERY_LOW).
        --
        -- THRESHOLDS (effective_fee_pct):
        --   - OVERPRICED:   > 0.5%  → Users likely comparing to cheaper bridges
        --   - COMPETITIVE:  0.2-0.5% → Market rate, sustainable
        --   - AGGRESSIVE:   0.1-0.2% → Undercutting competitors
        --   - VERY_LOW:     < 0.1%  → Possibly subsidizing (intentional?)
        --
        -- USE:  Superset filter "Show OVERPRICED routes", pricing strategy review.
        -- ========================================================================
        CASE 
            WHEN effective_fee_pct > 0.5 THEN 'OVERPRICED'
            WHEN effective_fee_pct > 0.2 THEN 'COMPETITIVE'
            WHEN effective_fee_pct > 0.1 THEN 'AGGRESSIVE'
            ELSE 'VERY_LOW'
        END AS pricing_tier,
        
        -- ========================================================================
        -- FEE VOLATILITY TIER (Pricing Stability)
        -- ========================================================================
        -- WHAT: Classifies fee consistency as HIGH_VOLATILITY / MODERATE / STABLE
        -- WHY:  Users prefer predictable fees. High volatility indicates:
        --       - Dynamic pricing swings (may be correct but confusing)
        --       - Liquidity issues causing fee spikes
        --       - Inconsistent relayer behavior
        --
        -- THRESHOLDS (fee_std_dev):
        --   - HIGH_VOLATILITY:     > 0.5  → Fees vary wildly, investigate
        --   - MODERATE_VOLATILITY: 0.2-0.5 → Some variation, monitor
        --   - STABLE:              < 0.2  → Consistent pricing, good UX
        --
        -- USE:  Alert on HIGH_VOLATILITY routes, UX improvement targeting.
        -- ========================================================================
        CASE 
            WHEN fee_std_dev > 0.5 THEN 'HIGH_VOLATILITY'
            WHEN fee_std_dev > 0.2 THEN 'MODERATE_VOLATILITY'
            ELSE 'STABLE'
        END AS fee_volatility_tier,
        
        -- ========================================================================
        -- FEE SPREAD (Range Indicator)
        -- ========================================================================
        -- WHAT: max_fee_pct - min_fee_pct (width of fee range)
        -- WHY:  Complements std_dev with a simpler measure. Large spread means
        --       some users got great rates while others got terrible rates.
        --       Example: min=0.1%, max=1.0% → spread=0.9% (very inconsistent!)
        -- USE:  Fairness analysis, identify routes with extreme outliers.
        -- ========================================================================
        ROUND((max_fee_pct - min_fee_pct)::NUMERIC, 4) AS fee_spread_pct
        
    FROM hourly_fee_metrics
)

SELECT
    -- Time dimension
    wi.deposit_hour,
    
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
    wi.total_fills,
    wi.total_deposit_volume_usd,
    wi.total_filled_volume_usd,
    
    -- Fee metrics (USD)
    wi.total_fees_usd,
    wi.effective_fee_pct,
    
    -- Fee distribution
    wi.avg_fee_pct,
    wi.median_fee_pct,
    wi.fee_std_dev,
    wi.min_fee_pct,
    wi.max_fee_pct,
    wi.fee_spread_pct,
    
    -- Slippage (for transparency)
    wi.avg_slippage_pct,
    wi.median_slippage_pct,
    
    -- Computed insights
    wi.pricing_tier,
    wi.fee_volatility_tier,
    
    -- Participant metrics
    wi.unique_depositors,
    wi.unique_relayers

FROM with_insights wi
LEFT JOIN chain_names oc ON wi.origin_chain_id = oc.chain_id
LEFT JOIN chain_names dc ON wi.destination_chain_id = dc.chain_id
LEFT JOIN token_metadata tm ON wi.origin_chain_id = tm.chain_id 
    AND LOWER(wi.deposit_token) = LOWER(tm.token_address)

ORDER BY wi.deposit_hour DESC, wi.total_deposit_volume_usd DESC
