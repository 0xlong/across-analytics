-- ============================================================================
-- mart_bridge_fee_analysis.sql
-- ============================================================================
-- PURPOSE: Analyze bridge fees to identify over-priced corridors and 
--          inform competitive pricing strategy
-- 
-- GRAIN: Minute × Route × Token
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
        DATE_TRUNC('minute', deposit_timestamp) AS deposit_minute
    FROM {{ ref('int_deposit_fill_matching') }}
    WHERE is_filled = TRUE  -- Only filled deposits have fee data
),

-- Chain name mapping (from centralized seed)
chain_names AS (
    SELECT chain_id, chain_name
    FROM {{ ref('chain_metadata') }}
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
        deposit_minute,
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
            ((SUM(bridge_fee_nominal * COALESCE(deposit_token_price_usd, 1)) / 
             NULLIF(SUM(deposit_amount_usd), 0)) * 100)::NUMERIC, 4) 
        AS effective_fee_pct,
        
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
        COUNT(DISTINCT relayer_address) AS unique_relayers,
        
        -- ========================================================================
        -- GAS COST METRICS (Relayer Cost Analysis)
        -- ========================================================================
        -- WHAT: Gas price and cost metrics for relayer fills
        -- WHY:  Gas costs are incurred by RELAYERS on the destination chain.
        --       High gas costs may:
        --       - Reduce relayer profitability → fewer fillers → slower fills
        --       - Force higher bridge fees to remain profitable
        --       - Explain fee volatility on certain corridors
        -- USE:  Relayer economics analysis, corridor cost comparison, fee justification.
        -- ========================================================================
        -- ========================================================================
        -- Average gas price in Gwei (wei / 10^9)
        -- NOTE: Cast to NUMERIC before aggregation to prevent bigint overflow
        ROUND((AVG(gas_price_wei::NUMERIC) / 1e9)::NUMERIC, 4) AS avg_gas_price_gwei,
        
        -- Median gas price in Gwei (more robust than average)
        ROUND(
            (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY gas_price_wei::NUMERIC) / 1e9)::NUMERIC, 
            4
        ) AS median_gas_price_gwei,
        
        -- Total gas cost in native token (sum of all relayer gas costs)
        -- NOTE: Different chains use different native tokens (ETH, MATIC, WLD, etc.)
        -- NOTE: Cast to NUMERIC before aggregation to prevent bigint overflow
        ROUND((SUM(gas_cost_wei::NUMERIC) / 1e18)::NUMERIC, 8) AS total_gas_cost_native,
        
        -- Average gas cost per fill in native token
        ROUND((AVG(gas_cost_wei::NUMERIC) / 1e18)::NUMERIC, 8) AS avg_gas_cost_native,
        
        -- ========================================================================
        -- GAS COST IN USD (Cross-Chain Comparable)
        -- ========================================================================
        -- WHAT: Gas costs converted to USD using native token price at fill time
        -- WHY:  Native token costs aren't comparable across chains (0.001 ETH ≠ 0.001 MATIC).
        --       USD conversion enables:
        --       - Cross-chain gas cost comparison
        --       - Total relayer operational cost analysis
        --       - Fee vs gas cost ratio calculations
        -- USE:  Relayer profitability analysis, corridor cost benchmarking.
        -- ========================================================================
        ROUND(SUM(gas_cost_usd)::NUMERIC, 2) AS total_gas_cost_usd,
        ROUND(AVG(gas_cost_usd)::NUMERIC, 4) AS avg_gas_cost_usd
        
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
-- PURPOSE: Transform numeric metrics into categorical labels for easy
--          Superset filtering, alerting, and executive reporting.
-- ============================================================================
with_insights AS (
    SELECT
        *,
        
        -- ========================================================================
        -- PRICING TIER (Categorical Competitive Position)
        -- ========================================================================
        -- WHAT: Classifies route as OVERPRICED / HIGH / COMPETITIVE / AGGRESSIVE / VERY_LOW
        -- WHY:  Enables quick identification of routes where Across may be
        --       losing users to competitors (OVERPRICED) or leaving money on
        --       the table (VERY_LOW).
        --
        -- DATA-DRIVEN THRESHOLDS (based on actual distribution from dataset):
        --   Distribution: median=0.0135%, p75=0.056%, p95=0.37%, p99=2.15%
        --
        --   - OVERPRICED:   > 0.5%    → True outliers (~5% of data), investigate
        --   - HIGH:         0.1-0.5%  → Above p75, higher than typical (~15%)
        --   - COMPETITIVE:  0.02-0.1% → Around median, market rate (~30-40%)
        --   - AGGRESSIVE:   0.005-0.02% → Below median, undercutting (~20-30%)
        --   - VERY_LOW:     < 0.005%  → Near-zero fees, possibly subsidized (~15%)
        --
        -- USE:  Superset filter "Show OVERPRICED routes", pricing strategy review.
        -- ========================================================================
        CASE 
            WHEN effective_fee_pct > 0.5 THEN 'OVERPRICED'
            WHEN effective_fee_pct > 0.1 THEN 'HIGH'
            WHEN effective_fee_pct > 0.02 THEN 'COMPETITIVE'
            WHEN effective_fee_pct > 0.005 THEN 'AGGRESSIVE'
            ELSE 'VERY_LOW'
        END AS pricing_tier,
        
        -- ========================================================================
        -- FEE VOLATILITY TIER (Pricing Stability)
        -- ========================================================================
        -- WHAT: Classifies fee consistency as HIGH_VOLATILITY / MODERATE / LOW / STABLE
        -- WHY:  Users prefer predictable fees. High volatility indicates:
        --       - Dynamic pricing swings (may be correct but confusing)
        --       - Liquidity issues causing fee spikes
        --       - Inconsistent relayer behavior
        --
        -- DATA-DRIVEN THRESHOLDS (based on actual distribution from dataset):
        --   Distribution: median=0.057, p75=0.36, p95=1.68, p99=8.28
        --
        --   - HIGH_VOLATILITY:     > 1.0   → True outliers (~10%), investigate
        --   - MODERATE_VOLATILITY: 0.3-1.0 → Above p75, notable variation (~15-20%)
        --   - LOW_VOLATILITY:      0.05-0.3 → Around median, acceptable (~40-50%)
        --   - STABLE:              < 0.05  → Below median, consistent pricing (~25%)
        --
        -- USE:  Alert on HIGH_VOLATILITY routes, UX improvement targeting.
        -- ========================================================================
        CASE 
            WHEN fee_std_dev > 1.0 THEN 'HIGH_VOLATILITY'
            WHEN fee_std_dev > 0.3 THEN 'MODERATE_VOLATILITY'
            WHEN fee_std_dev > 0.05 THEN 'LOW_VOLATILITY'
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
        ROUND((max_fee_pct - min_fee_pct)::NUMERIC, 4) AS fee_spread_pct,
        
        -- ========================================================================
        -- GAS COST TIER (Relayer Economics)
        -- ========================================================================
        -- WHAT: Classifies corridors by gas cost level
        -- WHY:  High gas costs affect relayer profitability and may explain:
        --       - Higher bridge fees on certain routes
        --       - Fewer relayers willing to fill (leading to slower fills)
        --       - Corridors that need optimization
        --
        -- DATA-DRIVEN THRESHOLDS (based on actual distribution from dataset):
        --   Distribution: median=0.00001, p75=0.00016, p95=0.067, p99=0.19
        --
        --   - VERY_HIGH: > 0.05 native   → Top ~5%, expensive corridors
        --   - HIGH:      0.001-0.05      → Above p75, notable cost (~10-15%)
        --   - MEDIUM:    0.0001-0.001    → Around p75, moderate (~15-20%)
        --   - LOW:       0.00001-0.0001  → Around median (~30-40%)
        --   - VERY_LOW:  < 0.00001       → Below median, very cheap (~25%)
        --
        -- USE:  Corridor optimization, relayer incentive analysis.
        -- ========================================================================
        CASE 
            WHEN avg_gas_cost_native > 0.05 THEN 'VERY_HIGH'
            WHEN avg_gas_cost_native > 0.001 THEN 'HIGH'
            WHEN avg_gas_cost_native > 0.0001 THEN 'MEDIUM'
            WHEN avg_gas_cost_native > 0.00001 THEN 'LOW'
            ELSE 'VERY_LOW'
        END AS gas_cost_tier
        
    FROM hourly_fee_metrics
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
    wi.unique_relayers,
    
    -- Gas cost metrics (relayer economics)
    wi.avg_gas_price_gwei,
    wi.median_gas_price_gwei,
    wi.total_gas_cost_native,
    wi.avg_gas_cost_native,
    wi.total_gas_cost_usd,
    wi.avg_gas_cost_usd,
    wi.gas_cost_tier

FROM with_insights wi
LEFT JOIN chain_names oc ON wi.origin_chain_id = oc.chain_id
LEFT JOIN chain_names dc ON wi.destination_chain_id = dc.chain_id
LEFT JOIN token_metadata tm ON wi.origin_chain_id = tm.chain_id 
    AND LOWER(wi.deposit_token) = LOWER(tm.token_address)

ORDER BY wi.deposit_minute DESC, wi.total_deposit_volume_usd DESC
