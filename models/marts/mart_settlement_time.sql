-- ============================================================================
-- mart_settlement_time.sql
-- ============================================================================
-- PURPOSE: Calculate Time-to-Settlement (TTS) for relayer refunds
-- 
-- WHAT IS TTS?
--   When a relayer fills a user's bridge request, they use their OWN money.
--   Later, the Across protocol pays them back (settles) via the optimistic oracle.
--   TTS = Time between FILL and SETTLEMENT (refund)
--
-- WHY THIS MATTERS:
--   - High TTS = Relayer capital is locked longer = Higher risk for relayers
--   - Low TTS = Faster capital recycling = More efficient for relayers
--   - Helps identify chains where settlement is slow
--
-- GRAIN: Minute × Chain × Relayer
-- ============================================================================

{{ config(materialized='table') }}

-- ============================================================================
-- STEP 1: Get all individual refunds (settlements) to relayers
-- ============================================================================
-- This model already expands batched refunds into one row per relayer payment
WITH refunds AS (
    SELECT
        refund_timestamp,           -- When the relayer got their money back
        chain_id,                   -- Which chain the refund was executed on
        relayer_address,            -- Who received the refund
        refund_token_address,
        refund_token_symbol,
        refund_amount,              -- How much they received (human-readable)
        root_bundle_id,             -- Groups refunds into batches
        source_blockchain
    FROM {{ ref('int_unified_refunds_expanded') }}
    WHERE relayer_address IS NOT NULL
),

-- ============================================================================
-- STEP 2: Get all fills (when relayers fronted their capital)
-- ============================================================================
-- Each fill = relayer used their own money to fulfill a user's bridge request
fills AS (
    SELECT
        fill_timestamp,             -- When the relayer filled the order
        destination_chain_id,       -- Where the fill happened
        relayer_address,            -- Who did the fill
        output_amount,              -- How much they paid out
        output_token_address
    FROM {{ ref('int_unified_fills') }}
    WHERE relayer_address IS NOT NULL
),

-- ============================================================================
-- STEP 3: Find the FIRST fill before each refund for each relayer
-- ============================================================================
-- Logic: For each refund, find the most recent fill by the same relayer
-- on the same chain. The difference = how long their capital was locked.
--
-- NOTE: This is an approximation because refunds are batched.
-- We match by relayer + chain + time window (refund must come AFTER fill)
refund_with_prior_fill AS (
    SELECT
        r.refund_timestamp,
        r.chain_id,
        r.relayer_address,
        r.refund_token_symbol,
        r.refund_amount,
        r.root_bundle_id,
        r.source_blockchain,
        
        -- Find the LATEST fill before this refund (closest in time)
        MAX(f.fill_timestamp) AS matched_fill_timestamp
        
    FROM refunds r
    
    -- Join fills where:
    -- 1. Same relayer
    -- 2. Same chain (destination chain = refund chain)
    -- 3. Fill happened BEFORE the refund (makes sense - can't refund before filling)
    LEFT JOIN fills f
        ON r.relayer_address = f.relayer_address
        AND r.chain_id = f.destination_chain_id
        AND f.fill_timestamp < r.refund_timestamp
        -- Only look at fills within 7 days of refund (reasonable settlement window)
        AND f.fill_timestamp > r.refund_timestamp - INTERVAL '7 days'
    
    GROUP BY
        r.refund_timestamp,
        r.chain_id,
        r.relayer_address,
        r.refund_token_symbol,
        r.refund_amount,
        r.root_bundle_id,
        r.source_blockchain
),

-- ============================================================================
-- STEP 4: Calculate Time-to-Settlement (TTS) for each matched pair
-- ============================================================================
with_tts AS (
    SELECT
        *,
        -- TTS in seconds = refund time - fill time
        CASE 
            WHEN matched_fill_timestamp IS NOT NULL 
            THEN EXTRACT(EPOCH FROM (refund_timestamp - matched_fill_timestamp))
            ELSE NULL 
        END AS settlement_time_seconds,
        
        -- TTS in minutes (matches grain)
        CASE 
            WHEN matched_fill_timestamp IS NOT NULL 
            THEN ROUND(
                (EXTRACT(EPOCH FROM (refund_timestamp - matched_fill_timestamp)) / 60)::NUMERIC, 
                2
            )
            ELSE NULL 
        END AS settlement_time_minutes
        
    FROM refund_with_prior_fill
),

-- ============================================================================
-- STEP 5: Aggregate by minute × Chain × Relayer
-- ============================================================================
-- Roll up individual TTS values into minute-level summaries
daily_aggregates AS (
    SELECT
        DATE_TRUNC('minute', refund_timestamp) AS settlement_date,
        chain_id,
        relayer_address,
        source_blockchain,
        
        -- Count metrics
        COUNT(*) AS settlement_count,
        COUNT(settlement_time_seconds) AS matched_settlement_count,
        
        -- TTS metrics (in minutes for readability)
        ROUND(AVG(settlement_time_minutes)::NUMERIC, 2) AS avg_settlement_time_minutes,
        ROUND(MIN(settlement_time_minutes)::NUMERIC, 2) AS min_settlement_time_minutes,
        ROUND(MAX(settlement_time_minutes)::NUMERIC, 2) AS max_settlement_time_minutes,
        
        -- Percentiles for distribution analysis
        ROUND(
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY settlement_time_minutes)::NUMERIC, 
            2
        ) AS median_settlement_time_minutes,
        ROUND(
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY settlement_time_minutes)::NUMERIC, 
            2
        ) AS p95_settlement_time_minutes,
        
        -- Volume settled
        SUM(refund_amount) AS total_settled_volume
        
    FROM with_tts
    WHERE settlement_time_seconds > 0  -- Only include valid TTS values
    GROUP BY
        DATE_TRUNC('minute', refund_timestamp),
        chain_id,
        relayer_address,
        source_blockchain
),

-- ============================================================================
-- STEP 6: Add human-readable chain names
-- ============================================================================
chain_names AS (
    SELECT chain_id, chain_name
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
)

-- ============================================================================
-- FINAL OUTPUT
-- ============================================================================
SELECT
    -- Time dimension
    da.settlement_date,
    
    -- Chain identification
    da.chain_id,
    cn.chain_name,
    da.source_blockchain,
    
    -- Relayer identification
    da.relayer_address,
    
    -- Settlement counts
    da.settlement_count,
    da.matched_settlement_count,
    
    -- TTS metrics (in minutes)
    da.avg_settlement_time_minutes,
    da.min_settlement_time_minutes,
    da.max_settlement_time_minutes,
    da.median_settlement_time_minutes,
    da.p95_settlement_time_minutes,
    
    -- Volume
    da.total_settled_volume

FROM daily_aggregates da
LEFT JOIN chain_names cn ON da.chain_id = cn.chain_id

ORDER BY da.settlement_date DESC, da.chain_id, da.total_settled_volume DESC
