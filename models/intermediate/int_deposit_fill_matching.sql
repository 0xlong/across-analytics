-- int_deposit_fill_matching.sql
-- PURPOSE: Match every deposit to its fill (if it exists)
-- WHY: This is the CORE model - connects money leaving one chain to money arriving on another.
-- 
-- HOW IT WORKS:
-- 1. User deposits on Chain A → creates deposit_id
-- 2. Relayer fills on Chain B → same deposit_id
-- 3. We JOIN on deposit_id + origin/destination chain match
-- 4. Unfilled deposits = rows with NULL fill (stuck capital)

{{ config(materialized='view') }}

WITH deposits AS (
    SELECT * FROM {{ ref('int_unified_deposits') }}
),

fills AS (
    SELECT * FROM {{ ref('int_unified_fills') }}
),

-- JOIN deposits to fills on deposit_id + chain matching
matched AS (
    SELECT
        -- === DEPOSIT INFO (Origin side) ===
        d.deposit_timestamp,
        d.transaction_hash AS deposit_tx_hash,
        d.origin_chain_id,
        d.destination_chain_id,
        d.deposit_id,
        d.depositor_address,
        d.recipient_address AS deposit_recipient,
        d.input_token_address AS deposit_token,
        d.input_amount AS deposit_amount,
        d.output_amount AS expected_output_amount,
        
        -- === FILL INFO (Destination side) ===
        f.fill_timestamp,
        f.transaction_hash AS fill_tx_hash,
        f.relayer_address,
        f.output_token_address AS fill_token,
        f.output_amount AS actual_output_amount,
        f.repayment_chain_id,
        
        -- === COMPUTED FIELDS ===
        -- Fill latency: How long did it take to fill? (in seconds)
        -- Use GREATEST(0, ...) to handle cross-chain timestamp sync issues
        GREATEST(0, EXTRACT(EPOCH FROM (f.fill_timestamp - d.deposit_timestamp))) AS fill_latency_seconds,
        
        -- Is this deposit filled?
        CASE WHEN f.deposit_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_filled,
        
        -- Slippage: Difference between expected and actual output
        CASE 
            WHEN f.output_amount IS NOT NULL AND d.output_amount > 0 
            THEN ROUND(((d.output_amount - f.output_amount) / d.output_amount * 100)::NUMERIC, 2)
            ELSE NULL 
        END AS slippage_percent

    FROM deposits d
    
    -- LEFT JOIN: Keep ALL deposits, even unfilled ones
    LEFT JOIN fills f 
        ON d.deposit_id = f.deposit_id
        AND d.origin_chain_id = f.origin_chain_id  -- Must match origin
        AND d.destination_chain_id = f.destination_chain_id  -- Must match destination
)

SELECT
    -- Identity
    deposit_timestamp,
    deposit_tx_hash,
    origin_chain_id,
    destination_chain_id,
    deposit_id,
    
    -- Deposit details
    depositor_address,
    deposit_recipient,
    deposit_token,
    deposit_amount,
    expected_output_amount,
    
    -- Fill details (NULL if unfilled)
    fill_timestamp,
    fill_tx_hash,
    relayer_address,
    fill_token,
    actual_output_amount,
    repayment_chain_id,
    
    -- Metrics
    fill_latency_seconds,
    is_filled,
    slippage_percent,
    
    -- Route identifier (for aggregations)
    origin_chain_id || '_' || destination_chain_id AS route_id

FROM matched
