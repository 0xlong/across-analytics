-- int_refunds_expanded.sql
-- PURPOSE: Expand comma-separated refund arrays into individual rows
-- WHY: Each row = one relayer receiving one refund amount
-- ENABLES: Easy aggregation, filtering by relayer, joins with relayer metrics

  -- Table for faster downstream joins

WITH unified AS (
    -- Get all refunds from all chains (already unified in int_unified_refunds)
    SELECT * 
    FROM "across_analytics"."dbt_intermediate"."int_unified_refunds"
    WHERE refund_count > 0  -- Only process batches with actual refunds
),

-- Unnest the comma-separated strings into individual rows
-- Uses CROSS JOIN LATERAL with UNNEST to expand arrays
-- WITH ORDINALITY gives us the position index for matching amounts to addresses
expanded AS (
    SELECT
        -- Batch-level identifiers (same for all rows from same batch)
        refund_timestamp,
        transaction_hash,
        chain_id,
        root_bundle_id,
        leaf_id,
        refund_token_address,
        total_refund_amount,
        refund_count,
        source_blockchain,
        
        -- Individual refund data (one row per relayer/amount pair)
        -- TRIM handles any whitespace that might exist in the CSV-like strings
        TRIM(amounts.amount)::NUMERIC AS refund_amount,
        TRIM(addresses.address) AS relayer_address,
        amounts.idx AS refund_index
        
    FROM unified
    -- Expand refund_amounts_string: "100,200,300" → 3 rows with values 100, 200, 300
    CROSS JOIN LATERAL UNNEST(
        string_to_array(refund_amounts_string, ',')
    ) WITH ORDINALITY AS amounts(amount, idx)
    -- Expand refund_addresses_string: "0xAAA,0xBBB,0xCCC" → 3 rows with addresses
    CROSS JOIN LATERAL UNNEST(
        string_to_array(refund_addresses_string, ',')
    ) WITH ORDINALITY AS addresses(address, idx)
    -- Match by position: 1st amount goes to 1st address, 2nd to 2nd, etc.
    WHERE amounts.idx = addresses.idx
)

SELECT
    -- Event identification
    refund_timestamp,
    transaction_hash,
    source_blockchain,
    
    -- Batch identifiers (for grouping back if needed)
    chain_id,
    root_bundle_id,
    leaf_id,
    
    -- Individual refund details
    refund_index,
    relayer_address,
    refund_amount,
    refund_token_address,
    
    -- Batch context (useful for analysis)
    total_refund_amount AS batch_total_amount,
    refund_count AS batch_refund_count,
    
    -- Unique identifier for each individual refund record
    transaction_hash || '-' || leaf_id || '-' || refund_index AS refund_id
    
FROM expanded