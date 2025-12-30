
  create view "across_analytics"."dbt_intermediate"."int_unified_refunds_expanded__dbt_tmp"
    
    
  as (
    -- int_refunds_expanded.sql
-- CHALLENGE: some refund_amounts_string are arrays that have to be unnested
-- PURPOSE: Expand comma-separated refund arrays into individual rows
-- WHY: Each row = one relayer receiving one refund amount
-- ENABLES: Easy aggregation, filtering by relayer, joins with relayer metrics



WITH unified AS (
    -- Get all refunds from all chains (already unified in int_unified_refunds)
    SELECT * 
    FROM "across_analytics"."dbt_intermediate"."int_unified_refunds"
    WHERE refund_count > 0  -- Only process batches with actual refunds
),

-- Hourly token prices for USD conversion
token_prices AS (
    SELECT 
        token_symbol,
        DATE_TRUNC('hour', timestamp::TIMESTAMP) AS price_hour,
        AVG(price_usd) AS price_usd
    FROM "across_analytics"."dbt"."token_prices"
    GROUP BY token_symbol, DATE_TRUNC('hour', timestamp::TIMESTAMP)
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
        chain_name,
        root_bundle_id,
        leaf_id,
        refund_token_address,
        refund_token_symbol,
        token_decimals,
        total_refund_amount,
        total_refund_amount_raw,
        refund_count,
        source_blockchain,
        
        -- Individual refund data (one row per relayer/amount pair)
        -- TRIM handles any whitespace that might exist in the CSV-like strings
        TRIM(amounts.amount)::NUMERIC AS refund_amount_raw,
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
    e.refund_timestamp,
    e.transaction_hash,
    e.source_blockchain,
    
    -- Batch identifiers (for grouping back if needed)
    e.chain_id,
    e.chain_name,
    e.root_bundle_id,
    e.leaf_id,
    
    -- Individual refund details
    e.refund_index,
    e.relayer_address,
    e.refund_token_address,
    e.refund_token_symbol,
    
    -- Individual refund amount (rescaled using token decimals)
    
    e.refund_amount_raw / POWER(10, COALESCE(e.token_decimals, 18))
 AS refund_amount,
    e.refund_amount_raw,
    
    -- USD price data
    tp.price_usd AS refund_token_price_usd,
    ROUND((
    e.refund_amount_raw / POWER(10, COALESCE(e.token_decimals, 18))
 * COALESCE(tp.price_usd, 1))::NUMERIC, 2) AS refund_amount_usd,
    
    -- Batch context (useful for analysis)
    e.total_refund_amount AS batch_total_amount,
    e.total_refund_amount_raw AS batch_total_amount_raw,
    e.refund_count AS batch_refund_count,
    
    -- Unique identifier for each individual refund record
    e.transaction_hash || '-' || e.leaf_id || '-' || e.refund_index AS refund_id
    
FROM expanded e
-- Join for refund token price at refund hour
LEFT JOIN token_prices tp
    ON e.refund_token_symbol = tp.token_symbol
    AND DATE_TRUNC('hour', e.refund_timestamp) = tp.price_hour
  );