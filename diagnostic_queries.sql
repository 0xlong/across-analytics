-- ============================================================
-- DIAGNOSTIC QUERY 1: Find deposits where output token join FAILED
-- These are rows causing the wrong output_amount conversion
-- ============================================================

SELECT 
    deposit_timestamp,
    transaction_hash,
    destination_chain_id,
    input_token_address,
    input_token_symbol,      -- Should have a value
    output_token_address,
    output_token_symbol,     -- Will be NULL if join failed!
    input_amount,
    output_amount,
    input_amount_raw,
    output_amount_raw
FROM intermediate.int_unified_deposits
WHERE output_token_symbol IS NULL
ORDER BY deposit_timestamp DESC
LIMIT 50;


-- ============================================================
-- DIAGNOSTIC QUERY 2: Find ALL missing token/chain combinations
-- These need to be added to token_metadata.csv
-- ============================================================

SELECT DISTINCT
    destination_chain_id,
    output_token_address,
    COUNT(*) as affected_deposits
FROM intermediate.int_unified_deposits
WHERE output_token_symbol IS NULL
GROUP BY destination_chain_id, output_token_address
ORDER BY affected_deposits DESC;


-- ============================================================
-- DIAGNOSTIC QUERY 3: Verify the mismatch magnitude
-- Shows how wrong the conversion is for affected rows
-- ============================================================

SELECT 
    destination_chain_id,
    output_token_address,
    output_token_symbol,
    AVG(input_amount) as avg_input,
    AVG(output_amount) as avg_output,
    AVG(output_amount / NULLIF(input_amount, 0)) as ratio,
    COUNT(*) as count
FROM intermediate.int_unified_deposits
WHERE output_token_symbol IS NULL
  AND input_amount > 0
GROUP BY destination_chain_id, output_token_address, output_token_symbol
ORDER BY count DESC;
