-- ============================================================================
-- TEST: Token Metadata Coverage
-- ============================================================================
-- WHY: Staging models JOIN with token_metadata to get decimals for amount 
--      rescaling. If a token is missing, decimals default to 18 (via COALESCE).
--      This causes USDC/USDT (6 decimals) to be divided by 10^18 instead of 
--      10^6, making amounts near-zero and breaking all downstream calculations.
--
-- WHAT: Finds deposits where input_token has no matching metadata (NULL symbol).
-- PASS: Returns 0 rows (all tokens have metadata).
-- FAIL: Returns rows = tokens we need to add to token_metadata.csv.
-- ============================================================================

SELECT 
    origin_chain_name,
    input_token_address,
    COUNT(*) AS missing_count
FROM {{ ref('int_unified_deposits') }}
WHERE input_token_symbol IS NULL
  -- Exclude intentionally unsupported tokens (POL on Polygon)
  AND LOWER(input_token_address) NOT IN (
      '0x25788a1a171ec66da6502f9975a15b609ff54cf6'  -- POL on Polygon (excluded)
  )
GROUP BY origin_chain_name, input_token_address
