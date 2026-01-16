-- ============================================================================
-- TEST: No Suspicious Amounts
-- ============================================================================
-- WHY: When token metadata is missing or has wrong decimals, the rescale_amount
--      macro divides by the wrong power of 10. For stablecoins (USDC/USDT),
--      dividing by 10^18 instead of 10^6 makes amounts 1 trillion times smaller.
--      A $1000 USDC deposit becomes 0.000000001 USDC - clearly wrong.
--
-- WHAT: Finds stablecoin deposits with suspiciously small amounts (< $0.01).
-- PASS: Returns 0 rows (all stablecoin amounts are reasonable).
-- FAIL: Returns rows = likely wrong decimals in token_metadata.csv.
-- ============================================================================

SELECT 
    origin_chain_name,
    input_token_symbol,
    input_token_address,
    COUNT(*) AS suspicious_count,
    AVG(input_amount) AS avg_suspicious_amount
FROM {{ ref('int_unified_deposits') }}
WHERE input_amount < 0.01  -- Amounts below 1 cent are suspicious for stablecoins
  AND input_token_symbol IN ('USDC', 'USDT', 'DAI')  -- Only check stablecoins
GROUP BY origin_chain_name, input_token_symbol, input_token_address
HAVING COUNT(*) > 10  -- Allow a few dust transactions, flag if pattern exists
