-- ============================================================================
-- TEST: No Negative Bridge Fees
-- ============================================================================
-- WHY: Bridge fee = deposit_amount - output_amount. When deposit_amount is
--      incorrectly tiny (due to wrong decimals) but output_amount is correct,
--      the fee becomes negative: 0.000000001 - 1000 = -999.999999999.
--      This was the root cause of negative profitability in Superset heatmaps.
--
-- WHAT: Finds filled deposits where output > input (impossible in reality).
-- PASS: Returns 0 rows (all fees are positive or zero).
-- FAIL: Returns rows = decimal mismatch between deposit and fill amounts.
-- ============================================================================

SELECT 
    origin_chain_name,
    destination_chain_name,
    deposit_token_symbol,
    COUNT(*) AS negative_fee_count,
    SUM(bridge_fee_nominal_usd) AS total_negative_usd
FROM {{ ref('int_deposit_fill_matching') }}
WHERE bridge_fee_nominal < 0
  AND is_filled = TRUE  -- Only check completed fills
GROUP BY origin_chain_name, destination_chain_name, deposit_token_symbol
