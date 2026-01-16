-- ============================================================================
-- TEST: Token Price Coverage
-- ============================================================================
-- WHY: The mart models (mart_bridge_fee_analysis, mart_general_overview) use 
--      token_prices to convert amounts to USD. If a token has deposits but no
--      price data, USD calculations will be NULL, breaking financial metrics.
--
-- WHAT: Finds tokens in deposits/fills that have no matching prices in token_prices.csv.
-- PASS: Returns 0 rows (all active tokens have price data).
-- FAIL: Returns rows = tokens we need to add to token_prices.csv.
--
-- ACTION: For each failing token, either:
--   1. Add price data to dbt/seeds/token_prices.csv, OR
--   2. Exclude from analysis in int_deposit_fill_matching.sql WHERE clause
-- ============================================================================

{{
    config(
        severity = 'warn',
        meta = {
            'owner': 'data-team',
            'action': 'Add above missing tokens to dbt/seeds/token_prices.csv OR add token addresses to WHERE NOT IN clause at end of int_deposit_fill_matching.sql'
        }
    )
}}

WITH deposit_tokens AS (
    -- Get unique base token symbols from deposits
    -- Use COALESCE to handle NULL symbols (missing metadata)
    SELECT DISTINCT
        input_token_symbol,
        origin_chain_name
    FROM {{ ref('int_unified_deposits') }}
    WHERE input_token_symbol IS NOT NULL
),

fill_tokens AS (
    -- Get unique base token symbols from fills
    SELECT DISTINCT
        fill_token_symbol,
        destination_chain_name
    FROM {{ ref('int_unified_fills') }}
    WHERE fill_token_symbol IS NOT NULL
),

all_used_tokens AS (
    -- Combine deposit and fill tokens into unified list
    SELECT DISTINCT input_token_symbol AS token_symbol
    FROM deposit_tokens
    UNION
    SELECT DISTINCT fill_token_symbol AS token_symbol
    FROM fill_tokens
),

available_prices AS (
    -- Get distinct tokens that have price data
    SELECT DISTINCT token_symbol
    FROM {{ ref('token_prices') }}
)

-- Find tokens used in transactions but missing from price data
SELECT
    '⚠️ Add to token_prices.csv OR add address to int_deposit_fill_matching.sql WHERE NOT IN' AS action_required,
    t.token_symbol,
    d.input_token_address,
    d.origin_chain_name,
    COUNT(*) AS deposit_count
FROM all_used_tokens t
LEFT JOIN available_prices p ON UPPER(t.token_symbol) = UPPER(p.token_symbol)
LEFT JOIN {{ ref('int_unified_deposits') }} d ON t.token_symbol = d.input_token_symbol
WHERE p.token_symbol IS NULL
GROUP BY t.token_symbol, d.input_token_address, d.origin_chain_name
HAVING COUNT(*) > 0  -- Only flag tokens that actually have activity

