-- int_unified_fills.sql
-- PURPOSE: Combine fills from ALL chains into ONE table
-- WHY: Fills happen on the DESTINATION chain. We need to see all fills to match with deposits.

{{ config(materialized='view') }}

-- Each CTE selects from a chain's staging model and adds the destination chain ID
WITH arbitrum_fills AS (
    SELECT 
        fill_timestamp,
        transaction_hash,
        origin_chain_id,
        42161 AS destination_chain_id,  -- Fill happened ON Arbitrum
        deposit_id,
        relayer_address,
        depositor_address,
        recipient_address,
        input_token_address,
        output_token_address,
        output_token_symbol,
        input_amount,
        output_amount,
        repayment_chain_id,
        gas_price_wei,
        gas_used,
        gas_cost_wei
    FROM {{ ref('stg_arbitrum__fills') }}
),

ethereum_fills AS (
    SELECT 
        fill_timestamp,
        transaction_hash,
        origin_chain_id,
        1 AS destination_chain_id,  -- Fill happened ON Ethereum
        deposit_id,
        relayer_address,
        depositor_address,
        recipient_address,
        input_token_address,
        output_token_address,
        output_token_symbol,
        input_amount,
        output_amount,
        repayment_chain_id,
        gas_price_wei,
        gas_used,
        gas_cost_wei
    FROM {{ ref('stg_ethereum__fills') }}
),

polygon_fills AS (
    SELECT 
        fill_timestamp,
        transaction_hash,
        origin_chain_id,
        137 AS destination_chain_id,  -- Fill happened ON Polygon
        deposit_id,
        relayer_address,
        depositor_address,
        recipient_address,
        input_token_address,
        output_token_address,
        output_token_symbol,
        input_amount,
        output_amount,
        repayment_chain_id,
        gas_price_wei,
        gas_used,
        gas_cost_wei
    FROM {{ ref('stg_polygon__fills') }}
),

linea_fills AS (
    SELECT 
        fill_timestamp,
        transaction_hash,
        origin_chain_id,
        59144 AS destination_chain_id,  -- Fill happened ON Linea
        deposit_id,
        relayer_address,
        depositor_address,
        recipient_address,
        input_token_address,
        output_token_address,
        output_token_symbol,
        input_amount,
        output_amount,
        repayment_chain_id,
        gas_price_wei,
        gas_used,
        gas_cost_wei
    FROM {{ ref('stg_linea__fills') }}
),

worldchain_fills AS (
    SELECT 
        fill_timestamp,
        transaction_hash,
        origin_chain_id,
        480 AS destination_chain_id,  -- Fill happened ON WorldChain
        deposit_id,
        relayer_address,
        depositor_address,
        recipient_address,
        input_token_address,
        output_token_address,
        output_token_symbol,
        input_amount,
        output_amount,
        repayment_chain_id,
        gas_price_wei,
        gas_used,
        gas_cost_wei
    FROM {{ ref('stg_worldchain__fills') }}
),

unichain_fills AS (
    SELECT 
        fill_timestamp,
        transaction_hash,
        origin_chain_id,
        130 AS destination_chain_id,  -- Fill happened ON Unichain
        deposit_id,
        relayer_address,
        depositor_address,
        recipient_address,
        input_token_address,
        output_token_address,
        output_token_symbol,
        input_amount,
        output_amount,
        repayment_chain_id,
        gas_price_wei,
        gas_used,
        gas_cost_wei
    FROM {{ ref('stg_unichain__fills') }}
),

hyperevm_fills AS (
    SELECT 
        fill_timestamp,
        transaction_hash,
        origin_chain_id,
        999 AS destination_chain_id,  -- Fill happened ON HyperEVM
        deposit_id,
        relayer_address,
        depositor_address,
        recipient_address,
        input_token_address,
        output_token_address,
        output_token_symbol,
        input_amount,
        output_amount,
        repayment_chain_id,
        gas_price_wei,
        gas_used,
        gas_cost_wei
    FROM {{ ref('stg_hyperevm__fills') }}
),

monad_fills AS (
    SELECT 
        fill_timestamp,
        transaction_hash,
        origin_chain_id,
        143 AS destination_chain_id,  -- Fill happened ON Monad
        deposit_id,
        relayer_address,
        depositor_address,
        recipient_address,
        input_token_address,
        output_token_address,
        output_token_symbol,
        input_amount,
        output_amount,
        repayment_chain_id,
        gas_price_wei,
        gas_used,
        gas_cost_wei
    FROM {{ ref('stg_monad__fills') }}
),

base_fills AS (
    SELECT 
        fill_timestamp,
        transaction_hash,
        origin_chain_id,
        8453 AS destination_chain_id,  -- Fill happened ON Base
        deposit_id,
        relayer_address,
        depositor_address,
        recipient_address,
        input_token_address,
        output_token_address,
        output_token_symbol,
        input_amount,
        output_amount,
        repayment_chain_id,
        gas_price_wei,
        gas_used,
        gas_cost_wei
    FROM {{ ref('stg_base__fills') }}
),

bsc_fills AS (
    SELECT 
        fill_timestamp,
        transaction_hash,
        origin_chain_id,
        56 AS destination_chain_id,  -- Fill happened ON BSC
        deposit_id,
        relayer_address,
        depositor_address,
        recipient_address,
        input_token_address,
        output_token_address,
        output_token_symbol,
        input_amount,
        output_amount,
        repayment_chain_id,
        gas_price_wei,
        gas_used,
        gas_cost_wei
    FROM {{ ref('stg_bsc__fills') }}
),

optimism_fills AS (
    SELECT 
        fill_timestamp,
        transaction_hash,
        origin_chain_id,
        10 AS destination_chain_id,  -- Fill happened ON Optimism
        deposit_id,
        relayer_address,
        depositor_address,
        recipient_address,
        input_token_address,
        output_token_address,
        output_token_symbol,
        input_amount,
        output_amount,
        repayment_chain_id,
        gas_price_wei,
        gas_used,
        gas_cost_wei
    FROM {{ ref('stg_optimism__fills') }}
),

-- Supported chain IDs (chains we have parquet data for)
-- 42161=Arbitrum, 1=Ethereum, 137=Polygon, 59144=Linea, 480=Worldchain, 130=Unichain, 999=HyperEVM, 143=Monad, 8453=Base, 56=BSC, 10=Optimism

-- Chain ID to Name mapping (from centralized seed)
chain_names AS (
    SELECT chain_id, chain_name
    FROM {{ ref('chain_metadata') }}
),

-- Hourly token prices for USD conversion
token_prices AS (
    SELECT 
        token_symbol,
        DATE_TRUNC('hour', timestamp::TIMESTAMP) AS price_hour,
        AVG(price_usd) AS price_usd
    FROM {{ ref('token_prices') }}
    GROUP BY token_symbol, DATE_TRUNC('hour', timestamp::TIMESTAMP)
),

-- UNION ALL: Stack all fills from all chains into one table
-- Filter: Only include fills where origin_chain_id is a supported chain
all_fills AS (
    SELECT * FROM arbitrum_fills WHERE origin_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143, 8453, 56, 10)
    UNION ALL
    SELECT * FROM ethereum_fills WHERE origin_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143, 8453, 56, 10)
    UNION ALL
    SELECT * FROM polygon_fills WHERE origin_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143, 8453, 56, 10)
    UNION ALL
    SELECT * FROM linea_fills WHERE origin_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143, 8453, 56, 10)
    UNION ALL
    SELECT * FROM worldchain_fills WHERE origin_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143, 8453, 56, 10)
    UNION ALL
    SELECT * FROM unichain_fills WHERE origin_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143, 8453, 56, 10)
    UNION ALL
    SELECT * FROM hyperevm_fills WHERE origin_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143, 8453, 56, 10)
    UNION ALL
    SELECT * FROM monad_fills WHERE origin_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143, 8453, 56, 10)
    UNION ALL
    SELECT * FROM base_fills WHERE origin_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143, 8453, 56, 10)
    UNION ALL
    SELECT * FROM bsc_fills WHERE origin_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143, 8453, 56, 10)
    UNION ALL
    SELECT * FROM optimism_fills WHERE origin_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143, 8453, 56, 10)
)

-- Final SELECT with descriptive chain names and USD amounts
SELECT
    f.fill_timestamp,
    f.transaction_hash,
    f.origin_chain_id,
    oc.chain_name AS origin_chain_name,
    f.destination_chain_id,
    dc.chain_name AS destination_chain_name,
    f.deposit_id,
    f.relayer_address,
    f.depositor_address,
    f.recipient_address,
    f.input_token_address,
    f.output_token_address,
    f.output_token_symbol AS fill_token_symbol,  -- Token symbol for the filled amount
    f.input_amount,
    f.output_amount,
    f.repayment_chain_id,
    -- Gas data (for relayer cost analysis)
    f.gas_price_wei,
    f.gas_used,
    f.gas_cost_wei,
    -- USD price data
    tp.price_usd AS output_token_price_usd,
    ROUND((f.output_amount * COALESCE(tp.price_usd, 1))::NUMERIC, 2) AS output_amount_usd
FROM all_fills f
LEFT JOIN chain_names oc ON f.origin_chain_id = oc.chain_id
LEFT JOIN chain_names dc ON f.destination_chain_id = dc.chain_id
-- Join for output token price at fill hour
LEFT JOIN token_prices tp
    ON f.output_token_symbol = tp.token_symbol
    AND DATE_TRUNC('hour', f.fill_timestamp) = tp.price_hour
