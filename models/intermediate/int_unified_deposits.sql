-- int_unified_deposits.sql
-- PURPOSE: Combine deposits from ALL chains into ONE table
-- WHY: Right now deposits are separate per chain. We need them unified to track cross-chain flows.

{{ config(materialized='view') }}

-- Each CTE selects from a chain's staging model and adds the origin chain ID
WITH arbitrum_deposits AS (
    SELECT 
        deposit_timestamp,
        transaction_hash,
        42161 AS origin_chain_id,  -- Arbitrum's chain ID
        destination_chain_id,
        deposit_id,
        depositor_address,
        recipient_address,
        input_token_address,
        input_token_symbol,
        output_token_address,
        output_token_symbol,
        input_amount,
        output_amount
    FROM {{ ref('stg_arbitrum__deposits') }}
),

ethereum_deposits AS (
    SELECT 
        deposit_timestamp,
        transaction_hash,
        1 AS origin_chain_id,  -- Ethereum's chain ID
        destination_chain_id,
        deposit_id,
        depositor_address,
        recipient_address,
        input_token_address,
        input_token_symbol,
        output_token_address,
        output_token_symbol,
        input_amount,
        output_amount
    FROM {{ ref('stg_ethereum__deposits') }}
),

polygon_deposits AS (
    SELECT 
        deposit_timestamp,
        transaction_hash,
        137 AS origin_chain_id,  -- Polygon's chain ID
        destination_chain_id,
        deposit_id,
        depositor_address,
        recipient_address,
        input_token_address,
        input_token_symbol,
        output_token_address,
        output_token_symbol,
        input_amount,
        output_amount
    FROM {{ ref('stg_polygon__deposits') }}
),

linea_deposits AS (
    SELECT 
        deposit_timestamp,
        transaction_hash,
        59144 AS origin_chain_id,  -- Linea's chain ID
        destination_chain_id,
        deposit_id,
        depositor_address,
        recipient_address,
        input_token_address,
        input_token_symbol,
        output_token_address,
        output_token_symbol,
        input_amount,
        output_amount
    FROM {{ ref('stg_linea__deposits') }}
),

worldchain_deposits AS (
    SELECT 
        deposit_timestamp,
        transaction_hash,
        480 AS origin_chain_id,  -- WorldChain's chain ID
        destination_chain_id,
        deposit_id,
        depositor_address,
        recipient_address,
        input_token_address,
        input_token_symbol,
        output_token_address,
        output_token_symbol,
        input_amount,
        output_amount
    FROM {{ ref('stg_worldchain__deposits') }}
),

unichain_deposits AS (
    SELECT 
        deposit_timestamp,
        transaction_hash,
        130 AS origin_chain_id,  -- Unichain's chain ID
        destination_chain_id,
        deposit_id,
        depositor_address,
        recipient_address,
        input_token_address,
        input_token_symbol,
        output_token_address,
        output_token_symbol,
        input_amount,
        output_amount
    FROM {{ ref('stg_unichain__deposits') }}
),

hyperevm_deposits AS (
    SELECT 
        deposit_timestamp,
        transaction_hash,
        999 AS origin_chain_id,  -- HyperEVM's chain ID
        destination_chain_id,
        deposit_id,
        depositor_address,
        recipient_address,
        input_token_address,
        input_token_symbol,
        output_token_address,
        output_token_symbol,
        input_amount,
        output_amount
    FROM {{ ref('stg_hyperevm__deposits') }}
),

monad_deposits AS (
    SELECT 
        deposit_timestamp,
        transaction_hash,
        143 AS origin_chain_id,  -- Monad's chain ID
        destination_chain_id,
        deposit_id,
        depositor_address,
        recipient_address,
        input_token_address,
        input_token_symbol,
        output_token_address,
        output_token_symbol,
        input_amount,
        output_amount
    FROM {{ ref('stg_monad__deposits') }}
)

-- Supported chain IDs (chains we have parquet data for)
-- 42161=Arbitrum, 1=Ethereum, 137=Polygon, 59144=Linea, 480=Worldchain, 130=Unichain, 999=HyperEVM, 143=Monad

-- UNION ALL: Stack all deposits from all chains into one table
-- Filter: Only include deposits where destination_chain_id is a supported chain
SELECT * FROM arbitrum_deposits WHERE destination_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143)
UNION ALL
SELECT * FROM ethereum_deposits WHERE destination_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143)
UNION ALL
SELECT * FROM polygon_deposits WHERE destination_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143)
UNION ALL
SELECT * FROM linea_deposits WHERE destination_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143)
UNION ALL
SELECT * FROM worldchain_deposits WHERE destination_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143)
UNION ALL
SELECT * FROM unichain_deposits WHERE destination_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143)
UNION ALL
SELECT * FROM hyperevm_deposits WHERE destination_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143)
UNION ALL
SELECT * FROM monad_deposits WHERE destination_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143)
