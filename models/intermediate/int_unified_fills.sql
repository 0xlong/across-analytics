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
        repayment_chain_id
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
        repayment_chain_id
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
        repayment_chain_id
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
        repayment_chain_id
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
        repayment_chain_id
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
        repayment_chain_id
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
        repayment_chain_id
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
        repayment_chain_id
    FROM {{ ref('stg_monad__fills') }}
),

-- Supported chain IDs (chains we have parquet data for)
-- 42161=Arbitrum, 1=Ethereum, 137=Polygon, 59144=Linea, 480=Worldchain, 130=Unichain, 999=HyperEVM, 143=Monad

-- Chain ID to Name mapping for chains with parquet data
chain_names AS (
    SELECT chain_id, chain_name
    FROM (
        VALUES
        (1, 'Ethereum'), (42161, 'Arbitrum'), (137, 'Polygon'),
        (59144, 'Linea'), (480, 'Worldchain'), (130, 'Unichain'),
        (999, 'HyperEVM'), (143, 'Monad')
    ) AS chains(chain_id, chain_name)
),

-- UNION ALL: Stack all fills from all chains into one table
-- Filter: Only include fills where origin_chain_id is a supported chain
all_fills AS (
    SELECT * FROM arbitrum_fills WHERE origin_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143)
    UNION ALL
    SELECT * FROM ethereum_fills WHERE origin_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143)
    UNION ALL
    SELECT * FROM polygon_fills WHERE origin_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143)
    UNION ALL
    SELECT * FROM linea_fills WHERE origin_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143)
    UNION ALL
    SELECT * FROM worldchain_fills WHERE origin_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143)
    UNION ALL
    SELECT * FROM unichain_fills WHERE origin_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143)
    UNION ALL
    SELECT * FROM hyperevm_fills WHERE origin_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143)
    UNION ALL
    SELECT * FROM monad_fills WHERE origin_chain_id IN (42161, 1, 137, 59144, 480, 130, 999, 143)
)

-- Final SELECT with descriptive chain names
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
    f.repayment_chain_id
FROM all_fills f
LEFT JOIN chain_names oc ON f.origin_chain_id = oc.chain_id
LEFT JOIN chain_names dc ON f.destination_chain_id = dc.chain_id

