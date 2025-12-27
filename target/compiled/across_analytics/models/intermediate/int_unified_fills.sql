-- int_unified_fills.sql
-- PURPOSE: Combine fills from ALL chains into ONE table
-- WHY: Fills happen on the DESTINATION chain. We need to see all fills to match with deposits.



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
        input_amount,
        output_amount,
        repayment_chain_id
    FROM "across_analytics"."dbt_staging"."stg_arbitrum__fills"
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
        input_amount,
        output_amount,
        repayment_chain_id
    FROM "across_analytics"."dbt_staging"."stg_ethereum__fills"
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
        input_amount,
        output_amount,
        repayment_chain_id
    FROM "across_analytics"."dbt_staging"."stg_polygon__fills"
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
        input_amount,
        output_amount,
        repayment_chain_id
    FROM "across_analytics"."dbt_staging"."stg_linea__fills"
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
        input_amount,
        output_amount,
        repayment_chain_id
    FROM "across_analytics"."dbt_staging"."stg_worldchain__fills"
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
        input_amount,
        output_amount,
        repayment_chain_id
    FROM "across_analytics"."dbt_staging"."stg_unichain__fills"
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
        input_amount,
        output_amount,
        repayment_chain_id
    FROM "across_analytics"."dbt_staging"."stg_hyperevm__fills"
),

monad_fills AS (
    SELECT 
        fill_timestamp,
        transaction_hash,
        origin_chain_id,
        140 AS destination_chain_id,  -- Fill happened ON Monad
        deposit_id,
        relayer_address,
        depositor_address,
        recipient_address,
        input_token_address,
        output_token_address,
        input_amount,
        output_amount,
        repayment_chain_id
    FROM "across_analytics"."dbt_staging"."stg_monad__fills"
)

-- UNION ALL: Stack all fills from all chains into one table
SELECT * FROM arbitrum_fills
UNION ALL
SELECT * FROM ethereum_fills
UNION ALL
SELECT * FROM polygon_fills
UNION ALL
SELECT * FROM linea_fills
UNION ALL
SELECT * FROM worldchain_fills
UNION ALL
SELECT * FROM unichain_fills
UNION ALL
SELECT * FROM hyperevm_fills
UNION ALL
SELECT * FROM monad_fills