-- int_unified_refunds.sql
-- PURPOSE: Combine refunds from ALL chains into ONE table
-- WHY: Refunds = capital returning to relayers. Tracks when relayers get paid back.

{{ config(materialized='view') }}

-- Each CTE selects from a chain's staging model
WITH arbitrum_refunds AS (
    SELECT 
        refund_timestamp,
        transaction_hash,
        chain_id,
        root_bundle_id,
        leaf_id,
        refund_token_address,
        total_refund_amount,
        refund_addresses_string,
        refund_amounts_string,
        refund_count,
        'arbitrum' AS source_blockchain
    FROM {{ ref('stg_arbitrum__refunds') }}
),

ethereum_refunds AS (
    SELECT 
        refund_timestamp,
        transaction_hash,
        chain_id,
        root_bundle_id,
        leaf_id,
        refund_token_address,
        total_refund_amount,
        refund_addresses_string,
        refund_amounts_string,
        refund_count,
        'ethereum' AS source_blockchain
    FROM {{ ref('stg_ethereum__refunds') }}
),

polygon_refunds AS (
    SELECT 
        refund_timestamp,
        transaction_hash,
        chain_id,
        root_bundle_id,
        leaf_id,
        refund_token_address,
        total_refund_amount,
        refund_addresses_string,
        refund_amounts_string,
        refund_count,
        'polygon' AS source_blockchain
    FROM {{ ref('stg_polygon__refunds') }}
),

linea_refunds AS (
    SELECT 
        refund_timestamp,
        transaction_hash,
        chain_id,
        root_bundle_id,
        leaf_id,
        refund_token_address,
        total_refund_amount,
        refund_addresses_string,
        refund_amounts_string,
        refund_count,
        'linea' AS source_blockchain
    FROM {{ ref('stg_linea__refunds') }}
),

worldchain_refunds AS (
    SELECT 
        refund_timestamp,
        transaction_hash,
        chain_id,
        root_bundle_id,
        leaf_id,
        refund_token_address,
        total_refund_amount,
        refund_addresses_string,
        refund_amounts_string,
        refund_count,
        'worldchain' AS source_blockchain
    FROM {{ ref('stg_worldchain__refunds') }}
),

unichain_refunds AS (
    SELECT 
        refund_timestamp,
        transaction_hash,
        chain_id,
        root_bundle_id,
        leaf_id,
        refund_token_address,
        total_refund_amount,
        refund_addresses_string,
        refund_amounts_string,
        refund_count,
        'unichain' AS source_blockchain
    FROM {{ ref('stg_unichain__refunds') }}
),

hyperevm_refunds AS (
    SELECT 
        refund_timestamp,
        transaction_hash,
        chain_id,
        root_bundle_id,
        leaf_id,
        refund_token_address,
        total_refund_amount,
        refund_addresses_string,
        refund_amounts_string,
        refund_count,
        'hyperevm' AS source_blockchain
    FROM {{ ref('stg_hyperevm__refunds') }}
),

monad_refunds AS (
    SELECT 
        refund_timestamp,
        transaction_hash,
        chain_id,
        root_bundle_id,
        leaf_id,
        refund_token_address,
        total_refund_amount,
        refund_addresses_string,
        refund_amounts_string,
        refund_count,
        'monad' AS source_blockchain
    FROM {{ ref('stg_monad__refunds') }}
)

-- UNION ALL: Stack all refunds from all chains
SELECT * FROM arbitrum_refunds
UNION ALL
SELECT * FROM ethereum_refunds
UNION ALL
SELECT * FROM polygon_refunds
UNION ALL
SELECT * FROM linea_refunds
UNION ALL
SELECT * FROM worldchain_refunds
UNION ALL
SELECT * FROM unichain_refunds
UNION ALL
SELECT * FROM hyperevm_refunds
UNION ALL
SELECT * FROM monad_refunds
