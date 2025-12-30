-- int_unified_refunds.sql
-- PURPOSE: Combine refunds from ALL chains into ONE table with converted amounts
-- WHY: Refunds = capital returning to relayers. Tracks when relayers get paid back.

{{ config(materialized='view') }}

-- Token metadata for amount conversion
WITH token_meta AS (
    {{ get_token_decimals_by_chain_id() }}
),

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

-- Each CTE selects from a chain's staging model
arbitrum_refunds AS (
    SELECT 
        refund_timestamp,
        transaction_hash,
        chain_id,
        root_bundle_id,
        leaf_id,
        LOWER(refund_token_address) AS refund_token_address,
        total_refund_amount AS total_refund_amount_raw,
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
        LOWER(refund_token_address) AS refund_token_address,
        total_refund_amount AS total_refund_amount_raw,
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
        LOWER(refund_token_address) AS refund_token_address,
        total_refund_amount AS total_refund_amount_raw,
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
        LOWER(refund_token_address) AS refund_token_address,
        total_refund_amount AS total_refund_amount_raw,
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
        LOWER(refund_token_address) AS refund_token_address,
        total_refund_amount AS total_refund_amount_raw,
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
        LOWER(refund_token_address) AS refund_token_address,
        total_refund_amount AS total_refund_amount_raw,
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
        LOWER(refund_token_address) AS refund_token_address,
        total_refund_amount AS total_refund_amount_raw,
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
        LOWER(refund_token_address) AS refund_token_address,
        total_refund_amount AS total_refund_amount_raw,
        refund_addresses_string,
        refund_amounts_string,
        refund_count,
        'monad' AS source_blockchain
    FROM {{ ref('stg_monad__refunds') }}
),

-- UNION ALL: Stack all refunds from all chains
all_refunds AS (
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
)

-- Final SELECT: Join with token metadata and convert amounts
SELECT
    r.refund_timestamp,
    r.transaction_hash,
    r.chain_id,
    cn.chain_name,
    r.root_bundle_id,
    r.leaf_id,
    r.refund_token_address,
    tok.token_symbol AS refund_token_symbol,
    tok.decimals AS token_decimals,
    -- Rescaled amount (human-readable)
    {{ rescale_amount('r.total_refund_amount_raw', 'tok.decimals') }} AS total_refund_amount,
    -- Raw amount (preserved for auditing)
    r.total_refund_amount_raw,
    r.refund_addresses_string,
    r.refund_amounts_string,
    r.refund_count,
    r.source_blockchain

FROM all_refunds r

-- Join for chain name
LEFT JOIN chain_names cn ON r.chain_id = cn.chain_id

-- Join with token metadata on address + chain_id
LEFT JOIN token_meta AS tok
    ON r.refund_token_address = tok.token_address
    AND r.chain_id = tok.chain_id

