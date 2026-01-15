-- Staging model for ExecutedRelayerRefundRoot events from BSC

WITH raw_refunds AS (

    SELECT
        timestamp_datetime,
        transactionHash,
        blockchain,
        source_file,
        topic_chain_id,
        topic_root_bundle_id,
        topic_leaf_id,
        amount_to_return,
        l2_token_address,
        refund_amounts,
        refund_addresses,
        refund_count
        
    FROM {{ source('raw_bsc', 'bsc_logs_processed') }}
    
    WHERE topic_0 = '0xf4ad92585b1bc117fbdd644990adf0827bc4c95baeae8a23322af807b6d0020e'
        AND amount_to_return IS NOT NULL
        AND l2_token_address IS NOT NULL
        AND refund_count IS NOT NULL
),

cleaned_refunds AS (
    SELECT
        CASE 
            WHEN timestamp_datetime ~ '^\d+$' THEN
                TO_TIMESTAMP(timestamp_datetime::BIGINT)
            ELSE 
                timestamp_datetime::TIMESTAMP
        END AS refund_timestamp,
        
        transactionHash AS transaction_hash,
        blockchain,
        source_file,
        (topic_chain_id::NUMERIC)::BIGINT AS chain_id,
        (topic_root_bundle_id::NUMERIC)::BIGINT AS root_bundle_id,
        (topic_leaf_id::NUMERIC)::BIGINT AS leaf_id,
        l2_token_address AS refund_token_address,
        amount_to_return::NUMERIC AS total_refund_amount,
        refund_amounts AS refund_amounts_string,
        refund_addresses AS refund_addresses_string,
        refund_count::INTEGER AS refund_count
        
    FROM raw_refunds
)

SELECT
    refund_timestamp,
    transaction_hash,
    blockchain,
    source_file,
    chain_id,
    root_bundle_id,
    leaf_id,
    refund_token_address,
    total_refund_amount,
    refund_amounts_string,
    refund_addresses_string,
    refund_count
    
FROM cleaned_refunds

WHERE chain_id IS NOT NULL
    AND refund_token_address IS NOT NULL
    AND total_refund_amount IS NOT NULL
    AND refund_count IS NOT NULL
