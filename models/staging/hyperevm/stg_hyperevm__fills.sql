-- Staging model for FilledRelay events from HyperEVM

WITH raw_fills AS (
    SELECT
        timestamp_datetime, transactionHash, blockchain, source_file,
        topic_origin_chain_id, topic_deposit_id, topic_relayer,
        filled_relay_data_input_token, filled_relay_data_output_token,
        filled_relay_data_input_amount, filled_relay_data_output_amount,
        filled_relay_data_repayment_chain_id, filled_relay_data_exclusive_relayer,
        filled_relay_data_depositor, filled_relay_data_recipient,
        gas_price_wei, gas_used
    FROM raw.hyperevm_logs_processed
    WHERE topic_0 = '0x44b559f101f8fbcc8a0ea43fa91a05a729a5ea6e14a7c75aa750374690137208'
        AND filled_relay_data_input_amount IS NOT NULL
        AND filled_relay_data_output_amount IS NOT NULL
),

input_token_meta AS (
    {{ get_token_decimals_by_chain_id() }}
),

output_token_meta AS (
    {{ get_token_decimals('hyperevm') }}
),

cleaned_fills AS (
    SELECT
        CASE 
            WHEN timestamp_datetime ~ '^\d+$' THEN TO_TIMESTAMP(timestamp_datetime::BIGINT)
            ELSE timestamp_datetime::TIMESTAMP
        END AS fill_timestamp,
        transactionHash AS transaction_hash,
        blockchain, source_file,
        (topic_origin_chain_id::NUMERIC)::BIGINT AS origin_chain_id,
        (topic_deposit_id::NUMERIC)::BIGINT AS deposit_id,
        topic_relayer AS relayer_address,
        LOWER(filled_relay_data_input_token) AS input_token_address,
        LOWER(filled_relay_data_output_token) AS output_token_address,
        filled_relay_data_input_amount::NUMERIC AS input_amount_raw,
        filled_relay_data_output_amount::NUMERIC AS output_amount_raw,
        (filled_relay_data_repayment_chain_id::NUMERIC)::BIGINT AS repayment_chain_id,
        filled_relay_data_exclusive_relayer AS exclusive_relayer_address,
        filled_relay_data_depositor AS depositor_address,
        filled_relay_data_recipient AS recipient_address,
        gas_price_wei::BIGINT AS gas_price_wei,
        gas_used::BIGINT AS gas_used
    FROM raw_fills
)

SELECT
    c.fill_timestamp, c.transaction_hash, c.blockchain, c.source_file,
    c.origin_chain_id, c.deposit_id, c.relayer_address,
    c.input_token_address, input_tok.token_symbol AS input_token_symbol,
    c.output_token_address, output_tok.token_symbol AS output_token_symbol,
    {{ rescale_amount('c.input_amount_raw', 'input_tok.decimals') }} AS input_amount,
    {{ rescale_amount('c.output_amount_raw', 'output_tok.decimals') }} AS output_amount,
    c.input_amount_raw, c.output_amount_raw,
    c.repayment_chain_id, c.exclusive_relayer_address,
    c.depositor_address, c.recipient_address,
    c.gas_price_wei, c.gas_used, (c.gas_price_wei * c.gas_used) AS gas_cost_wei
FROM cleaned_fills c
LEFT JOIN input_token_meta AS input_tok
    ON c.input_token_address = input_tok.token_address AND c.origin_chain_id = input_tok.chain_id
LEFT JOIN output_token_meta AS output_tok
    ON c.output_token_address = output_tok.token_address
WHERE c.deposit_id IS NOT NULL AND c.relayer_address IS NOT NULL
    AND c.input_token_address IS NOT NULL AND c.output_token_address IS NOT NULL
    AND c.input_amount_raw IS NOT NULL AND c.output_amount_raw IS NOT NULL
