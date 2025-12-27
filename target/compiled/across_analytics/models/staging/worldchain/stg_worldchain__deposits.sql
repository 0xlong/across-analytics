-- Staging model for FundsDeposited events from Worldchain
-- This model extracts deposit events where users initiate cross-chain bridge transactions

WITH raw_deposits AS (

    SELECT
        timestamp_datetime,
        transactionHash,
        blockchain,
        source_file,
        topic_destination_chain_id,
        topic_deposit_id,
        topic_depositor,
        funds_deposited_data_input_token,
        funds_deposited_data_output_token,
        funds_deposited_data_input_amount,
        funds_deposited_data_output_amount,
        funds_deposited_data_recipient
        
    FROM raw.worldchain_logs_processed
    
    WHERE topic_0 = '0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3'
        AND funds_deposited_data_input_amount IS NOT NULL
        AND funds_deposited_data_output_amount IS NOT NULL
),

-- INPUT token metadata: filtered to origin chain (Worldchain)
input_token_meta AS (
    
    SELECT 
        LOWER(token_address) AS token_address,
        token_symbol,
        decimals
    FROM "across_analytics"."dbt"."token_metadata"
    WHERE chain = 'worldchain'

),

-- OUTPUT token metadata: includes chain_id for matching with destination_chain_id
output_token_meta AS (
    
    SELECT 
        LOWER(token_address) AS token_address,
        token_symbol,
        decimals,
        chain_id
    FROM "across_analytics"."dbt"."token_metadata"

),

cleaned_deposits AS (
    SELECT
        CASE 
            WHEN timestamp_datetime ~ '^\d+$' THEN
                TO_TIMESTAMP(timestamp_datetime::BIGINT)
            ELSE 
                timestamp_datetime::TIMESTAMP
        END AS deposit_timestamp,
        
        transactionHash AS transaction_hash,
        blockchain,
        source_file,
        (topic_destination_chain_id::NUMERIC)::BIGINT AS destination_chain_id,
        (topic_deposit_id::NUMERIC)::BIGINT AS deposit_id,
        topic_depositor AS depositor_address,
        LOWER(funds_deposited_data_input_token) AS input_token_address,
        LOWER(funds_deposited_data_output_token) AS output_token_address,
        funds_deposited_data_input_amount::NUMERIC AS input_amount_raw,
        funds_deposited_data_output_amount::NUMERIC AS output_amount_raw,
        funds_deposited_data_recipient AS recipient_address
        
    FROM raw_deposits
)

SELECT
    c.deposit_timestamp,
    c.transaction_hash,
    c.blockchain,
    c.source_file,
    c.destination_chain_id,
    c.deposit_id,
    c.depositor_address,
    c.input_token_address,
    input_tok.token_symbol AS input_token_symbol,
    c.output_token_address,
    output_tok.token_symbol AS output_token_symbol,
    
    c.input_amount_raw / POWER(10, COALESCE(input_tok.decimals, 18))
 AS input_amount,
    
    c.output_amount_raw / POWER(10, COALESCE(output_tok.decimals, 18))
 AS output_amount,
    c.input_amount_raw,
    c.output_amount_raw,
    c.recipient_address
    
FROM cleaned_deposits c

LEFT JOIN input_token_meta AS input_tok
    ON c.input_token_address = input_tok.token_address

LEFT JOIN output_token_meta AS output_tok
    ON c.output_token_address = output_tok.token_address
    AND c.destination_chain_id = output_tok.chain_id

WHERE c.deposit_id IS NOT NULL
    AND c.depositor_address IS NOT NULL
    AND c.input_token_address IS NOT NULL
    AND c.output_token_address IS NOT NULL
    AND c.input_amount_raw IS NOT NULL
    AND c.output_amount_raw IS NOT NULL