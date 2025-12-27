-- Staging model for FundsDeposited events from Ethereum
-- This model extracts deposit events where users initiate cross-chain bridge transactions

WITH raw_deposits AS (

    SELECT
        -- Core event metadata - these identify WHEN and WHERE the event happened
        timestamp_datetime,              -- When the event occurred (from blockchain)
        transactionHash,                 -- Unique transaction identifier
        blockchain,                      -- Which blockchain (should be 'ethereum')
        source_file,                     -- Which source file this came from (for lineage)
        
        -- Indexed fields from topics (these are searchable/filterable in blockchain)
        -- Topics are like "indexed columns" in SQL - they're stored separately for fast queries
        topic_destination_chain_id,      -- Chain ID where the funds are being sent TO
        topic_deposit_id,                -- Unique deposit identifier (links deposits to fills)
        topic_depositor,                  -- Address of the user who initiated the deposit
        
        -- Non-indexed fields from the event's data field
        -- These are the actual business data about the deposit
        funds_deposited_data_input_token,          -- Token address being deposited (on origin chain)
        funds_deposited_data_output_token,         -- Token address to receive (on destination chain)
        funds_deposited_data_input_amount,          -- Amount deposited (in input token units)
        funds_deposited_data_output_amount,         -- Expected amount to receive (in output token units)
        funds_deposited_data_recipient             -- Final recipient of the bridged funds
        
    FROM raw.ethereum_logs_processed
    
    -- Filter: Only include rows where FundsDeposited data exists
    -- Topic_0 is the event signature hash that identifies the event type
    WHERE topic_0 = '0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3'
        AND funds_deposited_data_input_amount IS NOT NULL
        AND funds_deposited_data_output_amount IS NOT NULL
),

-- INPUT token metadata: filtered to origin chain (Ethereum)
-- This is correct because input_token is always on the origin chain
input_token_meta AS (
    
    SELECT 
        LOWER(token_address) AS token_address,
        token_symbol,
        decimals
    FROM "across_analytics"."dbt"."token_metadata"
    WHERE chain = 'ethereum'

),

-- OUTPUT token metadata: includes chain_id for matching with destination_chain_id
-- This allows us to find the correct token on whichever chain the funds are going to
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
        
        -- Timestamp: Convert text to proper timestamp type
        CASE 
            WHEN timestamp_datetime ~ '^\d+$' THEN
                TO_TIMESTAMP(timestamp_datetime::BIGINT)
            ELSE 
                timestamp_datetime::TIMESTAMP
        END AS deposit_timestamp,
        
        transactionHash AS transaction_hash,
        blockchain,
        source_file,
        
        -- Destination chain ID
        (topic_destination_chain_id::NUMERIC)::BIGINT AS destination_chain_id,
        
        -- Deposit ID
        (topic_deposit_id::NUMERIC)::BIGINT AS deposit_id,
        
        -- Depositor address
        topic_depositor AS depositor_address,
        
        -- Token addresses (normalized to lowercase for joining)
        LOWER(funds_deposited_data_input_token) AS input_token_address,
        LOWER(funds_deposited_data_output_token) AS output_token_address,
        
        -- Raw amounts (before rescaling)
        funds_deposited_data_input_amount::NUMERIC AS input_amount_raw,
        funds_deposited_data_output_amount::NUMERIC AS output_amount_raw,
        
        -- Recipient
        funds_deposited_data_recipient AS recipient_address
        
    FROM raw_deposits
)

-- Final SELECT: Join with token metadata and rescale amounts
SELECT
    -- Event identity
    c.deposit_timestamp,
    c.transaction_hash,
    c.blockchain,
    c.source_file,
    
    -- Indexed fields
    c.destination_chain_id,
    c.deposit_id,
    c.depositor_address,
    
    -- Input token (what was deposited on origin chain - Ethereum)
    c.input_token_address,
    input_tok.token_symbol AS input_token_symbol,
    
    -- Output token (what will be received on destination chain)
    c.output_token_address,
    output_tok.token_symbol AS output_token_symbol,
    
    -- Rescaled amounts (human-readable)
    
    c.input_amount_raw / POWER(10, COALESCE(input_tok.decimals, 18))
 AS input_amount,
    
    c.output_amount_raw / POWER(10, COALESCE(output_tok.decimals, 18))
 AS output_amount,
    
    -- Raw amounts (preserved for auditing)
    c.input_amount_raw,
    c.output_amount_raw,
    
    -- User info
    c.recipient_address
    
FROM cleaned_deposits c

-- Join with token metadata for INPUT token (origin chain)
LEFT JOIN input_token_meta AS input_tok
    ON c.input_token_address = input_tok.token_address

-- Join with token metadata for OUTPUT token (destination chain)
LEFT JOIN output_token_meta AS output_tok
    ON c.output_token_address = output_tok.token_address
    AND c.destination_chain_id = output_tok.chain_id

-- Data quality: Only include rows with essential fields populated
WHERE c.deposit_id IS NOT NULL
    AND c.depositor_address IS NOT NULL
    AND c.input_token_address IS NOT NULL
    AND c.output_token_address IS NOT NULL
    AND c.input_amount_raw IS NOT NULL
    AND c.output_amount_raw IS NOT NULL