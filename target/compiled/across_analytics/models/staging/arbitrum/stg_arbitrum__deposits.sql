-- Staging model for FundsDeposited events from Arbitrum
-- This model extracts deposit events where users initiate cross-chain bridge transactions

WITH raw_deposits AS (

    SELECT
        -- Core event metadata - these identify WHEN and WHERE the event happened
        timestamp_datetime,              -- When the event occurred (from blockchain)
        transactionHash,                 -- Unique transaction identifier
        blockchain,                      -- Which blockchain (should be 'arbitrum')
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
        -- Optional fields: Not required for capital flow analysis, commented out in database schema
        -- funds_deposited_data_quote_timestamp,       -- When the exchange rate quote was generated
        -- funds_deposited_data_fill_deadline,         -- Deadline by which the deposit must be filled
        -- funds_deposited_data_exclusivity_deadline,  -- Deadline for exclusive relayer rights
        -- funds_deposited_data_exclusive_relayer      -- Address with exclusive fill rights (if any)
        
    FROM raw.arbitrum_logs_processed
    
    -- Filter: Only include rows where FundsDeposited data exists
    -- Topic_0 is the event signature hash that identifies the event type
    WHERE topic_0 = '0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3'
        AND funds_deposited_data_input_amount IS NOT NULL
        AND funds_deposited_data_output_amount IS NOT NULL
),

-- INPUT token metadata: filtered to origin chain (Arbitrum)
-- This is correct because input_token is always on the origin chain
input_token_meta AS (
    
    SELECT 
        LOWER(token_address) AS token_address,
        token_symbol,
        decimals
    FROM "across_analytics"."dbt"."token_metadata"
    WHERE chain = 'arbitrum'

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
        -- The ETL process may store timestamps as Unix integers or ISO strings
        CASE 
            WHEN timestamp_datetime ~ '^\d+$' THEN          -- If it's pure digits, treat as Unix timestamp (seconds since 1970-01-01)
                TO_TIMESTAMP(timestamp_datetime::BIGINT)    -- PostgreSQL's TO_TIMESTAMP converts Unix seconds to timestamp
            ELSE 
                timestamp_datetime::TIMESTAMP               -- Otherwise, try to parse as ISO format string (e.g., "2025-12-03 10:30:00")
        END AS deposit_timestamp,
        
        transactionHash AS transaction_hash,
        blockchain,
        source_file,
        
        -- ============================================================
        -- INDEXED FIELDS (from topics) - Already decoded by ETL
        -- ============================================================
        -- These come from the event's "topics" array, which is indexed for fast blockchain queries
        
        -- Destination chain ID: Which blockchain the funds are being sent TO
        -- Already converted by ETL from hex to integer (stored as text in raw table)
        -- Example: 1 = Ethereum, 8453 = Base, 137 = Polygon
        -- Cast via NUMERIC first to handle decimal strings like "8453.0" from ETL
        (topic_destination_chain_id::NUMERIC)::BIGINT AS destination_chain_id,
        
        -- Deposit ID: Unique identifier linking this deposit to its fill(s)
        -- Already converted by ETL from hex to integer (stored as text in raw table)
        -- This is the KEY that connects deposits ↔ fills across chains
        -- Cast via NUMERIC first to handle decimal strings like "12345.0" from ETL
        (topic_deposit_id::NUMERIC)::BIGINT AS deposit_id,
        
        -- Depositor address: Who initiated the bridge transaction
        -- Already decoded by ETL to proper address format (0x...)
        topic_depositor AS depositor_address,
        
        -- ============================================================
        -- TOKEN INFORMATION (what was deposited)
        -- ============================================================
        
        -- Input token: The token address being deposited on the origin chain
        -- Already decoded by ETL to proper address format
        -- Example: 0xaf88d065aC88dCc5619a6eeFdD463aAbdE3eE2c3 = USDC on Arbitrum
        -- Normalize to lowercase for joining with token metadata
        LOWER(funds_deposited_data_input_token) AS input_token_address,
        
        -- Output token: The token address to receive on the destination chain
        -- Already decoded by ETL to proper address format
        -- Example: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 = USDC on Ethereum
        -- Normalize to lowercase for joining with token metadata
        LOWER(funds_deposited_data_output_token) AS output_token_address,
        
        -- ============================================================
        -- AMOUNT INFORMATION (how much was deposited)
        -- ============================================================
        
        -- Input amount: How much was deposited on the origin chain
        -- Already converted by ETL from hex to numeric (stored as text in raw table)
        -- Store as RAW amount first (before rescaling by decimals)
        funds_deposited_data_input_amount::NUMERIC AS input_amount_raw,
        
        -- Output amount: Expected amount to receive on destination chain
        -- Already converted by ETL from hex to numeric (stored as text in raw table)
        -- Store as RAW amount first (before rescaling by decimals)
        funds_deposited_data_output_amount::NUMERIC AS output_amount_raw,
        
        -- ============================================================
        -- TIMING INFORMATION (deadlines and quotes)
        -- ============================================================
        -- NOTE: These fields are NOT required for capital flow analysis and are commented out in database schema
        
        -- Quote timestamp: When the exchange rate quote was generated
        -- Already converted by ETL from hex to numeric (stored as text in raw table)
        -- Used to determine which exchange rate was used for the bridge
        -- Cast via NUMERIC first to handle decimal strings like "1733234567.0" from ETL
        -- (funds_deposited_data_quote_timestamp::NUMERIC)::BIGINT AS quote_timestamp,
        
        -- Fill deadline: Latest timestamp by which the deposit must be filled
        -- Already converted by ETL from hex to numeric (stored as text in raw table)
        -- If not filled by this time, the deposit can be refunded
        -- Cast via NUMERIC first to handle decimal strings like "1733234567.0" from ETL
        -- (funds_deposited_data_fill_deadline::NUMERIC)::BIGINT AS fill_deadline,
        
        -- Exclusivity deadline: Latest timestamp for exclusive relayer rights
        -- Already converted by ETL from hex to numeric (stored as text in raw table)
        -- Before this deadline, only the exclusive_relayer can fill this deposit
        -- Cast via NUMERIC first to handle decimal strings like "1733234567.0" from ETL
        -- (funds_deposited_data_exclusivity_deadline::NUMERIC)::BIGINT AS exclusivity_deadline,
        
        -- ============================================================
        -- USER & RELAYER INFORMATION
        -- ============================================================
        
        -- Recipient: Who receives the funds on the destination chain
        -- Already decoded by ETL to proper address format
        -- Usually the same as depositor, but can be different (gift/transfer)
        funds_deposited_data_recipient AS recipient_address
        
        -- Exclusive relayer: Address with exclusive fill rights (if any)
        -- Already decoded by ETL to proper address format
        -- Used for priority/guaranteed fills (NULL if no exclusive relayer)
        -- NOTE: Field is NOT required for capital flow analysis and is commented out in database schema
        -- funds_deposited_data_exclusive_relayer AS exclusive_relayer_address
        
    FROM raw_deposits
)


-- ============================================================
-- FINAL SELECT: Join with token metadata and rescale amounts
-- ============================================================
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
    
    -- ✨ NEW: Token information WITH NAMES
    -- Input token (what was deposited on origin chain - Arbitrum)
    c.input_token_address,
    input_tok.token_symbol AS input_token_symbol,        -- e.g., 'USDC', 'WETH'
    
    -- Output token (what will be received on destination chain)
    c.output_token_address,
    output_tok.token_symbol AS output_token_symbol,      -- e.g., 'USDC', 'WETH'
    
    -- ✨ NEW: Rescaled amounts (human-readable)
    -- These use the rescale_amount macro which divides raw amount by 10^decimals
    -- Example: 5000000 raw USDC (6 decimals) → 5.0 USDC
    
    c.input_amount_raw / POWER(10, COALESCE(input_tok.decimals, 18))
 AS input_amount,
    
    c.output_amount_raw / POWER(10, COALESCE(output_tok.decimals, 18))
 AS output_amount,
    
    -- ✨ NEW: Raw amounts (preserved for auditing)
    -- These are the original blockchain values before rescaling
    -- Example: 5.0 USDC is stored as 5000000 on blockchain
    c.input_amount_raw,
    c.output_amount_raw,
    
    -- Timing (commented out - available in schema but not included in output)
    -- quote_timestamp,
    -- fill_deadline,
    -- exclusivity_deadline,
    
    -- User info
    c.recipient_address
    -- exclusive_relayer_address  -- Commented out - available in schema but not included in output
    
FROM cleaned_deposits c

-- Join with token metadata to get decimals and symbols for INPUT token
-- Join on address only - we already filtered to origin chain (Arbitrum)
LEFT JOIN input_token_meta AS input_tok
    ON c.input_token_address = input_tok.token_address

-- Join with token metadata to get decimals and symbols for OUTPUT token
-- Join on BOTH address AND chain_id to match the correct destination chain
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