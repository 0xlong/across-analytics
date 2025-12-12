-- Staging model for FundsDeposited events from Linea
-- This model extracts deposit events where users initiate cross-chain bridge transactions

WITH raw_deposits AS (

    SELECT
        -- Core event metadata - these identify WHEN and WHERE the event happened
        timestamp_datetime,              -- When the event occurred (from blockchain)
        transactionHash,                 -- Unique transaction identifier
        blockchain,                      -- Which blockchain (should be 'linea')
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
        
    FROM raw.linea_logs_processed
    
    -- Filter: Only include rows where FundsDeposited data exists
    -- Topic_0 is the event signature hash that identifies the event type
    WHERE topic_0 = '0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3'
        AND funds_deposited_data_input_amount IS NOT NULL
        AND funds_deposited_data_output_amount IS NOT NULL
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
        -- Example: 1 = Ethereum, 42161 = Arbitrum, 8453 = Base
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
        -- Example: Token address on Linea
        funds_deposited_data_input_token AS input_token_address,
        
        -- Output token: The token address to receive on the destination chain
        -- Already decoded by ETL to proper address format
        -- Example: Token address on destination chain
        funds_deposited_data_output_token AS output_token_address,
        
        -- ============================================================
        -- AMOUNT INFORMATION (how much was deposited)
        -- ============================================================
        
        -- Input amount: How much was deposited on the origin chain
        -- Already converted by ETL from hex to numeric (stored as text in raw table)
        -- Stored as NUMERIC to handle large token amounts (18 decimals)
        funds_deposited_data_input_amount::NUMERIC AS input_amount,
        
        -- Output amount: Expected amount to receive on destination chain
        -- Already converted by ETL from hex to numeric (stored as text in raw table)
        -- Usually slightly less than input due to fees/spread, but can vary with exchange rates
        funds_deposited_data_output_amount::NUMERIC AS output_amount,
        
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

-- Final SELECT: Add any computed fields and ensure data quality
SELECT
    -- Event identity
    deposit_timestamp,
    transaction_hash,
    blockchain,
    source_file,
    
    -- Indexed fields
    destination_chain_id,
    deposit_id,
    depositor_address,
    
    -- Token info
    input_token_address,
    output_token_address,
    
    -- Amounts
    input_amount,
    output_amount,
    
    -- Timing (commented out - available in schema but not included in output)
    -- quote_timestamp,
    -- fill_deadline,
    -- exclusivity_deadline,
    
    -- User info
    recipient_address
    -- exclusive_relayer_address  -- Commented out - available in schema but not included in output
    
FROM cleaned_deposits

-- Data quality: Only include rows with essential fields populated
WHERE deposit_id IS NOT NULL
    AND depositor_address IS NOT NULL
    AND input_token_address IS NOT NULL
    AND output_token_address IS NOT NULL
    AND input_amount IS NOT NULL
    AND output_amount IS NOT NULL
