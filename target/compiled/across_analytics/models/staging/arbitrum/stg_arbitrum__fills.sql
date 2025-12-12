-- Staging model for FilledRelay events from Arbitrum

WITH raw_fills AS (

    SELECT
        -- Core event metadata - these identify WHEN and WHERE the event happened
        timestamp_datetime,              -- When the event occurred (from blockchain)
        transactionHash,                 -- Unique transaction identifier
        blockchain,                      -- Which blockchain (should be 'arbitrum')
        source_file,                     -- Which source file this came from (for lineage)
        
        -- Indexed fields from topics (these are searchable/filterable in blockchain)
        -- Topics are like "indexed columns" in SQL - they're stored separately for fast queries
        topic_origin_chain_id,           -- Chain ID where the original deposit happened
        topic_deposit_id,                -- Unique deposit identifier (links to deposit event)
        topic_relayer,                   -- Address of the relayer who provided liquidity
        
        -- Non-indexed fields from the event's data field
        -- These are the actual business data about the fill
        filled_relay_data_input_token,          -- Token address on origin chain
        filled_relay_data_output_token,          -- Token address on destination chain
        filled_relay_data_input_amount,          -- Amount sent (in origin token units)
        filled_relay_data_output_amount,         -- Amount received (in destination token units)
        filled_relay_data_repayment_chain_id,     -- Where relayer gets reimbursed
        filled_relay_data_exclusive_relayer,     -- Address with exclusive fill rights (if any)
        filled_relay_data_depositor,             -- Original user who initiated the bridge
        filled_relay_data_recipient              -- Final recipient of the bridged funds
        
    FROM raw.arbitrum_logs_processed
    
    -- Filter: Only include rows where FilledRelay data exists
    WHERE topic_0 = '0x44b559f101f8fbcc8a0ea43fa91a05a729a5ea6e14a7c75aa750374690137208'
        AND filled_relay_data_input_amount IS NOT NULL
        AND filled_relay_data_output_amount IS NOT NULL
),

cleaned_fills AS (
    SELECT
        
        -- Timestamp: Convert text to proper timestamp type
        
        CASE 
            WHEN timestamp_datetime ~ '^\d+$' THEN          -- If it's pure digits, treat as Unix timestamp (seconds since 1970-01-01)
                TO_TIMESTAMP(timestamp_datetime::BIGINT)    -- PostgreSQL's TO_TIMESTAMP can parse various formats, but we use the simplest approach
            ELSE 
                timestamp_datetime::TIMESTAMP               -- Otherwise, try to parse as ISO format string
        END AS fill_timestamp,
        
        transactionHash AS transaction_hash,
        blockchain,
        source_file,
        
        -- ============================================================
        -- INDEXED FIELDS (from topics) - Already decoded by ETL
        -- ============================================================
        -- These come from the event's "topics" array, which is indexed for fast blockchain queries
        
        -- Origin chain ID: Which blockchain the funds came FROM
        -- Already converted by ETL from hex to integer (stored as text in raw table)
        -- Example: 1 = Ethereum, 42161 = Arbitrum, 8453 = Base
        -- Cast via NUMERIC first to handle decimal strings like "8453.0" from ETL
        (topic_origin_chain_id::NUMERIC)::BIGINT AS origin_chain_id,
        
        -- Deposit ID: Unique identifier linking this fill to its original deposit
        -- Already converted by ETL from hex to integer (stored as text in raw table)
        -- This is the KEY that connects deposits ↔ fills across chains
        -- Cast via NUMERIC first to handle decimal strings like "12345.0" from ETL
        (topic_deposit_id::NUMERIC)::BIGINT AS deposit_id,
        
        -- Relayer address: Who provided the liquidity
        -- Already decoded by ETL to proper address format (0x...)
        topic_relayer AS relayer_address,
        
        -- ============================================================
        -- TOKEN INFORMATION (what was bridged)
        -- ============================================================
        
        -- Input token: The token address on the origin chain
        -- Already decoded by ETL to proper address format
        -- Example: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 = USDC on Ethereum
        filled_relay_data_input_token AS input_token_address,
        
        -- Output token: The token address on the destination chain (Arbitrum in this case)
        -- Already decoded by ETL to proper address format
        filled_relay_data_output_token AS output_token_address,
        
        -- ============================================================
        -- AMOUNT INFORMATION (how much was bridged)
        -- ============================================================
        
        -- Input amount: How much was sent from origin chain
        -- Already converted by ETL from hex to numeric (stored as text in raw table)
        filled_relay_data_input_amount::NUMERIC AS input_amount,
        
        -- Output amount: How much was received on destination chain
        -- Already converted by ETL from hex to numeric (stored as text in raw table)
        -- Usually slightly less than input due to fees/spread
        filled_relay_data_output_amount::NUMERIC AS output_amount,
        
        -- ============================================================
        -- RELAYER & ROUTING INFORMATION
        -- ============================================================
        
        -- Repayment chain ID: Where the relayer gets reimbursed
        -- Already converted by ETL from hex to numeric (stored as text in raw table)
        -- Relayers front capital, then get paid back (often on a different chain)
        -- Cast via NUMERIC first to handle decimal strings like "1.0" from ETL
        (filled_relay_data_repayment_chain_id::NUMERIC)::BIGINT AS repayment_chain_id,
        
        -- Exclusive relayer: If set, only this address can fill this deposit
        -- Already decoded by ETL to proper address format
        -- Used for priority/guaranteed fills (NULL if no exclusive relayer)
        filled_relay_data_exclusive_relayer AS exclusive_relayer_address,
        
        -- ============================================================
        -- USER INFORMATION
        -- ============================================================
        
        -- Depositor: The original user who initiated the bridge
        -- Already decoded by ETL to proper address format
        filled_relay_data_depositor AS depositor_address,
        
        -- Recipient: Who receives the funds on the destination chain
        -- Already decoded by ETL to proper address format
        -- Usually the same as depositor, but can be different (gift/transfer)
        filled_relay_data_recipient AS recipient_address
        
    FROM raw_fills
)

-- Final SELECT: Add any computed fields and ensure data quality
SELECT
    -- Event identity
    fill_timestamp,
    transaction_hash,
    blockchain,
    source_file,
    
    -- Indexed fields
    origin_chain_id,
    deposit_id,
    relayer_address,
    
    -- Token info
    input_token_address,
    output_token_address,
    
    -- Amounts
    input_amount,
    output_amount,
    
    -- Relayer routing
    repayment_chain_id,
    exclusive_relayer_address,
    
    -- User info
    depositor_address,
    recipient_address
    
FROM cleaned_fills

-- Data quality: Only include rows with essential fields populated
WHERE deposit_id IS NOT NULL
    AND relayer_address IS NOT NULL
    AND input_token_address IS NOT NULL
    AND output_token_address IS NOT NULL
    AND input_amount IS NOT NULL
    AND output_amount IS NOT NULL