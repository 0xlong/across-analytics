-- Staging model for FilledRelay events from Worldchain
-- This model extracts fill events where relayers complete cross-chain bridge transactions

WITH raw_fills AS (

    SELECT
        -- Core event metadata - these identify WHEN and WHERE the event happened
        timestamp_datetime,              -- When the event occurred (from blockchain)
        transactionHash,                 -- Unique transaction identifier
        blockchain,                      -- Which blockchain (should be 'worldchain')
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
        filled_relay_data_recipient,              -- Final recipient of the bridged funds
        
        -- Gas data (for relayer cost analysis)
        gas_price_wei,
        gas_used
        
    FROM raw.worldchain_logs_processed
    
    -- Filter: Only include rows where FilledRelay data exists
    WHERE topic_0 = '0x44b559f101f8fbcc8a0ea43fa91a05a729a5ea6e14a7c75aa750374690137208'
        AND filled_relay_data_input_amount IS NOT NULL
        AND filled_relay_data_output_amount IS NOT NULL
),

-- INPUT token metadata: includes chain_id for matching with origin_chain_id
-- This allows us to find the correct token on whichever chain the deposit came from
input_token_meta AS (
    {{ get_token_decimals_by_chain_id() }}
),

-- OUTPUT token metadata: filtered to destination chain (Worldchain)
-- This is correct because on fills, output_token is always on the destination chain
output_token_meta AS (
    {{ get_token_decimals('worldchain') }}
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
        -- Normalize to lowercase for joining with token metadata
        LOWER(filled_relay_data_input_token) AS input_token_address,
        
        -- Output token: The token address on the destination chain (Worldchain in this case)
        -- Already decoded by ETL to proper address format
        -- Normalize to lowercase for joining with token metadata
        LOWER(filled_relay_data_output_token) AS output_token_address,
        
        -- ============================================================
        -- AMOUNT INFORMATION (how much was bridged)
        -- ============================================================
        
        -- Input amount: How much was sent from origin chain
        -- Already converted by ETL from hex to numeric (stored as text in raw table)
        -- Store as RAW amount first (before rescaling by decimals)
        filled_relay_data_input_amount::NUMERIC AS input_amount_raw,
        
        -- Output amount: How much was received on destination chain
        -- Already converted by ETL from hex to numeric (stored as text in raw table)
        -- Usually slightly less than input due to fees/spread
        -- Store as RAW amount first (before rescaling by decimals)
        filled_relay_data_output_amount::NUMERIC AS output_amount_raw,
        
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
        filled_relay_data_recipient AS recipient_address,
        
        -- ============================================================
        -- GAS DATA (for relayer cost analysis)
        -- ============================================================
        gas_price_wei::BIGINT AS gas_price_wei,
        gas_used::BIGINT AS gas_used
        
    FROM raw_fills
)

-- ============================================================
-- FINAL SELECT: Join with token metadata and rescale amounts
-- ============================================================
SELECT
    -- Event identity
    c.fill_timestamp,
    c.transaction_hash,
    c.blockchain,
    c.source_file,
    
    -- Indexed fields
    c.origin_chain_id,
    c.deposit_id,
    c.relayer_address,
    
    -- Token info WITH NAMES
    -- Input token (what was deposited on origin chain)
    c.input_token_address,
    input_tok.token_symbol AS input_token_symbol,        -- e.g., 'USDC', 'WETH'
    
    -- Output token (what was received on destination chain - Worldchain)
    c.output_token_address,
    output_tok.token_symbol AS output_token_symbol,      -- e.g., 'USDC', 'WETH'
    
    -- Rescaled amounts (human-readable)
    -- These use the rescale_amount macro which divides raw amount by 10^decimals
    -- Example: 5000000 raw USDC (6 decimals) → 5.0 USDC
    {{ rescale_amount('c.input_amount_raw', 'input_tok.decimals') }} AS input_amount,
    {{ rescale_amount('c.output_amount_raw', 'output_tok.decimals') }} AS output_amount,
    
    -- Raw amounts (preserved for auditing)
    -- These are the original blockchain values before rescaling
    -- Example: 5.0 USDC is stored as 5000000 on blockchain
    c.input_amount_raw,
    c.output_amount_raw,
    
    -- Relayer routing
    c.repayment_chain_id,
    c.exclusive_relayer_address,
    
    -- User info
    c.depositor_address,
    c.recipient_address,
    
    -- Gas data (for relayer cost analysis)
    c.gas_price_wei,
    c.gas_used,
    (c.gas_price_wei * c.gas_used) AS gas_cost_wei
    
FROM cleaned_fills c

-- Join with token metadata to get decimals and symbols for INPUT token
-- Join on BOTH address AND chain_id to match the correct origin chain
LEFT JOIN input_token_meta AS input_tok
    ON c.input_token_address = input_tok.token_address
    AND c.origin_chain_id = input_tok.chain_id

-- Join with token metadata to get decimals and symbols for OUTPUT token
-- Join on address only - we already filtered to destination chain (Worldchain)
LEFT JOIN output_token_meta AS output_tok
    ON c.output_token_address = output_tok.token_address

-- Data quality: Only include rows with essential fields populated
WHERE c.deposit_id IS NOT NULL
    AND c.relayer_address IS NOT NULL
    AND c.input_token_address IS NOT NULL
    AND c.output_token_address IS NOT NULL
    AND c.input_amount_raw IS NOT NULL
    AND c.output_amount_raw IS NOT NULL
