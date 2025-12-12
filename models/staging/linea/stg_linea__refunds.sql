-- Staging model for ExecutedRelayerRefundRoot events from Linea

WITH raw_refunds AS (

    SELECT
        -- Core event metadata - these identify WHEN and WHERE the event happened
        timestamp_datetime,              -- When the event occurred (from blockchain)
        transactionHash,                 -- Unique transaction identifier
        blockchain,                      -- Which blockchain (should be 'linea')
        source_file,                     -- Which source file this came from (for lineage)
        
        -- Indexed fields from topics (these are searchable/filterable in blockchain)
        -- Topics are like "indexed columns" in SQL - they're stored separately for fast queries
        topic_chain_id,                  -- Chain ID where the refund was executed
        topic_root_bundle_id,            -- Merkle root bundle identifier (groups multiple refunds)
        topic_leaf_id,                   -- Leaf index in the merkle tree (identifies this specific refund batch)
        
        -- Non-indexed fields from the event's data field
        -- These are the actual business data about the refund
        amount_to_return,                -- Total amount of capital returned in this batch
        l2_token_address,                -- Token address being refunded (on Linea)
        refund_amounts,                  -- Comma-separated list of individual refund amounts
        refund_addresses,                -- Comma-separated list of relayer addresses receiving refunds
        refund_count                     -- Number of relayers being refunded in this batch
        
    FROM raw.linea_logs_processed
    
    -- Filter: Only include rows where ExecutedRelayerRefundRoot data exists
    -- Topic_0 is the event signature hash that identifies the event type
    WHERE topic_0 = '0xf4ad92585b1bc117fbdd644990adf0827bc4c95baeae8a23322af807b6d0020e'
        AND amount_to_return IS NOT NULL
        AND l2_token_address IS NOT NULL
        AND refund_count IS NOT NULL
),

cleaned_refunds AS (
    SELECT
        
        -- Timestamp: Convert text to proper timestamp type
        CASE 
            WHEN timestamp_datetime ~ '^\d+$' THEN          -- If it's pure digits, treat as Unix timestamp (seconds since 1970-01-01)
                TO_TIMESTAMP(timestamp_datetime::BIGINT)    -- PostgreSQL's TO_TIMESTAMP converts Unix seconds to timestamp
            ELSE 
                timestamp_datetime::TIMESTAMP               -- Otherwise, try to parse as ISO format string (e.g., "2025-12-03 10:30:00")
        END AS refund_timestamp,
        
        transactionHash AS transaction_hash,
        blockchain,
        source_file,
        
        -- ============================================================
        -- INDEXED FIELDS (from topics) - Already decoded by ETL
        -- ============================================================
        -- These come from the event's "topics" array, which is indexed for fast blockchain queries
        
        -- Chain ID: Which blockchain this refund was executed on
        -- Already converted by ETL from hex to integer (stored as text in raw table)
        -- Example: 42161 = Arbitrum, 1 = Ethereum, 59144 = Linea
        -- Cast via NUMERIC first to handle decimal strings like "42161.0" from ETL
        (topic_chain_id::NUMERIC)::BIGINT AS chain_id,
        
        -- Root bundle ID: Groups multiple refunds together in a merkle tree
        -- Already converted by ETL from hex to integer (stored as text in raw table)
        -- Multiple refund batches can be grouped into one merkle root for gas efficiency
        -- Cast via NUMERIC first to handle decimal strings like "12345.0" from ETL
        (topic_root_bundle_id::NUMERIC)::BIGINT AS root_bundle_id,
        
        -- Leaf ID: Identifies this specific refund batch within the merkle tree
        -- Already converted by ETL from hex to integer (stored as text in raw table)
        -- Each leaf represents one batch of refunds to multiple relayers
        -- Cast via NUMERIC first to handle decimal strings like "789.0" from ETL
        (topic_leaf_id::NUMERIC)::BIGINT AS leaf_id,
        
        -- ============================================================
        -- TOKEN INFORMATION (what was refunded)
        -- ============================================================
        
        -- L2 token address: The token being refunded on Linea
        -- Already decoded by ETL to proper address format (0x...)
        -- Example: 0x176211869cA2b568f2A7D4EE941E07aA25fee00b = USDC on Linea
        l2_token_address AS refund_token_address,
        
        -- ============================================================
        -- AMOUNT INFORMATION (how much was refunded)
        -- ============================================================
        
        -- Amount to return: Total capital returned in this batch
        -- Already converted by ETL from hex to numeric (stored as text in raw table)
        -- This is the sum of all individual refund amounts in the batch
        -- Stored as NUMERIC to handle large token amounts (18 decimals)
        amount_to_return::NUMERIC AS total_refund_amount,
        
        -- Refund amounts: Individual refund amounts as comma-separated string
        -- Format: "1000000000000000000,2000000000000000000,3000000000000000000"
        -- Each value is in token's base units (e.g., wei for ETH, 6 decimals for USDC)
        -- This is a comma-separated string because the ETL processes dynamic arrays
        refund_amounts AS refund_amounts_string,
        
        -- Refund addresses: Relayer addresses receiving refunds as comma-separated string
        -- Format: "0xAAA...,0xBBB...,0xCCC..."
        -- Matches 1:1 with refund_amounts (first address gets first amount, etc.)
        -- This is a comma-separated string because the ETL processes dynamic arrays
        refund_addresses AS refund_addresses_string,
        
        -- Refund count: Number of relayers being refunded in this batch
        -- Already converted by ETL to integer
        -- This tells us how many individual refunds are in the arrays above
        refund_count::INTEGER AS refund_count
        
    FROM raw_refunds
)

-- Final SELECT: Add any computed fields and ensure data quality
SELECT
    -- Event identity
    refund_timestamp,
    transaction_hash,
    blockchain,
    source_file,
    
    -- Indexed fields (merkle tree identifiers)
    chain_id,
    root_bundle_id,
    leaf_id,
    
    -- Token info
    refund_token_address,
    
    -- Amounts
    total_refund_amount,
    refund_amounts_string,
    refund_addresses_string,
    refund_count
    
FROM cleaned_refunds

-- Data quality: Only include rows with essential fields populated
WHERE chain_id IS NOT NULL
    AND refund_token_address IS NOT NULL
    AND total_refund_amount IS NOT NULL
    AND refund_count IS NOT NULL
