-- int_deposit_fill_matching.sql
-- PURPOSE: Match every deposit to its fill (if it exists)
-- WHY: This is the CORE model - connects money leaving one chain to money arriving on another.
-- 
-- HOW IT WORKS:
-- 1. User deposits on Chain A → creates deposit_id
-- 2. Relayer fills on Chain B → same deposit_id
-- 3. We JOIN on deposit_id + origin/destination chain match
-- 4. Unfilled deposits = rows with NULL fill (stuck capital)

{{ config(materialized='view') }}

-- Chain ID to Name mapping for chains with parquet data
WITH chain_names AS (
    SELECT 
        chain_id,
        chain_name
    FROM (
        VALUES
        -- Only chains we have parquet data for:
        (1, 'Ethereum'),
        (42161, 'Arbitrum'),
        (137, 'Polygon'),
        (59144, 'Linea'),
        (480, 'Worldchain'),
        (130, 'Unichain'),
        (999, 'HyperEVM'),
        (143, 'Monad')
    ) AS chains(chain_id, chain_name)
),

-- Token metadata for symbol lookups
token_metadata AS (
    SELECT * FROM {{ ref('token_metadata') }}
),

-- Hourly token prices for USD conversion
-- Truncate to hour for matching with transaction timestamps
token_prices AS (
    SELECT 
        token_symbol,
        DATE_TRUNC('hour', timestamp::TIMESTAMP) AS price_hour,
        AVG(price_usd) AS price_usd  -- Average in case of duplicate hours
    FROM {{ ref('token_prices') }}
    GROUP BY token_symbol, DATE_TRUNC('hour', timestamp::TIMESTAMP)
),

-- Native token symbol per chain (for gas cost USD conversion)
-- Gas fees are paid in the chain's native token
native_tokens AS (
    SELECT chain_id, native_token_symbol
    FROM (
        VALUES
        (1, 'WETH'),       -- Ethereum: ETH (use WETH price)
        (42161, 'WETH'),   -- Arbitrum: ETH
        (137, 'MATIC'),    -- Polygon: MATIC
        (59144, 'WETH'),   -- Linea: ETH
        (480, 'WETH'),     -- Worldchain: ETH (WLD for app, but gas in ETH)
        (130, 'WETH'),     -- Unichain: ETH
        (999, 'WETH'),     -- HyperEVM: ETH
        (143, 'WETH')      -- Monad: ETH (assumed)
    ) AS nt(chain_id, native_token_symbol)
),

deposits AS (
    SELECT * FROM {{ ref('int_unified_deposits') }}
),

fills AS (
    SELECT * FROM {{ ref('int_unified_fills') }}
),

-- JOIN deposits to fills on deposit_id + chain matching
matched AS (
    SELECT
        -- === DEPOSIT INFO (Origin side) ===
        d.deposit_timestamp,
        d.transaction_hash AS deposit_tx_hash,
        d.origin_chain_id,
        d.destination_chain_id,
        d.deposit_id,
        d.depositor_address,
        d.recipient_address AS deposit_recipient,
        d.input_token_address AS deposit_token,
        d.input_amount AS deposit_amount,
        d.output_amount AS expected_output_amount,
        
        -- === FILL INFO (Destination side) ===
        f.fill_timestamp,
        f.transaction_hash AS fill_tx_hash,
        f.relayer_address,
        f.output_token_address AS fill_token,
        f.output_amount AS actual_output_amount,
        f.repayment_chain_id,
        
        -- === GAS DATA (for relayer cost analysis) ===
        f.gas_price_wei,
        f.gas_used,
        f.gas_cost_wei,
        
        -- === COMPUTED FIELDS ===
        -- Fill latency: How long did it take to fill? (in seconds)
        -- Use GREATEST(0, ...) to handle cross-chain timestamp sync issues
        GREATEST(0, EXTRACT(EPOCH FROM (f.fill_timestamp - d.deposit_timestamp))) AS fill_latency_seconds,
        
        -- Is this deposit filled?
        CASE WHEN f.deposit_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_filled,
        
        -- Fee: difference between deposited amount and filled amount
        CASE 
            WHEN f.output_amount IS NOT NULL
            THEN ROUND((d.input_amount - f.output_amount)::NUMERIC, 2)
            ELSE NULL 
        END AS bridge_fee_nominal,
        
        CASE 
            WHEN f.output_amount IS NOT NULL
            THEN ROUND(((d.input_amount - f.output_amount) / d.input_amount * 100)::NUMERIC, 2)
            ELSE NULL 
        END AS bridge_fee_percent,
        
        -- Slippage: Difference between expected and actual output
        CASE 
            WHEN f.output_amount IS NOT NULL AND d.output_amount > 0 
            THEN ROUND(((d.output_amount - f.output_amount) / d.output_amount * 100)::NUMERIC, 2)
            ELSE NULL 
        END AS slippage_percent

    FROM deposits d
    
    -- LEFT JOIN: Keep ALL deposits, even unfilled ones
    LEFT JOIN fills f 
        ON d.deposit_id = f.deposit_id
        AND d.origin_chain_id = f.origin_chain_id  -- Must match origin
        AND d.destination_chain_id = f.destination_chain_id  -- Must match destination
)

SELECT
    -- Identity
    m.deposit_timestamp,
    m.deposit_tx_hash,
    m.origin_chain_id,
    oc.chain_name AS origin_chain_name,
    m.destination_chain_id,
    dc.chain_name AS destination_chain_name,
    m.deposit_id,
    
    -- Deposit details
    m.depositor_address,
    m.deposit_recipient,
    m.deposit_token,
    dt.token_symbol AS deposit_token_symbol,
    m.deposit_amount,
    m.expected_output_amount,
    
    -- USD-normalized amounts (joined from hourly price data)
    dp.price_usd AS deposit_token_price_usd,
    ROUND((m.deposit_amount * COALESCE(dp.price_usd, 1))::NUMERIC, 2) AS deposit_amount_usd,
    
    -- Fill details (NULL if unfilled)
    m.fill_timestamp,
    m.fill_tx_hash,
    m.relayer_address,
    m.fill_token,
    ft.token_symbol AS fill_token_symbol,
    m.actual_output_amount,
    m.repayment_chain_id,
    
    -- USD-normalized fill amount
    fp.price_usd AS fill_token_price_usd,
    CASE 
        WHEN m.actual_output_amount IS NOT NULL 
        THEN ROUND((m.actual_output_amount * COALESCE(fp.price_usd, 1))::NUMERIC, 2)
        ELSE NULL 
    END AS fill_amount_usd,
    
    -- Metrics
    m.fill_latency_seconds,
    m.is_filled,
    m.bridge_fee_nominal,
    m.bridge_fee_percent,
    m.slippage_percent,
    
    -- Gas data (for relayer cost analysis)
    -- These represent the cost incurred by the relayer on the destination chain
    -- NOTE: Different chains use different native tokens (ETH, MATIC, WLD, etc.)
    m.gas_price_wei,
    m.gas_used,
    m.gas_cost_wei,
    -- Convert gas cost to native token units for readability (wei / 10^18)
    ROUND((m.gas_cost_wei / 1e18)::NUMERIC, 8) AS gas_cost_native,
    -- Gas cost in USD (using native token price at fill hour)
    CASE 
        WHEN m.gas_cost_wei IS NOT NULL 
        THEN ROUND(((m.gas_cost_wei / 1e18) * COALESCE(np.price_usd, 0))::NUMERIC, 2)
        ELSE NULL 
    END AS gas_cost_usd,
    
    -- Bridge fee in USD (fee * deposit token price)
    CASE 
        WHEN m.bridge_fee_nominal IS NOT NULL 
        THEN ROUND((m.bridge_fee_nominal * COALESCE(dp.price_usd, 1))::NUMERIC, 2)
        ELSE NULL 
    END AS bridge_fee_nominal_usd,
    
    -- Route identifier (for aggregations)
    oc.chain_name || ' → ' || dc.chain_name AS route_name,
    m.origin_chain_id || '_' || m.destination_chain_id AS route_id

FROM matched m

-- Join for origin chain name
LEFT JOIN chain_names oc ON m.origin_chain_id = oc.chain_id

-- Join for destination chain name  
LEFT JOIN chain_names dc ON m.destination_chain_id = dc.chain_id

-- Join for deposit token symbol (origin chain token)
LEFT JOIN token_metadata dt 
    ON m.origin_chain_id = dt.chain_id 
    AND LOWER(m.deposit_token) = LOWER(dt.token_address)

-- Join for fill token symbol (destination chain token)
LEFT JOIN token_metadata ft 
    ON m.destination_chain_id = ft.chain_id 
    AND LOWER(m.fill_token) = LOWER(ft.token_address)

-- Join for deposit token price at deposit hour
LEFT JOIN token_prices dp
    ON dt.token_symbol = dp.token_symbol
    AND DATE_TRUNC('hour', m.deposit_timestamp) = dp.price_hour

-- Join for fill token price at fill hour
LEFT JOIN token_prices fp
    ON ft.token_symbol = fp.token_symbol
    AND DATE_TRUNC('hour', m.fill_timestamp) = fp.price_hour

-- Join for native token symbol on destination chain (for gas cost USD)
LEFT JOIN native_tokens nt ON m.destination_chain_id = nt.chain_id

-- Join for native token price at fill hour (for gas cost USD)
LEFT JOIN token_prices np
    ON nt.native_token_symbol = np.token_symbol
    AND DATE_TRUNC('hour', m.fill_timestamp) = np.price_hour
