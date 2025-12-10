import polars as pl
from pathlib import Path
import time
from eth_abi import decode as abi_decode  # For decoding dynamic arrays in event data
import os
import glob

#import helper functions
from transform_utils import get_chain_params
from validate_parquet import validate_schema, validate_data_quality, EXPECTED_SCHEMA

# Project root directory (3 levels up from this script)
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent


def hex_to_int(hex_col: pl.Expr) -> pl.Expr:
    """
    Convert hex string (like "0x692f6f7b") to integer.
    Returns NULL if value exceeds Int64 max (~9.2×10^18).
    """
    return hex_col.str.replace("0x", "").str.to_integer(base=16, strict=False)

def hex_to_address(hex_col: pl.Expr) -> pl.Expr:
    """
    Extract address from 32-byte padded hex topic (last 40 chars).
    Topics are 32 bytes, but addresses are only 20 bytes (right-aligned).
    """
    return pl.lit("0x") + hex_col.str.slice(-40)

def timestamp_to_datetime(col: pl.Expr) -> pl.Expr:
    """Convert hex timestamp to datetime, truncated to minutes."""
    return pl.from_epoch(hex_to_int(col), time_unit="s")

def save_to_parquet(df: pl.DataFrame, output_path: Path) -> None:
    """Save DataFrame to Parquet format with directory creation."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)
    print(f"✓ Saved {len(df):,} rows to: {output_path}")


def decode_topics(
    topic0: pl.Expr,
    filled_relay: str,
    funds_deposited: str,
    executed_relayer_refund_root: str
) -> pl.Expr:
    """
    Decode indexed topics (topic_1, topic_2, topic_3) based on event type.
    
    Each event type uses topics differently:
    - FILLED_RELAY: origin_chain_id, deposit_id, relayer
    - FUNDS_DEPOSITED: destination_chain_id, deposit_id, depositor
    - EXECUTED_RELAYER_REFUND_ROOT: chain_id, root_bundle_id, leaf_id
    
    Args:
        topic0: Polars expression for topic_0 (event signature)
        filled_relay: Event signature hash for FilledRelay event
        funds_deposited: Event signature hash for FundsDeposited event
        executed_relayer_refund_root: Event signature hash for ExecutedRelayerRefundRoot event
    """
    # FILLED_RELAY topics
    filled_relay_struct = pl.struct([
        hex_to_int(pl.col("topic_1")).alias("topic_origin_chain_id"),
        hex_to_int(pl.col("topic_2")).alias("topic_deposit_id"),
        hex_to_address(pl.col("topic_3")).alias("topic_relayer"),
    ])
    
    # EXECUTED_RELAYER_REFUND_ROOT topics
    executed_refund_struct = pl.struct([
        hex_to_int(pl.col("topic_1")).alias("topic_chain_id"),
        hex_to_int(pl.col("topic_2")).alias("topic_root_bundle_id"),
        hex_to_int(pl.col("topic_3")).alias("topic_leaf_id"),
    ])
    
    # FUNDS_DEPOSITED topics
    funds_deposited_struct = pl.struct([
        hex_to_int(pl.col("topic_1")).alias("topic_destination_chain_id"),
        hex_to_int(pl.col("topic_2")).alias("topic_deposit_id"),
        hex_to_address(pl.col("topic_3")).alias("topic_depositor"),
    ])
    
    return (
        pl.when(topic0 == filled_relay).then(filled_relay_struct)
        .when(topic0 == executed_relayer_refund_root).then(executed_refund_struct)
        .when(topic0 == funds_deposited).then(funds_deposited_struct)
        .otherwise(None)
        .alias("decoded_topics")
    )


# SLOT EXTRACTORS (for decoding the 'data' field) - atomic helpers that extract ONE 32-byte slot from hex data
def _slot_as_int(data_col: pl.Expr, slot: int) -> pl.Expr:
    """
    Extract a 32-byte slot as Float64 (handles both small and large uint256 values).
    
    Why Float64 for everything?
    ─────────────────────────────────────────────────────────────────────────────
    - Int64 max is ~9.2×10^18, which overflows for token amounts (18 decimals)
    - Float64 can handle up to ~10^308, covering all practical blockchain values
    - Float64 represents integers EXACTLY up to 2^53 (~9×10^15), which covers:
      • Chain IDs (1-10000)           ✓ exact
      • Unix timestamps (~1.7×10^9)   ✓ exact
      • Block numbers (~20×10^6)      ✓ exact
      • Token amounts (10^21+)        ✓ works, ~15 digits precision
    
    How it works:
    ─────────────────────────────────────────────────────────────────────────────
    1. Try parsing full 64-char hex as Int64 (fast path, works for ~99% of values)
    2. If overflow (NULL), split into 8 × 8-char chunks (32 bits each):
       - Each 8-char chunk = 32 bits, max 4.29×10^9, always fits in Int64
       - Combine: v7×2^224 + v6×2^192 + ... + v1×2^32 + v0
    3. Return Float64 in all cases
    
    Trade-off: For very large numbers (> 2^53), may lose precision in the
    least significant digits. Acceptable for analytics purposes.
    """
    offset = 2 + (slot * 64)
    
    # Extract the 64-char hex string for this slot
    hex_str = data_col.str.slice(offset, 64)
    
    # Fast path: try parsing as Int64 directly, then cast to Float64
    # Works for values up to ~9.2×10^18 (covers 99% of cases)
    fast_result = hex_str.str.to_integer(base=16, strict=False).cast(pl.Float64)
    
    # ─────────────────────────────────────────────────────────────────────────
    # Fallback for large values: split into 8 × 8-char chunks (32 bits each)
    # Each 8-char hex = 32 bits = max 4,294,967,295. ALWAYS fits in Int64.
    # ─────────────────────────────────────────────────────────────────────────
    # Slot layout (64 hex chars = 256 bits):
    # [v7: 8 chars][v6: 8 chars][v5: 8 chars][v4: 8 chars][v3: 8 chars][v2: 8 chars][v1: 8 chars][v0: 8 chars]
    #  bits 224-255  192-223     160-191     128-159      96-127       64-95        32-63        0-31
    
    v7 = data_col.str.slice(offset,      8).str.to_integer(base=16, strict=False).cast(pl.Float64).fill_null(0.0)
    v6 = data_col.str.slice(offset + 8,  8).str.to_integer(base=16, strict=False).cast(pl.Float64).fill_null(0.0)
    v5 = data_col.str.slice(offset + 16, 8).str.to_integer(base=16, strict=False).cast(pl.Float64).fill_null(0.0)
    v4 = data_col.str.slice(offset + 24, 8).str.to_integer(base=16, strict=False).cast(pl.Float64).fill_null(0.0)
    v3 = data_col.str.slice(offset + 32, 8).str.to_integer(base=16, strict=False).cast(pl.Float64).fill_null(0.0)
    v2 = data_col.str.slice(offset + 40, 8).str.to_integer(base=16, strict=False).cast(pl.Float64).fill_null(0.0)
    v1 = data_col.str.slice(offset + 48, 8).str.to_integer(base=16, strict=False).cast(pl.Float64).fill_null(0.0)
    v0 = data_col.str.slice(offset + 56, 8).str.to_integer(base=16, strict=False).cast(pl.Float64).fill_null(0.0)
    
    # Combine: v7×2^224 + v6×2^192 + v5×2^160 + v4×2^128 + v3×2^96 + v2×2^64 + v1×2^32 + v0
    # Pre-computed powers of 2^32 for efficiency
    TWO_32  = 4294967296.0                    # 2^32
    TWO_64  = 18446744073709551616.0          # 2^64
    TWO_96  = 79228162514264337593543950336.0 # 2^96
    TWO_128 = TWO_64 * TWO_64                 # 2^128
    TWO_160 = TWO_128 * TWO_32                # 2^160
    TWO_192 = TWO_128 * TWO_64                # 2^192
    TWO_224 = TWO_192 * TWO_32                # 2^224
    
    large_result = (
        v7 * TWO_224 +
        v6 * TWO_192 +
        v5 * TWO_160 +
        v4 * TWO_128 +
        v3 * TWO_96 +
        v2 * TWO_64 +
        v1 * TWO_32 +
        v0
    )
    
    # Use fast path if it worked, otherwise use the large number fallback
    return fast_result.fill_null(large_result)

def _slot_as_address(data_col: pl.Expr, slot: int) -> pl.Expr:
    """
    Extract a 32-byte slot as Ethereum address (20 bytes).
    
    Address layout in slot:
    [000000000000000000000000][a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48]
     ↑ 24 zeros (padding)      ↑ 40 chars (actual address)
    """
    offset = 2 + (slot * 64)
    # Skip 24-char left-padding, take the 40-char address
    return pl.lit("0x") + data_col.str.slice(offset + 24, 40)

def _slot_as_bytes32(data_col: pl.Expr, slot: int) -> pl.Expr:
    """
    Extract a 32-byte slot as raw hex string (for hashes, identifiers).
    No interpretation - just extract and prefix with "0x".
    """
    offset = 2 + (slot * 64)
    return pl.lit("0x") + data_col.str.slice(offset, 64)

# EVENT STRUCT BUILDERS (for 'data' field) - build Polars structs for each event type
def _build_filled_relay_struct(data_col: pl.Expr) -> pl.Expr:
    """
    Build decoded struct for FilledRelay V3 event (NON-INDEXED fields only).
    
    IMPORTANT: origin_chain_id, deposit_id, and relayer are INDEXED parameters!
    They appear in topics (topic_1, topic_2, topic_3), NOT in the data field.
    The data field only contains non-indexed parameters.
    
    Data Field Layout (non-indexed params):
    ─────┬─────────────────────┬─────────────────────────────────────────────────
    Slot | Field               | Description
    ─────┼─────────────────────┼─────────────────────────────────────────────────
     0   | input_token         | Token deposited on origin chain (address)
     1   | output_token        | Token to receive on destination chain (address)
     2   | input_amount        | Amount deposited in input token units (uint256)
     3   | output_amount       | Amount received in output token units (uint256)
     4   | repayment_chain_id  | Chain where relayer gets repaid (uint256)
     5   | fill_deadline       | Unix timestamp deadline for fill (uint32)
     6   | exclusivity_deadline| Deadline for exclusive relayer rights (uint32)
     7   | exclusive_relayer   | Address with exclusive fill rights (address)
     8   | depositor           | Original depositor address (address)
     9   | recipient           | Final recipient of funds (address)
     10  | message_hash        | Hash of cross-chain message, if any (bytes32)
    ─────┴─────────────────────┴─────────────────────────────────────────────────
    
    Indexed params (from topics, NOT in data):
    - topic_1 = origin_chain_id
    - topic_2 = deposit_id  
    - topic_3 = relayer
    """
    return pl.struct([
        # Token information
        _slot_as_address(data_col, 0).alias("filled_relay_data_input_token"),
        _slot_as_address(data_col, 1).alias("filled_relay_data_output_token"),
        
        # Amounts (Float64 handles large token amounts automatically)
        _slot_as_int(data_col, 2).alias("filled_relay_data_input_amount"),
        _slot_as_int(data_col, 3).alias("filled_relay_data_output_amount"),
        
        # Chain and timing
        _slot_as_int(data_col, 4).alias("filled_relay_data_repayment_chain_id"),
        _slot_as_int(data_col, 5).alias("filled_relay_data_fill_deadline"),
        _slot_as_int(data_col, 6).alias("filled_relay_data_exclusivity_deadline"),
        
        # Addresses
        _slot_as_address(data_col, 7).alias("filled_relay_data_exclusive_relayer"),
        _slot_as_address(data_col, 8).alias("filled_relay_data_depositor"),
        _slot_as_address(data_col, 9).alias("filled_relay_data_recipient"),
        
        # Message
        _slot_as_bytes32(data_col, 10).alias("filled_relay_data_message_hash"),
    ])

def _build_funds_deposited_struct(data_col: pl.Expr) -> pl.Expr:
    """
    Build decoded struct for FundsDeposited V3 event.
    
    Slot | Field               | Type
    ─────┼─────────────────────┼─────────
     0   | input_token         | address
     1   | output_token        | address
     2   | input_amount        | uint256
     3   | output_amount       | uint256
     4   | quote_timestamp     | uint32
     5   | fill_deadline       | uint32
     6   | exclusivity_deadline| uint32
     7   | recipient           | address
     8   | exclusive_relayer   | address
     9   | message_offset      | (dynamic pointer - skip)
    """
    return pl.struct([
        # Token information
        _slot_as_address(data_col, 0).alias("funds_deposited_data_input_token"),
        _slot_as_address(data_col, 1).alias("funds_deposited_data_output_token"),
        
        # Amounts (Float64 handles large token amounts automatically)
        _slot_as_int(data_col, 2).alias("funds_deposited_data_input_amount"),
        _slot_as_int(data_col, 3).alias("funds_deposited_data_output_amount"),
        
        # Timing
        _slot_as_int(data_col, 4).alias("funds_deposited_data_quote_timestamp"),
        _slot_as_int(data_col, 5).alias("funds_deposited_data_fill_deadline"),
        _slot_as_int(data_col, 6).alias("funds_deposited_data_exclusivity_deadline"),
        
        # Addresses
        _slot_as_address(data_col, 7).alias("funds_deposited_data_recipient"),
        _slot_as_address(data_col, 8).alias("funds_deposited_data_exclusive_relayer"),
    ])

def _decode_executed_refund_data(data_hex: str) -> dict | None:
    """
    Decode ExecutedRelayerRefundRoot event data including dynamic arrays.
    
    This function uses eth_abi.decode() which handles the ABI encoding automatically:
    - Static types are read directly from their slots
    - Dynamic arrays (uint256[], address[]) use offset pointers in the head section,
      with actual array data (length + elements) in the tail section
    
    Event signature (non-indexed params in data field):
    ─────────────────────────────────────────────────────────────────────────────
    ExecutedRelayerRefundRoot(
        uint256 amountToReturn,      # Slot 0: static
        uint256[] refundAmounts,     # Slot 1: OFFSET POINTER → tail section
        address l2TokenAddress,      # Slot 2: static
        address[] refundAddresses,   # Slot 3: OFFSET POINTER → tail section  
        bool deferredRefunds,        # Slot 4: static
        address caller               # Slot 5: static
    )
    ─────────────────────────────────────────────────────────────────────────────
    
    Note: chainId, rootBundleId, leafId are INDEXED (in topics, not in data).
    
    IMPORTANT: This function is called via map_elements which applies to ALL rows
    before when/then filtering. Returns None for non-matching event data layouts
    to prevent crashes when decoding other event types.
    
    Args:
        data_hex: Raw hex string from event data field (e.g., "0x000...abc")
    
    Returns:
        Dictionary with decoded fields, or None if decoding fails (wrong event type)
    """
    # ─────────────────────────────────────────────────────────────────────────
    # Null struct template - returned when decoding fails (wrong event type)
    # Polars requires a dict with all fields present; None values become nulls
    # NOTE: deferred_refunds and caller SKIPPED (not needed for capital flow)
    # ─────────────────────────────────────────────────────────────────────────
    NULL_STRUCT = {
        "amount_to_return": None,
        "l2_token_address": None,
        "refund_amounts": None,
        "refund_addresses": None,
        "refund_count": None,
        # "deferred_refunds": None,  # SKIPPED: execution flag, not capital
        # "caller": None,            # SKIPPED: who called, not who receives
    }
    
    try:
        # ─────────────────────────────────────────────────────────────────────
        # Step 0: Guard against None input (can happen with skip_nulls=False)
        # ─────────────────────────────────────────────────────────────────────
        if data_hex is None:
            return NULL_STRUCT
            
        # ─────────────────────────────────────────────────────────────────────
        # Step 1: Convert hex string to bytes (remove '0x' prefix)
        # ─────────────────────────────────────────────────────────────────────
        data_bytes = bytes.fromhex(data_hex[2:])
        
        # ─────────────────────────────────────────────────────────────────────
        # Step 2: Define ABI types for non-indexed params (in declaration order)
        # eth_abi handles offset pointers and tail section parsing automatically
        # ─────────────────────────────────────────────────────────────────────
        types = [
            'uint256',    # amountToReturn
            'uint256[]',  # refundAmounts (dynamic array)
            'address',    # l2TokenAddress
            'address[]',  # refundAddresses (dynamic array)
            'bool',       # deferredRefunds
            'address'     # caller
        ]
        
        # ─────────────────────────────────────────────────────────────────────
        # Step 3: Decode all fields in one call
        # The library returns arrays as Python tuples
        # ─────────────────────────────────────────────────────────────────────
        (
            amount_to_return,
            refund_amounts,      # Tuple of uint256 values
            l2_token_address,
            refund_addresses,    # Tuple of address strings
            deferred_refunds,
            caller
        ) = abi_decode(types, data_bytes)
        
        # ─────────────────────────────────────────────────────────────────────
        # Step 4: Return as dictionary for Polars struct conversion
        # Arrays are stored as comma-separated strings (Polars-friendly format)
        # NOTE: deferred_refunds and caller SKIPPED (not needed for capital flow)
        # ─────────────────────────────────────────────────────────────────────
        return {
            "amount_to_return": float(amount_to_return),        # Float64 for large values
            "l2_token_address": l2_token_address,               # Checksum address string
            "refund_amounts": ",".join(str(a) for a in refund_amounts),    # "100,200,300"
            "refund_addresses": ",".join(refund_addresses),                 # "0xAAA,0xBBB"
            "refund_count": len(refund_amounts),                # Number of refunds in this leaf
            # "deferred_refunds": deferred_refunds,             # SKIPPED: execution flag
            # "caller": caller                                  # SKIPPED: who called execute
        }
    except Exception:
        # ─────────────────────────────────────────────────────────────────────
        # Return null struct for non-ExecutedRelayerRefundRoot events
        # map_elements applies to ALL rows; the when/then filter happens AFTER
        # ─────────────────────────────────────────────────────────────────────
        return NULL_STRUCT


def _build_executed_refund_struct(data_col: pl.Expr) -> pl.Expr:
    """
    Build decoded struct for ExecutedRelayerRefundRoot event.
    
    Uses map_elements to apply _decode_executed_refund_data() row-by-row.
    This is necessary because dynamic arrays require offset-based decoding
    that cannot be expressed as pure Polars columnar operations.
    
    Trade-off: Slightly slower than pure Polars expressions, but:
    - Correct handling of ABI-encoded dynamic arrays
    - Clean, maintainable code using battle-tested eth_abi library
    - Returns proper struct that integrates with existing pipeline
    """
    # NOTE: deferred_refunds and caller SKIPPED at source (not needed for capital flow)
    # Since map_elements is a black box to Polars optimizer, we skip early here
    return data_col.map_elements(
        _decode_executed_refund_data,
        return_dtype=pl.Struct([
            pl.Field("amount_to_return", pl.Float64),
            pl.Field("l2_token_address", pl.Utf8),
            pl.Field("refund_amounts", pl.Utf8),        # Comma-separated string
            pl.Field("refund_addresses", pl.Utf8),      # Comma-separated string
            pl.Field("refund_count", pl.Int64),         # Count of refunds
            # pl.Field("deferred_refunds", pl.Boolean), # SKIPPED: execution flag
            # pl.Field("caller", pl.Utf8),              # SKIPPED: who called execute
        ])
    )

# Decode the data field based on the event type
def decode_data(
    data_col: pl.Expr,
    topic0_col: pl.Expr,
    filled_relay: str,
    funds_deposited: str,
    executed_relayer_refund_root: str
) -> pl.Expr:
    """
    Decode event 'data' field based on event signature (topic0).
    
    This is the MAIN dispatcher for data field decoding:
    1. Checks topic0 to identify event type
    2. Routes to appropriate struct builder
    3. Returns struct with decoded fields (or NULL for unknown events)
    
    Args:
        data_col: Polars expression for the 'data' field
        topic0_col: Polars expression for topic_0 (event signature)
        filled_relay: Event signature hash for FilledRelay event
        funds_deposited: Event signature hash for FundsDeposited event
        executed_relayer_refund_root: Event signature hash for ExecutedRelayerRefundRoot event
    """
    return (
        pl.when(topic0_col == filled_relay)
            .then(_build_filled_relay_struct(data_col))
        .when(topic0_col == funds_deposited)
            .then(_build_funds_deposited_struct(data_col))
        .when(topic0_col == executed_relayer_refund_root)
            .then(_build_executed_refund_struct(data_col))
        .otherwise(pl.lit(None))
    ).alias("decoded_data")



# MAIN TRANSFORMATION PIPELINE
def transform_data(
    chain: str,
    start_date: str,
    end_date: str
) -> pl.DataFrame:
    """
    Transform raw event logs into structured, typed columns.
    
    This function is designed to be called from Airflow DAGs with date-based parameters.
    It processes event logs from a specified chain and date range, decoding Ethereum
    event data into structured columns for capital flow analysis.
    
    Args:
        chain: Blockchain name (e.g., "ethereum", "polygon", "arbitrum")
        start_date: Start date in YYYY-MM-DD format (e.g., "2025-12-03")
        end_date: End date in YYYY-MM-DD format (e.g., "2025-12-04")
    
    Returns:
        Polars DataFrame with transformed event log data
    
    Pipeline steps:
    1. Construct input/output file paths from parameters
    2. Load chain-specific event signatures (topics)
    3. Scan JSONL file (lazy - no data loaded yet)
    4. Extract topics list into individual columns
    5. Decode topics based on event type
    6. Decode data field based on event type
    7. Select and rename final columns
    8. Execute (collect) and save to Parquet
    """
    time_start = time.time()
    
    # Construct file paths from parameters using PROJECT_ROOT constant
    input_file = PROJECT_ROOT / f"data/raw/etherscan_api/logs_{chain}_{start_date}_to_{end_date}.jsonl"
    output_file = PROJECT_ROOT / f"data/processed/logs_{chain}_{start_date}_to_{end_date}_processed.parquet"
    
    # Load chain-specific event signatures (topic0 hashes)
    chain_params = get_chain_params(chain)
    if chain_params is None:
        raise ValueError(f"Chain '{chain}' not found in configuration file")
    
    filled_relay = chain_params["topics"][0]
    funds_deposited = chain_params["topics"][1]
    executed_relayer_refund_root = chain_params["topics"][2]
    
    result = (
        # STEP 1: Create LazyFrame (no data loaded yet - just a query plan)
        pl.scan_ndjson(input_file)
        
        # STEP 2: Extract topics[0..3] into separate columns
        # The topics list always has 4 elements for our events
        .with_columns([
            pl.col("topics")
            .list.to_struct(fields=["topic_0", "topic_1", "topic_2", "topic_3"])
            .alias("topics_struct")
        ])
        .unnest("topics_struct")
        
        # STEP 3: Decode everything in parallel
        # - Convert hex timestamps/numbers to proper types
        # - Decode topics based on event type
        # - Decode data field based on event type
        .with_columns([
            # Metadata conversions
            timestamp_to_datetime(pl.col("timeStamp")).alias("timestamp_datetime"),
            hex_to_int(pl.col("timeStamp")).alias("timestamp_unix"),
            hex_to_int(pl.col("blockNumber")).alias("block_number_int"),
            hex_to_int(pl.col("gasPrice")).alias("gas_price_wei"),
            hex_to_int(pl.col("gasUsed")).alias("gas_used"),
            
            # Decode topics (indexed parameters) - pass chain-specific event signatures
            decode_topics(
                pl.col("topic_0"),
                filled_relay,
                funds_deposited,
                executed_relayer_refund_root
            ),
            
            # Decode data field (non-indexed parameters) - pass chain-specific event signatures
            decode_data(
                pl.col("data"),
                pl.col("topic_0"),
                filled_relay,
                funds_deposited,
                executed_relayer_refund_root
            ),
        ])
        
        # STEP 4: Unnest decoded structs into individual columns
        .unnest("decoded_topics")
        .unnest("decoded_data")
        
        # STEP 5: Select final columns for CAPITAL FLOW ANALYSIS
        # ─────────────────────────────────────────────────────────────────────
        # SKIPPED columns (commented out below - can re-enable if needed):
        #   - filled_relay_data_fill_deadline          → timing constraint
        #   - filled_relay_data_exclusivity_deadline   → exclusive relayer window
        #   - filled_relay_data_message_hash           → cross-chain message hash
        #   - topic_root_bundle_id, topic_leaf_id      → merkle tree internals
        # SKIPPED at source (_decode_executed_refund_data - map_elements can't optimize):
        #   - deferred_refunds, caller                 → execution mechanics
        # ─────────────────────────────────────────────────────────────────────
        .select([
            # ═══════════════════════════════════════════════════════════════════
            # CORE IDENTITY (common to all events)
            # ═══════════════════════════════════════════════════════════════════
            "timestamp_datetime",           # When the event occurred (truncated to minute)
            "transactionHash",              # Transaction hash for lookups/deduplication
            "topic_0",                      # Event signature - filter by event type
            
            # ═══════════════════════════════════════════════════════════════════
            # FILLED_RELAY - Capital flow: origin → destination (fill side)
            # ═══════════════════════════════════════════════════════════════════
            # --- Topics (indexed) ---
            "topic_origin_chain_id",        # Where capital came FROM
            "topic_deposit_id",             # Matching key to link deposits ↔ fills
            "topic_relayer",                # Who provided liquidity (relayer address)
            # --- Data (non-indexed) ---
            "filled_relay_data_input_token",            # Token on origin chain
            "filled_relay_data_output_token",           # Token on destination chain
            "filled_relay_data_input_amount",           # Amount sent from origin
            "filled_relay_data_output_amount",          # Amount received on destination
            "filled_relay_data_repayment_chain_id",     # Where relayer gets reimbursed
            #"filled_relay_data_fill_deadline",         # SKIPPED: timing constraint for fill
            #"filled_relay_data_exclusivity_deadline",  # SKIPPED: exclusive relayer window
            "filled_relay_data_exclusive_relayer",      # Address with exclusive fill rights
            "filled_relay_data_depositor",              # Who initiated the bridge
            "filled_relay_data_recipient",              # Who received the funds
            #"filled_relay_data_message_hash",          # SKIPPED: cross-chain message hash
            
            # ═══════════════════════════════════════════════════════════════════
            # FUNDS_DEPOSITED - Capital flow: user → protocol (deposit side)
            # ═══════════════════════════════════════════════════════════════════
            # --- Topics (indexed) ---
            "topic_destination_chain_id",   # Where capital is going TO
            # topic_deposit_id              # (shared column - see above)
            "topic_depositor",              # Who is depositing funds
            # --- Data (non-indexed) ---
            "funds_deposited_data_input_token",         # Token deposited
            "funds_deposited_data_output_token",        # Token to receive on destination
            "funds_deposited_data_input_amount",        # How much deposited
            "funds_deposited_data_output_amount",       # How much expected on destination
            "funds_deposited_data_recipient",           # Final recipient of funds
            
            # ═══════════════════════════════════════════════════════════════════
            # EXECUTED_RELAYER_REFUND - Capital flow: protocol → relayers (refund)
            # ═══════════════════════════════════════════════════════════════════
            # --- Topics (indexed) ---
            "topic_chain_id",               # Chain where refund executed
            #"topic_root_bundle_id",        # SKIPPED: merkle root bundle ID
            #"topic_leaf_id",               # SKIPPED: merkle tree leaf index
            # --- Data (non-indexed) ---
            "amount_to_return",             # Total capital returned in this batch
            "l2_token_address",             # Token being refunded
            "refund_amounts",               # Individual refund amounts (CSV string)
            "refund_addresses",             # Who receives each refund (CSV string)
            "refund_count",                 # Number of relayers refunded
            # deferred_refunds, caller → SKIPPED at source (_decode_executed_refund_data)
        ])
        
        # STEP 6: Execute the query plan (all optimizations applied here)
        .collect()
    )

    print(f"✓ Transformed in {time.time() - time_start:.2f}s")
    
    # Validate data before saving (fail fast if there are issues)
    print("\n🔍 Validating transformed data...")
    schema_valid, schema_errors = validate_schema(result, EXPECTED_SCHEMA)
    if not schema_valid:
        print("❌ Schema validation failed:")
        for error in schema_errors:
            print(f"   {error}")
        raise ValueError("Data validation failed: schema mismatch")
    
    quality_valid, quality_errors = validate_data_quality(result)
    if not quality_valid:
        print("❌ Data quality validation failed:")
        for error in quality_errors:
            print(f"   {error}")
        raise ValueError("Data validation failed: data quality issues")
    
    print("✓ Validation passed")
    save_to_parquet(result, output_file)
    return result


if __name__ == "__main__":

    # list all the files to be processed from the data/raw/etherscan_api directory
    files = glob.glob(os.path.join(PROJECT_ROOT, "data", "raw", "etherscan_api", "*.jsonl"))
    
    # define the chain, start_date, and end_date
    for file in files:

        # print file name to be processed
        print("\n"+file)

        # get chain name, start date, and end date from file name
        chain = file.split("logs_")[1].split("_")[0]
        start_date = file.split("logs_")[1].split("_")[1]
        end_date = file.split("logs_")[1].split("_")[3].strip(".jsonl")
        print(f"Chain: {chain} \nStart date: {start_date} \nEnd date: {end_date}")

        # transform data
        transform_data(chain, start_date, end_date) 