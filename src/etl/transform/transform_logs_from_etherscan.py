import polars as pl
from pathlib import Path
import time

# Input and output files
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
INPUT_FILE = PROJECT_ROOT / "data/raw/etherscan_api/logs_ethereum_2025-12-03_to_2025-12-04.jsonl"
OUTPUT_FILE = PROJECT_ROOT / "data/processed/logs_ethereum_transformed.parquet"

# Event signatures (keccak256 hashes) - topic[0] identifies which event was emitted
FILLED_RELAY = "0x44b559f101f8fbcc8a0ea43fa91a05a729a5ea6e14a7c75aa750374690137208"
FUNDS_DEPOSITED = "0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3"
EXECUTED_RELAYER_REFUND_ROOT = "0xf4ad92585b1bc117fbdd644990adf0827bc4c95baeae8a23322af807b6d0020e"


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
    return pl.from_epoch(hex_to_int(col), time_unit="s").dt.truncate("1m")


def save_to_parquet(df: pl.DataFrame, output_path: Path) -> None:
    """Save DataFrame to Parquet format with directory creation."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)
    print(f"✓ Saved {len(df):,} rows to: {output_path}")

def decode_topics(topic0: pl.Expr) -> pl.Expr:
    """
    Decode indexed topics (topic_1, topic_2, topic_3) based on event type.
    
    Each event type uses topics differently:
    - FILLED_RELAY: origin_chain_id, deposit_id, relayer
    - FUNDS_DEPOSITED: destination_chain_id, deposit_id, depositor
    - EXECUTED_RELAYER_REFUND_ROOT: chain_id, root_bundle_id, leaf_id
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
        pl.when(topic0 == FILLED_RELAY).then(filled_relay_struct)
        .when(topic0 == EXECUTED_RELAYER_REFUND_ROOT).then(executed_refund_struct)
        .when(topic0 == FUNDS_DEPOSITED).then(funds_deposited_struct)
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

def _build_executed_refund_struct(data_col: pl.Expr) -> pl.Expr:
    """
    Build decoded struct for ExecutedRelayerRefundRoot event.
    
    Slot | Field               | Type
    ─────┼─────────────────────┼─────────
     0   | amount_to_return    | uint256
     1   | chain_id            | uint256
     2   | refund_amounts_ptr  | (offset pointer - skip)
     3   | l2_token_address    | address
     4   | refund_addresses_ptr| (offset pointer - skip)
    
    Note: Dynamic arrays at slots 2, 4+ require offset-based decoding.
    """
    return pl.struct([
        # Amount (Float64 handles large token amounts automatically)
        _slot_as_int(data_col, 0).alias("executed_refund_root_data_amount_to_return"),
        
        # Chain ID
        _slot_as_int(data_col, 1).alias("executed_refund_root_data_chain_id"),
        
        # Token address
        _slot_as_address(data_col, 3).alias("executed_refund_root_data_l2_token_address"),
    ])

# Decode the data field based on the event type
def decode_data(data_col: pl.Expr, topic0_col: pl.Expr) -> pl.Expr:
    """
    Decode event 'data' field based on event signature (topic0).
    
    This is the MAIN dispatcher for data field decoding:
    1. Checks topic0 to identify event type
    2. Routes to appropriate struct builder
    3. Returns struct with decoded fields (or NULL for unknown events)

    """
    return (
        pl.when(topic0_col == FILLED_RELAY)
            .then(_build_filled_relay_struct(data_col))
        .when(topic0_col == FUNDS_DEPOSITED)
            .then(_build_funds_deposited_struct(data_col))
        .when(topic0_col == EXECUTED_RELAYER_REFUND_ROOT)
            .then(_build_executed_refund_struct(data_col))
        .otherwise(pl.lit(None))
    ).alias("decoded_data")



# MAIN TRANSFORMATION PIPELINE
def transform_data() -> pl.DataFrame:
    """
    Transform raw event logs into structured, typed columns.
    
    Pipeline steps:
    1. Scan JSONL file (lazy - no data loaded yet)
    2. Extract topics list into individual columns
    3. Decode topics based on event type
    4. Decode data field based on event type
    5. Select and rename final columns
    6. Execute (collect) and save to Parquet
    """
    time_start = time.time()
    
    result = (
        # STEP 1: Create LazyFrame (no data loaded yet - just a query plan)
        pl.scan_ndjson(INPUT_FILE)
        
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
            
            # Decode topics (indexed parameters)
            decode_topics(pl.col("topic_0")),
            
            # Decode data field (non-indexed parameters)
            decode_data(pl.col("data"), pl.col("topic_0")),
        ])
        
        # STEP 4: Unnest decoded structs into individual columns
        .unnest("decoded_topics")
        .unnest("decoded_data")
        
        # STEP 5: Select final columns organized by event type
        # Each event populates only its section; other sections are NULL
        .select([
            # ═══════════════════════════════════════════════════════════════════
            # METADATA (common to all events)
            # ═══════════════════════════════════════════════════════════════════
            "timestamp_datetime",           # Human-readable timestamp (truncated to minute)
            #"timestamp_unix",               # Unix timestamp for calculations
            #"block_number_int",             # Block number as integer
            "transactionHash",              # Transaction hash for lookups
            "topic_0",                      # Event signature (keccak256) - use to filter by event type
            #"gas_price_wei",                # Gas price paid (in wei)
            #"gas_used",                     # Gas consumed by this log
            
            # ═══════════════════════════════════════════════════════════════════
            # FILLED_RELAY (FilledRelay V3) - Bridge fill completed on destination
            # ═══════════════════════════════════════════════════════════════════
            # --- Topics (indexed parameters) ---
            "topic_origin_chain_id",        # Chain where deposit originated (indexed)
            "topic_deposit_id",             # Unique deposit ID (indexed, shared with FUNDS_DEPOSITED)
            "topic_relayer",                # Address of relayer who filled (indexed)
            # --- Data (non-indexed parameters) ---
            "filled_relay_data_input_token",            # Token deposited on origin chain
            "filled_relay_data_output_token",           # Token received on destination chain
            "filled_relay_data_input_amount",           # Amount deposited (in input token units)
            "filled_relay_data_output_amount",          # Amount received (in output token units)
            "filled_relay_data_repayment_chain_id",     # Chain where relayer gets repaid
            "filled_relay_data_fill_deadline",          # Unix timestamp deadline for fill
            "filled_relay_data_exclusivity_deadline",   # Deadline for exclusive relayer
            "filled_relay_data_exclusive_relayer",      # Address with exclusive fill rights (if any)
            "filled_relay_data_depositor",              # Original depositor address
            "filled_relay_data_recipient",              # Final recipient of funds
            "filled_relay_data_message_hash",           # Hash of cross-chain message (if any)
            
            # ═══════════════════════════════════════════════════════════════════
            # FUNDS_DEPOSITED (FundsDeposited V3) - Bridge deposit on origin chain
            # ═══════════════════════════════════════════════════════════════════
            # --- Topics (indexed) ---
            "topic_destination_chain_id",   # Target chain for the bridge
            # topic_deposit_id              # (shared column - see FILLED_RELAY section above)
            "topic_depositor",              # Address initiating the deposit
            # --- Data (non-indexed) ---
            "funds_deposited_data_input_token",         # Token being deposited
            "funds_deposited_data_output_token",        # Token to receive on destination
            "funds_deposited_data_input_amount",        # Amount being deposited
            "funds_deposited_data_output_amount",       # Expected output amount
            "funds_deposited_data_quote_timestamp",     # Timestamp when quote was obtained
            "funds_deposited_data_fill_deadline",       # Deadline for fill to occur
            "funds_deposited_data_exclusivity_deadline",# Deadline for exclusive relayer
            "funds_deposited_data_recipient",           # Who receives funds on destination
            "funds_deposited_data_exclusive_relayer",   # Exclusive relayer (if any)
            
            # ═══════════════════════════════════════════════════════════════════
            # EXECUTED_RELAYER_REFUND_ROOT - Refund merkle root executed
            # ═══════════════════════════════════════════════════════════════════
            # --- Topics (indexed) ---
            "topic_chain_id",               # Chain where refund is executed
            "topic_root_bundle_id",         # ID of the merkle root bundle
            "topic_leaf_id",                # Leaf index in the merkle tree
            # --- Data (non-indexed) ---
            "executed_refund_root_data_amount_to_return",   # Amount being refunded
            "executed_refund_root_data_chain_id",           # Chain ID (duplicates topic)
            "executed_refund_root_data_l2_token_address",   # Token address for the refund
        ])
        
        # STEP 6: Execute the query plan (all optimizations applied here)
        .collect()
    )

    print(f"✓ Transformed in {time.time() - time_start:.2f}s")
    save_to_parquet(result, OUTPUT_FILE)
    return result


if __name__ == "__main__":
    
    # transform the data
    transform_data()

    # read the data from the output file
    df = pl.read_parquet(OUTPUT_FILE)

    # write the first 5 rows to a csv file
    df.head(5).write_csv("first_5_rows.csv")

    # print the first 5 rows
    print(df.head(5))
