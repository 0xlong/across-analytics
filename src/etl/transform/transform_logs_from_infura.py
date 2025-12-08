import polars as pl
from pathlib import Path
import time
from eth_abi import decode as abi_decode  # For decoding dynamic arrays in event data


# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

CHAIN_NAME = "bsc"

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

# Default input/output paths (can be overridden by calling transform_data() directly)
INPUT_FILE = PROJECT_ROOT / f"data/raw/infura_api/logs_{CHAIN_NAME}_2025-12-03_to_2025-12-04_with_timestamps.jsonl"
OUTPUT_FILE = PROJECT_ROOT / f"data/processed/logs_{CHAIN_NAME}_transformed.parquet"

# Event signatures (keccak256 hashes) - topic[0] identifies which event was emitted
# These are IDENTICAL to Etherscan - events are the same, only the API response format differs
FILLED_RELAY = "0x44b559f101f8fbcc8a0ea43fa91a05a729a5ea6e14a7c75aa750374690137208"
FUNDS_DEPOSITED = "0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3"
EXECUTED_RELAYER_REFUND_ROOT = "0xf4ad92585b1bc117fbdd644990adf0827bc4c95baeae8a23322af807b6d0020e"


# ═══════════════════════════════════════════════════════════════════════════
# HEX CONVERSION UTILITIES
# ═══════════════════════════════════════════════════════════════════════════
def hex_to_int(hex_col: pl.Expr) -> pl.Expr:
    """
    Convert hex string (like "0x692f6f7b") to integer.
    
    Returns NULL if value exceeds Int64 max (~9.2×10^18).
    Works for block numbers, log indices, transaction indices, etc.
    """
    return hex_col.str.replace("0x", "").str.to_integer(base=16, strict=False)


def hex_to_address(hex_col: pl.Expr) -> pl.Expr:
    """
    Extract address from 32-byte padded hex topic (last 40 chars).
    
    Topics are 32 bytes (64 hex chars), but addresses are only 20 bytes (40 hex chars).
    Addresses are right-aligned in the topic, so we take the last 40 characters.
    
    Example:
        Input:  "0x000000000000000000000000394311a6aaa0d8e3411d8b62de4578d41322d1bd"
        Output: "0x394311a6aaa0d8e3411d8b62de4578d41322d1bd"
    """
    return pl.lit("0x") + hex_col.str.slice(-40)


def save_to_parquet(df: pl.DataFrame, output_path: Path) -> None:
    """
    Save DataFrame to Parquet format with automatic directory creation.
    
    Parquet format is chosen for:
    - Columnar storage (efficient for analytics queries)
    - Compression (typically 5-10x smaller than JSON)
    - Type preservation (no re-parsing needed)
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)
    print(f"✓ Saved {len(df):,} rows to: {output_path}")


# ═══════════════════════════════════════════════════════════════════════════
# TOPICS DECODING
# ═══════════════════════════════════════════════════════════════════════════
def decode_topics(topic0: pl.Expr) -> pl.Expr:
    """
    Decode indexed topics (topic_1, topic_2, topic_3) based on event type.
    
    Each event type uses topics differently:
    ─────────────────────────────────────────────────────────────────────────
    | Event Type               | topic_1         | topic_2       | topic_3     |
    |--------------------------|-----------------|---------------|-------------|
    | FILLED_RELAY             | origin_chain_id | deposit_id    | relayer     |
    | FUNDS_DEPOSITED          | dest_chain_id   | deposit_id    | depositor   |
    | EXECUTED_RELAYER_REFUND  | chain_id        | root_bundle_id| leaf_id     |
    ─────────────────────────────────────────────────────────────────────────
    
    Returns a struct with all possible fields (nulls where not applicable).
    """
    # FILLED_RELAY: Track which chain sent funds, which deposit was filled, who relayed
    filled_relay_struct = pl.struct([
        hex_to_int(pl.col("topic_1")).alias("topic_origin_chain_id"),
        hex_to_int(pl.col("topic_2")).alias("topic_deposit_id"),
        hex_to_address(pl.col("topic_3")).alias("topic_relayer"),
    ])
    
    # EXECUTED_RELAYER_REFUND_ROOT: Track merkle tree execution details
    executed_refund_struct = pl.struct([
        hex_to_int(pl.col("topic_1")).alias("topic_chain_id"),
        hex_to_int(pl.col("topic_2")).alias("topic_root_bundle_id"),
        hex_to_int(pl.col("topic_3")).alias("topic_leaf_id"),
    ])
    
    # FUNDS_DEPOSITED: Track where funds are going, which deposit, who's depositing
    funds_deposited_struct = pl.struct([
        hex_to_int(pl.col("topic_1")).alias("topic_destination_chain_id"),
        hex_to_int(pl.col("topic_2")).alias("topic_deposit_id"),
        hex_to_address(pl.col("topic_3")).alias("topic_depositor"),
    ])
    
    # Route to correct struct based on event signature
    return (
        pl.when(topic0 == FILLED_RELAY).then(filled_relay_struct)
        .when(topic0 == EXECUTED_RELAYER_REFUND_ROOT).then(executed_refund_struct)
        .when(topic0 == FUNDS_DEPOSITED).then(funds_deposited_struct)
        .otherwise(None)
        .alias("decoded_topics")
    )


# ═══════════════════════════════════════════════════════════════════════════
# SLOT EXTRACTORS - Atomic helpers for decoding the 'data' field
# ═══════════════════════════════════════════════════════════════════════════
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
    
    Args:
        data_col: Column containing hex data string (starts with "0x")
        slot: Which 32-byte slot to extract (0-indexed)
    
    Returns:
        Float64 expression representing the decoded value
    """
    # Calculate byte offset: skip "0x" prefix (2 chars), then each slot is 64 hex chars
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
    
    Address layout in a 32-byte slot (64 hex chars):
    [000000000000000000000000][a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48]
     ↑ 24 zeros (left-padding)  ↑ 40 chars (actual address, right-aligned)
    
    Args:
        data_col: Column containing hex data string (starts with "0x")
        slot: Which 32-byte slot to extract (0-indexed)
    
    Returns:
        String expression with "0x" + 40-char address
    """
    offset = 2 + (slot * 64)
    # Skip 24-char left-padding, take the 40-char address
    return pl.lit("0x") + data_col.str.slice(offset + 24, 40)


def _slot_as_bytes32(data_col: pl.Expr, slot: int) -> pl.Expr:
    """
    Extract a 32-byte slot as raw hex string (for hashes, identifiers).
    
    No interpretation - just extract the full 64 hex chars and prefix with "0x".
    Used for message hashes, merkle roots, and other opaque identifiers.
    
    Args:
        data_col: Column containing hex data string (starts with "0x")
        slot: Which 32-byte slot to extract (0-indexed)
    
    Returns:
        String expression with "0x" + 64-char hex value
    """
    offset = 2 + (slot * 64)
    return pl.lit("0x") + data_col.str.slice(offset, 64)


# ═══════════════════════════════════════════════════════════════════════════
# EVENT STRUCT BUILDERS - Build Polars structs for each event type's data field
# ═══════════════════════════════════════════════════════════════════════════
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
    - topic_1 = origin_chain_id (where funds came from)
    - topic_2 = deposit_id (matching key for deposit↔fill)
    - topic_3 = relayer (who provided liquidity)
    """
    return pl.struct([
        # Token information - what was bridged
        _slot_as_address(data_col, 0).alias("filled_relay_data_input_token"),
        _slot_as_address(data_col, 1).alias("filled_relay_data_output_token"),
        
        # Amounts - how much was bridged (Float64 handles large token amounts)
        _slot_as_int(data_col, 2).alias("filled_relay_data_input_amount"),
        _slot_as_int(data_col, 3).alias("filled_relay_data_output_amount"),
        
        # Chain and timing
        _slot_as_int(data_col, 4).alias("filled_relay_data_repayment_chain_id"),
        _slot_as_int(data_col, 5).alias("filled_relay_data_fill_deadline"),
        _slot_as_int(data_col, 6).alias("filled_relay_data_exclusivity_deadline"),
        
        # Key addresses - who's involved in this relay
        _slot_as_address(data_col, 7).alias("filled_relay_data_exclusive_relayer"),
        _slot_as_address(data_col, 8).alias("filled_relay_data_depositor"),
        _slot_as_address(data_col, 9).alias("filled_relay_data_recipient"),
        
        # Cross-chain message (if any)
        _slot_as_bytes32(data_col, 10).alias("filled_relay_data_message_hash"),
    ])


def _build_funds_deposited_struct(data_col: pl.Expr) -> pl.Expr:
    """
    Build decoded struct for FundsDeposited V3 event.
    
    This event fires when a user deposits funds to bridge to another chain.
    It's the "request" side of a bridge transaction.
    
    Data Field Layout:
    ─────┬─────────────────────┬─────────────────────────────────────────────────
    Slot | Field               | Type
    ─────┼─────────────────────┼─────────────────────────────────────────────────
     0   | input_token         | address - Token being deposited
     1   | output_token        | address - Token to receive on destination
     2   | input_amount        | uint256 - Amount being deposited
     3   | output_amount       | uint256 - Expected amount on destination
     4   | quote_timestamp     | uint32  - When the quote was generated
     5   | fill_deadline       | uint32  - Latest time to fill this deposit
     6   | exclusivity_deadline| uint32  - Exclusive relayer window end
     7   | recipient           | address - Who receives on destination
     8   | exclusive_relayer   | address - Address with exclusive fill rights
     9   | message_offset      | (dynamic pointer - skip for now)
    ─────┴─────────────────────┴─────────────────────────────────────────────────
    
    Indexed params (from topics):
    - topic_1 = destination_chain_id
    - topic_2 = deposit_id
    - topic_3 = depositor
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
        uint256 amountToReturn,      # Slot 0: static - total refund amount
        uint256[] refundAmounts,     # Slot 1: OFFSET POINTER → tail section
        address l2TokenAddress,      # Slot 2: static - token being refunded
        address[] refundAddresses,   # Slot 3: OFFSET POINTER → tail section  
        bool deferredRefunds,        # Slot 4: static - was this deferred?
        address caller               # Slot 5: static - who executed
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
    }
    
    try:
        # Guard against None input (can happen with skip_nulls=False)
        if data_hex is None:
            return NULL_STRUCT
            
        # Convert hex string to bytes (remove '0x' prefix)
        data_bytes = bytes.fromhex(data_hex[2:])
        
        # Define ABI types for non-indexed params (in declaration order)
        # eth_abi handles offset pointers and tail section parsing automatically
        types = [
            'uint256',    # amountToReturn
            'uint256[]',  # refundAmounts (dynamic array)
            'address',    # l2TokenAddress
            'address[]',  # refundAddresses (dynamic array)
            'bool',       # deferredRefunds
            'address'     # caller
        ]
        
        # Decode all fields in one call
        # The library returns arrays as Python tuples
        (
            amount_to_return,
            refund_amounts,      # Tuple of uint256 values
            l2_token_address,
            refund_addresses,    # Tuple of address strings
            deferred_refunds,    # Not used but must be captured
            caller               # Not used but must be captured
        ) = abi_decode(types, data_bytes)
        
        # Return as dictionary for Polars struct conversion
        # Arrays are stored as comma-separated strings (Polars-friendly format)
        return {
            "amount_to_return": float(amount_to_return),        # Float64 for large values
            "l2_token_address": l2_token_address,               # Checksum address string
            "refund_amounts": ",".join(str(a) for a in refund_amounts),    # "100,200,300"
            "refund_addresses": ",".join(refund_addresses),                 # "0xAAA,0xBBB"
            "refund_count": len(refund_amounts),                # Number of refunds in this leaf
        }
    except Exception:
        # Return null struct for non-ExecutedRelayerRefundRoot events
        # map_elements applies to ALL rows; the when/then filter happens AFTER
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
    return data_col.map_elements(
        _decode_executed_refund_data,
        return_dtype=pl.Struct([
            pl.Field("amount_to_return", pl.Float64),
            pl.Field("l2_token_address", pl.Utf8),
            pl.Field("refund_amounts", pl.Utf8),        # Comma-separated string
            pl.Field("refund_addresses", pl.Utf8),      # Comma-separated string
            pl.Field("refund_count", pl.Int64),         # Count of refunds
        ])
    )


def decode_data(data_col: pl.Expr, topic0_col: pl.Expr) -> pl.Expr:
    """
    Decode event 'data' field based on event signature (topic0).
    
    This is the MAIN dispatcher for data field decoding:
    1. Checks topic0 to identify event type
    2. Routes to appropriate struct builder
    3. Returns struct with decoded fields (or NULL for unknown events)
    
    Args:
        data_col: Column containing the hex data string
        topic0_col: Column containing topic0 (event signature)
    
    Returns:
        Struct expression with decoded fields for the event type
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


# ═══════════════════════════════════════════════════════════════════════════
# MAIN TRANSFORMATION PIPELINE
# ═══════════════════════════════════════════════════════════════════════════
def transform_data(
    input_file: Path = INPUT_FILE,
    output_file: Path = OUTPUT_FILE
) -> pl.DataFrame:
    """
    Transform raw Infura event logs into structured, typed columns.
    
    Pipeline steps:
    1. Scan JSONL file (lazy - no data loaded yet)
    2. Extract topics list into individual columns
    3. Convert hex values to proper types (block number, log index, etc.)
    4. Decode topics based on event type
    5. Decode data field based on event type
    6. Select and rename final columns
    7. Execute (collect) and save to Parquet
    
    KEY DIFFERENCE FROM ETHERSCAN:
    - No timestamp conversion (Infura logs don't include block timestamps)
    - No gas price/used fields (not in Infura log response)
    - Block number used as primary ordering reference
    
    Args:
        input_file: Path to input JSONL file from Infura API
        output_file: Path for output Parquet file
    
    Returns:
        DataFrame with decoded event fields
    """
    time_start = time.time()
    
    print(f"Processing: {input_file}")
    
    result = (
        # ═══════════════════════════════════════════════════════════════════
        # STEP 1: Create LazyFrame (no data loaded yet - just a query plan)
        # ═══════════════════════════════════════════════════════════════════
        pl.scan_ndjson(input_file).head(200)
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 2: Extract topics[0..3] into separate columns
        # The topics list always has 4 elements for our events
        # ═══════════════════════════════════════════════════════════════════
        .with_columns([
            pl.col("topics")
            .list.to_struct(fields=["topic_0", "topic_1", "topic_2", "topic_3"])
            .alias("topics_struct")
        ])
        .unnest("topics_struct")
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 3: Decode everything in parallel
        # - Convert hex block numbers/indices to integers
        # - Decode topics based on event type
        # - Decode data field based on event type
        # 
        # NOTE: Unlike Etherscan, Infura logs don't include:
        #   - timeStamp (block timestamp)
        #   - gasPrice
        #   - gasUsed
        # We use block_number as the primary ordering reference.
        # ═══════════════════════════════════════════════════════════════════
        .with_columns([
            # Block and transaction metadata (hex → int)
            hex_to_int(pl.col("blockNumber")).alias("block_number_int"),
            hex_to_int(pl.col("logIndex")).alias("log_index_int"),
            hex_to_int(pl.col("transactionIndex")).alias("transaction_index_int"),
            
            # Decode topics (indexed parameters)
            decode_topics(pl.col("topic_0")),
            
            # Decode data field (non-indexed parameters)
            decode_data(pl.col("data"), pl.col("topic_0")),
        ])
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 4: Unnest decoded structs into individual columns
        # ═══════════════════════════════════════════════════════════════════
        .unnest("decoded_topics")
        .unnest("decoded_data")
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 5: Select final columns for CAPITAL FLOW ANALYSIS
        # ─────────────────────────────────────────────────────────────────────
        # SKIPPED columns (commented out - can re-enable if needed):
        #   - filled_relay_data_fill_deadline          → timing constraint
        #   - filled_relay_data_exclusivity_deadline   → exclusive relayer window
        #   - filled_relay_data_message_hash           → cross-chain message hash
        #   - topic_root_bundle_id, topic_leaf_id      → merkle tree internals
        #   - removed                                  → chain reorg indicator
        # ─────────────────────────────────────────────────────────────────────
        .select([
            # ═══════════════════════════════════════════════════════════════════
            # CORE IDENTITY (common to all events)
            # NOTE: No timestamp available from Infura - use block_number for ordering
            # ═══════════════════════════════════════════════════════════════════
            "block_datetime",               # Block timestamp
            "block_number_int",             # Block number
            "log_index_int",                # Position within block
            "transaction_index_int",        # Position within transaction
            "transactionHash",              # Transaction hash
            "topic_0",                      # Event signature
            "address",                      # Contract that emitted the event
            "data",                         # Event data
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
            "filled_relay_data_exclusive_relayer",      # Address with exclusive fill rights
            "filled_relay_data_depositor",              # Who initiated the bridge
            "filled_relay_data_recipient",              # Who received the funds
            
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
            # --- Data (non-indexed) ---
            "amount_to_return",             # Total capital returned in this batch
            "l2_token_address",             # Token being refunded
            "refund_amounts",               # Individual refund amounts (CSV string)
            "refund_addresses",             # Who receives each refund (CSV string)
            "refund_count",                 # Number of relayers refunded
        ])
        
        # ═══════════════════════════════════════════════════════════════════
        # STEP 6: Execute the query plan (all optimizations applied here)
        # ═══════════════════════════════════════════════════════════════════
        .collect()
    )

    print(f"✓ Transformed {len(result):,} rows in {time.time() - time_start:.2f}s")
    save_to_parquet(result, output_file)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    # Transform the data using default input/output paths
    df = transform_data()

    # Preview: write first 5 rows to CSV for inspection
    preview_path = Path(__file__).parent / "first_5_rows_infura.csv"
    df.head(5).write_csv(preview_path)
    print(f"✓ Preview saved to: {preview_path}")
    
    # Print sample output
    print("\n" + "═" * 80)
    print("SAMPLE OUTPUT (first 5 rows):")
    print("═" * 80)
    print(df.head(5))
