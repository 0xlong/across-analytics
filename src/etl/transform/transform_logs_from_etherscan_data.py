import polars as pl  # Standard import alias for Polars (like 'pd' for pandas)
from pathlib import Path  # For cross-platform file path handling
import time

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
# Input file: JSONL format (JSON Lines - one JSON object per line)
INPUT_FILE = PROJECT_ROOT / "data/raw/etherscan_api/logs_ethereum_2025-12-03_to_2025-12-04.jsonl"

# Output file: We'll save transformed data as Parquet (efficient columnar format)
OUTPUT_FILE = PROJECT_ROOT / "data/processed/logs_ethereum_transformed.parquet"

def read_jsonl_file(file_path: Path) -> pl.DataFrame:
    """
    Read a JSONL file into a Polars DataFrame.
    
    JSONL FORMAT EXPLAINED:
    -----------------------
    Each line is a separate JSON object. Example:
    {"address": "0x5c7bcd...", "timeStamp": "0x692f6f7b", ...}
    {"address": "0x5c7bcd...", "timeStamp": "0x692f6f87", ...}
    
    This is different from regular JSON which wraps everything in an array.
    JSONL is great for streaming/appending data.
    
    Parameters:
        file_path: Path to the JSONL file
        
    Returns:
        pl.DataFrame: A Polars DataFrame with all the data
    """
    # pl.read_ndjson() reads "newline-delimited JSON" (same as JSONL)
    # This is an EAGER operation - it loads all data into memory immediately
    df = pl.read_ndjson(file_path)
    
    return df

def hex_string_to_int(hex_col: pl.Expr) -> pl.Expr:
    """
    Convert hexadecimal strings (like "0x692f6f7b") to integers.
    
    WHAT'S HAPPENING IN THE DATA:
    -----------------------------
    Ethereum/blockchain data uses hexadecimal (base-16) for numbers.
    Example: "0x692f6f7b" = 1761374075 in decimal (Unix timestamp)
    
    POLARS EXPRESSIONS EXPLAINED:
    -----------------------------
    - Expressions are LAZY: they describe WHAT to do, not do it immediately
    - Expressions are COMPOSABLE: you chain operations like .method1().method2()
    - The expression only runs when you apply it to a DataFrame
    
    Parameters:
        hex_col: A Polars expression representing a column with hex strings
        
    Returns:
        pl.Expr: An expression that converts hex to integer
    """
    # Breaking down the expression chain:
    # 1. .str.replace("0x", "")  - Remove the "0x" prefix from hex strings
    # 2. .str.to_integer(base=16) - Parse the remaining string as base-16 number
    
    return hex_col.str.replace("0x", "").str.to_integer(base=16)

def timestamp_to_datetime(timestamp_col: pl.Expr) -> pl.Expr:
    """
    Convert timestamp strings to datetime (as a reusable Expression).
    
    Parameters:
        timestamp_col: A Polars expression representing a column with timestamp strings
        
    Returns:
        pl.Expr: An expression that converts hex timestamp to datetime

    """

    return pl.from_epoch(
        hex_string_to_int(timestamp_col),   # First convert hex → integer
        time_unit="s"                       # Then integer → datetime (seconds)
    )

def save_to_parquet(df: pl.DataFrame, output_path: Path) -> None:
    """
    Save DataFrame to Parquet format.
    
    WHY PARQUET?
    ------------
    1. Columnar: Stores data by column, not by row (great for analytics)
    2. Compressed: Much smaller file size than CSV or JSON
    3. Typed: Preserves data types (datetime stays datetime, not string)
    4. Fast: Much faster to read than CSV for large files
    5. Universal: Works with Polars, Pandas, Spark, DuckDB, etc.
    
    Parameters:
        df: DataFrame to save
        output_path: Where to save the file
    """
    # Create the output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write to Parquet - simple one-liner!
    df.write_parquet(output_path)
    
    print(f"✓ Saved {len(df):,} rows to: {output_path}")



# =============================================================================
# EVENT SIGNATURES - Keccak256 hashes identifying each event type
# =============================================================================
# topic0 always contains this signature, identifying which event was emitted

FILLED_RELAY_SIGNATURE = "0x44b559f101f8fbcc8a0ea43fa91a05a729a5ea6e14a7c75aa750374690137208"
FUNDS_DEPOSITED_SIGNATURE = "0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3"
EXECUTED_REFUND_SIGNATURE = "0xf4ad92585b1bc117fbdd644990adf0827bc4c95baeae8a23322af807b6d0020e"

# =============================================================================
# SHARED SLOT DECODERS - Reusable helpers for extracting data from slots
# =============================================================================
# Each slot = 32 bytes = 64 hex characters in the data field
# data field format: "0x" + [slot0: 64 chars] + [slot1: 64 chars] + ...

def _slot_as_address(data_col: pl.Expr, slot: int) -> pl.Expr:
    """
    Extract slot as Ethereum address.
    
    Address is 20 bytes (40 hex chars), RIGHT-aligned in 32-byte slot.
    Layout: [24 zeros padding][40 char address]
    """
    offset = 2 + (slot * 64)  # Skip "0x" + previous slots
    return pl.lit("0x") + data_col.str.slice(offset + 24, 40)


def _slot_as_int(data_col: pl.Expr, slot: int) -> pl.Expr:
    """
    Extract slot as integer (uint256).
    
    Value is LEFT-padded with zeros. Parse full 64 hex chars as base-16.
    strict=False returns NULL for values exceeding Int64 range (>9.2e18).
    """
    offset = 2 + (slot * 64)
    return data_col.str.slice(offset, 64).str.to_integer(base=16, strict=False)


def _slot_as_bytes32(data_col: pl.Expr, slot: int) -> pl.Expr:
    """Extract slot as raw bytes32 hex string (for hashes, identifiers)."""
    offset = 2 + (slot * 64)
    return pl.lit("0x") + data_col.str.slice(offset, 64)



# =============================================================================
# UNIFIED EVENT DECODER - Single function, one pass, transparent structs
# =============================================================================

def decode_event(data_col: pl.Expr, topic0_col: pl.Expr) -> pl.Expr:
    """
    Decode event data based on topic0 signature.
    
    Single-pass decoder that:
    1. Checks topic0 ONCE
    2. Returns appropriate struct based on event type
    3. Returns NULL for unknown events
    
    Each struct is defined explicitly inline for full transparency.
    
    Parameters:
        data_col:   Polars expression for 'data' column (hex string)
        topic0_col: Polars expression for 'topic_0' column (event signature)
    
    Returns:
        pl.Expr: Struct with decoded fields matching the event type
    """
    
    # STRUCT: FilledRelay V3 - filled relay event
    # Slot | Field               | Type
    # ─────┼─────────────────────┼─────────
    #  0   | input_token         | address
    #  1   | output_token        | address
    #  2   | input_amount        | uint256
    #  3   | output_amount       | uint256
    #  4   | repayment_chain_id  | uint256
    #  5   | origin_chain_id     | uint256
    #  6   | deposit_id          | uint256
    #  7   | fill_deadline       | uint32
    #  8   | exclusivity_deadline| uint32
    #  9   | exclusive_relayer   | address
    #  10  | relayer             | address
    #  11  | depositor           | address
    #  12  | recipient           | address
    #  13  | message_hash        | bytes32

    filled_relay_struct = pl.struct([
        _slot_as_address(data_col, 0).alias("input_token"),
        _slot_as_address(data_col, 1).alias("output_token"),
        _slot_as_int(data_col, 2).alias("input_amount"),
        _slot_as_int(data_col, 3).alias("output_amount"),
        _slot_as_int(data_col, 4).alias("repayment_chain_id"),
        _slot_as_int(data_col, 5).alias("origin_chain_id_from_filled_relay"),
        _slot_as_int(data_col, 6).alias("deposit_id_from_filled_relay"),
        _slot_as_int(data_col, 7).alias("fill_deadline"),
        _slot_as_int(data_col, 8).alias("exclusivity_deadline"),
        _slot_as_address(data_col, 9).alias("exclusive_relayer"),
        _slot_as_address(data_col, 10).alias("relayer"),
        _slot_as_address(data_col, 11).alias("depositor"),
        _slot_as_address(data_col, 12).alias("recipient"),
        _slot_as_bytes32(data_col, 13).alias("message_hash"),
    ])
    
    # STRUCT: FundsDeposited V3 - funds deposited event
    # Slot | Field               | Type
    # ─────┼─────────────────────┼─────────
    #  0   | input_token         | address
    #  1   | output_token        | address
    #  2   | input_amount        | uint256
    #  3   | output_amount       | uint256
    #  4   | quote_timestamp     | uint32
    #  5   | fill_deadline       | uint32
    #  6   | exclusivity_deadline| uint32
    #  7   | recipient           | address
    #  8   | exclusive_relayer   | address
    #  9   | message             | bytes (dynamic - offset pointer, skipped)
    
    funds_deposited_struct = pl.struct([
        _slot_as_address(data_col, 0).alias("input_token"),
        _slot_as_address(data_col, 1).alias("output_token"),
        _slot_as_int(data_col, 2).alias("input_amount"),
        _slot_as_int(data_col, 3).alias("output_amount"),
        _slot_as_int(data_col, 4).alias("quote_timestamp"),
        _slot_as_int(data_col, 5).alias("fill_deadline"),
        _slot_as_int(data_col, 6).alias("exclusivity_deadline"),
        _slot_as_address(data_col, 7).alias("recipient"),
        _slot_as_address(data_col, 8).alias("exclusive_relayer"),
        # message (bytes) at slot 9 is dynamic - skipped
    ])
    
    # STRUCT: ExecutedRelayerRefundRoot - executed relayer refund root event
    # Slot | Field               | Type
    # ─────┼─────────────────────┼─────────
    #  0   | amount_to_return    | uint256
    #  1   | chain_id            | uint256
    #  2   | refund_amounts_ptr  | offset to uint256[] array data
    #  3   | l2_token_address    | address
    #  4   | refund_addresses_ptr| offset to address[] array data
    #  5   | refund_amounts      | uint256[]

    executed_refund_struct = pl.struct([
        _slot_as_int(data_col, 0).alias("amount_to_return"),      # Total refund amount
        _slot_as_int(data_col, 1).alias("chain_id"),              # Chain for refund
        _slot_as_address(data_col, 3).alias("l2_token_address"),  # Token being refunded
        # refund_amounts (uint256[]) - dynamic, requires offset decoding
        # refund_addresses (address[]) - dynamic, requires offset decoding
    ])
    
    # SINGLE-PASS CONDITIONAL: If-else statement to check topic0 and choose the struct
    return (
        pl.when(topic0_col == FILLED_RELAY_SIGNATURE)
            .then(filled_relay_struct)
        .when(topic0_col == FUNDS_DEPOSITED_SIGNATURE)
            .then(funds_deposited_struct)
        .when(topic0_col == EXECUTED_REFUND_SIGNATURE)
            .then(executed_refund_struct)
        .otherwise(pl.lit(None))  # Unknown event → NULL
    )


# Main transformation function
def transform_data() -> pl.DataFrame:
    """
    LAZY APPROACH: Build a query plan first, execute all at once at the end.
    
    Timeline:
    ---------
    1. scan_ndjson() → Just creates a "recipe", NO data loaded
    2. with_columns() → Adds to the recipe, NO execution
    3. with_columns() → Adds to the recipe, NO execution  
    4. select() → Adds to the recipe, NO execution
    5. collect() → NOW everything runs in one optimized pass!
    
    Polars optimizes the entire pipeline before running it.
    """
    time_start = time.time()
    # STEP 1: Create a LazyFrame - NO data loaded yet!
    # scan_ndjson returns a LazyFrame, not a DataFrame
    lazy_df = pl.scan_ndjson(INPUT_FILE)  # ← Just a plan, no data yet
    
    # STEP 2-4: Chain transformations - still NO execution!
    # We're just building a query plan (like writing SQL without running it)
    result = (
        lazy_df
        
        # Extract topics list into struct and unnest it into individual columns
        .with_columns([
            pl.col("topics")
            .list.to_struct(fields=["topic_0", "topic_1", "topic_2", "topic_3"])
            .alias("topics_struct")
        ])
        .unnest("topics_struct")

        .with_columns([
            # Convert hex strings to datetime, integer, and other types
            timestamp_to_datetime(pl.col("timeStamp")).alias("timestamp_datetime"),
            hex_string_to_int(pl.col("timeStamp")).alias("timestamp_unix"),
            hex_string_to_int(pl.col("blockNumber")).alias("block_number_int"),
            hex_string_to_int(pl.col("gasPrice")).alias("gas_price_wei"),
            hex_string_to_int(pl.col("gasUsed")).alias("gas_used"),
            
            decode_event(
                data_col=pl.col("data"),       # Raw hex data from event log
                topic0_col=pl.col("topic_0")   # Event signature to identify type
            ).alias("decoded_event"),
            
            # Topic decoding - just pass column names, not list indices
            decode_hex_to_int_if_event(
                pl.col("topic_1"),                      # Already extracted column
                pl.col("topic_0"),              # Check this column
                FILLED_RELAY_SIGNATURE               # Against this value
            ).alias("origin_chain_id"),
            
            decode_hex_to_int_if_event(
                pl.col("topic_2"),
                pl.col("topic_0"),
                FILLED_RELAY_SIGNATURE
            ).alias("deposit_id"),
            
            decode_hex_to_address_if_event(
                pl.col("topic_3"),
                pl.col("topic_0"),
                FILLED_RELAY_SIGNATURE
            ).alias("relayer_address"),
        ])
        .unnest("decoded_event") # unnest the decoded event struct into individual columns
        .select([
        # ═══════════════════════════════════════════════════════════════════════
        # METADATA COLUMNS (timing and identification)
        # ═══════════════════════════════════════════════════════════════════════
        "timestamp_datetime",      # datetime: 2025-12-03 00:02:55
        "transactionHash",         # unique transaction ID
        #"address",                 # contract address that emitted the log
        #"topic_0",                 # event signature (identifies event type)
        
        # ═══════════════════════════════════════════════════════════════════════
        # DECODED TOPICS (indexed event parameters, from topics[1..3])
        # ═══════════════════════════════════════════════════════════════════════
        "origin_chain_id",         # integer: topic_1 - origin chain ID
        "deposit_id",              # integer: topic_2 - unique deposit identifier  
        "relayer_address",         # string: topic_3 - relayer address
        
        "input_token",             # address: token sent by depositor
        "output_token",            # address: token received on destination chain
        "input_amount",            # uint256: amount of input_token deposited
        "output_amount",           # uint256: amount of output_token to receive
        "depositor",               # address: who initiated the deposit (FilledRelay only)
        "recipient",               # address: who receives funds on destination
        "relayer",                 # address: who filled the relay (FilledRelay only)
        ])
        
        # STEP 5: .collect() triggers execution of the ENTIRE optimized plan
        .collect()
    )

    time_end = time.time()
    print("Time taken: ", time.time() - time_start)
    save_to_parquet(result, OUTPUT_FILE)
    print(f"✓ Saved to Parquet: {OUTPUT_FILE}")
    return result


if __name__ == "__main__":
    transform_data()
    df = pl.read_parquet(OUTPUT_FILE)
    df.head(5).write_csv("first_5_rows.csv")
    print(df.head(5))
    print(df['transactionHash'].head(1).to_list())
