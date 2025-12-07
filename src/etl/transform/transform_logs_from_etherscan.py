import polars as pl
from pathlib import Path
import time

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
INPUT_FILE = PROJECT_ROOT / "data/raw/etherscan_api/logs_ethereum_2025-12-03_to_2025-12-04.jsonl"
OUTPUT_FILE = PROJECT_ROOT / "data/processed/logs_ethereum_transformed.parquet"

# Event signatures (keccak256 hashes)
FILLED_RELAY = "0x44b559f101f8fbcc8a0ea43fa91a05a729a5ea6e14a7c75aa750374690137208"
FUNDS_DEPOSITED = "0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3"
EXECUTED_RELAYER_REFUND_ROOT = "0xf4ad92585b1bc117fbdd644990adf0827bc4c95baeae8a23322af807b6d0020e"


def hex_to_int(hex_col: pl.Expr) -> pl.Expr:
    """Convert hex string to integer. Returns null if value too large."""
    return hex_col.str.replace("0x", "").str.to_integer(base=16, strict=False)

def hex_to_address(hex_col: pl.Expr) -> pl.Expr:
    """Extract address from 32-byte padded hex (last 40 chars)."""
    return pl.lit("0x") + hex_col.str.slice(-40)

def timestamp_to_datetime(col: pl.Expr) -> pl.Expr:
    """Convert hex timestamp to datetime (truncated to minutes)."""
    return pl.from_epoch(hex_to_int(col), time_unit="s").dt.truncate("1m")

def save_to_parquet(df: pl.DataFrame, output_path: Path) -> None:
    """Save DataFrame to Parquet."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)
    print(f"✓ Saved {len(df):,} rows to: {output_path}")


def decode_topics(topic0: pl.Expr) -> pl.Expr:
    """Decode topics into a struct based on event type."""
    
    # FILLED_RELAY: origin_chain_id (int), deposit_id (int), relayer (address)
    filled_relay_struct = pl.struct([
        hex_to_int(pl.col("topic_1")).alias("origin_chain_id"),
        hex_to_int(pl.col("topic_2")).alias("deposit_id"),
        hex_to_address(pl.col("topic_3")).alias("relayer"),
    ])
    
    # EXECUTED_RELAYER_REFUND_ROOT: chain_id (int), root_bundle_id (int), caller (address)
    executed_relayer_refund_root_struct = pl.struct([
        hex_to_int(pl.col("topic_1")).alias("chain_id"),
        hex_to_int(pl.col("topic_2")).alias("root_bundle_id"),
        hex_to_int(pl.col("topic_3")).alias("leaf_id"),
    ])
    
    # FUNDS_DEPOSITED: dest_chain_id (int), deposit_id (int), quote_timestamp (int)
    funds_deposited_struct = pl.struct([
        hex_to_int(pl.col("topic_1")).alias("destination_chain_id"),
        hex_to_int(pl.col("topic_2")).alias("deposit_id"),
        hex_to_address(pl.col("topic_3")).alias("depositor"),
    ])
    
    return (
        pl.when(topic0 == FILLED_RELAY).then(filled_relay_struct)
        .when(topic0 == EXECUTED_RELAYER_REFUND_ROOT).then(executed_relayer_refund_root_struct)
        .when(topic0 == FUNDS_DEPOSITED).then(funds_deposited_struct)
        .otherwise(None)
        .alias("decoded_topics")
    )


def transform_data() -> pl.DataFrame:
    """Transform raw logs: extract topics, decode based on event type."""
    
    time_start = time.time()
    
    result = (
        pl.scan_ndjson(INPUT_FILE) # scan the input file into a polars dataframe
        
        # Extract topics list into columns
        # Use lambda to name struct fields, with upper_bound=4 since we always have 4 topics
        # The lambda receives the index (0, 1, 2, 3) and returns the field name
        .with_columns([
            pl.col("topics")
            .list.to_struct(fields=["topic_0", "topic_1", "topic_2", "topic_3"])
            .alias("topics_struct")
        ])
        .unnest("topics_struct")
        
        # Decode and transform
        .with_columns([
            timestamp_to_datetime(pl.col("timeStamp")).alias("timestamp_datetime"),
            hex_to_int(pl.col("timeStamp")).alias("timestamp_unix"),
            hex_to_int(pl.col("blockNumber")).alias("block_number_int"),
            hex_to_int(pl.col("gasPrice")).alias("gas_price_wei"),
            hex_to_int(pl.col("gasUsed")).alias("gas_used"),
            decode_topics(pl.col("topic_0")), # decode the topics based on the topic0
        ])
        .unnest("decoded_topics")
        
        # Select final columns
        .select([
            "timestamp_datetime",
            "transactionHash",
            "topic_0",                  # Keep event signature (useful for filtering by event type)
            "origin_chain_id",          # DECODED: int from topic_1 (for FILLED_RELAY)
            "deposit_id",               # DECODED: int from topic_2 (for FILLED_RELAY / FUNDS_DEPOSITED)
            "relayer",                  # DECODED: address from topic_3 (for FILLED_RELAY)
            "destination_chain_id",     # DECODED: int from topic_1 (for FUNDS_DEPOSITED)
            "depositor",                # DECODED: address from topic_3 (for FUNDS_DEPOSITED)
            "chain_id",                 # DECODED: int from topic_1 (for EXECUTED_RELAYER_REFUND_ROOT)
            "root_bundle_id",           # DECODED: int from topic_2 (for EXECUTED_RELAYER_REFUND_ROOT)
            "leaf_id",                  # DECODED: int from topic_3 (for EXECUTED_RELAYER_REFUND_ROOT)
            ])
        .collect()
    )

    print(f"Time taken: {time.time() - time_start:.2f}s")
    save_to_parquet(result, OUTPUT_FILE)
    return result


if __name__ == "__main__":
    transform_data()
    df = pl.read_parquet(OUTPUT_FILE)
    df.tail(5).write_csv("first_5_rows.csv")
    print(df.tail(5))
