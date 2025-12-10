import os
from io import StringIO
from typing import Optional
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

import polars as pl

# Load environment variables from .env so local runs work without exporting.
load_dotenv()

# Project root directory (3 levels up from this script)
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

def get_db_connection(
    host: Optional[str] = None,
    port: Optional[str] = None,
    dbname: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
):
    """
    Open a Postgres connection using psycopg2.

    - Parameters override environment variables; if omitted, we read env vars.
    - Env vars: POSTGRES_HOST (default "localhost"), POSTGRES_PORT (default "5432"),
      POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD.
    - Autocommit stays OFF so callers control commit/rollback explicitly.
    """
    conn = psycopg2.connect(
        host=host or os.getenv("POSTGRES_HOST", "localhost"),
        port=port or os.getenv("POSTGRES_PORT", "5432"),
        database=dbname or os.getenv("POSTGRES_DB", "across_analytics"),
        user=user or os.getenv("POSTGRES_USER"),
        password=password or os.getenv("POSTGRES_PASSWORD"),
    )

    conn.autocommit = False
    return conn


def create_raw_table(conn, table_name: str="across_bridge_logs_raw") -> None:
    """
    Create the raw (bronze) landing table.
    """

    # first create schema if not exists
    # then create table if not exists
    create_sql = f"""
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE TABLE IF NOT EXISTS raw.{table_name} (
        timestamp_datetime          TEXT,
        transactionHash             TEXT,
        topic_0                     TEXT,
        topic_origin_chain_id       TEXT,
        topic_deposit_id            TEXT,
        topic_relayer               TEXT,
        filled_relay_data_input_token   TEXT,
        filled_relay_data_output_token  TEXT,
        filled_relay_data_input_amount  TEXT,
        filled_relay_data_output_amount TEXT,
        filled_relay_data_repayment_chain_id TEXT,
        filled_relay_data_exclusive_relayer  TEXT,
        filled_relay_data_depositor TEXT,
        filled_relay_data_recipient TEXT,
        topic_destination_chain_id  TEXT,
        topic_depositor             TEXT,
        funds_deposited_data_input_token  TEXT,
        funds_deposited_data_output_token TEXT,
        funds_deposited_data_input_amount TEXT,
        funds_deposited_data_output_amount TEXT,
        funds_deposited_data_recipient    TEXT,
        topic_chain_id              TEXT,
        amount_to_return            TEXT,
        l2_token_address            TEXT,
        refund_amounts              TEXT,
        refund_addresses            TEXT,
        refund_count                TEXT,
        -- Lineage metadata
        blockchain                  TEXT,
        api_extracted_start_date    TEXT,
        api_extracted_end_date      TEXT,
        source_file                 TEXT,
        loaded_at                   TIMESTAMP DEFAULT NOW()
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_sql)
    conn.commit()

    return table_name


def load_parquet_to_raw_copy(conn, parquet_path: str, table_name: str = "across_bridge_logs_raw") -> int:
    """
    COPY-based file loader: stream Parquet -> CSV -> Postgres COPY STDIN
    """
    # read parquet file into polars dataframe
    df = pl.read_parquet(parquet_path)
    if df.is_empty():
        print("No rows to load; Parquet is empty.")
        return 0

    columns = [
        "timestamp_datetime",
        "transactionHash",
        "topic_0",
        "topic_origin_chain_id",
        "topic_deposit_id",
        "topic_relayer",
        "filled_relay_data_input_token",
        "filled_relay_data_output_token",
        "filled_relay_data_input_amount",
        "filled_relay_data_output_amount",
        "filled_relay_data_repayment_chain_id",
        "filled_relay_data_exclusive_relayer",
        "filled_relay_data_depositor",
        "filled_relay_data_recipient",
        "topic_destination_chain_id",
        "topic_depositor",
        "funds_deposited_data_input_token",
        "funds_deposited_data_output_token",
        "funds_deposited_data_input_amount",
        "funds_deposited_data_output_amount",
        "funds_deposited_data_recipient",
        "topic_chain_id",
        "amount_to_return",
        "l2_token_address",
        "refund_amounts",
        "refund_addresses",
        "refund_count",
        "blockchain",
        "api_extracted_start_date",
        "api_extracted_end_date",
        "source_file",
    ]

    # Add blockchain, api_extracted_start_date, api_extracted_end_date columns with the values from the parquet file
    df = df.with_columns(pl.lit(os.path.basename(parquet_path).split("_")[1]).alias("blockchain"))
    df = df.with_columns(pl.lit(os.path.basename(parquet_path).split("_")[2].split(".")[0]).alias("api_extracted_start_date"))
    df = df.with_columns(pl.lit(os.path.basename(parquet_path).split("_")[4].split(".")[0]).alias("api_extracted_end_date"))

    # Add source_file column with the name of corresponding parquet file
    df = df.with_columns(pl.lit(os.path.basename(parquet_path)).alias("source_file"))

    # Ensure all columns exist; missing ones become NULL
    for col in columns:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))
    df = df.select(columns)

    # Stream DataFrame to CSV in-memory for COPY
    buffer = StringIO()
    df.write_csv(buffer, include_header=False)
    buffer.seek(0)

    copy_sql = f"""
        COPY raw.{table_name} ({", ".join(columns)})
        FROM STDIN WITH (FORMAT CSV)
    """

    with conn.cursor() as cur:
        cur.copy_expert(sql=copy_sql, file=buffer)
    conn.commit()

    inserted = len(df)
    print(f"COPY inserted {inserted} rows from {parquet_path} into raw.{table_name}")
    return inserted


def check_table_loaded(conn, table_name: str = "across_bridge_logs_raw", source_file: Optional[str] = None) -> dict:
    """
    Check if table was loaded and return status information.
    
    Args:
        conn: psycopg2 connection
        table_name: Name of the table to check (default: across_bridge_logs_raw)
        source_file: Optional filename to check if it was already loaded
    
    Returns:
        - exists: bool (does table exist?)
        - row_count: int (total rows, 0 if table doesn't exist)
        - file_loaded: bool (was source_file loaded? None if source_file not provided)
    """
    result = {
        "exists": False,
        "row_count": 0,
        "file_loaded": None
    }
    
    with conn.cursor() as cur:
        # Check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'raw' 
                AND table_name = %s
            );
        """, (table_name,))
        table_exists = cur.fetchone()[0]
        result["exists"] = table_exists
        
        if not table_exists:
            return result
        
        # Count total rows
        cur.execute(f"SELECT COUNT(*) FROM raw.{table_name};")
        result["row_count"] = cur.fetchone()[0]
        
        # Check if specific file was loaded (if provided)
        if source_file:
            cur.execute(f"""
                SELECT COUNT(*) > 0 
                FROM raw.{table_name} 
                WHERE source_file = %s;
            """, (source_file,))
            result["file_loaded"] = cur.fetchone()[0]
    
    return result



def load_all_parquet_files_to_raw(
    processed_dir: Optional[Path] = None,
    table_name: str = "across_bridge_logs_raw",
) -> int:
    """
    Airflow-friendly wrapper to load all Parquet files in a directory into raw.

    - Makes its own DB connection, commits/rolls back, and closes cleanly.
    - Skips files already ingested (checks source_file).
    - Returns total rows inserted across all files.
    """
    if processed_dir is None:
        processed_dir = PROJECT_ROOT / "data/processed"

    parquet_files = list(Path(processed_dir).glob("*.parquet"))
    if not parquet_files:
        print(f"No parquet files found in {processed_dir}")
        return 0

    conn = get_db_connection()
    total_inserted = 0
    try:
        create_raw_table(conn, table_name)
        print(f"Table: {table_name} created")

        for parquet_file in parquet_files:
            status = check_table_loaded(
                conn,
                table_name,
                source_file=os.path.basename(parquet_file),
            )
            print(f"Table status for {parquet_file.name}: {status}")

            if not status["file_loaded"]:
                inserted = load_parquet_to_raw_copy(conn, parquet_file, table_name)
                print(f"Loaded {inserted} rows from {parquet_file} into {table_name}")
                total_inserted += inserted
            else:
                print(f"File {parquet_file.name} already loaded, skipping...")

        print(f"Total rows loaded: {total_inserted}")
        return total_inserted
    finally:
        conn.close()


if __name__ == "__main__":
    load_all_parquet_files_to_raw()