"""
Post-load validation: Compare parquet files with database tables.
Ensures all data was correctly loaded to PostgreSQL.
"""
import os
from pathlib import Path
from typing import Dict, Tuple
import polars as pl
import psycopg2
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent


def get_db_connection():
    """Open a Postgres connection."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "across_analytics"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


# Critical columns that should never have unexpected NULLs
CRITICAL_COLUMNS = [
    "transactionhash",
    "timestamp_datetime", 
    "topic_0",
    "gas_price_wei",
    "gas_used",
]


def get_parquet_stats(processed_dir: Path) -> Dict[str, dict]:
    """Get row counts and stats from parquet files."""
    stats = {}
    for parquet_file in sorted(processed_dir.glob("*.parquet")):
        chain = parquet_file.stem.split("_")[1]  # logs_arbitrum_... -> arbitrum
        table_name = f"{chain}_logs_processed"
        
        df = pl.read_parquet(parquet_file)
        
        # Count NULLs for critical columns (use lowercase for consistency)
        null_counts = {}
        for col in CRITICAL_COLUMNS:
            # Parquet uses camelCase, DB uses lowercase
            pq_col = "transactionHash" if col == "transactionhash" else col
            if pq_col in df.columns:
                null_counts[col] = df[pq_col].null_count()
        
        stats[table_name] = {
            "source_file": parquet_file.name,
            "row_count": len(df),
            "min_ts": df["timestamp_datetime"].min(),
            "max_ts": df["timestamp_datetime"].max(),
            "unique_tx": df["transactionHash"].n_unique(),
            "null_counts": null_counts,
        }
    return stats


def get_db_stats(conn) -> Dict[str, dict]:
    """Get row counts and stats from database tables."""
    stats = {}
    
    # First get list of all tables in raw schema
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'raw' AND table_name LIKE '%_logs_processed'
        """)
        tables = [row[0] for row in cur.fetchall()]
    
    for table_name in tables:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT 
                    COUNT(*) AS row_count,
                    MIN(timestamp_datetime) AS min_ts,
                    MAX(timestamp_datetime) AS max_ts,
                    COUNT(DISTINCT transactionhash) AS unique_tx
                FROM raw.{table_name}
            """)
            row = cur.fetchone()
            
            # Count NULLs for critical columns
            null_counts = {}
            for col in CRITICAL_COLUMNS:
                cur.execute(f"""
                    SELECT COUNT(*) FROM raw.{table_name} WHERE {col} IS NULL
                """)
                null_counts[col] = cur.fetchone()[0]
            
            stats[table_name] = {
                "row_count": row[0],
                "min_ts": row[1],
                "max_ts": row[2],
                "unique_tx": row[3],
                "null_counts": null_counts,
            }
    return stats


def validate_load() -> Tuple[bool, list]:
    """
    Compare parquet files with database tables.
    
    Returns:
        Tuple of (all_valid, list of validation messages)
    """
    processed_dir = PROJECT_ROOT / "data" / "processed"
    messages = []
    all_valid = True
    
    # Get stats from both sources
    parquet_stats = get_parquet_stats(processed_dir)
    
    conn = get_db_connection()
    try:
        db_stats = get_db_stats(conn)
    finally:
        conn.close()
    
    # Compare each table
    for table_name, pq_stat in parquet_stats.items():
        if table_name not in db_stats:
            messages.append(f"✗ {table_name}: Table not found in database!")
            all_valid = False
            continue
        
        db_stat = db_stats[table_name]
        
        # Check row counts match
        if pq_stat["row_count"] != db_stat["row_count"]:
            messages.append(
                f"✗ {table_name}: Row count mismatch! "
                f"Parquet: {pq_stat['row_count']}, DB: {db_stat['row_count']}"
            )
            all_valid = False
        else:
            messages.append(f"✓ {table_name}: {db_stat['row_count']} rows OK")
        
        # Check unique transactions match
        if pq_stat["unique_tx"] != db_stat["unique_tx"]:
            messages.append(
                f"  ⚠ Unique transactions differ: "
                f"Parquet: {pq_stat['unique_tx']}, DB: {db_stat['unique_tx']}"
            )
        
        # Check NULL counts match for critical columns
        pq_nulls = pq_stat.get("null_counts", {})
        db_nulls = db_stat.get("null_counts", {})
        for col in CRITICAL_COLUMNS:
            pq_null = pq_nulls.get(col, 0)
            db_null = db_nulls.get(col, 0)
            if pq_null != db_null:
                messages.append(
                    f"  ⚠ {table_name}.{col}: NULL count mismatch! "
                    f"Parquet: {pq_null}, DB: {db_null}"
                )
    
    # Check for tables in DB that aren't in parquet (orphaned data)
    for table_name in db_stats:
        if table_name not in parquet_stats:
            messages.append(f"⚠ {table_name}: Exists in DB but no parquet file found")
    
    return all_valid, messages


if __name__ == "__main__":
    print("=" * 60)
    print("POST-LOAD VALIDATION: Parquet vs Database")
    print("=" * 60)
    
    all_valid, messages = validate_load()
    
    for msg in messages:
        print(msg)
    
    print("=" * 60)
    if all_valid:
        print("✓ ALL VALIDATIONS PASSED")
    else:
        print("✗ SOME VALIDATIONS FAILED")
    print("=" * 60)
