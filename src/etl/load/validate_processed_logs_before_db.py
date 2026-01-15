import polars as pl
from pathlib import Path
from typing import Tuple, List, Optional
from datetime import datetime

# Project root directory (3 levels up from this script)
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent


def validate(file_path: Path) -> Tuple[bool, Optional[str], Optional[datetime], Optional[datetime], Optional[int]]:
    """
    Validate Parquet file before loading to Bronze layer.
    
    What we check (file-level only):
    - File exists and is readable
    - Valid Parquet format
    - Has required columns
    - Not empty
    
    Args:
        file_path: Path to Parquet file
        
    Returns:
        Tuple of (is_valid, error_message, min_date, max_date, row_count)
    """
    REQUIRED_COLUMNS = ["timestamp_datetime",
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
                        "refund_count"]
    
    try:
        # Check 1: File exists
        if not file_path.exists():
            return False, f"File does not exist: {file_path}", None, None, None
        
        # Check 2: Can read as Parquet (valid format)
        try:
            df = pl.read_parquet(file_path)
        except Exception as e:
            return False, f"Cannot read Parquet file: {str(e)}", None, None, None
        
        # Check 3: Not empty
        row_count = len(df)
        if row_count == 0:
            return False, "File is empty (0 rows)", None, None, None
        
        # Check 3b: Duplicates
        if df.is_duplicated().any():
             duplicate_count = df.is_duplicated().sum()
             return False, f"Found {duplicate_count} duplicate rows", None, None, None

        # Check 4: Has required columns
        missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
        if missing_columns:
            return False, f"Missing required columns: {missing_columns}", None, None, None
        
        # check 5: check if the columns are the same as the required columns
        if df["transactionHash"].is_null().any():
            return False, "transactionHash is null", None, None, None
        if df["timestamp_datetime"].is_null().any():
            return False, "timestamp_datetime is null", None, None, None
        if df["topic_0"].is_null().any():
            return False, "topic_0 is null", None, None, None
        
        # Check 6: Amounts: non-negative
        if (df["filled_relay_data_input_amount"] < 0).any():
            return False, "filled_relay_data_input_amount is negative", None, None, None
        if (df["filled_relay_data_output_amount"] < 0).any():
            return False, "filled_relay_data_output_amount is negative", None, None, None
        if (df["funds_deposited_data_input_amount"] < 0).any():
            return False, "funds_deposited_data_input_amount is negative", None, None, None
        if (df["funds_deposited_data_output_amount"] < 0).any():
            return False, "funds_deposited_data_output_amount is negative", None, None, None
        if (df["amount_to_return"] < 0).any():
            return False, "amount_to_return is negative", None, None, None
        # refund_count is Int64, so check it separately
        if (df["refund_count"].is_not_null() & (df["refund_count"] < 0)).any():
            return False, "refund_count is negative", None, None, None

        # Check 7: Data types vs. expected types (at least high-level, e.g., amounts numeric-like, timestamps parseable).
        if df["filled_relay_data_input_amount"].is_not_null().any() and df["filled_relay_data_input_amount"].dtype != pl.Float64:
            return False, "filled_relay_data_input_amount is not a float64", None, None, None
        if df["filled_relay_data_output_amount"].is_not_null().any() and df["filled_relay_data_output_amount"].dtype != pl.Float64:
            return False, "filled_relay_data_output_amount is not a float64", None, None, None
        if df["funds_deposited_data_input_amount"].is_not_null().any() and df["funds_deposited_data_input_amount"].dtype != pl.Float64:
            return False, "funds_deposited_data_input_amount is not a float64", None, None, None
        if df["funds_deposited_data_output_amount"].is_not_null().any() and df["funds_deposited_data_output_amount"].dtype != pl.Float64:
            return False, "funds_deposited_data_output_amount is not a float64", None, None, None
        if df["amount_to_return"].is_not_null().any() and df["amount_to_return"].dtype != pl.Float64:
            return False, "amount_to_return is not a float64", None, None, None
        # refund_amounts and refund_addresses are comma-separated strings (Utf8)
        if df["refund_amounts"].is_not_null().any() and df["refund_amounts"].dtype != pl.Utf8:
            return False, "refund_amounts is not a string (Utf8)", None, None, None
        if df["refund_addresses"].is_not_null().any() and df["refund_addresses"].dtype != pl.Utf8:
            return False, "refund_addresses is not a string (Utf8)", None, None, None
        if df["refund_count"].is_not_null().any() and df["refund_count"].dtype != pl.Int64:
            return False, "refund_count is not an Int64", None, None, None

        # Check 8: Timestamps: parseable; not in scientific notation.
        if df["timestamp_datetime"].is_not_null().any() and df["timestamp_datetime"].dtype != pl.Datetime:
            return False, "timestamp_datetime is not a datetime", None, None, None

        # Check 9: Strict Address & Hash format (startswith 0x, length 42 or 66)
        # Note: refund_addresses is excluded as it is a CSV string of addresses
        address_cols = [
            "topic_relayer",
            "filled_relay_data_input_token",
            "filled_relay_data_output_token",
            "filled_relay_data_exclusive_relayer",
            "filled_relay_data_depositor",
            "filled_relay_data_recipient",
            "topic_depositor",
            "funds_deposited_data_input_token",
            "funds_deposited_data_output_token",
            "funds_deposited_data_recipient",
            "l2_token_address"
        ]

        for col in address_cols:
            if col in df.columns and df[col].is_not_null().any():
                if df[col].dtype != pl.Utf8:
                    return False, f"{col} is not a string", None, None, None
                
                # Check for 0x prefix and length 42
                bad_addr = df.filter(
                    pl.col(col).is_not_null() & 
                    (~pl.col(col).str.starts_with("0x") | (pl.col(col).str.len_chars() != 42))
                )
                if not bad_addr.is_empty():
                     return False, f"{col} contains invalid addresses (must be 0x... and 42 chars)", None, None, None

        if "transactionHash" in df.columns:
            if df["transactionHash"].is_not_null().any():
                if df["transactionHash"].dtype != pl.Utf8:
                    return False, "transactionHash is not a string", None, None, None
                
                bad_hash = df.filter(
                    pl.col("transactionHash").is_not_null() & 
                    (~pl.col("transactionHash").str.starts_with("0x") | (pl.col("transactionHash").str.len_chars() != 66))
                )
                if not bad_hash.is_empty():
                     return False, "transactionHash contains invalid hashes (must be 0x... and 66 chars)", None, None, None

        # Check 10: Gas data completeness - ensure no missing gasPrice/gasUsed for any log
        total_logs = len(df)
        gas_price_count = df["gas_price_wei"].is_not_null().sum()
        gas_used_count = df["gas_used"].is_not_null().sum()
        if gas_price_count != total_logs:
            missing_count = total_logs - gas_price_count
            return False, f"gas_price_wei has {missing_count} missing values out of {total_logs} logs", None, None, None
        if gas_used_count != total_logs:
            missing_count = total_logs - gas_used_count
            return False, f"gas_used has {missing_count} missing values out of {total_logs} logs", None, None, None

        # Get date range from timestamp_datetime column
        min_date, max_date = None, None
        if "timestamp_datetime" in df.columns:
            dates = df.select(pl.col("timestamp_datetime")).to_series()
            min_date = dates.min()
            max_date = dates.max()

        # All checks passed
        return True, None, min_date, max_date, row_count
        
    except Exception as e:
        # Catch any unexpected errors
        return False, f"Validation error: {str(e)}", None, None, None


if __name__ == "__main__":

    # LIST ALL PARQUET FILES IN THE DATA/PROCESSED FOLDER
    parquet_files = list(Path(PROJECT_ROOT, "data/processed").glob("*.parquet"))
    print(f"Found {len(parquet_files)} parquet files in the data/processed folder")
    for file in parquet_files:
        print(f"  - {file}")
    
    # Validate all parquet files before loading into database
    print("\nValidating parquet files before loading into database...")
    for file in parquet_files:
        is_valid, error, min_date, max_date, row_count = validate(file)
        if is_valid:
            print(f"✓ File is valid: {file}")
            print(f"    Rows: {row_count}")
            print(f"    Min Date: {min_date}")
            print(f"    Max Date: {max_date}")
        else:
            print(f"✗ File validation failed: {error}")
