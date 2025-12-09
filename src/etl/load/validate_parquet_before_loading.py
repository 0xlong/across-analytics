import polars as pl
from pathlib import Path
from typing import Tuple, List, Optional

# Project root directory (3 levels up from this script)
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent


def validate_parquet_file_before_loading_into_database(file_path: Path) -> Tuple[bool, Optional[str]]:
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
        Tuple of (is_valid: bool, error_message: Optional[str])
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
            return False, f"File does not exist: {file_path}"
        
        # Check 2: Can read as Parquet (valid format)
        try:
            df = pl.read_parquet(file_path)
        except Exception as e:
            return False, f"Cannot read Parquet file: {str(e)}"
        
        # Check 3: Not empty
        if len(df) == 0:
            return False, "File is empty (0 rows)"
        
        # Check 4: Has required columns
        missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
        if missing_columns:
            return False, f"Missing required columns: {missing_columns}"
        
        # check 5: check if the columns are the same as the required columns
        if df["transactionHash"].is_null().any():
            return False, "transactionHash is null"
        if df["timestamp_datetime"].is_null().any():
            return False, "timestamp_datetime is null"    
        if df["topic_0"].is_null().any():
            return False, "topic_0 is null"
        
        # Check 6: Amounts: non-negative
        if (df["filled_relay_data_input_amount"] < 0).any():
            return False, "filled_relay_data_input_amount is negative"
        if (df["filled_relay_data_output_amount"] < 0).any():
            return False, "filled_relay_data_output_amount is negative"
        if (df["funds_deposited_data_input_amount"] < 0).any():
            return False, "funds_deposited_data_input_amount is negative"
        if (df["funds_deposited_data_output_amount"] < 0).any():
            return False, "funds_deposited_data_output_amount is negative"
        if (df["amount_to_return"] < 0).any():
            return False, "amount_to_return is negative"
        if (df["refund_amounts"] < 0).any():
            return False, "refund_amounts is negative"
        if (df["refund_addresses"] < 0).any():
            return False, "refund_addresses is negative"
        if (df["refund_count"] < 0).any():
            return False, "refund_count is negative"

        # Check 7: Data types vs. expected types (at least high-level, e.g., amounts numeric-like, timestamps parseable).
        if df["filled_relay_data_input_amount"].is_not_null().any() and df["filled_relay_data_input_amount"].dtype != pl.Float64:
            return False, "filled_relay_data_input_amount is not a float64"
        if df["filled_relay_data_output_amount"].is_not_null().any() and df["filled_relay_data_output_amount"].dtype != pl.Float64:
            return False, "filled_relay_data_output_amount is not a float64"
        if df["funds_deposited_data_input_amount"].is_not_null().any() and df["funds_deposited_data_input_amount"].dtype != pl.Float64:
            return False, "funds_deposited_data_input_amount is not a float64"
        if df["funds_deposited_data_output_amount"].is_not_null().any() and df["funds_deposited_data_output_amount"].dtype != pl.Float64:
            return False, "funds_deposited_data_output_amount is not a float64"
        if df["amount_to_return"].is_not_null().any() and df["amount_to_return"].dtype != pl.Float64:
            return False, "amount_to_return is not a float64"
        if df["refund_amounts"].is_not_null().any() and df["refund_amounts"].dtype != pl.Float64:
            return False, "refund_amounts is not a float64"
        if df["refund_addresses"].is_not_null().any() and df["refund_addresses"].dtype != pl.Float64:
            return False, "refund_addresses is not a float64"
        if df["refund_count"].is_not_null().any() and df["refund_count"].dtype != pl.Float64:
            return False, "refund_count is not a float64"

        # Check 8: Timestamps: parseable; not in scientific notation.
        if df["timestamp_datetime"].is_not_null().any() and df["timestamp_datetime"].dtype != pl.Datetime:
            return False, "timestamp_datetime is not a datetime"

        # Check 9: Addresses: lowercase/uppercase hex, length 42, starts with 0x.
        if df["filled_relay_data_input_token"].is_not_null().any() and df["filled_relay_data_input_token"].dtype != pl.Utf8:
            return False, "filled_relay_data_input_token is not a string"
        if df["filled_relay_data_output_token"].is_not_null().any() and df["filled_relay_data_output_token"].dtype != pl.Utf8:
            return False, "filled_relay_data_output_token is not a string"
        if df["funds_deposited_data_input_token"].is_not_null().any() and df["funds_deposited_data_input_token"].dtype != pl.Utf8:
            return False, "funds_deposited_data_input_token is not a string"
        if df["funds_deposited_data_output_token"].is_not_null().any() and df["funds_deposited_data_output_token"].dtype != pl.Utf8:
            return False, "funds_deposited_data_output_token is not a string"
        if df["l2_token_address"].is_not_null().any() and df["l2_token_address"].dtype != pl.Utf8:
            return False, "l2_token_address is not a string"
        if df["refund_addresses"].is_not_null().any() and df["refund_addresses"].dtype != pl.Utf8:
            return False, "refund_addresses is not a string"
        if df["refund_addresses"].is_not_null().any() and df["refund_addresses"].dtype != pl.Utf8:
            return False, "refund_addresses is not a string"

        # All checks passed
        return True, None
        
    except Exception as e:
        # Catch any unexpected errors
        return False, f"Validation error: {str(e)}"


if __name__ == "__main__":

    # LIST ALL PARQUET FILES IN THE DATA/PROCESSED FOLDER
    parquet_files = list(Path(PROJECT_ROOT, "data/processed").glob("*.parquet"))
    print(f"Found {len(parquet_files)} parquet files in the data/processed folder")
    for file in parquet_files:
        print(f"  - {file}")
    
    # Validate all parquet files before loading into database
    print("\nValidating parquet files before loading into database...")
    for file in parquet_files:
        is_valid, error = validate_parquet_file_before_loading_into_database(file)
        if is_valid:
            print(f"✓ File is valid: {file}")
        else:
            print(f"✗ File validation failed: {error}")


