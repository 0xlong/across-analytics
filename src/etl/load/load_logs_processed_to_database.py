"""
validate_parquet_file.py

Simple validation function for Parquet files before loading to Bronze layer.
Validates file integrity and schema - NOT data quality (that's for Silver layer).
"""

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
    
    print("\nValidating parquet files before loading into database...")
    for file in parquet_files:
    # VALIDATE PARQUET FILE BEFORE LOADING INTO DATABASE
        is_valid, error = validate_parquet_file_before_loading_into_database(file)
        if is_valid:
            print(f"✓ File is valid: {file}")
        else:
            print(f"✗ File validation failed: {error}")


