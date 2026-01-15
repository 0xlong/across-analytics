"""
Validate extracted raw data immediately after extraction.

WHY VALIDATE AT EXTRACTION STAGE?
- Catch garbage data before wasting compute on transforms
- Fail fast if API returned errors or empty responses  
- Ensure data integrity before it enters the pipeline

WHAT WE CHECK:
- File exists and is readable
- Valid JSONL/CSV format
- Not empty (has at least 1 record)
- Required fields present in each record
- Basic type/format checks for critical fields
"""

import json
import csv
from pathlib import Path
from typing import Tuple, Optional, List, Dict, Any

# Add src to path for config import
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import PATHS


# =============================================================================
# LOGS VALIDATION (JSONL FORMAT - Alchemy/Etherscan)
# =============================================================================

def validate_logs_jsonl(file_path: Path) -> Tuple[bool, Optional[str], Optional[int], Optional[Dict]]:
    """
    Validate raw blockchain logs extracted from Alchemy or Etherscan APIs.
    
    Expected JSONL format (one JSON object per line):
    {
        "blockNumber": "0x...",
        "transactionHash": "0x...",
        "logIndex": "0x...",
        "topics": ["0x...event_sig", ...],
        "data": "0x...",
        "address": "0x...",
        "gasPrice": "0x...",  # Added during extraction
        "gasUsed": "0x..."    # Added during extraction
    }
    
    Args:
        file_path: Path to the .jsonl file
        
    Returns:
        Tuple of (is_valid, error_message, record_count, metadata)
    """
    REQUIRED_FIELDS = ["blockNumber", "transactionHash", "logIndex", "topics", "data", "address"]
    
    file_path = Path(file_path)
    
    try:
        # Check 1: File exists
        if not file_path.exists():
            return False, f"File does not exist: {file_path}", None, None
        
        # Check 2: File is not empty
        file_size = file_path.stat().st_size
        if file_size == 0:
            return False, "File is empty (0 bytes)", None, None
        
        # Check 3: Parse JSONL and validate each record
        records = []
        block_numbers = []
        tx_hashes = set()
        seen_keys = set()
        duplicate_count = 0
        logs_with_gas = 0
        logs_without_gas = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                # Try to parse JSON
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as e:
                    return False, f"Line {line_num}: Invalid JSON - {e}", None, None
                
                # Check required fields
                missing_fields = [field for field in REQUIRED_FIELDS if field not in record]
                if missing_fields:
                    return False, f"Line {line_num}: Missing fields: {missing_fields}", None, None
                
                # Validate transactionHash format (0x + 64 hex = 66 chars)
                tx_hash = record.get("transactionHash", "")
                if not tx_hash.startswith("0x") or len(tx_hash) != 66:
                    return False, f"Line {line_num}: Invalid transactionHash: {tx_hash[:20]}...", None, None
                
                # Validate address format (0x + 40 hex = 42 chars)
                address = record.get("address", "")
                if not address.startswith("0x") or len(address) != 42:
                    return False, f"Line {line_num}: Invalid address: {address}", None, None
                
                # Check topics is a non-empty list
                topics = record.get("topics", [])
                if not isinstance(topics, list) or len(topics) < 1:
                    return False, f"Line {line_num}: topics must be a non-empty list", None, None
                
                # Validate topic0 (event signature) format
                topic0 = topics[0]
                if not topic0.startswith("0x") or len(topic0) != 66:
                    return False, f"Line {line_num}: Invalid topic0: {topic0[:20]}...", None, None
                
                # Check for duplicates
                key = f"{tx_hash}-{record.get('logIndex', '')}"
                if key in seen_keys:
                    duplicate_count += 1
                seen_keys.add(key)
                
                # Check for gas data fields
                has_gas_price = "gasPrice" in record and record["gasPrice"]
                has_gas_used = "gasUsed" in record and record["gasUsed"]
                if has_gas_price and has_gas_used:
                    # Validate gas fields are valid hex
                    gas_price = record.get("gasPrice", "")
                    gas_used = record.get("gasUsed", "")
                    if isinstance(gas_price, str) and gas_price.startswith("0x") and \
                       isinstance(gas_used, str) and gas_used.startswith("0x"):
                        logs_with_gas += 1
                    else:
                        logs_without_gas += 1
                else:
                    logs_without_gas += 1
                
                records.append(record)
                tx_hashes.add(tx_hash)
                
                try:
                    block_numbers.append(int(record["blockNumber"], 16))
                except (ValueError, TypeError):
                    pass
        
        # Check 4: Not empty after parsing
        if len(records) == 0:
            return False, "File has no valid log records", None, None
        
        # Check 5: No duplicates
        if duplicate_count > 0:
            return False, f"Found {duplicate_count} duplicate logs (same tx+logIndex)", None, None
        
        # Build metadata
        metadata = {
            "min_block": min(block_numbers) if block_numbers else None,
            "max_block": max(block_numbers) if block_numbers else None,
            "unique_transactions": len(tx_hashes),
            "file_size_bytes": file_size,
            "logs_with_gas": logs_with_gas,
            "logs_without_gas": logs_without_gas,
            "gas_coverage": f"{logs_with_gas}/{len(records)}" if records else "0/0"
        }
        
        return True, None, len(records), metadata
        
    except Exception as e:
        return False, f"Validation error: {str(e)}", None, None


# =============================================================================
# PRICES VALIDATION (CSV FORMAT - Alchemy Prices API)
# =============================================================================

def validate_prices_csv(file_path: Path) -> Tuple[bool, Optional[str], Optional[int], Optional[Dict]]:
    """
    Validate extracted price data CSV.
    
    Expected CSV format:
    token_symbol,timestamp,price_usd
    ETH,2026-01-05 00:00:00+00:00,3456.78
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        Tuple of (is_valid, error_message, record_count, metadata)
    """
    REQUIRED_COLUMNS = ["token_symbol", "timestamp", "price_usd"]
    
    file_path = Path(file_path)
    
    try:
        # Check 1: File exists
        if not file_path.exists():
            return False, f"File does not exist: {file_path}", None, None
        
        # Check 2: File not empty
        file_size = file_path.stat().st_size
        if file_size == 0:
            return False, "File is empty (0 bytes)", None, None
        
        # Check 3: Parse CSV
        records = []
        tokens_seen = set()
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # Check 4: Header exists with required columns
            if reader.fieldnames is None:
                return False, "CSV has no header row", None, None
            
            missing_columns = [col for col in REQUIRED_COLUMNS if col not in reader.fieldnames]
            if missing_columns:
                return False, f"Missing required columns: {missing_columns}", None, None
            
            # Validate each row
            for row_num, row in enumerate(reader, 2):
                # Check token_symbol not empty
                token = row.get("token_symbol", "").strip()
                if not token:
                    return False, f"Row {row_num}: Empty token_symbol", None, None
                
                # Check price_usd is valid positive number
                try:
                    price = float(row.get("price_usd", ""))
                    if price < 0:
                        return False, f"Row {row_num}: Negative price: {price}", None, None
                except (ValueError, TypeError):
                    return False, f"Row {row_num}: Invalid price_usd: {row.get('price_usd')}", None, None
                
                # Check timestamp exists
                timestamp = row.get("timestamp", "").strip()
                if not timestamp or len(timestamp) < 10:
                    return False, f"Row {row_num}: Invalid timestamp: {timestamp}", None, None
                
                records.append(row)
                tokens_seen.add(token)
        
        # Check 5: Not empty
        if len(records) == 0:
            return False, "CSV has no data rows", None, None
        
        metadata = {
            "tokens": list(tokens_seen),
            "token_count": len(tokens_seen),
            "file_size_bytes": file_size
        }
        
        return True, None, len(records), metadata
        
    except Exception as e:
        return False, f"Validation error: {str(e)}", None, None


# =============================================================================
# TRANSACTION RECEIPTS VALIDATION (JSONL FORMAT)
# =============================================================================

def validate_receipts_jsonl(file_path: Path) -> Tuple[bool, Optional[str], Optional[int], Optional[Dict]]:
    """
    Validate extracted transaction receipts with gas data.
    
    Expected JSONL format:
    {
        "transactionHash": "0x...",
        "gasUsed": "0x...",
        "effectiveGasPrice": "0x..." OR "gasPrice": "0x..."
    }
    
    Args:
        file_path: Path to the .jsonl file
        
    Returns:
        Tuple of (is_valid, error_message, record_count, metadata)
    """
    REQUIRED_FIELDS = ["transactionHash", "gasUsed", "gasPrice"]
    
    file_path = Path(file_path)
    
    try:
        # Check 1: File exists
        if not file_path.exists():
            return False, f"File does not exist: {file_path}", None, None
        
        # Check 2: File not empty
        file_size = file_path.stat().st_size
        if file_size == 0:
            return False, "File is empty (0 bytes)", None, None
        
        # Check 3: Parse JSONL
        records = []
        tx_hashes = set()
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as e:
                    return False, f"Line {line_num}: Invalid JSON - {e}", None, None
                
                # Check required fields
                missing_fields = [f for f in REQUIRED_FIELDS if f not in record]
                if missing_fields:
                    return False, f"Line {line_num}: Missing fields: {missing_fields}", None, None
                
                # Check gas price field exists (either effectiveGasPrice or gasPrice)
                has_gas_price = "effectiveGasPrice" in record or "gasPrice" in record
                if not has_gas_price:
                    return False, f"Line {line_num}: Missing gas price field", None, None
                
                # Check transactionHash format
                tx_hash = record.get("transactionHash", "")
                if not tx_hash.startswith("0x") or len(tx_hash) != 66:
                    return False, f"Line {line_num}: Invalid transactionHash: {tx_hash[:20]}...", None, None
                
                # Check gasUsed is hex
                gas_used = record.get("gasUsed", "")
                if not gas_used.startswith("0x"):
                    return False, f"Line {line_num}: Invalid gasUsed format: {gas_used}", None, None

                 # Check gasPrice is hex
                gas_price = record.get("gasPrice", "")
                if not gas_price.startswith("0x"):
                    return False, f"Line {line_num}: Invalid gasPrice format: {gas_price}", None, None
                
                records.append(record)
                tx_hashes.add(tx_hash)
        
        # Check 4: Not empty
        if len(records) == 0:
            return False, "File has no valid receipt records", None, None
        
        # Check 5: No duplicates
        if len(tx_hashes) < len(records):
            duplicate_count = len(records) - len(tx_hashes)
            return False, f"Found {duplicate_count} duplicate receipts", None, None
        
        metadata = {
            "unique_transactions": len(tx_hashes),
            "file_size_bytes": file_size
        }
        
        return True, None, len(records), metadata
        
    except Exception as e:
        return False, f"Validation error: {str(e)}", None, None


# =============================================================================
# CONVENIENCE FUNCTION
# =============================================================================

def validate(file_path: Path) -> Tuple[bool, Optional[str], Optional[int], Optional[Dict]]:
    """
    Auto-detect file type and run appropriate validation.
    
    Args:
        file_path: Path to the extracted file
        
    Returns:
        Tuple of (is_valid, error_message, record_count, metadata)
    """
    file_path = Path(file_path)
    filename = file_path.name.lower()
    
    if filename.endswith(".jsonl"):
        if "receipt" in filename:
            return validate_receipts_jsonl(file_path)
        else:
            return validate_logs_jsonl(file_path)
    
    elif filename.endswith(".csv"):
        if "price" in filename:
            return validate_prices_csv(file_path)
        else:
            return False, f"Unknown CSV file type: {filename}", None, None
    
    else:
        return False, f"Unsupported file format: {file_path.suffix}", None, None


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("Extract Data Validation")
    print("=" * 60 + "\n")
    
    # Validate all files in raw directory
    raw_dir = PATHS.get("raw_data", Path("data/raw"))
    print(f"Scanning: {raw_dir}\n")
    
    files = list(raw_dir.glob("**/*.jsonl")) + list(raw_dir.glob("**/*.csv"))
    
    if not files:
        print("No files found.")
    else:
        valid_count = 0
        gas_issues_count = 0
        for file_path in sorted(files):
            is_valid, error, count, metadata = validate(file_path)
            if is_valid:
                # Check for gas coverage in JSONL log files
                gas_info = ""
                if file_path.suffix == ".jsonl" and metadata and "logs_with_gas" in metadata:
                    logs_with_gas = metadata.get("logs_with_gas", 0)
                    logs_without_gas = metadata.get("logs_without_gas", 0)
                    if logs_without_gas == 0 and logs_with_gas > 0:
                        gas_info = f" | Gas: ✓ {logs_with_gas}/{count}"
                    elif logs_without_gas > 0:
                        gas_info = f" | Gas: ⚠ {logs_with_gas}/{count} (missing: {logs_without_gas})"
                        gas_issues_count += 1
                    else:
                        gas_info = " | Gas: N/A"
                
                print(f"✓ {file_path.name}: {count:,} records{gas_info}")
                valid_count += 1
            else:
                print(f"✗ {file_path.name}: {error}")
        
        print(f"\n{valid_count}/{len(files)} files valid")
        if gas_issues_count > 0:
            print(f"⚠ {gas_issues_count} files have logs missing gas data")
    
    print("\n" + "=" * 60 + "\n")

