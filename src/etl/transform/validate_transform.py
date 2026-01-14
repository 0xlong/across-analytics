"""
Validate that all raw JSONL files have been transformed to Parquet.
Checks for missing files, validates parquet readability, and compares row counts.
"""
import polars as pl
from pathlib import Path
import sys
import json

# Add src to path for config import
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import PATHS

PROJECT_ROOT = PATHS["project_root"]
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "etherscan_api"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def validate_gas_fields(file_path: Path) -> dict:
    """
    Check that every log entry has gasPrice and gasUsed fields.
    
    Returns dict with:
        - valid: bool
        - total_logs: int
        - missing_gasPrice: int
        - missing_gasUsed: int
        - missing_both: int
    """
    total_logs = 0
    missing_gasPrice = 0
    missing_gasUsed = 0
    missing_both = 0
    
    with open(file_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue
            
            total_logs += 1
            log = json.loads(line)
            
            has_gasPrice = 'gasPrice' in log and log['gasPrice'] is not None
            has_gasUsed = 'gasUsed' in log and log['gasUsed'] is not None
            
            if not has_gasPrice and not has_gasUsed:
                missing_both += 1
            elif not has_gasPrice:
                missing_gasPrice += 1
            elif not has_gasUsed:
                missing_gasUsed += 1
    
    return {
        "valid": missing_gasPrice == 0 and missing_gasUsed == 0 and missing_both == 0,
        "total_logs": total_logs,
        "missing_gasPrice": missing_gasPrice + missing_both,
        "missing_gasUsed": missing_gasUsed + missing_both,
        "missing_both": missing_both
    }


def validate_transforms():
    """Check all raw files have corresponding processed parquet files."""
    
    raw_files = list(RAW_DIR.glob("*.jsonl"))
    
    if not raw_files:
        print("⚠ No raw JSONL files found in:", RAW_DIR)
        return
    
    print(f"Found {len(raw_files)} raw JSONL files\n")
    print("-" * 70)
    
    missing = []
    valid = []
    errors = []
    gas_issues = []
    
    for raw_file in sorted(raw_files):
        # Expected processed file name
        processed_name = raw_file.stem + "_processed.parquet"
        processed_file = PROCESSED_DIR / processed_name
        
        if not processed_file.exists():
            missing.append(raw_file.name)
            print(f"✗ MISSING: {processed_name}")
            continue
        
        try:
            # Try loading parquet to verify it's valid
            df = pl.read_parquet(processed_file)
            
            # Count raw lines (each line = 1 log entry)
            with open(raw_file, 'r') as f:
                raw_count = sum(1 for _ in f)
            
            processed_count = len(df)
            
            # Validate gas fields
            gas_result = validate_gas_fields(raw_file)
            
            # Check if counts match
            count_match = raw_count == processed_count
            gas_valid = gas_result["valid"]
            
            if count_match and gas_valid:
                status = "✓"
            elif not gas_valid:
                status = "⚠"
            else:
                status = "⚠"
            
            print(f"{status} {raw_file.name}")
            print(f"   Raw: {raw_count:,} → Processed: {processed_count:,}")
            
            # Report gas field issues
            if not gas_valid:
                gas_issues.append({
                    "file": raw_file.name,
                    **gas_result
                })
                print(f"   ⚠ Gas Fields: {gas_result['missing_gasPrice']} missing gasPrice, {gas_result['missing_gasUsed']} missing gasUsed")
            else:
                print(f"   ✓ Gas Fields: All {gas_result['total_logs']} logs have gasPrice and gasUsed")
            
            valid.append({
                "file": raw_file.name,
                "raw": raw_count,
                "processed": processed_count,
                "gas_valid": gas_valid
            })
            
        except Exception as e:
            errors.append((raw_file.name, str(e)))
            print(f"✗ ERROR reading {processed_name}: {e}")
    
    # Summary
    print("-" * 70)
    print(f"\nSUMMARY:")
    print(f"  ✓ Valid:   {len(valid)}")
    print(f"  ✗ Missing: {len(missing)}")
    print(f"  ✗ Errors:  {len(errors)}")
    print(f"  ⚠ Gas Issues: {len(gas_issues)}")
    
    if missing:
        print(f"\nMissing files need to be transformed:")
        for f in missing:
            print(f"  - {f}")
    
    if errors:
        print(f"\nFiles with errors:")
        for f, e in errors:
            print(f"  - {f}: {e}")
    
    if gas_issues:
        print(f"\nFiles with missing gas fields:")
        for g in gas_issues:
            print(f"  - {g['file']}: {g['missing_gasPrice']} missing gasPrice, {g['missing_gasUsed']} missing gasUsed (of {g['total_logs']} logs)")
    
    return len(missing) == 0 and len(errors) == 0 and len(gas_issues) == 0


if __name__ == "__main__":
    success = validate_transforms()
    sys.exit(0 if success else 1)
