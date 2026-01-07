"""
Validate that all raw JSONL files have been transformed to Parquet.
Checks for missing files, validates parquet readability, and compares row counts.
"""
import polars as pl
from pathlib import Path
import sys

# Add src to path for config import
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import PATHS

PROJECT_ROOT = PATHS["project_root"]
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "etherscan_api"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


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
            
            # Check if counts match
            status = "✓" if raw_count == processed_count else "⚠"
            print(f"{status} {raw_file.name}")
            print(f"   Raw: {raw_count:,} → Processed: {processed_count:,}")
            
            valid.append({
                "file": raw_file.name,
                "raw": raw_count,
                "processed": processed_count
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
    
    if missing:
        print(f"\nMissing files need to be transformed:")
        for f in missing:
            print(f"  - {f}")
    
    if errors:
        print(f"\nFiles with errors:")
        for f, e in errors:
            print(f"  - {f}: {e}")
    
    return len(missing) == 0 and len(errors) == 0


if __name__ == "__main__":
    success = validate_transforms()
    sys.exit(0 if success else 1)
