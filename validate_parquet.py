import polars as pl
from pathlib import Path

processed_dir = Path(r"c:\Users\Longin\Desktop\Projects\across_analytics\data\processed")
files = list(processed_dir.glob("*.parquet"))

print(f"Found {len(files)} parquet files.")

for f in files:
    df = pl.read_parquet(f)
    print(f"\nChecking {f.name}:")
    if "timestamp_datetime" in df.columns:
        dates = df.select(pl.col("timestamp_datetime")).to_series()
        # Convert to proper datetime if it's string
        try:
            # Assuming format might be ISO or similar, but let's just sort string first or try cast
             # If it's stored as string in 'timestamp_datetime'
            min_date = dates.min()
            max_date = dates.max()
            print(f"  Min Date: {min_date}")
            print(f"  Max Date: {max_date}")
        except Exception as e:
            print(f"  Error checking dates: {e}")
    else:
        print("  Column 'timestamp_datetime' not found.")
