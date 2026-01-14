"""
Extract historical token prices from Alchemy Prices API using SYMBOL lookup.

Advantages over CoinGecko:
- Higher rate limits (Compute Units based, ~300k CU/month free)
- Configurable granularity intervals (5min, 1hour, 1day)
- Symbol-based lookup (no need for contract addresses!)
- Aggregated prices from CEX + DEX weighted by volume

Usage:
    python extract_prices_from_alchemy.py --start-date 2025-12-02 --end-date 2025-12-03
    
Airflow:
    Call extract_all_prices() directly with date parameters.
"""

import os
import sys
import time
import argparse
from datetime import datetime
from pathlib import Path

import requests
import pandas as pd

# Add src to path for config import
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import PATHS, RUN_CONFIG, API_KEYS, TOKENS_PRICES


# =============================================================================
# CONFIGURATION
# =============================================================================

ALCHEMY_API_KEY = API_KEYS.get("alchemy") or os.getenv("ALCHEMY_API_KEY")
BASE_URL = f"https://api.g.alchemy.com/prices/v1/{ALCHEMY_API_KEY}/tokens/historical"

# Rate limiting
RATE_LIMIT_DELAY = 0.2  # 200ms between requests
MAX_RETRIES = 3

# Tokens to fetch - from config.py
TOKENS_TO_FETCH = TOKENS_PRICES["tokens_to_fetch"]

# Date range from config with ±1 day buffer for overlap coverage
from datetime import timedelta
_config_start = datetime.strptime(RUN_CONFIG["start_date"], "%Y-%m-%d")
_config_end = datetime.strptime(RUN_CONFIG["end_date"], "%Y-%m-%d")
PRICE_DATE_RANGE = {
    "start_date": (_config_start - timedelta(days=1)).strftime("%Y-%m-%d"),
    "end_date": (_config_end + timedelta(days=1)).strftime("%Y-%m-%d"),
}


# =============================================================================
# MAIN EXTRACTION FUNCTIONS
# =============================================================================

def fetch_price_history_by_symbol(
    symbol: str,
    start_time: str,
    end_time: str,
    interval: str = "1h"
) -> list[dict]:
    """
    Fetch historical prices from Alchemy using token SYMBOL.
    
    Args:
        symbol: Token symbol (e.g., "ETH", "USDC", "HYPE")
        start_time: ISO 8601 timestamp (e.g., "2025-12-01T00:00:00Z")
        end_time: ISO 8601 timestamp
        interval: "5m", "1h", or "1d"
    
    Returns:
        List of {"timestamp": datetime, "price_usd": float}
    """
    payload = {
        "symbol": symbol,
        "startTime": start_time,
        "endTime": end_time,
        "interval": interval
    }
    
    headers = {"Content-Type": "application/json"}
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(BASE_URL, json=payload, headers=headers, timeout=30)
            
            if response.status_code == 429:
                wait_time = RATE_LIMIT_DELAY * (2 ** attempt)
                print(f"Rate limited, waiting {wait_time:.1f}s...", end=" ")
                time.sleep(wait_time)
                continue
            
            if response.status_code == 404:
                return []  # Token not found
            
            response.raise_for_status()
            data = response.json()
            
            # Parse response: {"data": [{"timestamp": "...", "value": "3456.78"}, ...]}
            result = []
            for item in data.get("data", []):
                result.append({
                    "timestamp": datetime.fromisoformat(item["timestamp"].replace("Z", "+00:00")),
                    "price_usd": float(item["value"])
                })
            
            return result
            
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(RATE_LIMIT_DELAY * (2 ** attempt))
    
    return []


def extract_all_prices(
    start_date: str,
    end_date: str,
    output_dir: str = None,
    tokens: list[str] = None,
    interval: str = "1h"
) -> pd.DataFrame:
    """
    Extract price data for all configured tokens using SYMBOL lookup.
    
    This is the main function to call from Airflow.
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        output_dir: Directory to save output (default: from config)
        tokens: List of token symbols (default: TOKENS_TO_FETCH)
        interval: Price interval - "5m", "1h", or "1d" (default: "1h")
    
    Returns:
        DataFrame with columns: token_symbol, timestamp, price_usd
    """
    output_dir = output_dir or str(PATHS["prices"])
    tokens = tokens or TOKENS_TO_FETCH
    
    if not ALCHEMY_API_KEY:
        raise ValueError("ALCHEMY_API_KEY not found in .env or config")
    
    start_time = f"{start_date}T00:00:00Z"
    end_time = f"{end_date}T23:59:59Z"
    
    print(f"\n{'='*60}")
    print(f"Alchemy Prices API (Symbol Lookup) - {len(tokens)} tokens")
    print(f"{'='*60}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Interval: {interval}")
    print(f"API Key: {ALCHEMY_API_KEY[:8]}...{ALCHEMY_API_KEY[-4:]}")
    print()
    
    all_prices = []
    
    for symbol in tokens:
        print(f"[FETCH] {symbol}...", end=" ")
        
        try:
            prices = fetch_price_history_by_symbol(symbol, start_time, end_time, interval)
            
            if not prices:
                print("✗ NOT FOUND")
                continue
            
            for p in prices:
                p["token_symbol"] = symbol
            
            all_prices.extend(prices)
            print(f"✓ {len(prices)} data points")
            
        except Exception as e:
            print(f"✗ ERROR: {e}")
        
        time.sleep(RATE_LIMIT_DELAY)
    
    # Build DataFrame
    df = pd.DataFrame(all_prices)
    
    if df.empty:
        print("\n⚠ No price data collected!")
        return df
    
    df = df[["token_symbol", "timestamp", "price_usd"]]
    df = df.sort_values(["token_symbol", "timestamp"])
    
    # Save to CSV
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = output_path / f"alchemy_prices_{start_date}_to_{end_date}_{run_ts}.csv"
    df.to_csv(csv_file, index=False)
    
    # Also save to data/seeds/token_prices.csv for dbt seeds
    seeds_dir = PATHS["project_root"] / "data" / "seeds"
    seeds_dir.mkdir(parents=True, exist_ok=True)
    seeds_file = seeds_dir / "token_prices.csv"
    df.to_csv(seeds_file, index=False)
    
    print(f"\n{'='*60}")
    print(f"✅ Saved {len(df)} price records to:")
    print(f"   {csv_file}")
    print(f"   {seeds_file}")
    print(f"{'='*60}\n")
    
    return df


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description="Extract historical token prices from Alchemy Prices API"
    )
    parser.add_argument("--start-date", default=None, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=None, help="End date (YYYY-MM-DD)")
    parser.add_argument("--interval", default="1h", choices=["5m", "1h", "1d"], help="Price interval")
    parser.add_argument("--output-dir", default=None, help="Output directory")
    
    args = parser.parse_args()
    
    start_date = args.start_date or PRICE_DATE_RANGE["start_date"]
    end_date = args.end_date or PRICE_DATE_RANGE["end_date"]
    
    extract_all_prices(
        start_date=start_date,
        end_date=end_date,
        output_dir=args.output_dir,
        interval=args.interval
    )


if __name__ == "__main__":
    main()
