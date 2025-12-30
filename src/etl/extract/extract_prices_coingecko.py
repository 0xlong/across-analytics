"""
Extract historical price data from CoinGecko API.

Free API limits:
- 30 calls/minute
- 10,000 calls/month
- Hourly granularity for dates > 1 day from now (auto-determined)
- 5-minute granularity only for last 24h from NOW

Usage:
    python extract_prices_coingecko.py --start-date 2025-12-02 --end-date 2025-12-03
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path

import requests
import pandas as pd


# CoinGecko API base URL (free demo API)
BASE_URL = "https://api.coingecko.com/api/v3"

# Rate limit: Free tier is stricter - use 6 seconds to be safe
RATE_LIMIT_DELAY = 3
MAX_RETRIES = 3

# Map token symbols to CoinGecko IDs
# Only unique tokens needed (same asset across chains = same price)
TOKEN_TO_COINGECKO_ID = {
    # Stablecoins (price ~1 USD, but still useful to track small deviations)
    "USDC": "usd-coin",
    "USDT": "tether",
    "DAI": "dai",
    "USDH": "usd-coin",  # Hyperliquid USD - map to USDC as proxy
    
    # Major crypto
    "WETH": "ethereum",
    "ETH": "ethereum",
    "WBTC": "bitcoin",
    
    # DeFi tokens
    "WLD": "worldcoin-wld",
    "BAL": "balancer",
    "SNX": "havven",
    "ACX": "across-protocol",
    
    # Chain native tokens
    "AVAX": "avalanche-2",
    "POL": "matic-network",  # Polygon
    "BNB": "binancecoin",
    "HYPE": "hyperliquid",
    "MONAD": "monad"
}


# Hardcoded list of tokens to fetch prices for
TOKENS_TO_FETCH = [
    "USDC",
    "USDT",
    "DAI",
    "WETH",
    "ETH",
    "WBTC",
    "WLD",
    "BAL",
    "SNX",
    "ACX",
    "AVAX",
    "POL",
    "BNB",
    "HYPE",
]


def date_to_unix(date_str: str) -> int:
    """Convert date string (YYYY-MM-DD) to Unix timestamp."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def fetch_price_history(
    coingecko_id: str,
    from_timestamp: int,
    to_timestamp: int,
) -> list[dict]:
    """
    Fetch historical price data from CoinGecko /market_chart/range endpoint.
    
    Returns list of dicts: [{"timestamp": ..., "price_usd": ...}, ...]
    Granularity is auto-determined by CoinGecko based on date range.
    Includes retry logic with exponential backoff for rate limiting.
    """
    url = f"{BASE_URL}/coins/{coingecko_id}/market_chart/range"
    params = {
        "vs_currency": "usd",
        "from": from_timestamp,
        "to": to_timestamp,
    }
    
    for attempt in range(MAX_RETRIES):
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 429:
            # Rate limited - wait and retry with exponential backoff
            wait_time = RATE_LIMIT_DELAY * (2 ** attempt)
            print(f"Rate limited, waiting {wait_time:.0f}s...", end=" ")
            time.sleep(wait_time)
            continue
        
        response.raise_for_status()
        break
    else:
        # All retries failed
        response.raise_for_status()
    
    data = response.json()
    prices = data.get("prices", [])
    
    # Convert [[timestamp_ms, price], ...] to list of dicts
    result = []
    for timestamp_ms, price in prices:
        result.append({
            "timestamp": datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc),
            "price_usd": price,
        })
    
    return result


def extract_all_prices(
    start_date: str,
    end_date: str,
    output_dir: str,
) -> pd.DataFrame:
    """
    Extract price data for all tokens in date range.
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        output_dir: Directory to save output files
    
    Returns:
        DataFrame with columns: token_symbol, timestamp, price_usd
    """
    print(f"Fetching prices for {len(TOKENS_TO_FETCH)} tokens")
    
    # Convert dates to timestamps
    from_ts = date_to_unix(start_date)
    # Add 23:59:59 to end date to include full day
    to_ts = date_to_unix(end_date) + 86399
    
    all_prices = []
    
    for symbol in TOKENS_TO_FETCH:
        coingecko_id = TOKEN_TO_COINGECKO_ID.get(symbol)
        
        if coingecko_id is None:
            print(f"[SKIP] {symbol}: No CoinGecko ID mapped")
            continue
        
        print(f"[FETCH] {symbol} ({coingecko_id})...", end=" ")
        
        try:
            prices = fetch_price_history(coingecko_id, from_ts, to_ts)
            
            for p in prices:
                p["token_symbol"] = symbol
            
            all_prices.extend(prices)
            print(f"Got {len(prices)} data points")
            
        except requests.exceptions.HTTPError as e:
            print(f"ERROR: {e}")
        except Exception as e:
            print(f"ERROR: {e}")
        
        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY)
    
    # Create DataFrame
    df = pd.DataFrame(all_prices)
    
    if df.empty:
        print("No price data collected!")
        return df
    
    # Reorder columns
    df = df[["token_symbol", "timestamp", "price_usd"]]
    df = df.sort_values(["token_symbol", "timestamp"])
    
    # Save to CSV and Parquet
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Generate unique filename with run timestamp
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = output_path / f"prices_{start_date}_to_{end_date}_{run_ts}.csv"
    df.to_csv(csv_file, index=False)
    print(f"\nSaved {len(df)} price records to:")
    print(f"  - {csv_file}")
    
    return df


def main():
    parser = argparse.ArgumentParser(
        description="Extract historical price data from CoinGecko API"
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory (default: data/raw/prices)"
    )
    
    args = parser.parse_args()
    
    # Default output directory
    if args.output_dir is None:
        project_root = Path(__file__).parent.parent.parent.parent
        output_dir = project_root / "data" / "raw" / "prices"
    else:
        output_dir = args.output_dir
    
    extract_all_prices(
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=str(output_dir),
    )


if __name__ == "__main__":
    main()
