"""
Extract Token Metadata from Parquet Files using Moralis API

This script:
1. Reads all parquet files in data/processed/
2. Extracts unique input/output token addresses paired with chain IDs
3. Fetches token metadata (name, symbol, decimals) from Moralis API
4. Outputs a CSV similar to data/seeds/token_metadata.csv
"""

import os
import json
import time
import requests
import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple, Set

# Add project root to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from src.config import API_KEYS, PATHS, ETL_CONFIG


# =============================================================================
# MORALIS CONFIGURATION
# =============================================================================

# Rate limiting: Moralis free tier allows ~25 req/s, but we'll be conservative
RATE_LIMIT_SECONDS = 0.15

# Maps chain_id to Moralis chain parameter
# Docs: https://docs.moralis.io/web3-data-api/evm/reference/endpoint-metadata
CHAIN_ID_TO_MORALIS = {
    1: "eth",
    10: "optimism",
    56: "bsc",
    137: "polygon",
    324: None,  # zkSync - not supported
    480: None,  # Worldchain - not supported
    999: None,  # HyperEVM - not supported
    130: None,  # Unichain - not supported
    143: None,  # Monad - not supported
    690: None,  # Redstone - not supported
    1135: "lisk",
    1868: None,  # Soneium - not supported
    8453: "base",
    9745: None,  # Unknown
    34443: None,  # Mode - not supported
    42161: "arbitrum",
    43114: "avalanche",
    57073: None,  # Ink - not supported
    59144: "linea",
    81457: "blast",
    534352: None,  # Scroll - not supported
    7777777: None,  # Zora - not supported
}

# Chain ID to chain name mapping
CHAIN_ID_TO_NAME = {
    1: "ethereum",
    10: "optimism",
    56: "bsc",
    137: "polygon",
    324: "zksync",
    480: "worldchain",
    999: "hyperevm",
    130: "unichain",
    143: "monad",
    690: "redstone",
    1135: "lisk",
    1868: "soneium",
    8453: "base",
    9745: "unknown_9745",
    34443: "mode",
    42161: "arbitrum",
    43114: "avalanche",
    57073: "ink",
    59144: "linea",
    81457: "blast",
    534352: "scroll",
    7777777: "zora",
}


# =============================================================================
# STEP 1: EXTRACT UNIQUE TOKENS FROM PARQUET FILES
# =============================================================================

def extract_chain_from_filename(filename: str) -> str:
    """Extract chain name from parquet filename like 'logs_ethereum_2026-01-05_to_...'"""
    parts = filename.replace(".parquet", "").split("_")
    if len(parts) >= 2:
        return parts[1]
    return "unknown"


def load_chain_config() -> Dict:
    """Load chain configuration to get chain_id mapping."""
    with open(PATHS["chain_config"], "r") as f:
        return json.load(f)


def get_unique_tokens_from_parquet(parquet_dir: Path) -> Set[Tuple[str, int]]:
    """
    Extract unique (token_address, chain_id) pairs from all parquet files.
    
    Returns:
        Set of tuples: (token_address, chain_id)
    """
    chain_config = load_chain_config()
    
    # Build chain_name -> chain_id mapping
    chain_name_to_id = {
        name: config["chain_id"] 
        for name, config in chain_config.items() 
        if config.get("chain_id")
    }
    
    unique_tokens: Set[Tuple[str, int]] = set()
    
    # Token address columns to check
    token_columns = [
        "filled_relay_data_input_token",
        "filled_relay_data_output_token",
        "funds_deposited_data_input_token",
        "funds_deposited_data_output_token",
        "l2_token_address",
    ]
    
    parquet_files = list(parquet_dir.glob("*.parquet"))
    print(f"Found {len(parquet_files)} parquet files in {parquet_dir}")
    
    for parquet_file in parquet_files:
        try:
            df = pd.read_parquet(parquet_file)
            chain_name = extract_chain_from_filename(parquet_file.name)
            file_chain_id = chain_name_to_id.get(chain_name)
            
            print(f"  Processing {parquet_file.name} ({len(df)} rows, chain: {chain_name})...")
            
            for _, row in df.iterrows():
                for token_col in token_columns:
                    if token_col not in df.columns:
                        continue
                    
                    token_addr = row.get(token_col)
                    if pd.isna(token_addr) or not token_addr:
                        continue
                    
                    # Normalize address
                    token_addr = str(token_addr).lower().strip()
                    if not token_addr.startswith("0x") or len(token_addr) != 42:
                        continue
                    
                    # Skip zero address (native tokens)
                    if token_addr == "0x0000000000000000000000000000000000000000":
                        continue
                    
                    # Determine chain ID for this token
                    if "input" in token_col:
                        chain_id = row.get("topic_origin_chain_id")
                    elif "output" in token_col:
                        chain_id = row.get("topic_destination_chain_id") or row.get("filled_relay_data_repayment_chain_id")
                    else:
                        chain_id = row.get("topic_chain_id")
                    
                    # Fallback to file's chain
                    if pd.isna(chain_id) or not chain_id:
                        chain_id = file_chain_id
                    
                    if chain_id:
                        unique_tokens.add((token_addr, int(chain_id)))
            
        except Exception as e:
            print(f"  Error processing {parquet_file.name}: {e}")
    
    print(f"\nFound {len(unique_tokens)} unique (token, chain) pairs")
    return unique_tokens


# =============================================================================
# STEP 2: FETCH METADATA FROM MORALIS API
# =============================================================================

def fetch_token_metadata_moralis(token_address: str, chain_id: int) -> Dict:
    """
    Fetch token metadata from Moralis API.
    
    Endpoint: GET /erc20/metadata
    Docs: https://docs.moralis.io/web3-data-api/evm/reference/get-token-metadata
    
    Returns:
        Dict with name, symbol, decimals, logo (or error if failed)
    """
    moralis_chain = CHAIN_ID_TO_MORALIS.get(chain_id)
    
    if not moralis_chain:
        return {"error": f"Chain {chain_id} not supported by Moralis"}
    
    url = f"{ETL_CONFIG['moralis_url']}/erc20/metadata"
    headers = {
        "X-API-Key": API_KEYS["moralis"],
        "Accept": "application/json",
    }
    params = {
        "chain": moralis_chain,
        "addresses[]": token_address,
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 429:
            print(f"    Rate limited, waiting 1 second...")
            time.sleep(1)
            return fetch_token_metadata_moralis(token_address, chain_id)
        
        if response.status_code != 200:
            return {"error": f"HTTP {response.status_code}: {response.text[:100]}"}
        
        data = response.json()
        
        if data and len(data) > 0:
            token_info = data[0]
            return {
                "name": token_info.get("name", ""),
                "symbol": token_info.get("symbol", ""),
                "decimals": token_info.get("decimals", 18),
                "logo": token_info.get("logo"),
            }
        
        return {"error": "No data returned"}
    
    except Exception as e:
        return {"error": str(e)}


def fetch_all_token_metadata(unique_tokens: Set[Tuple[str, int]]) -> List[Dict]:
    """
    Fetch metadata for all unique tokens from Moralis.
    
    Args:
        unique_tokens: Set of (token_address, chain_id) tuples
    
    Returns:
        List of dicts with token metadata
    """
    results = []
    total = len(unique_tokens)
    
    print(f"\nFetching metadata for {total} tokens from Moralis API...")
    print(f"Rate limit: {RATE_LIMIT_SECONDS}s between calls\n")
    
    for i, (token_addr, chain_id) in enumerate(sorted(unique_tokens, key=lambda x: x[1])):
        chain_name = CHAIN_ID_TO_NAME.get(chain_id, f"unknown_{chain_id}")
        moralis_supported = CHAIN_ID_TO_MORALIS.get(chain_id) is not None
        
        print(f"  [{i+1}/{total}] {token_addr[:10]}... on {chain_name} (chain_id={chain_id})", end="")
        
        if not moralis_supported:
            print(" -> SKIPPED (chain not supported by Moralis)")
            results.append({
                "chain": chain_name,
                "chain_id": chain_id,
                "token_symbol": "UNKNOWN",
                "token_address": token_addr,
                "decimals": 18,
                "source": "unsupported",
                "error": "Chain not supported by Moralis API",
            })
            continue
        
        metadata = fetch_token_metadata_moralis(token_addr, chain_id)
        
        if "error" in metadata:
            print(f" -> ERROR: {metadata['error']}")
            results.append({
                "chain": chain_name,
                "chain_id": chain_id,
                "token_symbol": "UNKNOWN",
                "token_address": token_addr,
                "decimals": 18,
                "source": "error",
                "error": metadata["error"],
            })
        else:
            print(f" -> {metadata['symbol']} ({metadata['name']})")
            results.append({
                "chain": chain_name,
                "chain_id": chain_id,
                "token_symbol": metadata["symbol"],
                "token_address": token_addr,
                "decimals": metadata["decimals"],
                "token_name": metadata["name"],
                "logo": metadata.get("logo"),
                "source": "moralis",
            })
        
        time.sleep(RATE_LIMIT_SECONDS)
    
    return results


# =============================================================================
# STEP 3: SAVE TO CSV
# =============================================================================

def save_to_csv(results: List[Dict], output_path: Path):
    """Save token metadata to CSV file."""
    df = pd.DataFrame(results)
    
    # Keep only core columns matching original token_metadata.csv format
    column_order = ["chain", "chain_id", "token_symbol", "token_address", "decimals"]
    df = df[column_order]
    
    # Sort by chain_id, then symbol
    df = df.sort_values(["chain_id", "token_symbol"])
    
    df.to_csv(output_path, index=False)
    print(f"\nSaved {len(df)} tokens to {output_path}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    """Main entry point."""
    print("=" * 60)
    print("TOKEN METADATA EXTRACTION (Moralis API)")
    print("=" * 60)
    
    # Validate Moralis API key
    if not API_KEYS.get("moralis"):
        raise ValueError("MORALIS_API_KEY not set in .env file")
    
    # Step 1: Extract unique tokens from parquet files
    parquet_dir = PATHS["processed_data"]
    unique_tokens = get_unique_tokens_from_parquet(parquet_dir)
    
    if not unique_tokens:
        print("No tokens found in parquet files!")
        return
    
    # Step 2: Fetch metadata from Moralis
    results = fetch_all_token_metadata(unique_tokens)
    
    # Step 3: Save to CSV
    output_path = PATHS["seeds"] / "token_metadata_auto.csv"
    save_to_csv(results, output_path)
    
    # Summary
    successful = sum(1 for r in results if r.get("source") == "moralis")
    unsupported = sum(1 for r in results if r.get("source") == "unsupported")
    errors = sum(1 for r in results if r.get("source") == "error")
    
    print(f"\n{'=' * 60}")
    print(f"SUMMARY")
    print(f"{'=' * 60}")
    print(f"Total unique tokens:  {len(unique_tokens)}")
    print(f"Successfully fetched: {successful}")
    print(f"Chain not supported:  {unsupported}")
    print(f"API errors:           {errors}")
    print(f"Output file: {output_path}")


if __name__ == "__main__":
    main()
