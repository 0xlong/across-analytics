"""
Fetch transaction receipts from a JSONL file containing logs.
Uses eth_getTransactionReceipt RPC call to get gasPrice and gasUsed.
"""

import json
import os
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

# RPC endpoints by chain (Alchemy for Optimism/Base, Ankr for BSC)
RPC_ENDPOINTS = {
    "optimism": f"https://opt-mainnet.g.alchemy.com/v2/{os.getenv('ALCHEMY_API_KEY')}",
    "base": f"https://base-mainnet.g.alchemy.com/v2/{os.getenv('ALCHEMY_API_KEY')}",
    "bsc": f"https://bnb-mainnet.g.alchemy.com/v2/{os.getenv('ALCHEMY_API_KEY')}",
}


def create_session_with_retries(retries=3, backoff_factor=0.5):
    """Create a requests session with retry logic and connection pooling."""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def get_unique_tx_hashes_from_jsonl(input_file: str) -> list:
    """Extract unique transaction hashes from a JSONL file."""
    tx_hashes = set()
    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                log = json.loads(line)
                tx_hashes.add(log["transactionHash"])
    return list(tx_hashes)


def fetch_receipt_batch(session: requests.Session, rpc_url: str, tx_hashes: list, max_retries: int = 5) -> dict:
    """
    Fetch multiple transaction receipts in a single batch RPC call.
    Returns dict mapping tx_hash -> receipt.
    Includes exponential backoff for 429 rate limit errors.
    """
    payload = [
        {"jsonrpc": "2.0", "id": idx, "method": "eth_getTransactionReceipt", "params": [tx_hash]}
        for idx, tx_hash in enumerate(tx_hashes)
    ]
    
    for attempt in range(max_retries):
        try:
            response = session.post(rpc_url, json=payload, timeout=30)
            
            # Handle HTTP 429 rate limit
            if response.status_code == 429:
                wait_time = 2 ** attempt
                print(f"  Rate limited (429). Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
                continue
            
            response.raise_for_status()
            results = response.json()
            
            # Handle rate limit error in response body
            if isinstance(results, dict) and results.get("error", {}).get("code") == 429:
                wait_time = 2 ** attempt
                print(f"  Rate limited (response). Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
                continue
            
            receipts = {}
            errors = 0
            
            for r in results:
                if "result" in r and r["result"]:
                    receipt = r["result"]
                    receipts[receipt["transactionHash"]] = {
                        "gasUsed": receipt.get("gasUsed"),
                        "effectiveGasPrice": receipt.get("effectiveGasPrice"),
                        "gasPrice": receipt.get("gasPrice"),  # fallback for older txs
                        "status": receipt.get("status"),
                    }
                elif "error" in r:
                    # Check for rate limit in individual responses
                    if r.get("error", {}).get("code") == 429:
                        wait_time = 2 ** attempt
                        print(f"  Rate limited (batch item). Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                        time.sleep(wait_time)
                        break
                    errors += 1
                    if errors <= 3:  # Only print first 3 errors
                        print(f"  RPC error: {r['error']}")
            else:
                # Only return if we didn't break out of the loop
                return receipts
                
        except Exception as e:
            print(f"Error fetching batch: {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"  Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
            else:
                return {}
    
    print(f"  Max retries ({max_retries}) exceeded for batch")
    return {}


def fetch_all_receipts(rpc_url: str, tx_hashes: list, batch_size: int = 30) -> dict:
    """Fetch all transaction receipts using batched RPC calls."""
    session = create_session_with_retries()
    all_receipts = {}
    
    print(f"Fetching {len(tx_hashes)} transaction receipts in batches of {batch_size}...")
    
    for i in tqdm(range(0, len(tx_hashes), batch_size), desc="Fetching receipts"):
        batch = tx_hashes[i:i + batch_size]
        receipts = fetch_receipt_batch(session, rpc_url, batch)
        all_receipts.update(receipts)
        time.sleep(1)  # 1 second delay to avoid Alchemy rate limits
    
    print(f"Successfully fetched {len(all_receipts)} receipts")
    return all_receipts


def save_receipts_to_jsonl(receipts: dict, output_file: str):
    """Save receipts dictionary to a JSONL file."""
    with open(output_file, "w", encoding="utf-8") as f:
        for tx_hash, receipt in receipts.items():
            record = {"transactionHash": tx_hash, **receipt}
            f.write(json.dumps(record) + "\n")
    print(f"Saved {len(receipts)} receipts to {output_file}")


def enrich_logs_with_receipts(input_file: str, receipts: dict, output_file: str):
    """Read logs from JSONL, add gas data from receipts, save to new JSONL."""
    enriched_count = 0
    with open(input_file, "r", encoding="utf-8") as f_in, \
         open(output_file, "w", encoding="utf-8") as f_out:
        for line in f_in:
            if line.strip():
                log = json.loads(line)
                tx_hash = log["transactionHash"]
                
                # Rename blockTimestamp -> timeStamp to match Etherscan format
                if "blockTimestamp" in log:
                    log["timeStamp"] = log.pop("blockTimestamp")
                
                # Remove 'removed' field to match Etherscan format
                log.pop("removed", None)
                
                if tx_hash in receipts:
                    log["gasUsed"] = receipts[tx_hash].get("gasUsed")
                    log["gasPrice"] = receipts[tx_hash].get("effectiveGasPrice") or receipts[tx_hash].get("gasPrice")
                    enriched_count += 1
                
                f_out.write(json.dumps(log) + "\n")
    
    print(f"Enriched {enriched_count} logs with gas data, saved to {output_file}")


def main(input_jsonl: str, chain: str, output_receipts: str = None, output_enriched: str = None):
    """
    Main function to fetch receipts and optionally enrich logs.
    
    Args:
        input_jsonl: Path to input JSONL file with logs
        chain: Chain name (optimism, arbitrum, base, bsc, polygon, ethereum)
        output_receipts: Optional path to save raw receipts JSONL
        output_enriched: Optional path to save enriched logs JSONL
    """
    if chain not in RPC_ENDPOINTS:
        raise ValueError(f"Unknown chain: {chain}. Available: {list(RPC_ENDPOINTS.keys())}")
    
    rpc_url = RPC_ENDPOINTS[chain]
    
    # Extract unique transaction hashes
    tx_hashes = get_unique_tx_hashes_from_jsonl(input_jsonl)
    print(f"Found {len(tx_hashes)} unique transactions in {input_jsonl}")
    
    # Fetch all receipts
    receipts = fetch_all_receipts(rpc_url, tx_hashes)
    
    # Save raw receipts if output path provided
    if output_receipts:
        save_receipts_to_jsonl(receipts, output_receipts)
    
    # Enrich original logs if output path provided
    if output_enriched:
        enrich_logs_with_receipts(input_jsonl, receipts, output_enriched)
    
    return receipts


if __name__ == "__main__":

    # ============ CONFIGURATION ============
    for chain in ["base"]:#, "bsc", "optimism",]:
        
        print("Chain:", chain)

        # Get project root (3 levels up from this file: extract -> etl -> src -> project_root)
        PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "..")
        
        INPUT_FILE = os.path.join(PROJECT_ROOT, "data", "raw", "alchemy_api", f"logs_{chain}_2026-01-05_to_2026-01-06.jsonl")
        OUTPUT_ENRICHED = os.path.join(PROJECT_ROOT, "data", "raw", "etherscan_api", f"logs_{chain}_2026-01-05_to_2026-01-06.jsonl")
        
        print("\nInput file name:", INPUT_FILE)
        print("\nOutput file name:", OUTPUT_ENRICHED)
        print("\n")
        # =======================================
        
        main(INPUT_FILE, chain, output_enriched=OUTPUT_ENRICHED)
