"""
Alchemy API - Extract eth_getLogs + Transaction Receipts (Gas Data)
Combined extraction: fetches logs and enriches with gas data in a single run.

NOTE: Free tier limits to 10 blocks per request - uses batching.
Saves progress every 5 minutes to avoid data loss.
"""

import os
import sys
import time
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
from src.config import RUN_CONFIG, CHAIN_SETTINGS, ETL_CONFIG

# Import helper functions from shared utils
from extract_utils import get_block_from_date

# Load environment variables
load_dotenv()

# ========================================
# CHAIN SELECTION - Change this to switch chains
# ========================================
CHAIN = "bsc"  # Options: "base", "optimism", "bsc"

# Date range from config
START_DATE = RUN_CONFIG["start_date"]
END_DATE = RUN_CONFIG["end_date"]

# Configuration
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")

# Load chain configuration from tokens_contracts_per_chain.json
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "..")
with open(os.path.join(PROJECT_ROOT, "data", "seeds", "tokens_contracts_per_chain.json")) as f:
    CHAIN_CONFIG = json.load(f)

# Apply chain-specific settings
ACTIVE_RPC_URL = CHAIN_SETTINGS[CHAIN]["rpc_url"]
SPOKEPOOL_ADDRESS = CHAIN_CONFIG[CHAIN]["spoke_pool_contract"]
MORALIS_CHAIN = CHAIN_SETTINGS[CHAIN]["moralis_chain"]
EVENT_TOPICS = CHAIN_CONFIG[CHAIN]["topics"]

# Alchemy free tier limit - MUST be 10 for free tier!
BLOCKS_PER_REQUEST = 10

# Receipt batch size (Alchemy supports up to 100)
RECEIPT_BATCH_SIZE = 50

# Save interval in seconds (5 minutes)
SAVE_INTERVAL_SECONDS = 300

# Output file (final enriched output)
OUTPUT_FILE = os.path.join(PROJECT_ROOT, "data", "raw", "etherscan_api", f"logs_{CHAIN}_{START_DATE}_to_{END_DATE}.jsonl")


def create_session() -> requests.Session:
    """Create a requests session with retry logic and connection pooling."""
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({"Content-Type": "application/json"})
    return session


# Create persistent session for all API calls
SESSION = create_session()





def get_current_block() -> int | None:
    """Fetch current block number."""
    response = SESSION.post(
        ACTIVE_RPC_URL,
        json={"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1},
        timeout=30
    )
    result = response.json()
    return int(result["result"], 16) if "result" in result else None


def fetch_logs_batch(from_block: int, to_block: int) -> dict:
    """Fetch logs for a single batch (max 10 blocks on free tier)."""
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getLogs",
        "params": [{
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "address": SPOKEPOOL_ADDRESS,
            "topics": [EVENT_TOPICS]
        }],
        "id": 1
    }
    
    response = SESSION.post(ACTIVE_RPC_URL, json=payload, timeout=30)
    return response.json()


def fetch_receipt_batch(tx_hashes: list, max_retries: int = 5) -> dict:
    """
    Fetch multiple transaction receipts in a single batch RPC call.
    Returns dict mapping tx_hash -> receipt data.
    """
    payload = [
        {"jsonrpc": "2.0", "id": idx, "method": "eth_getTransactionReceipt", "params": [tx_hash]}
        for idx, tx_hash in enumerate(tx_hashes)
    ]
    
    for attempt in range(max_retries):
        try:
            response = SESSION.post(ACTIVE_RPC_URL, json=payload, timeout=30)
            
            if response.status_code == 429:
                wait_time = 2 ** attempt
                print(f"  Rate limited (429). Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
                continue
            
            response.raise_for_status()
            results = response.json()
            
            if isinstance(results, dict) and results.get("error", {}).get("code") == 429:
                wait_time = 2 ** attempt
                print(f"  Rate limited (response). Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                continue
            
            receipts = {}
            for r in results:
                if "result" in r and r["result"]:
                    receipt = r["result"]
                    receipts[receipt["transactionHash"]] = {
                        "gasUsed": receipt.get("gasUsed"),
                        "effectiveGasPrice": receipt.get("effectiveGasPrice"),
                        "gasPrice": receipt.get("gasPrice"),
                        "status": receipt.get("status"),
                    }
            return receipts
                
        except Exception as e:
            print(f"Error fetching receipt batch: {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                time.sleep(wait_time)
            else:
                return {}
    
    return {}


def fetch_all_receipts(logs: list) -> dict:
    """Fetch all transaction receipts for the given logs using batched RPC calls."""
    # Extract unique transaction hashes
    tx_hashes = list({log["transactionHash"] for log in logs})
    
    if not tx_hashes:
        return {}
    
    print(f"\n📥 Fetching gas data for {len(tx_hashes)} unique transactions...")
    
    all_receipts = {}
    total_batches = (len(tx_hashes) + RECEIPT_BATCH_SIZE - 1) // RECEIPT_BATCH_SIZE
    
    for i in range(0, len(tx_hashes), RECEIPT_BATCH_SIZE):
        batch_num = i // RECEIPT_BATCH_SIZE + 1
        batch = tx_hashes[i:i + RECEIPT_BATCH_SIZE]
        receipts = fetch_receipt_batch(batch)
        all_receipts.update(receipts)
        print(f"  Receipt batch {batch_num}/{total_batches} | Got {len(receipts)} receipts")
        time.sleep(0.5)  # Rate limiting between batches
    
    print(f"✅ Fetched {len(all_receipts)} transaction receipts")
    return all_receipts


def enrich_logs_with_gas(logs: list, receipts: dict) -> list:
    """Merge gas data from receipts into logs."""
    enriched = []
    enriched_count = 0
    
    for log in logs:
        tx_hash = log["transactionHash"]
        
        # Rename blockTimestamp -> timeStamp to match Etherscan format
        if "blockTimestamp" in log:
            log["timeStamp"] = log.pop("blockTimestamp")
        
        # Remove 'removed' field to match Etherscan format
        log.pop("removed", None)
        
        # Add gas data if available
        if tx_hash in receipts:
            log["gasUsed"] = receipts[tx_hash].get("gasUsed")
            log["gasPrice"] = receipts[tx_hash].get("effectiveGasPrice") or receipts[tx_hash].get("gasPrice")
            enriched_count += 1
        
        enriched.append(log)
    
    print(f"✅ Enriched {enriched_count}/{len(logs)} logs with gas data")
    return enriched


def save_logs_to_jsonl(logs: list, filepath: str) -> int:
    """Save logs to JSONL file with deduplication."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    # Load existing logs for deduplication
    existing_keys = set()
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r') as f:
                for line in f:
                    if line.strip():
                        log = json.loads(line)
                        key = f"{log.get('transactionHash', '')}-{log.get('logIndex', '')}"
                        existing_keys.add(key)
        except (json.JSONDecodeError, FileNotFoundError):
            pass
    
    # Add new logs (deduplicated)
    new_count = 0
    with open(filepath, 'a') as f:
        for log in logs:
            key = f"{log.get('transactionHash', '')}-{log.get('logIndex', '')}"
            if key not in existing_keys:
                existing_keys.add(key)
                f.write(json.dumps(log) + '\n')
                new_count += 1
    
    return len(existing_keys)


def print_batch_progress(batch_count: int, total_batches: int, current: int, batch_end: int, 
                         logs_in_batch: int, total_logs: int, start_time: float, last_timestamp):
    """Display progress information for current batch."""
    elapsed = time.time() - start_time
    pct = (batch_count / total_batches) * 100
    eta = (elapsed / batch_count) * (total_batches - batch_count) / 60 if batch_count > 0 else 0
    
    date_str = last_timestamp.strftime("%Y-%m-%d %H:%M") if last_timestamp else "..."
    print(f"  Batch {batch_count:,}/{total_batches:,} | Blocks {current}-{batch_end} | {date_str} | {logs_in_batch} logs | Total: {total_logs:,} | {pct:.1f}% | ETA: {eta:.1f}m")


def extract_and_enrich(from_block: int, to_block: int):
    """
    Main extraction pipeline: fetch logs and enrich with gas data.
    Saves progress every 5 minutes.
    """
    print(f"\n{'='*60}")
    print("Alchemy API - Log Extraction + Gas Enrichment")
    print(f"{'='*60}")
    
    if not ALCHEMY_API_KEY:
        print("❌ ERROR: ALCHEMY_API_KEY not found in .env file")
        return []
    
    print(f"\n✓ API Key loaded: {ALCHEMY_API_KEY[:8]}...{ALCHEMY_API_KEY[-4:]}")
    print(f"✓ Chain: {CHAIN.upper()}")
    print(f"✓ Target contract: {SPOKEPOOL_ADDRESS}")
    print(f"✓ Block range: {from_block:,} to {to_block:,}")
    
    total_blocks = to_block - from_block
    total_batches = (total_blocks + BLOCKS_PER_REQUEST - 1) // BLOCKS_PER_REQUEST
    print(f"✓ Total blocks: {total_blocks:,}")
    print(f"✓ Total batches: {total_batches:,} ({BLOCKS_PER_REQUEST} blocks each)")
    print(f"✓ Estimated time: ~{total_batches * 0.15 / 60:.1f} minutes")
    print(f"✓ Auto-save every: {SAVE_INTERVAL_SECONDS // 60} minutes")
    print(f"✓ Output file: {OUTPUT_FILE}")
    
    current_block = get_current_block()
    if current_block:
        print(f"✓ Current chain block: {current_block:,}")
    
    print(f"\n{'='*60}")
    print("Phase 1: Extracting logs...")
    print(f"{'='*60}\n")
    
    all_logs = []
    batch_count = 0
    current = from_block
    start_time = time.time()
    last_save_time = time.time()
    last_timestamp = None
    
    try:
        while current < to_block:
            batch_end = min(current + BLOCKS_PER_REQUEST - 1, to_block)
            
            result = fetch_logs_batch(current, batch_end)
            
            if "error" in result:
                print(f"\n❌ API Error at batch {batch_count}: {result['error']}")
                print(f"   Retrying in 2 seconds...")
                time.sleep(2)
                continue
            
            logs_in_batch = 0
            if "result" in result:
                logs = result["result"]
                all_logs.extend(logs)
                logs_in_batch = len(logs)
                
                if logs_in_batch > 0:
                    first_log_timestamp = int(logs[0].get('blockTimestamp', '0x0'), 16)
                    last_timestamp = datetime.fromtimestamp(first_log_timestamp)
            
            batch_count += 1
            
            print_batch_progress(batch_count, total_batches, current, batch_end, 
                               logs_in_batch, len(all_logs), start_time, last_timestamp)
            
            current = batch_end + 1
            
            # Save checkpoint every 5 minutes
            if time.time() - last_save_time >= SAVE_INTERVAL_SECONDS:
                print(f"\n💾 CHECKPOINT: Processing {len(all_logs)} logs...")
                receipts = fetch_all_receipts(all_logs)
                enriched = enrich_logs_with_gas(all_logs, receipts)
                total_saved = save_logs_to_jsonl(enriched, OUTPUT_FILE)
                print(f"💾 Saved {total_saved:,} total logs to file")
                print(f"   Continuing extraction...\n")
                last_save_time = time.time()
                all_logs = []  # Clear buffer after saving
            
            time.sleep(0.07)  # Rate limiting
        
        # Final processing
        if all_logs:
            print(f"\n{'='*60}")
            print("Phase 2: Fetching gas data...")
            print(f"{'='*60}")
            
            receipts = fetch_all_receipts(all_logs)
            enriched = enrich_logs_with_gas(all_logs, receipts)
            total_saved = save_logs_to_jsonl(enriched, OUTPUT_FILE)
            print(f"\n💾 FINAL SAVE: {total_saved:,} total logs saved")
        
        elapsed = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"✅ COMPLETE! Extraction finished in {elapsed/60:.1f} minutes")
        print(f"{'='*60}")
        
        # Load and display final count
        final_logs = []
        with open(OUTPUT_FILE, 'r') as f:
            for line in f:
                if line.strip():
                    final_logs.append(json.loads(line))
        
        if final_logs:
            print(f"\n📊 Final Results:")
            print(f"   First log block: {int(final_logs[0]['blockNumber'], 16):,}")
            print(f"   Last log block: {int(final_logs[-1]['blockNumber'], 16):,}")
            print(f"   Total logs: {len(final_logs):,}")
            
            # Count logs with gas data
            with_gas = sum(1 for log in final_logs if log.get("gasUsed"))
            print(f"   Logs with gas data: {with_gas:,}")
        
        return final_logs
        
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Request failed: {e}")
        if all_logs:
            receipts = fetch_all_receipts(all_logs)
            enriched = enrich_logs_with_gas(all_logs, receipts)
            total_saved = save_logs_to_jsonl(enriched, OUTPUT_FILE)
            print(f"💾 EMERGENCY SAVE: {total_saved:,} logs saved before error")
        return []
    except KeyboardInterrupt:
        print(f"\n\n⚠️ Interrupted by user!")
        if all_logs:
            receipts = fetch_all_receipts(all_logs)
            enriched = enrich_logs_with_gas(all_logs, receipts)
            total_saved = save_logs_to_jsonl(enriched, OUTPUT_FILE)
            print(f"💾 INTERRUPT SAVE: {total_saved:,} logs saved")
        raise


if __name__ == "__main__":
    print("\n" + "="*60)
    print(f"Alchemy API - {CHAIN.upper()} Mainnet Extraction + Gas Enrichment")
    print(f"Date range: {START_DATE} to {END_DATE}")
    print("="*60)
    
    # Fetch block numbers from Moralis API
    print(f"\nFetching block numbers for {CHAIN} from {START_DATE} to {END_DATE}...")
    FROM_BLOCK = get_block_from_date(MORALIS_CHAIN, START_DATE)
    TO_BLOCK = get_block_from_date(MORALIS_CHAIN, END_DATE)
    print(f"  FROM_BLOCK: {FROM_BLOCK}")
    print(f"  TO_BLOCK: {TO_BLOCK}")
    
    if FROM_BLOCK and TO_BLOCK:
        # Run combined extraction
        logs = extract_and_enrich(FROM_BLOCK, TO_BLOCK)
        
        print(f"\n{'='*60}")
        print("Extraction complete!")
        print(f"{'='*60}\n")
    else:
        print("❌ Failed to get block numbers from Moralis API")
