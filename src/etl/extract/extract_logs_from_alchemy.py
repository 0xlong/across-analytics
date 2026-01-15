"""
Alchemy API - Extract eth_getLogs for Optimism, Base, BSC mainnet
NOTE: Free tier limits to 10 blocks per request - uses batching.
Saves progress every 10 minutes to avoid data loss.
"""

import os
import sys
import time
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

# Add project root to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
from src.config import RUN_CONFIG, CHAIN_SETTINGS, ETL_CONFIG

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
with open(os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "seeds", "tokens_contracts_per_chain.json")) as f:
    CHAIN_CONFIG = json.load(f)


def get_block_from_date(chain: str, date: str) -> int | None:
    """
    Fetch block number for a specific date using Moralis API.
    
    Args:
        chain: Chain name (e.g., 'bsc', 'eth', 'polygon', 'base', 'optimism')
        date: Date string in format 'YYYY-MM-DD' (e.g., '2026-01-05' from config)
    
    Returns:
        Block number as integer, or None if request fails
    """
    moralis_api_key = os.getenv("MORALIS_API_KEY")
    if not moralis_api_key:
        print("❌ ERROR: MORALIS_API_KEY not found in .env file")
        return None
    
    # Convert YYYY-MM-DD to URL-encoded format: YYYY-MM-DDTHH%3AMM%3ASSZ
    date_encoded = f"{date}T00%3A00%3A00Z"
    
    url = f"{ETL_CONFIG['moralis_url']}/dateToBlock?chain={chain}&date={date_encoded}"
    
    headers = {
        "Accept": "application/json",
        "X-API-Key": moralis_api_key
    }
    
    response = requests.get(url, headers=headers, timeout=30)
    
    if response.status_code == 200:
        result = response.json()
        return result.get("block")
    else:
        print(f"❌ Moralis API Error: {response.status_code} - {response.text}")
        return None

# Apply chain-specific settings based on CHAIN flag
ACTIVE_RPC_URL = CHAIN_SETTINGS[CHAIN]["rpc_url"]
SPOKEPOOL_ADDRESS = CHAIN_CONFIG[CHAIN]["spoke_pool_contract"]
MORALIS_CHAIN = CHAIN_SETTINGS[CHAIN]["moralis_chain"]

# Fetch block numbers dynamically from Moralis API based on config dates
print(f"Fetching block numbers for {CHAIN} from {START_DATE} to {END_DATE}...")
FROM_BLOCK = get_block_from_date(MORALIS_CHAIN, START_DATE)
TO_BLOCK = get_block_from_date(MORALIS_CHAIN, END_DATE)
print(f"  FROM_BLOCK: {FROM_BLOCK}")
print(f"  TO_BLOCK: {TO_BLOCK}")

# Output filename uses dates from config
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "raw", "alchemy_api", f"logs_{CHAIN}_{START_DATE}_to_{END_DATE}.jsonl")

# Event topics to fetch from chain config (FilledV3Relay, V3FundsDeposited, RequestedSpeedUpV3Deposit)
EVENT_TOPICS = CHAIN_CONFIG[CHAIN]["topics"]

# Alchemy free tier limit - MUST be 10 for free tier!
BLOCKS_PER_REQUEST = 10

# Save interval in seconds (5 minutes)
SAVE_INTERVAL_SECONDS = 300

# Create persistent session for connection pooling (reuses TCP/SSL connections)
SESSION = requests.Session()
SESSION.headers.update({"Content-Type": "application/json"})


def get_current_block():
    """Fetch current block number."""
    response = SESSION.post(
        ACTIVE_RPC_URL,
        json={"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1},
        timeout=30
    )
    result = response.json()
    return int(result["result"], 16) if "result" in result else None


def get_block_timestamp(block_number: int):
    """Fetch timestamp for a specific block."""
    response = SESSION.post(
        ACTIVE_RPC_URL,
        json={
            "jsonrpc": "2.0", 
            "method": "eth_getBlockByNumber", 
            "params": [hex(block_number), False], 
            "id": 1
        },
        timeout=30
    )
    result = response.json()
    if "result" in result and result["result"]:
        timestamp = int(result["result"]["timestamp"], 16)
        return datetime.fromtimestamp(timestamp)
    return None


def fetch_logs_batch(from_block: int, to_block: int):
    """Fetch logs for a single batch (max 10 blocks on free tier)."""
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getLogs",
        "params": [{
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "address": SPOKEPOOL_ADDRESS,
            "topics": [EVENT_TOPICS]  # Array of topics = OR condition
        }],
        "id": 1
    }
    
    response = SESSION.post(
        ACTIVE_RPC_URL,
        json=payload,
        timeout=30
    )
    return response.json()


def save_logs_to_jsonl(logs: list, filepath: str):
    """Save logs to JSONL file (append mode - one JSON object per line)."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    # Load existing logs if file exists (for deduplication)
    existing_keys = set()
    existing_count = 0
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        log = json.loads(line)
                        key = f"{log.get('transactionHash', '')}-{log.get('logIndex', '')}"
                        existing_keys.add(key)
                        existing_count += 1
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
    
    return existing_count + new_count


def print_batch_progress(batch_count: int, total_batches: int, current: int, batch_end: int, 
                         logs_in_batch: int, total_logs: int, start_time: float, last_timestamp):
    """Display progress information for current batch."""
    elapsed = time.time() - start_time
    pct = (batch_count / total_batches) * 100
    eta = (elapsed / batch_count) * (total_batches - batch_count) / 60 if batch_count > 0 else 0
    
    date_str = last_timestamp.strftime("%Y-%m-%d %H:%M") if last_timestamp else "..."
    print(f"  Batch {batch_count:,}/{total_batches:,} | Blocks {current:,}-{batch_end:,} | {date_str} | {logs_in_batch} logs | Total: {total_logs:,} | {pct:.1f}% | ETA: {eta:.1f}m")


def extract_all_logs(from_block: int, to_block: int):
    """
    Extract ALL logs using batched requests (10 blocks per batch).
    Saves progress every 10 minutes.
    """
    print(f"\n{'='*60}")
    print("Alchemy API - Full Log Extraction")
    print(f"{'='*60}")
    
    if not ALCHEMY_API_KEY:
        print("❌ ERROR: ALCHEMY_API_KEY not found in .env file")
        return []
    
    print(f"\n✓ API Key loaded: {ALCHEMY_API_KEY[:8]}...{ALCHEMY_API_KEY[-4:]}")
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
    print("Extracting all logs...")
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
                
                # Get timestamp from first log in batch (zero API cost!)
                if logs_in_batch > 0:
                    first_log_timestamp = int(logs[0].get('blockTimestamp', '0x0'), 16)
                    last_timestamp = datetime.fromtimestamp(first_log_timestamp)
            
            batch_count += 1
            
            # Display progress
            print_batch_progress(batch_count, total_batches, current, batch_end, 
                               logs_in_batch, len(all_logs), start_time, last_timestamp)
            
            current = batch_end + 1
            
            # Save every 10 minutes
            if time.time() - last_save_time >= SAVE_INTERVAL_SECONDS:
                total_saved = save_logs_to_jsonl(all_logs, OUTPUT_FILE)
                print(f"\n💾 AUTO-SAVE: {total_saved:,} total logs saved to file")
                print(f"   Continuing extraction...\n")
                last_save_time = time.time()
                all_logs = []  # Clear buffer after saving
            
            # Rate limiting
            time.sleep(0.07) # allows for 500CU/s with some margin on Alchemy API
        
        # Final save
        if all_logs:
            total_saved = save_logs_to_jsonl(all_logs, OUTPUT_FILE)
            print(f"\n💾 FINAL SAVE: {total_saved:,} total logs saved")
        
        elapsed = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"✅ COMPLETE! Extraction finished in {elapsed/60:.1f} minutes")
        print(f"{'='*60}")
        
        # Load final count from file (JSONL format)
        final_logs = []
        with open(OUTPUT_FILE, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    final_logs.append(json.loads(line))
        
        # display final results
        if final_logs:
            print(f"\n📊 Final Results:")
            print(f"   First log block: {int(final_logs[0]['blockNumber'], 16):,}")
            print(f"   Last log block: {int(final_logs[-1]['blockNumber'], 16):,}")
            print(f"   Total logs: {len(final_logs):,}")
        
        return final_logs
        
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Request failed: {e}")
        # Save whatever we have on error
        if all_logs:
            total_saved = save_logs_to_jsonl(all_logs, OUTPUT_FILE)
            print(f"💾 EMERGENCY SAVE: {total_saved:,} logs saved before error")
        return []
    except KeyboardInterrupt:
        print(f"\n\n⚠️ Interrupted by user!")
        # Save on Ctrl+C
        if all_logs:
            total_saved = save_logs_to_jsonl(all_logs, OUTPUT_FILE)
            print(f"💾 INTERRUPT SAVE: {total_saved:,} logs saved")
        raise



if __name__ == "__main__":

    print("\n" + "="*60)
    print(f"Alchemy API - {CHAIN.upper()} Mainnet Full Extraction")
    print(f"Date range: {START_DATE} to {END_DATE}")
    print("="*60)
    
    # Extract all logs
    logs = extract_all_logs(FROM_BLOCK, TO_BLOCK)
    
    print(f"\n{'='*60}")
    print("Extraction complete!")
    print(f"{'='*60}\n")
