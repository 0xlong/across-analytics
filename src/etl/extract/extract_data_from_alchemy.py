"""
Alchemy API - Extract eth_getLogs for Optimism mainnet
Fetches all logs for Across Protocol SpokePool for Jan 6-7, 2026.
NOTE: Free tier limits to 10 blocks per request - uses batching.
Saves progress every 10 minutes to avoid data loss.
"""

import os
import time
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")
ALCHEMY_BASE_URL = f"https://base-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
ALCHEMY_OPTIMISM_URL = f"https://opt-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
ALCHEMY_BINANCE_URL = f"https://bnb-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"

# Load chain configuration from tokens_contracts_per_chain.json
with open(os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "seeds", "tokens_contracts_per_chain.json")) as f:
    CHAIN_CONFIG = json.load(f)

# Across Protocol SpokePool contracts (loaded from config)
SPOKEPOOL_ADDRESS_BASE = CHAIN_CONFIG["base"]["spoke_pool_contract"]
SPOKEPOOL_ADDRESS_OPTIMISM = CHAIN_CONFIG["optimism"]["spoke_pool_contract"]
SPOKEPOOL_ADDRESS_BINANCE = CHAIN_CONFIG["bsc"]["spoke_pool_contract"]

# Active SpokePool address (must match ACTIVE_RPC_URL chain)
SPOKEPOOL_ADDRESS = SPOKEPOOL_ADDRESS_BINANCE

# Event topics to fetch (FilledV3Relay, V3FundsDeposited, RequestedSpeedUpV3Deposit)
EVENT_TOPICS = [
    "0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3",
    "0x44b559f101f8fbcc8a0ea43fa91a05a729a5ea6e14a7c75aa750374690137208",
    "0xf4ad92585b1bc117fbdd644990adf0827bc4c95baeae8a23322af807b6d0020e"]

# Optimism chain block numbers for 06-07.01.2026
FROM_BLOCK = 74207093  # Approximate start of Jan 6, 2026 UTC
TO_BLOCK = 74322271   # Approximate end of Jan 7, 2026 UTC

# Active RPC URL (switch between chains)
ACTIVE_RPC_URL = ALCHEMY_BINANCE_URL

# Alchemy free tier limit - MUST be 10 for free tier!
BLOCKS_PER_REQUEST = 10

# Save interval in seconds (5 minutes)
SAVE_INTERVAL_SECONDS = 300

# Output file path
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "raw", "moralis_api", "binance_logs_jan6_7_2026.jsonl")

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
            
            batch_count += 1
            
            # Get timestamp every 50 batches or on first batch
            if batch_count == 1 or batch_count % 50 == 0:
                last_timestamp = get_block_timestamp(current)
                time.sleep(0.03)  # Small delay for timestamp fetch
            
            # Print every batch with timestamp info
            elapsed = time.time() - start_time
            pct = (batch_count / total_batches) * 100
            eta = (elapsed / batch_count) * (total_batches - batch_count) / 60 if batch_count > 0 else 0
            
            date_str = last_timestamp.strftime("%Y-%m-%d %H:%M") if last_timestamp else "..."
            print(f"  Batch {batch_count:,}/{total_batches:,} | Blocks {current:,}-{batch_end:,} | {date_str} | {logs_in_batch} logs | Total: {len(all_logs):,} | {pct:.1f}% | ETA: {eta:.1f}m")
            
            current = batch_end + 1
            
            # Save every 10 minutes
            if time.time() - last_save_time >= SAVE_INTERVAL_SECONDS:
                total_saved = save_logs_to_jsonl(all_logs, OUTPUT_FILE)
                print(f"\n💾 AUTO-SAVE: {total_saved:,} total logs saved to file")
                print(f"   Continuing extraction...\n")
                last_save_time = time.time()
                all_logs = []  # Clear buffer after saving
            
            # Rate limiting
            time.sleep(0.07) # allows for 500CU/s with some margin on ALchemy API
        
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
        
        if final_logs:
            print(f"\n📊 Final Results:")
            print(f"   First log block: {int(final_logs[0]['blockNumber'], 16):,}")
            print(f"   Last log block: {int(final_logs[-1]['blockNumber'], 16):,}")
            print(f"   Total logs: {len(final_logs):,}")
            
            # Count by topic
            topic_counts = {}
            for log in final_logs:
                topic = log['topics'][0] if log['topics'] else 'unknown'
                topic_counts[topic] = topic_counts.get(topic, 0) + 1
            
            print(f"\n   Logs by event:")
            for topic, count in topic_counts.items():
                event_name = {
                    "0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3": "FilledV3Relay",
                    "0x44b559f101f8fbcc8a0ea43fa91a05a729a5ea6e14a7c75aa750374690137208": "V3FundsDeposited", 
                    "0xf4ad92585b1bc117fbdd644990adf0827bc4c95baeae8a23322af807b6d0020e": "RequestedSpeedUpV3Deposit"
                }.get(topic, topic[:20] + "...")
                print(f"     {event_name}: {count:,}")
        
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
    print("Alchemy API - Optimism Mainnet Full Extraction")
    print("Date range: January 6-7, 2026")
    print("="*60)
    
    # Extract all logs
    logs = extract_all_logs(FROM_BLOCK, TO_BLOCK)
    
    print(f"\n{'='*60}")
    print("Extraction complete!")
    print(f"{'='*60}\n")
