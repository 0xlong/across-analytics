"""
extract_data_infura_api.py

Extract eth_getLogs from Across Spoke Pool contracts using Infura API (JSON-RPC).

Key characteristics of Infura API:
- Uses JSON-RPC protocol (eth_getLogs, eth_getBlockByNumber, etc.)
- Requires manual binary search for timestamp-to-block conversion
- Manual chunking by block ranges to avoid RPC limits
- Topic filters as array format
"""

import requests
import json
from datetime import datetime
from typing import Optional, Dict, Any
from pathlib import Path
import os
from dotenv import load_dotenv
load_dotenv()

#import helper functions
from extract_utils import get_block_by_timestamp_using_moralis as get_block_by_timestamp
from extract_utils import save_logs_to_jsonl

# project root directory (3 levels up from this script)
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

def get_chain_params(chain_name: str, json_path: str = os.path.join(PROJECT_ROOT, "data", "seeds", "tokens_contracts_per_chain.json")) -> Optional[Dict[str, Any]]:
    """
    Retrieve all parameters for a specific blockchain chain from the tokens configuration file.
    
    """
    # Load the JSON configuration file
    config_path = Path(json_path)
    with open(config_path, 'r', encoding='utf-8') as file:
        chains_data = json.load(file)
    
    # Convert the input chain_name to lowercase for case-insensitive matching
    chain_name_lower = chain_name.lower()
    
    # Retrieve the chain parameters using the lowercase key
    chain_params = chains_data.get(chain_name_lower)
    
    return chain_params

def make_rpc_call(method: str, params: list) -> dict:
    """
    Makes a JSON-RPC call to the Infura API.
    
    How it works:
    1. Constructs a JSON-RPC 2.0 request with method and parameters
    2. Sends POST request to Infura endpoint
    3. Returns the result or raises error if failed
    
    Args:
        method: The Ethereum RPC method (e.g., 'eth_getLogs', 'eth_blockNumber')
        params: List of parameters for the method
    
    Returns:
        The 'result' field from the JSON-RPC response
    """
    # JSON-RPC 2.0 standard payload structure
    payload = {
        "jsonrpc": "2.0",      # Protocol version
        "method": method,       # RPC method to call
        "params": params,       # Method parameters
        "id": 1                 # Request identifier (can be any number)
    }
    
    # Required headers for JSON content
    headers = {"Content-Type": "application/json"}
    
    # Make the POST request to Infura
    response = requests.post(INFURA_URL, json=payload, headers=headers)
    
    # Parse JSON response
    result = response.json()
    # Check for RPC errors
    if "error" in result:
        raise Exception(f"RPC Error: {result['error']}")
    
    return result.get("result")

def date_to_timestamp(date_str: str) -> int:
    """
    Converts a date string to Unix timestamp.
    
    Args:
        date_str: Date in format 'YYYY-MM-DD'
    
    Returns:
        Unix timestamp (seconds since 1970-01-01)
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.timestamp())

def get_logs(
    contract_address: str,
    from_block: int,
    to_block: int,
    topics: Optional[list] = None) -> list:
    """
    Fetches event logs from the blockchain using eth_getLogs.
    
    How it works:
    1. Constructs a filter object with address and block range
    2. Optionally adds topic filters for specific events
    3. Calls eth_getLogs RPC method
    4. Returns list of matching log entries
    
    Args:
        contract_address: The smart contract address to query
        from_block: Starting block number
        to_block: Ending block number
        topics: Optional list of event topic hashes to filter
    
    Returns:
        List of log objects containing event data
    """
    # Build the filter object for eth_getLogs
    # Block numbers must be hex strings for the RPC call
    filter_params = {
        "address": contract_address,           # Contract to query
        "fromBlock": hex(from_block),          # Start block (hex)
        "toBlock": hex(to_block)               # End block (hex)
    }
    
    # Add topics filter if specified
    # Topics array: [event_signature, indexed_param1, indexed_param2, ...]
    if topics:
        filter_params["topics"] = topics
    
    # Execute the RPC call
    logs = make_rpc_call("eth_getLogs", [filter_params])
    
    return logs if logs else []

def extract_logs(
    contract_address: str,
    start_block: int,
    end_block: int,
    topics: Optional[list] = None,
    chunk_size: int = 1000) -> list:
    """
    Extracts logs in smaller chunks to avoid RPC limits.
    
    Why chunking by BLOCKS (not logs):
    - Infura limits block RANGE per request (not log count)
    - You can't know log count until you query
    - Large block ranges timeout regardless of result size
    
    Args:
        contract_address: The smart contract address
        start_block: Starting block number
        end_block: Ending block number  
        topics: Optional event topic filters
        chunk_size: 1000 blocks per request (max limit with Infura API otherwise RPC Error: {'code': -32005, 'message': 'Exceeded max range limit for eth_getLogs: 1000'})
    
    Returns:
        Combined list of all logs from all chunks
    """
    all_logs = []
    current_block = start_block
    
    print(f"Extracting logs from block range {start_block} to {end_block}...")
    
    while current_block <= end_block:
        # Calculate chunk end (don't exceed final block)
        chunk_end = min(current_block + chunk_size - 1, end_block)
        
        print(f"  Fetching blocks {current_block} - {chunk_end}...")
        
        try:
            # Fetch logs for this chunk
            logs = get_logs(contract_address, current_block, chunk_end, topics)
            all_logs.extend(logs)
            print(f"    Found {len(logs)} logs")
            
        except Exception as e:
            print(f"    Error: {e}")
        
        # Move to next chunk
        current_block = chunk_end + 1
    
    return all_logs


# =============================================================================
# CONFIGURATION - Edit these values as needed
# =============================================================================

# Your Infura API key (get one at https://infura.io)
INFURA_API_KEY = os.getenv("INFURA_API_KEY")

# chains available for infura api
chains_available_for_infura = ["base", "optimism", "polygon", "bsc", "zksync", "linea", "zora", "scroll", "blast", "mode", "redstone", "unichain", "worldchain", "mona"]

# Infura endpoint for Arbitrum mainnet
INFURA_URL = f"https://{chain_name}-mainnet.infura.io/v3/{INFURA_API_KEY}"

# Date range for log extraction (format: YYYY-MM-DD)
START_DATE = "2025-12-03"
END_DATE = "2025-12-04"

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """
    Main function that orchestrates the log extraction process.
    
    Flow:
    1. Validate API key
    2. Convert date strings to Unix timestamps
    3. Find corresponding block numbers using binary search
    4. Extract logs in chunks from the block range
    5. Save results to JSON files (raw + formatted versions)
    """
    # configuration
    chain_params = get_chain_params(chain_name)

    CHAIN_ID = chain_params["chain_id"]
    CONTRACT_ADDRESS = chain_params["spoke_pool_contract"]
    event_topics = chain_params["topics"]
    print("event_topics: ", event_topics)
    print("=" * 60)
    print(f"Across {chain_name} Spoke Pool - Log Extractor (Infura API)")
    print("=" * 60)
    
    
    # Step 1: Convert dates to timestamps
    print(f"\n📅 Date range: {START_DATE} to {END_DATE}")
    start_timestamp = date_to_timestamp(START_DATE)
    end_timestamp = date_to_timestamp(END_DATE)
    print(f"   Start timestamp: {start_timestamp}")
    print(f"   End timestamp: {end_timestamp}")
    
    # Step 2: Find block numbers using binary search
    print("\n🔍 Finding block numbers for date range...")
    start_block = get_block_by_timestamp(start_timestamp, chain_name)
    end_block = get_block_by_timestamp(end_timestamp, chain_name)
    print(f"   Start block: {start_block}")
    print(f"   End block: {end_block}")
    

    # Step 4: Extract logs in chunks
    print(f"\n📥 Extracting logs from contract: {CONTRACT_ADDRESS}")
    for topic in event_topics:
        print(f"   Extracting topic: {topic}")
        logs = extract_logs(
            contract_address=CONTRACT_ADDRESS,
            start_block=start_block,
            end_block=end_block,
            topics=[topic],
            chunk_size=1000 
        )
        print(f"   Extracted {len(logs)} logs for topic: {topic}")

        # save logs to jsonl
        output_file = f"{PROJECT_ROOT}/data/raw/infura_api/logs_{chain_name.lower()}_{START_DATE}_to_{END_DATE}.jsonl"
        saved_logs = save_logs_to_jsonl(logs, output_file)
        print(f"   Saved {saved_logs} logs to: {output_file}")

if __name__ == "__main__":
    main()

