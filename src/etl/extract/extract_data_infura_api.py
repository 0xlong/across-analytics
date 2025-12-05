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
from typing import Optional
import os
from dotenv import load_dotenv
load_dotenv()

# =============================================================================
# CONFIGURATION - Edit these values as needed
# =============================================================================

# Your Infura API key (get one at https://infura.io)
INFURA_API_KEY = os.getenv("INFURA_API_KEY")

# Infura endpoint for Arbitrum mainnet
INFURA_URL = f"https://arbitrum-mainnet.infura.io/v3/{INFURA_API_KEY}"

# Across Protocol Arbitrum Spoke Pool contract address
CONTRACT_ADDRESS = "0xe35e9842fceaCA96570B734083f4a58e8F7C5f2A"

# Date range for log extraction (format: YYYY-MM-DD)
START_DATE = "2025-12-02"
END_DATE = "2025-12-03"

# Event topics to extract - array format for Infura JSON-RPC
# Topic0 is the event signature hash
# None = all events, or specify list of topic hashes
TOPICS_TO_EXTRACT = [
    #"0x571749edf1d5c9599318cdbc4e28a6475d65e87fd3b2ddbe1e9a8d5e7a0f0ff7",  # FilledV3Relay
    #"0x44b559f101f8fbcc8a0ea43fa91a05a729a5ea6e14a7c75aa750374690137208",  # FilledRelay
    "0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3",  # FundsDeposited
]


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


def get_block_by_timestamp(timestamp: int) -> int:
    """
    Finds the approximate block number for a given Unix timestamp.
    
    How it works:
    1. Uses binary search between block 0 and latest block
    2. Compares block timestamps to find closest match
    3. Returns block number at or just before the target timestamp
    
    Note: This is an approximation since block times vary.
    
    Args:
        timestamp: Unix timestamp (seconds since epoch)
    
    Returns:
        Block number closest to the timestamp
    """
    # Get the latest block number
    latest_hex = make_rpc_call("eth_blockNumber", [])
    latest_block = int(latest_hex, 16)  # Convert hex string to int
    
    # Binary search parameters
    low = 0
    high = latest_block
    
    while low < high:
        mid = (low + high) // 2
        
        # Get the block at mid position
        block = make_rpc_call("eth_getBlockByNumber", [hex(mid), False])
        
        if block is None:
            low = mid + 1
            continue
        
        # Extract and convert block timestamp
        block_time = int(block["timestamp"], 16)
        
        # Narrow search range based on comparison
        if block_time < timestamp:
            low = mid + 1
        else:
            high = mid
    
    return low


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
    topics: Optional[list] = None
) -> list:
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


def extract_logs_in_chunks(
    contract_address: str,
    start_block: int,
    end_block: int,
    topics: Optional[list] = None,
    chunk_size: int = 100000
) -> list:
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
        chunk_size: Number of BLOCKS per request (not logs!)
                    - 10,000 = conservative, many API calls
                    - 100,000 = balanced for moderate activity
                    - 500,000+ = risky, may hit limits
    
    Returns:
        Combined list of all logs from all chunks
    """
    all_logs = []
    current_block = start_block
    
    print(f"Extracting logs from block {start_block} to {end_block}...")
    
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
            # If chunk is too large, try smaller chunk
            if chunk_size > 1000:
                print(f"    Retrying with smaller chunk size...")
                smaller_logs = extract_logs_in_chunks(
                    contract_address, 
                    current_block, 
                    chunk_end, 
                    topics, 
                    chunk_size // 2
                )
                all_logs.extend(smaller_logs)
        
        # Move to next chunk
        current_block = chunk_end + 1
    
    return all_logs


def format_log_for_display(log: dict) -> dict:
    """
    Formats a log entry for better readability.
    
    How it works:
    1. Converts hex values to decimal where appropriate
    2. Formats timestamps as human-readable dates
    3. Keeps original hex values in separate fields
    
    Args:
        log: Raw log entry from Infura API
    
    Returns:
        Formatted log dictionary with additional decoded fields
    """
    formatted = log.copy()
    
    # Convert hex block number to decimal
    if "blockNumber" in log:
        formatted["blockNumber_decimal"] = int(log["blockNumber"], 16)
    
    # Convert hex timestamp to human-readable date
    if "timeStamp" in log:
        timestamp = int(log["timeStamp"], 16)
        formatted["timeStamp_decimal"] = timestamp
        formatted["timeStamp_readable"] = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")
    
    # Convert gas price and gas used to decimal
    if "gasPrice" in log:
        formatted["gasPrice_gwei"] = int(log["gasPrice"], 16) / 1e9
    
    if "gasUsed" in log:
        formatted["gasUsed_decimal"] = int(log["gasUsed"], 16)
    
    # Convert log index and transaction index to decimal
    if "logIndex" in log:
        formatted["logIndex_decimal"] = int(log["logIndex"], 16)
    
    if "transactionIndex" in log:
        formatted["transactionIndex_decimal"] = int(log["transactionIndex"], 16)
    
    return formatted


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
    print("=" * 60)
    print("Across Arbitrum Spoke Pool - Log Extractor (Infura API)")
    print("=" * 60)
    
    # Validate API key
    if INFURA_API_KEY == "YOUR_INFURA_API_KEY_HERE":
        print("\n⚠️  ERROR: Please set your Infura API key!")
        print("   Edit INFURA_API_KEY in the script or set INFURA_API_KEY env variable")
        return
    
    # Step 1: Convert dates to timestamps
    print(f"\n📅 Date range: {START_DATE} to {END_DATE}")
    start_timestamp = date_to_timestamp(START_DATE)
    end_timestamp = date_to_timestamp(END_DATE)
    print(f"   Start timestamp: {start_timestamp}")
    print(f"   End timestamp: {end_timestamp}")
    
    # Step 2: Find block numbers using binary search
    print("\n🔍 Finding block numbers for date range...")
    start_block = get_block_by_timestamp(start_timestamp)
    end_block = get_block_by_timestamp(end_timestamp)
    print(f"   Start block: {start_block}")
    print(f"   End block: {end_block}")
    
    # Step 3: Display topic filters
    print(f"\n🔎 Topic filters:")
    if TOPICS_TO_EXTRACT:
        for i, topic in enumerate(TOPICS_TO_EXTRACT):
            print(f"   topic{i}: {topic}")
    else:
        print("   None (fetching all events)")
    
    # Step 4: Extract logs in chunks
    print(f"\n📥 Extracting logs from contract: {CONTRACT_ADDRESS}")
    logs = extract_logs_in_chunks(
        contract_address=CONTRACT_ADDRESS,
        start_block=start_block,
        end_block=end_block,
        topics=TOPICS_TO_EXTRACT,
        chunk_size=100000  # Increased: fewer API calls for low-activity contracts
    )
    
    # Step 5: Save results
    # Save raw logs
    output_file = f"logs_infura_{START_DATE}_to_{END_DATE}.json"
    with open(output_file, "w") as f:
        json.dump(logs, f, indent=2)
    
    print(f"\n✅ Done! Extracted {len(logs)} logs")
    print(f"   Saved to: {output_file}")
    
    # Save formatted logs for easier reading
    if logs:
        formatted_logs = [format_log_for_display(log) for log in logs]
        formatted_file = f"logs_infura_{START_DATE}_to_{END_DATE}_formatted.json"
        with open(formatted_file, "w") as f:
            json.dump(formatted_logs, f, indent=2)
        print(f"   Formatted version: {formatted_file}")
        
        # Preview first log (optional - commented out to reduce console output)
        #print("\n📋 Sample log entry (formatted):")
        #print(json.dumps(formatted_logs[0], indent=2))
        
        # Summary statistics
        print(f"\n📊 Summary:")
        print(f"   Total logs: {len(logs)}")
        print(f"   Block range: {start_block} to {end_block}")
        print(f"   Total blocks: {end_block - start_block + 1}")
        print(f"   Logs per block (avg): {len(logs) / (end_block - start_block + 1):.2f}")


if __name__ == "__main__":
    main()

