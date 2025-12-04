"""
extract_data_arbiscan_api.py

Extract eth_getLogs from Across Spoke Pool contracts using Arbiscan API (Etherscan for Arbitrum).

Key differences from Infura API:
- Uses REST API instead of JSON-RPC
- Built-in pagination (max 1000 records per page)
- No need for block chunking - API handles it
- Simpler topic filtering with topic0, topic1, etc.
"""

import requests
import json
from datetime import datetime
from typing import Optional
import os
import time

# Your Arbiscan API key (get one at https://arbiscan.io/myapikey)
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
print(f"ETHERSCAN_API_KEY: {ETHERSCAN_API_KEY}")
# Arbiscan API endpoint for Arbitrum mainnet
# Note: Arbiscan uses Etherscan's v2 API format with chainid parameter
ETHERSCAN_API_URL = "https://api.etherscan.io/v2/api"

# Chain ID for Arbitrum mainnet (required in all API calls)
CHAIN_ID = "42161"

# Across Protocol Arbitrum Spoke Pool contract address
CONTRACT_ADDRESS = "0xe35e9842fceaCA96570B734083f4a58e8F7C5f2A"

# Date range for log extraction (format: YYYY-MM-DD)
START_DATE = "2025-12-02"
END_DATE = "2025-12-03"

# Event topic (event signature hash) to extract - only one topic at a time allowed
TOPICS_CONFIG = {
    "topic0": "0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3"   # FundsDeposited
}

# Pagination settings - limits to 1000 records per request
RECORDS_PER_PAGE = 1000


def make_api_call(params: dict, retry_count: int = 3) -> dict:
    """
    Makes a REST API call to Arbiscan.
    
    How it works:
    1. Constructs URL with query parameters
    2. Sends GET request to Arbiscan endpoint
    3. Parses JSON response
    4. Retries on rate limit errors
    
    Args:
        params: Dictionary of query parameters
        retry_count: Number of retries on rate limit errors
    
    Returns:
        The full JSON response from Arbiscan
        
    Raises:
        Exception: If API returns error status
    """
    # Add required parameters for v2 API
    # chainid is required for Etherscan v2 API format
    params["chainid"] = CHAIN_ID
    params["apikey"] = ARBISCAN_API_KEY
    
    for attempt in range(retry_count):
        try:
            # Make GET request with query parameters
            response = requests.get(ARBISCAN_URL, params=params, timeout=30)
            
            # Parse JSON response
            result = response.json()
            
            # Check API status
            # Status "1" = success, "0" = error
            if result.get("status") == "0":
                error_msg = result.get("message", "Unknown error")
                
                # Handle rate limiting (wait and retry)
                if "rate limit" in error_msg.lower():
                    if attempt < retry_count - 1:
                        wait_time = 5 * (attempt + 1)  # Exponential backoff
                        print(f"    Rate limit hit, waiting {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                
                # Handle "No records found" as valid empty result
                if "no records found" in error_msg.lower():
                    return {"status": "1", "message": "OK", "result": []}
                
                raise Exception(f"Arbiscan API Error: {error_msg}")
            
            return result
            
        except requests.exceptions.RequestException as e:
            if attempt < retry_count - 1:
                print(f"    Network error, retrying... ({e})")
                time.sleep(2)
                continue
            raise Exception(f"Network error: {e}")
    
    raise Exception("Max retries exceeded")


def get_block_by_timestamp(timestamp: int) -> int:
    """
    Gets the block number closest to a given timestamp using Arbiscan API.
    
    How it works:
    1. Uses Arbiscan's getblocknobytime endpoint
    2. Automatically finds the closest block to timestamp
    3. Much simpler than binary search used in Infura version
    
    API module: block
    API action: getblocknobytime
    
    Args:
        timestamp: Unix timestamp (seconds since epoch)
    
    Returns:
        Block number closest to the timestamp
    """
    params = {
        "module": "block",                    # Block-related queries
        "action": "getblocknobytime",         # Get block by timestamp
        "timestamp": timestamp,               # Unix timestamp
        "closest": "before"                   # Get block before or at timestamp
    }
    
    result = make_api_call(params)
    
    # Result is the block number as string
    block_number = int(result.get("result", "0"))
    
    return block_number


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


def get_logs_page(
    contract_address: str,
    from_block: int,
    to_block: int,
    topics: dict,
    page: int = 1
) -> dict:
    """
    Fetches one page of event logs using Arbiscan API.
    
    How it works:
    1. Builds query parameters for getLogs endpoint
    2. Adds topic filters if specified
    3. Uses pagination (max 1000 records per page)
    4. Returns raw API response with logs
    
    API module: logs
    API action: getLogs
    
    Args:
        contract_address: The smart contract address to query
        from_block: Starting block number
        to_block: Ending block number
        topics: Dictionary of topic filters (topic0, topic1, etc.)
        page: Page number for pagination (starts at 1)
    
    Returns:
        Full API response containing status, message, and result array
    """
    # Base parameters for getLogs
    params = {
        "module": "logs",                     # Logs-related queries
        "action": "getLogs",                  # Get event logs
        "address": contract_address,          # Contract to query
        "fromBlock": from_block,              # Start block (decimal or hex)
        "toBlock": to_block,                  # End block (decimal or hex)
        "page": page,                         # Page number (1-indexed)
        "offset": RECORDS_PER_PAGE           # Records per page (max 1000)
    }
    
    # Add topic filters if specified
    # Topics follow Ethereum event structure:
    # - topic0: Event signature (keccak256 of event definition)
    # - topic1-3: Indexed event parameters
    # - Operators: "and"/"or" between topics for filtering logic
    if topics:
        for key, value in topics.items():
            if value is not None:
                params[key] = value
    
    # Make the API call
    result = make_api_call(params)
    
    return result


def extract_all_logs(
    contract_address: str,
    from_block: int,
    to_block: int,
    topics: dict
) -> list:
    """
    Extracts all logs by paginating through results.
    
    How it works:
    1. Fetches first page of logs
    2. If page has 1000 records (the max), fetches next page
    3. Continues until page has < 1000 records (last page)
    4. Combines all pages into single list
    
    Why pagination:
    - Arbiscan limits responses to 1000 records per request
    - If result count = 1000, there might be more records
    - Must fetch all pages to get complete dataset
    
    Args:
        contract_address: The smart contract address
        from_block: Starting block number
        to_block: Ending block number
        topics: Dictionary of topic filters
    
    Returns:
        Combined list of all log entries from all pages
    """
    all_logs = []
    page = 1
    
    print(f"Extracting logs from block {from_block} to {to_block}...")
    
    while True:
        print(f"  Fetching page {page}...")
        
        try:
            # Get one page of logs
            response = get_logs_page(
                contract_address,
                from_block,
                to_block,
                topics,
                page
            )
            
            # Extract log entries from response
            logs = response.get("result", [])
            
            if not logs:
                print(f"    No more logs (empty page)")
                break
            
            all_logs.extend(logs)
            print(f"    Found {len(logs)} logs")
            
            # If we got fewer than max records, this is the last page
            if len(logs) < RECORDS_PER_PAGE:
                print(f"    Last page reached")
                break
            
            # Move to next page
            page += 1
            
            # Small delay to avoid rate limiting (max 5 calls per second)
            time.sleep(0.2)
            
        except Exception as e:
            print(f"    Error on page {page}: {e}")
            break
    
    return all_logs


def format_log_for_display(log: dict) -> dict:
    """
    Formats a log entry for better readability.
    
    How it works:
    1. Converts hex values to decimal where appropriate
    2. Formats timestamps as human-readable dates
    3. Keeps original hex values in separate fields
    
    Args:
        log: Raw log entry from Arbiscan API
    
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
    3. Find corresponding block numbers using Arbiscan API
    4. Extract all logs with automatic pagination
    5. Save results to JSON file with formatted output
    """
    print("=" * 60)
    print("Across Arbitrum Spoke Pool - Log Extractor (Arbiscan API)")
    print("=" * 60)
    
    # Validate API key
    if ARBISCAN_API_KEY == "YOUR_ARBISCAN_API_KEY_HERE":
        print("\n⚠️  ERROR: Please set your Arbiscan API key!")
        print("   Get one at: https://arbiscan.io/myapikey")
        print("   Then edit ARBISCAN_API_KEY in the script or set ARBISCAN_API_KEY env variable")
        return
    
    # Step 1: Convert dates to timestamps
    print(f"\n📅 Date range: {START_DATE} to {END_DATE}")
    start_timestamp = date_to_timestamp(START_DATE)
    end_timestamp = date_to_timestamp(END_DATE)
    print(f"   Start timestamp: {start_timestamp}")
    print(f"   End timestamp: {end_timestamp}")
    
    # Step 2: Find block numbers using Arbiscan API
    print("\n🔍 Finding block numbers for date range...")
    try:
        start_block = get_block_by_timestamp(start_timestamp)
        end_block = get_block_by_timestamp(end_timestamp)
        print(f"   Start block: {start_block}")
        print(f"   End block: {end_block}")
    except Exception as e:
        print(f"   Error finding blocks: {e}")
        return
    
    # Step 3: Display topic filters
    print(f"\n🔎 Topic filters:")
    active_topics = {k: v for k, v in TOPICS_CONFIG.items() if v is not None}
    if active_topics:
        for key, value in active_topics.items():
            print(f"   {key}: {value}")
    else:
        print("   None (fetching all events)")
    
    # Step 4: Extract logs with pagination
    print(f"\n📥 Extracting logs from contract: {CONTRACT_ADDRESS}")
    try:
        logs = extract_all_logs(
            contract_address=CONTRACT_ADDRESS,
            from_block=start_block,
            to_block=end_block,
            topics=TOPICS_CONFIG
        )
    except Exception as e:
        print(f"\n❌ Error extracting logs: {e}")
        return
    
    # Step 5: Save results
    # Save raw logs
    output_file = f"logs_{CHAIN_ID}_{START_DATE}_to_{END_DATE}.json"
    with open(output_file, "w") as f:
        json.dump(logs, f, indent=2)
    
    print(f"\n✅ Done! Extracted {len(logs)} logs")
    print(f"   Saved to: {output_file}")
    
    # Save formatted logs for easier reading
    if logs:
        formatted_logs = [format_log_for_display(log) for log in logs]
        formatted_file = f"logs_{CHAIN_ID}_{START_DATE}_to_{END_DATE}_formatted.json"
        with open(formatted_file, "w") as f:
            json.dump(formatted_logs, f, indent=2)
        print(f"   Formatted version: {formatted_file}")
        
        # Preview first log
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