"""
extract_data_etherscan_api.py

ETL extraction module for Etherscan-compatible APIs.
Saves raw data to JSON files for downstream processing.
"""

import requests
import json
from datetime import datetime
from typing import Optional, Dict, List
import os
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Calculate project root directory (3 levels up from this script)
# This script is at: PROJECT_ROOT/src/etl/extract/extract_data_etherscan_api.py
# We need to go up 3 levels to reach PROJECT_ROOT
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))



# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_chain_id_from_name(chain_name: str, seeds_file: str = None) -> str:
    """
    Converts chain name to chain ID using seeds file.
    
    How it works:
    1. Loads chain_ids.json from data/seeds folder
    2. Searches for matching chain name (case-insensitive)
    3. Returns the chain ID as string
    
    Args:
        chain_name: Name of blockchain (e.g., "Arbitrum", "Ethereum", "Base")
        seeds_file: Path to chain_ids.json file (defaults to PROJECT_ROOT/data/seeds/chain_ids.json)
    
    Returns:
        Chain ID as string (e.g., "42161" for Arbitrum, "1" for Ethereum)
    
    Raises:
        ValueError: If chain name not found in seeds file
    
    Example:
        >>> get_chain_id_from_name("Arbitrum")
        "42161"
        >>> get_chain_id_from_name("ethereum")  # case-insensitive
        "1"
    """
    # Use default path if not provided
    if seeds_file is None:
        seeds_file = os.path.join(PROJECT_ROOT, "data", "seeds", "chain_ids.json")
    
    # Load the seeds file
    with open(seeds_file, 'r') as f:
        chains = json.load(f)
    
    # Search for chain name (case-insensitive)
    chain_name_lower = chain_name.lower()
    
    for chain in chains:
        if chain["chain_name"].lower() == chain_name_lower:
            # Return chain_id as string (API requires string)
            return str(chain["chain_id"])
    
    # Chain not found
    raise ValueError(f"Chain '{chain_name}' not found in {seeds_file}")


def get_contract_address_from_chain(chain_name: str, contracts_file: str = None) -> str:
    """
    Gets Across Spoke Pool contract address for a given chain.
    
    How it works:
    1. Loads across_spoke_pools_contracts.json from data/seeds folder
    2. Looks up contract address by chain name (case-insensitive)
    3. Returns the contract address
    
    Args:
        chain_name: Name of blockchain (e.g., "Arbitrum", "Ethereum", "Base")
        contracts_file: Path to contracts JSON file (defaults to PROJECT_ROOT/data/seeds/across_spoke_pools_contracts.json)
    
    Returns:
        Contract address for the chain (e.g., "0xe35e9842fceaca96570b734083f4a58e8f7c5f2a")
    
    Raises:
        ValueError: If chain name not found in contracts file
    
    Example:
        >>> get_contract_address_from_chain("Arbitrum")
        "0xe35e9842fceaca96570b734083f4a58e8f7c5f2a"
        >>> get_contract_address_from_chain("arbitrum")  # case-insensitive
        "0xe35e9842fceaca96570b734083f4a58e8f7c5f2a"
    """
    # Use default path if not provided
    if contracts_file is None:
        contracts_file = os.path.join(PROJECT_ROOT, "data", "seeds", "across_spoke_pools_contracts.json")
    
    # Load the contracts file
    with open(contracts_file, 'r') as f:
        contracts = json.load(f)
    
    # Look up contract address (case-insensitive key match)
    # Convert input chain_name to lowercase for comparison
    chain_name_lower = chain_name.lower()
    
    # Loop through all contract keys and compare in lowercase
    for key in contracts.keys():
        if key.lower() == chain_name_lower:
            # Return the contract address in lowercase (standardized format)
            return contracts[key].lower()
    
    # Chain not found
    raise ValueError(f"Chain '{chain_name}' not found in {contracts_file}")


# =============================================================================
# CORE API FUNCTIONS
# =============================================================================

def make_api_call(
    api_url: str,
    api_key: str,
    chain_id: str,
    params: dict,
    retry_count: int = 3
) -> dict:
    """
    Makes a REST API call to any Etherscan-compatible API.
    
    How it works:
    1. Adds authentication (API key + chain ID)
    2. Sends GET request with retry logic
    3. Handles rate limiting automatically
    
    Args:
        api_url: Base URL of the API (e.g., https://api.etherscan.io/v2/api)
        api_key: Your API authentication key
        chain_id: Blockchain chain ID (e.g., "42161" for Arbitrum)
        params: Query parameters for the specific API call
        retry_count: Number of retry attempts on failures
    
    Returns:
        Parsed JSON response from API
    """
    # Add required authentication parameters
    params["chainid"] = chain_id
    params["apikey"] = api_key
    
    for attempt in range(retry_count):
        try:
            print(f"API call attempt {attempt + 1}/3")
            # Make GET request with timeout
            response = requests.get(api_url, params=params, timeout=30)
            result = response.json()
            
            # Check API status ("1" = success, "0" = error)
            if result.get("status") == "0":
                error_msg = result.get("message", "Unknown error")
                
                # Handle rate limiting with exponential backoff
                if "rate limit" in error_msg.lower():
                    if attempt < retry_count - 1:
                        wait_time = 5 * (attempt + 1)
                        print(f"    Rate limit hit, waiting {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                
                # "No records found" is valid - return empty result
                if "no records found" in error_msg.lower():
                    return {"status": "1", "message": "OK", "result": []}
                
                raise Exception(f"API Error: {error_msg} | Details: {error_result}")
            
            return result
            
        except requests.exceptions.RequestException as e:
            if attempt < retry_count - 1:
                print(f"    Network error, retrying... ({e})")
                time.sleep(2)
                continue
            raise Exception(f"Network error: {e}")
    
    raise Exception("Max retries exceeded")


def get_block_by_timestamp(
    api_url: str,
    api_key: str,
    chain_id: str,
    timestamp: int
) -> int:
    """
    Converts Unix timestamp to blockchain block number.
    
    Why needed: Blockchain data is indexed by blocks, not timestamps.
    To query "all events on Dec 1st", we must convert dates to block numbers.
    
    Args:
        api_url: Etherscan-compatible API endpoint
        api_key: API authentication key
        chain_id: Chain ID (e.g., "1" = Ethereum, "42161" = Arbitrum)
        timestamp: Unix timestamp (seconds since epoch)
    
    Returns:
        Block number closest to the timestamp
    """
    params = {
        "module": "block",
        "action": "getblocknobytime",
        "timestamp": timestamp,
        "closest": "before"
    }

    result = make_api_call(api_url, api_key, chain_id, params)
    block_number = int(result.get("result", "0"))

    return block_number


def date_to_timestamp(date_str: str) -> int:
    """
    Converts date string to Unix timestamp.
    
    Args:
        date_str: Date in format 'YYYY-MM-DD'
    
    Returns:
        Unix timestamp (seconds since 1970-01-01)
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.timestamp())


def get_logs_page(
    api_url: str,
    api_key: str,
    chain_id: str,
    contract_address: str,
    from_block: int,
    to_block: int,
    topic0: str,
    page: int = 1,
    records_per_page: int = 1000
) -> dict:
    """
    Fetches one page of event logs from blockchain.
    
    What are topics?
    - Topics are indexed event parameters in Ethereum
    - topic0 = event signature hash (identifies which event)
    
    Args:
        api_url: Etherscan-compatible API endpoint
        api_key: API key
        chain_id: Chain ID
        contract_address: Smart contract to query
        from_block: Start block (inclusive)
        to_block: End block (inclusive)
        topic0: Topic0 (event signature hash)
        page: Page number (starts at 1)
        records_per_page: Max records per page (Etherscan max = 1000)
    
    Returns:
        API response with array of log entries
    """
    params = {
        "module": "logs",
        "action": "getLogs",
        "address": contract_address,
        "fromBlock": from_block,
        "toBlock": to_block,
        "page": page,
        "offset": records_per_page,
        "topic0": topic0
    }    
    result = make_api_call(api_url, api_key, chain_id, params)
    return result


def extract_all_logs(
    api_url: str,
    api_key: str,
    chain_id: str,
    contract_address: str,
    from_block: int,
    to_block: int,
    topics: dict,
    records_per_page: int = 1000
) -> list:
    """
    Extracts all logs by handling pagination automatically.
    
    Why pagination? APIs limit response size (usually 1000 records).
    If there are 5000 events, we need 5 requests to get them all.
    
    How it works:
    1. Fetch page 1 → got 1000 records? Fetch page 2
    2. Fetch page 2 → got 1000 records? Fetch page 3
    3. Fetch page 3 → got 500 records? Done (last page)
    
    Args:
        api_url: Etherscan-compatible API endpoint
        api_key: API key
        chain_id: Chain ID
        contract_address: Contract address
        from_block: Start block
        to_block: End block
        topics: Topic filters
        records_per_page: Records per page (max 1000)
    
    Returns:
        Complete list of all log entries
    """
    all_logs = []
    page = 1
    
    print(f"Extracting logs from block {from_block} to {to_block}...")
    
    while True:
        print(f"  Fetching page {page}...")
        
        try:
            response = get_logs_page(
                api_url,
                api_key,
                chain_id,
                contract_address,
                from_block,
                to_block,
                topics.get("topic0", ""),
                page,
                records_per_page
            )
            
            logs = response.get("result", [])
            
            # Empty page = done
            if not logs:
                print(f"    No more logs (empty page)")
                break
            
            all_logs.extend(logs)
            print(f"    Found {len(logs)} logs")
            
            # Partial page = last page
            if len(logs) < records_per_page:
                print(f"    Last page reached")
                break
            
            page += 1
            
            # Respect rate limits (5 calls/second for free tier)
            time.sleep(0.5)
            
        except Exception as e:
            print(f"    Error on page {page}: {e}")
            break
    
    return all_logs


# =============================================================================
# MAIN EXTRACTION FUNCTION (Airflow Task)
# =============================================================================

def extract_logs_to_file(
    api_url: str,
    api_key: str,
    chain_name: str,
    start_date: str,
    end_date: str,
    output_dir: str,
    contract_address: Optional[str] = None,
    topics: Optional[Dict[str, str]] = None,
    seeds_file: str = None,
    contracts_file: str = None
) -> str:
    """
    Main extraction function - Airflow calls this as a task.
    
    This function:
    1. Converts chain name to chain ID using seeds file
    2. Loads contract address for chain (if not provided)
    3. Extracts logs from blockchain
    4. Saves raw data to JSON file
    5. Returns file path for next task
    
    Airflow workflow:
    Task 1 (Extract): extract_logs_to_file() → saves logs_*.json
    Task 2 (Transform): decode_events() → loads logs_*.json, saves decoded_*.json
    Task 3 (Load): load_to_database() → loads decoded_*.json, inserts to DB
    
    Args:
        api_url: Etherscan-compatible API endpoint URL
        api_key: API authentication key
        chain_name: Blockchain name (e.g., "Arbitrum", "Ethereum", "Base")
        start_date: Start date as "YYYY-MM-DD"
        end_date: End date as "YYYY-MM-DD"
        output_dir: Directory to save output file (e.g., "data/raw/etherscan_api")
        contract_address: Optional contract address (auto-loaded from seeds if not provided)
        topics: Optional topic filters for event filtering
        seeds_file: Path to chain_ids.json file (defaults to PROJECT_ROOT/data/seeds/chain_ids.json)
        contracts_file: Path to across_spoke_pools_contracts.json file (defaults to PROJECT_ROOT/data/seeds/across_spoke_pools_contracts.json)
    
    Returns:
        Path to the saved JSON file (for passing to next Airflow task)
    """
    # Set default paths if not provided (allows function to work from any directory)
    if seeds_file is None:
        seeds_file = os.path.join(PROJECT_ROOT, "data", "seeds", "chain_ids.json")
    if contracts_file is None:
        contracts_file = os.path.join(PROJECT_ROOT, "data", "seeds", "across_spoke_pools_contracts.json")
    
    print("=" * 60)
    print("Etherscan API Log Extractor")
    print("=" * 60)
    
    # Step 0: Convert chain name to chain ID and load contract address
    print(f"\n🔗 Chain: {chain_name}")
    try:
        chain_id = get_chain_id_from_name(chain_name, seeds_file)
        print(f"   Chain ID: {chain_id}")
    except ValueError as e:
        print(f"   Error: {e}")
        raise
    
    # Load contract address if not provided
    if not contract_address:
        try:
            contract_address = get_contract_address_from_chain(chain_name, contracts_file)
            print(f"   Contract (auto-loaded): {contract_address}")
        except ValueError as e:
            print(f"   Error: {e}")
            raise
    else:
        print(f"   Contract: {contract_address}")
    
    # Step 1: Convert dates to timestamps
    print(f"\n📅 Date range: {start_date} to {end_date}")
    start_timestamp = date_to_timestamp(start_date)
    end_timestamp = date_to_timestamp(end_date)
    print(f"   Start timestamp: {start_timestamp}")
    print(f"   End timestamp: {end_timestamp}")
    
    # Step 2: Find block numbers using API
    print("\n🔍 Finding block numbers for date range...")
    try:
        start_block = get_block_by_timestamp(api_url, api_key, chain_id, start_timestamp)
        end_block = get_block_by_timestamp(api_url, api_key, chain_id, end_timestamp)
        print(f"   Start block: {start_block}")
        print(f"   End block: {end_block}")
    except Exception as e:
        print(f"   Error finding blocks: {e}")
        raise
    
    # Step 3: Display topic filters
    print(f"\n🔎 Topic filters:")
    if topics:
        for key, value in topics.items():
            print(f"   {key}: {value}")
    else:
        print("   None (fetching all events)")
    
    # Step 4: Extract logs with pagination
    print(f"\n📥 Extracting logs from contract: {contract_address}")
    print(f"   Chain ID: {chain_id}")
    
    try:
        logs = extract_all_logs(
            api_url=api_url,
            api_key=api_key,
            chain_id=chain_id,
            contract_address=contract_address,
            from_block=start_block,
            to_block=end_block,
            topics=topics or {}
        )
    except Exception as e:
        print(f"\n❌ Error extracting logs: {e}")
        raise
    
    # Step 5: Create output directory if needed
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 6: Build filename using chain_name (lowercase for consistency)
    chain_name_lower = chain_name.lower()
    output_filename = f"logs_{chain_name_lower}_{start_date}_to_{end_date}.json"
    output_path = os.path.join(output_dir, output_filename)
    
    # Step 7: Save raw logs to file
    with open(output_path, "w") as f:
        json.dump(logs, f, indent=2)
    
    # Step 8: Display summary
    print(f"\n  Done! Extracted {len(logs)} logs")
    print(f"   Saved to: {output_path}")
    print(f"\n   Summary:")
    print(f"   Total logs: {len(logs)}")
    print(f"   Block range: {start_block} to {end_block}")
    print(f"   Total blocks: {end_block - start_block + 1}")
    if end_block > start_block:
        print(f"   Logs per block (avg): {len(logs) / (end_block - start_block + 1):.2f}")
    
    # Return file path for next Airflow task
    # Next task can use this path: ti.xcom_pull(task_ids='extract_logs')
    return output_path


# =============================================================================
# STANDALONE EXECUTION (for testing without Airflow)
# =============================================================================

def main():
    """
    Standalone execution with multiple topic0 values.
    
    This function runs extractions for three different event types.
    Each topic0 value represents a different smart contract event.
    Contract address is automatically loaded based on chain name.
    
    How it works:
    1. Loads API key from environment variable (ETHERSCAN_API_KEY)
    2. Defines base configuration (chain, dates, output directory)
    3. Loops through three topic0 values
    4. Extracts and saves logs for each event type separately
    5. Each extraction creates its own JSON file
    """
    
    # Base configuration (shared across all three extractions)
    # Modify these values for your specific use case
    base_config = {
        "api_url": "https://api.etherscan.io/v2/api",  # Etherscan API endpoint
        "api_key": os.getenv("ETHERSCAN_API_KEY"),  # Loaded from environment
        "chain_name": "base",  # Chain to extract from (Arbitrum, Ethereum, Base, etc.)
        "start_date": "2025-12-03",  # Start date in YYYY-MM-DD format
        "end_date": "2025-12-04",    # End date in YYYY-MM-DD format
        "output_dir": os.path.join(PROJECT_ROOT, "data", "raw", "etherscan_api"),  # Where to save JSON files (absolute path)
    }
    
    # Define your three topic0 values
    # Each topic0 identifies a different smart contract event type
    # Replace these with your actual event signature hashes
    topic0_list = [
        {
            "name": "FundsDeposited", # event occurs when a user deposits funds into Across Protocol
            "topic0": "0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3"
        },
        {
            "name": "ExecutedRelayerRefundRoot", # event occurs when a relayer gets a refund from Across Protocol
            "topic0": "0xf4ad92585b1bc117fbdd644990adf0827bc4c95baeae8a23322af807b6d0020e"
        },
        {
            "name": "FilledRelay", # event occurs when a relayer fills a deposit of an user
            "topic0": "0x44b559f101f8fbcc8a0ea43fa91a05a729a5ea6e14a7c75aa750374690137208"
        }
    ]
    
    # Run extraction for each topic0 value sequentially
    print(f"\n{'='*60}")
    print(f"STARTING MULTI-TOPIC EXTRACTION")
    print(f"Chain: {base_config['chain_name']}")
    print(f"Date Range: {base_config['start_date']} to {base_config['end_date']}")
    print(f"Total Extractions: {len(topic0_list)}")
    print(f"{'='*60}")
    
    for i, topic_config in enumerate(topic0_list, 1):
        print(f"\n{'='*60}")
        print(f"EXTRACTION {i}/{len(topic0_list)}: {topic_config['name']}")
        print(f"Topic0: {topic_config['topic0']}")
        print(f"{'='*60}")
        
        # Create config for this specific extraction
        # Each extraction gets its own topic filter
        config = base_config.copy()
        config["topics"] = {"topic0": topic_config["topic0"]}
        
        try:
            # Run extraction (automatically handles pagination and saves to file)
            output_path = extract_logs_to_file(**config)
            print(f"\n✅ {topic_config['name']} extraction complete!")
            print(f"   File saved: {output_path}")
        except Exception as e:
            print(f"\n❌ {topic_config['name']} extraction failed: {e}")
            # Continue to next extraction even if one fails
            continue
    
    print(f"\n{'='*60}")
    print("ALL EXTRACTIONS COMPLETE")
    print(f"Check output directory: {base_config['output_dir']}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
