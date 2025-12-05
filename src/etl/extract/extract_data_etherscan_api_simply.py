"""
extract_data_etherscan_api_simply.py

Simple Etherscan API extractor - extracts blockchain event logs and saves to JSONL.

Output Format: JSONL (JSON Lines) - one JSON object per line for memory efficiency.
"""

import requests
import json
import time
import os
from datetime import datetime
from typing import Optional, Dict, Any
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

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


# -----------------------------------------------------------------------------
# User-defined configuration (edit these values to change extraction settings)
# -----------------------------------------------------------------------------
CHAIN_NAME = "HyperEVM"
START_DATE = "2025-12-03"
END_DATE = "2025-12-04"


base_config = {
    "api_url": "https://api.etherscan.io/v2/api",       # Etherscan API endpoint
    "api_key": os.getenv("ETHERSCAN_API_KEY"),          # Loaded from environment variable
    "chain_name": CHAIN_NAME,                           # Chain to extract from (Arbitrum, Ethereum, Base, etc.)
    "start_date": START_DATE,                           # Start date in YYYY-MM-DD format
    "end_date": END_DATE,                               # End date in YYYY-MM-DD format
    "output_dir": os.path.join(PROJECT_ROOT, "data", "raw", "etherscan_api"),  # Where to save JSON files
}

chain_params = get_chain_params(CHAIN_NAME)             # Load ALL chain parameters from JSON config file
CHAIN_ID = chain_params["chain_id"]                     # Extract chain ID (e.g., 1 for Ethereum, 8453 for Base)
CONTRACT_ADDRESS = chain_params["spoke_pool_contract"]  # Extract contract address to monitor
EVENT_TOPICS = chain_params["topics"]                   # Extract event topics (signature hashes) to filter
API_KEY = base_config["api_key"]                        # API key for Etherscan authentication
API_URL = base_config["api_url"]                        # Base URL for Etherscan API requests


# =============================================================================
# CORE FUNCTIONS
# =============================================================================

def api_call(params):
    """
    Makes API call to Etherscan with retry logic.
    
    How it works:
    - Adds API key and chain ID to parameters
    - Sends GET request to Etherscan API
    - Retries 3 times if network fails
    - Returns parsed JSON response
    """
    # Add authentication parameters
    params["chainid"] = CHAIN_ID
    params["apikey"] = API_KEY
    
    # Retry up to 3 times
    for attempt in range(3):
        try:

            # print(f"\nAPI call attempt {attempt + 1}/3") # for debugging
            # Make API request with 30 second timeout
            response = requests.get(API_URL, params=params, timeout=30)
            result = response.json()
            
            # Check if API returned success status
            if result.get("status") == "1":
                return result
            
            # Handle "no records found" as valid empty result
            if "no records found" in result.get("message", "").lower():
                return {"status": "1", "result": []}
            
            # If failed, print error and retry
            print(f"API Error: {result}")
            time.sleep(1)
            
        except Exception as e:
            # Network error - wait and retry
            print(f"Request failed (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                time.sleep(2)
    
    # All retries failed
    return {"status": "0", "result": []}


def date_to_timestamp(date_str):
    """
    Converts date string to Unix timestamp.
    
    Example: "2025-12-02" → 1733097600
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.timestamp())


def get_block_number(timestamp):
    """
    Converts timestamp to blockchain block number.
    
    Why needed: Blockchain data is indexed by blocks, not dates.
    We must convert dates to block numbers to query events.
    """
    params = {
        "module": "block",
        "action": "getblocknobytime",
        "timestamp": timestamp,
        "closest": "before"
    }
    
    result = api_call(params)
    return int(result.get("result", 0))


def get_logs_page(from_block, to_block, topic, page=1):
    """
    Fetches one page of event logs from blockchain.
    
    Parameters:
    - from_block: Starting block number
    - to_block: Ending block number
    - topic: Event topic0 (event signature hash)
    - page: Page number (1-indexed)
    
    Returns: List of log entries for this page
    """
    params = {
        "module": "logs",
        "action": "getLogs",
        "address": CONTRACT_ADDRESS,
        "fromBlock": from_block,
        "toBlock": to_block,
        "topic0": topic,
        "page": page,
        "offset": 1000  # Max 1000 records per page
    }
    
    result = api_call(params)
    return result.get("result", [])


def extract_all_logs(from_block, to_block, topic):
    """
    Extracts all logs using pagination.
    
    How pagination works:
    - API returns max 1000 records per request
    - If we get 1000 records → fetch next page
    - If we get < 1000 records → it's the last page
    - Keep fetching until we get an empty or partial page
    """
    all_logs = []
    page = 1
    
    print(f"Extracting from block {from_block} to {to_block}...")
    
    # Keep fetching pages until we're done
    while True:
        print(f"  Fetching page {page}...", end=" ")
        
        # Get one page of logs
        logs = get_logs_page(from_block, to_block, topic, page)
        
        # If empty page, we're done
        if not logs:
            print("Done (no more logs)")
            break
        
        # Add logs to our collection
        all_logs.extend(logs)
        print(f"Got {len(logs)} logs")
        
        # If partial page (< 1000), it's the last page
        if len(logs) < 1000:
            print("  No more logs - last page")
            break
        
        # Move to next page
        page += 1
        
        # Wait to respect rate limits (5 calls/second)
        time.sleep(0.2)
    
    return all_logs


def append_logs_to_jsonl(logs: list, output_file: str) -> int:
    """
    Appends logs to a JSONL (JSON Lines) file.
    
    Parameters:
    -----------
    logs : list
        List of log dictionaries to append to file
    output_file : str
        Path to the .jsonl file (will be created if doesn't exist)
    
    Returns:
    --------
    int: Number of logs written in this call
    """
    # Open file in append mode ("a")
    # Each call adds new lines without overwriting existing content
    with open(output_file, "a", encoding="utf-8") as f:
        for log in logs:
            # json.dumps() converts dict to compact JSON string (no indent)
            # We add newline (\n) after each log to create one-log-per-line format
            f.write(json.dumps(log) + "\n")
    
    return len(logs)


def airflow_extract_logs(chain_name: str, start_date: str, end_date: str) -> Dict[str, Any]:
    """
    Airflow-compatible extraction function.
    
    Parameters:
    -----------
    chain_name : str
        Blockchain name (e.g., "ETHEREUM", "Base", "Arbitrum")
        Start date in "YYYY-MM-DD" format (Airflow typically passes this via {{ ds }} template) 
    
    end_date : str
        End date in "YYYY-MM-DD" format (Airflow typically passes this via {{ next_ds }} template)
    
    Returns:
    --------
    Dict with extraction metadata (useful for downstream tasks via XCom)
    
    Raises:
    -------
    ValueError: If chain_name not found in config
    Exception: If extraction fails after retries (Airflow will catch and mark task failed)
    """
    
    # -------------------------------------------------------------------------
    # STEP 1: Load chain-specific configuration
    # -------------------------------------------------------------------------
    # Instead of using global CHAIN_ID, CONTRACT_ADDRESS, etc.,
    # we load fresh config based on the chain_name parameter.
    # This makes each run independent and reproducible.
    
    chain_params = get_chain_params(chain_name)
    
    # Validate that chain exists in our config
    if chain_params is None:
        raise ValueError(f"Chain '{chain_name}' not found in tokens_contracts_per_chain.json")
    
    # Extract chain-specific values
    chain_id = chain_params["chain_id"]
    contract_address = chain_params["spoke_pool_contract"]
    event_topics = chain_params["topics"]
    
    # API configuration (these could also be parameters if you want multi-API support)
    api_url = "https://api.etherscan.io/v2/api"
    api_key = os.getenv("ETHERSCAN_API_KEY")
    
    # Validate API key exists
    if not api_key:
        raise ValueError("ETHERSCAN_API_KEY environment variable not set")
    
    print(f"[Airflow] Starting extraction for {chain_name}")
    print(f"[Airflow] Date range: {start_date} to {end_date}")
    print(f"[Airflow] Contract: {contract_address}")
    
    # -------------------------------------------------------------------------
    # STEP 2: Convert dates to block numbers
    # -------------------------------------------------------------------------
    # Blockchain data is indexed by blocks, not dates.
    # We convert dates → timestamps → block numbers.
    
    start_ts = date_to_timestamp(start_date)
    end_ts = date_to_timestamp(end_date)
    
    # Note: These internal calls still use globals for now.
    # For full isolation, you'd pass chain_id/api_key to these functions too.
    # See "FUTURE IMPROVEMENT" comment below.
    start_block = get_block_number(start_ts)
    end_block = get_block_number(end_ts)
    
    print(f"[Airflow] Block range: {start_block} to {end_block}")
    
    # -------------------------------------------------------------------------
    # STEP 3: Prepare output file (JSONL format)
    # -------------------------------------------------------------------------
    # We use JSONL (JSON Lines) format for memory efficiency:
    # - Each log is written immediately after extraction
    # - Memory holds only ONE topic's logs at a time
    # - File can be appended safely (no need to load existing content)
    
    output_file = f"{PROJECT_ROOT}/data/raw/etherscan_api/logs_{chain_name}_{start_date}_to_{end_date}.jsonl"
    
    # Clear file if it exists (fresh extraction for this date range)
    # This prevents duplicate data if re-running the same extraction
    if os.path.exists(output_file):
        os.remove(output_file)
        print(f"[Airflow] Cleared existing file: {output_file}")
    
    # -------------------------------------------------------------------------
    # STEP 4: Extract and save logs PER TOPIC (memory efficient)
    # -------------------------------------------------------------------------
    # Instead of accumulating ALL logs in memory, we:
    # 1. Extract logs for ONE topic
    # 2. Immediately append to JSONL file
    # 3. Let Python garbage collect the topic_logs list
    # 4. Move to next topic
    #
    # Memory usage: O(max_topic_size) instead of O(total_all_topics)
    
    total_logs = 0  # Counter only, not storing actual logs
    
    for topic in event_topics:
        print(f"[Airflow] Extracting topic: {topic[:20]}...")  # Truncate for readability
        
        # Extract logs for this topic (held in memory temporarily)
        topic_logs = extract_all_logs(start_block, end_block, topic)
        
        # Immediately write to file and free memory
        logs_written = append_logs_to_jsonl(topic_logs, output_file)
        total_logs += logs_written
        
        print(f"[Airflow] Saved {logs_written} logs for this topic → {output_file}")
        
        # topic_logs goes out of scope here → garbage collected → memory freed
    
    print(f"[Airflow] Total logs extracted and saved: {total_logs}")
    
    # -------------------------------------------------------------------------
    # STEP 5: Return metadata for Airflow XCom
    # -------------------------------------------------------------------------
    # XCom (cross-communication) lets downstream tasks access this data.
    # Example: A "transform" task can read the output_file path from XCom.
    #
    # In downstream task:
    #   file_path = ti.xcom_pull(task_ids="extract_etherscan_logs")["output_file"]
    
    result = {
        "chain_name": chain_name,
        "chain_id": chain_id,
        "start_date": start_date,
        "end_date": end_date,
        "start_block": start_block,
        "end_block": end_block,
        "total_logs": total_logs,
        "output_file": output_file,
        "format": "jsonl",  # Indicates file format for downstream tasks
        "status": "success"
    }
    
    print(f"[Airflow] Extraction complete!")
    
    return result


# =============================================================================
# FUTURE IMPROVEMENT: Fully parameterized helper functions
# =============================================================================
# Currently, api_call() and get_logs_page() still use global CHAIN_ID, API_KEY, etc.
# For 100% isolation, you'd refactor them to accept these as parameters:
#
#   def api_call_v2(params, chain_id, api_key, api_url):
#       params["chainid"] = chain_id
#       params["apikey"] = api_key
#       response = requests.get(api_url, params=params, timeout=30)
#       ...
#
# This is optional for now since Airflow sets globals before calling the function.
# But it's best practice for testing and parallel execution.
# =============================================================================

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """
    CLI entry point - calls the Airflow-compatible function with global config.
    
    This allows you to:
    - Run directly: python extract_data_etherscan_api_simply.py
    - Use with Airflow: PythonOperator(python_callable=airflow_extract_logs, ...)
    
    Both use the SAME core logic, keeping code DRY (Don't Repeat Yourself).
    """
    # Simply delegate to the Airflow-compatible function
    # Pass the global configuration values as parameters
    result = airflow_extract_logs(
        chain_name=CHAIN_NAME,
        start_date=START_DATE,
        end_date=END_DATE
    )
    
    # Print the result summary (optional, for CLI feedback)
    print(f"\nResult: {result}")


if __name__ == "__main__":
    main()

