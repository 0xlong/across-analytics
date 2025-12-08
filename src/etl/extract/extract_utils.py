import requests
import os
import json
from datetime import datetime, UTC
from dotenv import load_dotenv
load_dotenv()

MORALIS_API_KEY = os.getenv("MORALIS_API_KEY")

def get_block_by_timestamp_using_moralis(timestamp: int, chain_name: str) -> int:
    """
    Get block number for a timestamp using Moralis API.
    Args:
        timestamp:  (format is 2025-12-03:00:00Z)
        chain_name: str (e.g. "bsc")
    Returns:
        int
    """
    url = f"https://deep-index.moralis.io/api/v2.2/dateToBlock?chain={chain_name}&date={str(timestamp)[:10]}"
    headers = {
        "Accept": "application/json",
        "X-API-Key": os.getenv("MORALIS_API_KEY")
    }

    try:
        response = requests.request("GET", url, headers=headers)
        #print(response.json())
        return response.json()["block"]
    except Exception as e:
        print(f"Error getting block number by timestamp with Moralis API: {e}")
        return None


def save_logs_to_jsonl(logs: list, output_file: str) -> int:
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


# get timestamp by block number hash with infura api eth_getBlockByNumber
def get_timestamp_by_block_number_hash_with_infura(block_number_hash: str, chain_name: str) -> int:
    """
    Get timestamp by block number hash with infura api eth_getBlockByNumber
    Args:
        block_number_hash: str
        chain_name: str
    Returns:
        int
    """
    url = f"https://{chain_name}-mainnet.infura.io/v3/{os.getenv('INFURA_API_KEY')}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [block_number_hash, True],
        "id": 1
    }
    try:
        response = requests.post(url, headers=headers, json=payload)
        timestamp_in_hex = response.json()["result"]["timestamp"]
        timestamp_unix = int(timestamp_in_hex, 16)
        timestamp_datetime = datetime.fromtimestamp(timestamp_unix, UTC).strftime("%d-%m-%Y %H:%M:%S")
        return timestamp_datetime
    except Exception as e:
        print(f"Error getting timestamp by block number hash with infura api: {e}")
        return None

# example usage:
#timestamp_infura = get_timestamp_by_block_number_hash_with_infura("0x89e7378", "optimism")
#print(timestamp_infura)