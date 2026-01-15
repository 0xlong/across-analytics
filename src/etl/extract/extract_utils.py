"""
Extract utilities - shared helper functions for all extractors.
"""

import json
import os
import requests
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

# Import config for API URLs
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import ETL_CONFIG


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
    with open(output_file, "a", encoding="utf-8") as f:
        for log in logs:
            f.write(json.dumps(log) + "\n")
    
    return len(logs)


def date_to_timestamp(date_str: str) -> int:
    """
    Converts date string to Unix timestamp.
    
    Example: "2025-12-02" → 1733097600
    
    Parameters:
    -----------
    date_str : str
        Date in "YYYY-MM-DD" format
    
    Returns:
    --------
    int: Unix timestamp
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return int(dt.timestamp())


def get_chain_params(chain_name: str, json_path: Path = None) -> Optional[Dict[str, Any]]:
    """
    Retrieve all parameters for a specific blockchain chain from the tokens configuration file.
    
    Parameters:
    -----------
    chain_name : str
        Chain name (e.g., "ethereum", "base", "arbitrum")
    json_path : Path, optional
        Path to config JSON. Defaults to standard location.
    
    Returns:
    --------
    Dict or None: Chain configuration dict, or None if chain not found
    """
    # Default path: project_root/data/seeds/tokens_contracts_per_chain.json
    if json_path is None:
        project_root = Path(__file__).parent.parent.parent.parent
        json_path = project_root / "data" / "seeds" / "tokens_contracts_per_chain.json"
    
    with open(json_path, 'r', encoding='utf-8') as file:
        chains_data = json.load(file)
    
    chain_name_lower = chain_name.lower()
    return chains_data.get(chain_name_lower)


def get_block_from_date(chain: str, date: str, moralis_url: str = None) -> int | None:
    """
    Fetch block number for a specific date using Moralis API.
    
    Parameters:
    -----------
    chain : str
        Moralis chain name (e.g., 'bsc', 'eth', 'polygon', 'base', 'optimism')
    date : str
        Date string in format 'YYYY-MM-DD'
    moralis_url : str, optional
        Moralis API base URL. Defaults to value from config.py
    
    Returns:
    --------
    int or None: Block number, or None if request fails
    """
    moralis_api_key = os.getenv("MORALIS_API_KEY")
    if not moralis_api_key:
        print("❌ ERROR: MORALIS_API_KEY not found in .env file")
        return None
    
    if moralis_url is None:
        moralis_url = ETL_CONFIG["moralis_url"]
    
    date_encoded = f"{date}T00%3A00%3A00Z"
    url = f"{moralis_url}/dateToBlock?chain={chain}&date={date_encoded}"
    
    headers = {
        "Accept": "application/json",
        "X-API-Key": moralis_api_key
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            return result.get("block")
        else:
            print(f"❌ Moralis API Error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"❌ Error fetching block from Moralis: {e}")
        return None