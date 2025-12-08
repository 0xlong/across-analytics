import os
import json
from pathlib import Path
from typing import Optional, Dict, Any
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