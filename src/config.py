"""
ETL Configuration
==================
Central config file for all ETL settings.
Import this directly in your ETL scripts.

Usage:
    from src.config import ETL_CONFIG, PATHS, RUN_CONFIG
    # or
    from config import ETL_CONFIG, PATHS, RUN_CONFIG
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# =============================================================================
# PROJECT PATHS
# =============================================================================

PROJECT_ROOT = Path(__file__).parent.parent

PATHS = {
    "project_root": PROJECT_ROOT,
    "raw_data": PROJECT_ROOT / "data" / "raw" / "etherscan_api",
    "raw_infura": PROJECT_ROOT / "data" / "raw" / "infura_api",
    "processed_data": PROJECT_ROOT / "data" / "processed",
    "seeds": PROJECT_ROOT / "data" / "seeds",
    "logs": PROJECT_ROOT / "logs",
    "chain_config": PROJECT_ROOT / "data" / "seeds" / "tokens_contracts_per_chain.json",
}

# =============================================================================
# EXTRACTION SETTINGS
# =============================================================================

ETL_CONFIG = {
    # Etherscan API settings
    "chunk_size": 10000,          # blocks per API call (Etherscan limit)
    "page_size": 1000,            # records per page (Etherscan max)
    "rate_limit_page": 0.2,       # seconds between pages
    "rate_limit_chunk": 0.25,     # seconds between chunks
    "max_retries": 3,
    "timeout": 30,
    
    # API URLs
    "etherscan_url": "https://api.etherscan.io/v2/api",
}

# =============================================================================
# RUN CONFIGURATION (Edit these for each extraction run)
# =============================================================================

RUN_CONFIG = {
    "chains": ["ethereum", "arbitrum", "base", "polygon", "linea", "worldchain"],
    "start_date": "2025-12-30",
    "end_date": "2026-01-06",
}

# =============================================================================
# API KEYS (from .env file)
# =============================================================================

API_KEYS = {
    "etherscan": os.getenv("ETHERSCAN_API_KEY"),
    "infura": os.getenv("INFURA_API_KEY"),
    "moralis": os.getenv("MORALIS_API_KEY"),
}

# =============================================================================
# VALIDATION
# =============================================================================

def validate_config():
    """Check that required settings are present."""
    errors = []
    
    if not API_KEYS["etherscan"]:
        errors.append("ETHERSCAN_API_KEY not set in .env")
    
    if not PATHS["chain_config"].exists():
        errors.append(f"Chain config not found: {PATHS['chain_config']}")
    
    if errors:
        raise ValueError("Config validation failed:\n" + "\n".join(errors))
    
    return True


# Validate on import (optional - comment out if too strict)
# validate_config()
