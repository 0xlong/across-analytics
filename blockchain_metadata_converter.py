"""
Blockchain Metadata Converter
=============================
Converts raw blockchain data to human-readable format:
- chainId → chainName (e.g., 1 → "Ethereum")
- tokenAddress → tokenName (e.g., 0xa0b8... → "USDC")
- rawAmount → convertedAmount (e.g., 1000000 → 1.0 for 6 decimals)

The converter uses lookup dictionaries for O(1) efficiency.
Token decimals are used to convert raw amounts (smallest unit) to human-readable values.
"""

import pandas as pd
import os
from typing import Dict, Tuple, Optional
from decimal import Decimal


# =============================================================================
# CHAIN ID → CHAIN NAME MAPPING
# =============================================================================
# Maps blockchain network chain IDs to human-readable names.
CHAIN_NAMES: Dict[int, str] = {

    1: "Ethereum",
    10: "Optimism",
    56: "BNB Chain", 
    100: "Gnosis",
    137: "Polygon",
    143: "Monad",
    250: "Fantom",
    288: "Boba",
    324: "zkSync Era",
    480: "Worldchain",
    999: "HypepEVM",
    1101: "Polygon zkEVM",
    1135: "Lisk",
    5000: "Mantle",
    7560: "Cyber",
    7777777: "Zora",
    8453: "Base",
    34443: "Mode",
    42161: "Arbitrum",
    42170: "Arbitrum Nova",
    43114: "Avalanche",
    57073: "Ink",
    59144: "Linea",
    81457: "Blast",
    534352: "Scroll",
    660279: "Xai",
    2741: "Abstract",
    130: "Unichain",
    34268394551451: "Solana",
    232: "Lens",
    9745: "Plasma",
    690: "Redstone",
    1868: "Soneium"
}

# Verification of data coverage:
# Original 'CHAIN_NAMES' contains 29 entries.
# Provided 'data' contains 24 entries.
# The reformatted dictionary combines all unique key-value pairs from both
# 'CHAIN_NAMES' and 'data', resolving name differences for the same Chain ID.
# (e.g., 56: "BNB Smart Chain" from data becomes 56: "BNB Chain" from original, 
# but 56 is only listed once).
# New unique entries added from 'data': Lens (232), Plasma (9745), 
# Redstone (690), Soneium (1868).


# =============================================================================
# TOKEN METADATA: (symbol, decimals) PER CHAIN
# =============================================================================
# Structure: { chainId: { tokenAddress (lowercase): (symbol, decimals) } }
# 
# Why nested by chain? Same token on different chains has different addresses.
# Example: USDC on Ethereum is 0xa0b86991..., but on Arbitrum it's 0xaf88d065...
#
# Decimals explanation:
# - Most tokens use 18 decimals (like ETH)
# - Stablecoins (USDC, USDT) typically use 6 decimals
# - WBTC uses 8 decimals (matching Bitcoin's 8 decimal places)
# 
# To convert: human_amount = raw_amount / (10 ** decimals)
# Example: 1000000 USDC raw → 1000000 / 10^6 = 1.0 USDC

TOKEN_METADATA: Dict[int, Dict[str, Tuple[str, int]]] = {
    # =========================================================================
    # ETHEREUM (Chain ID: 1)
    # =========================================================================
    1: {
        # Stablecoins
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": ("USDC", 6),
        "0xdac17f958d2ee523a2206206994597c13d831ec7": ("USDT", 6),
        "0x6b175474e89094c44da98b954eedeac495271d0f": ("DAI", 18),
        # Wrapped ETH
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": ("WETH", 18),
        # Wrapped BTC
        "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": ("WBTC", 8),
        # Liquid Staking Derivatives (LSDs)
        "0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0": ("wstETH", 18),
        "0xae78736cd615f374d3085123a210448e74fc6393": ("rETH", 18),
        "0xbe9895146f7af43049ca1c1ae358b0541ea49704": ("cbETH", 18),
        # Other major tokens
        "0x44108f0223a3c3028f5fe7aec7f9bb2e66bef82f": ("ACX", 18),
        "0x04c17b9d3b29a78f7bd062a57cf44fc633e71f85": ("POOL", 18),
    },
    
    # =========================================================================
    # OPTIMISM (Chain ID: 10)
    # =========================================================================
    10: {
        "0x0b2c639c533813f4aa9d7837caf62653d097ff85": ("USDC", 6),  # Native USDC
        "0x7f5c764cbc14f9669b88837ca1490cca17c31607": ("USDC.e", 6),  # Bridged USDC
        "0x94b008aa00579c1307b0ef2c499ad98a8ce58e58": ("USDT", 6),
        "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1": ("DAI", 18),
        "0x4200000000000000000000000000000000000006": ("WETH", 18),
        "0x68f180fcce6836688e9084f035309e29bf0a2095": ("WBTC", 8),
        "0x1f32b1c2345538c0c6f582fcb022739c4a194ebb": ("wstETH", 18),
        "0x9bcef72be871e61ed4fbbc7630889bee758eb81d": ("rETH", 18),
        "0xaddb6a0412de1ba0f936dcaeb8aaa24578dcf3b2": ("cbETH", 18),
        "0xFf733b2A3557a7ed6571E4a12b050b6f776dD6A9": ("ACX", 18),
        "0x395ae52bb17aef68c2888d941736a71dc6d4e125": ("POOL", 18),
        "0x50c5725949a6f0c72e6c4a641f24049a917db0cb": ("LYRA", 18),
        "0x9560e827af36c94d2ac33a39bce1fe78631088db": ("VELO", 18),
        "0x8700daec35af8ff88c16bdf0418774cb3d7599b4": ("SNX", 18),
    },
    
    # =========================================================================
    # BNB CHAIN / BSC (Chain ID: 56)
    # =========================================================================
    56: {
        "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d": ("USDC", 18),  # BSC USDC has 18 decimals!
        "0x55d398326f99059ff775485246999027b3197955": ("USDT", 18),  # BSC USDT has 18 decimals!
        "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3": ("DAI", 18),
        "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c": ("WBNB", 18),
        "0x2170ed0880ac9a755fd29b2688956bd959f933f8": ("ETH", 18),
        "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c": ("BTCB", 18),
    },
    
    # =========================================================================
    # POLYGON (Chain ID: 137)
    # =========================================================================
    137: {
        "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359": ("USDC", 6),  # Native USDC
        "0x2791bca1f2de4661ed88a30c99a7a9449aa84174": ("USDC.e", 6),  # Bridged USDC
        "0xc2132d05d31c914a87c6611c10748aeb04b58e8f": ("USDT", 6),
        "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063": ("DAI", 18),
        "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619": ("WETH", 18),
        "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270": ("WMATIC", 18),
        "0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6": ("WBTC", 8),
        "0x03b54a6e9a984069379fae1a4fc4dbae93b3bccd": ("wstETH", 18),
        "0xb23c20efce6e24acca0cef9b7b7aa196b84ec942": ("rETH", 18),
        "0x4e3decbb3645551b8a19f0ea1678079fcb33fb4c": ("cbETH", 18),
        "0xf328b73b6c685831f238c30a23fc19140cb4d8fc": ("ACX", 18),
        "0x25788a1a171ec66da6502f9975a15b609ff54cf6": ("POOL", 18),
    },
    
    # =========================================================================
    # ARBITRUM (Chain ID: 42161)
    # =========================================================================
    42161: {
        "0xaf88d065e77c8cc2239327c5edb3a432268e5831": ("USDC", 6),  # Native USDC
        "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8": ("USDC.e", 6),  # Bridged USDC
        "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9": ("USDT", 6),
        "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1": ("DAI", 18),
        "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": ("WETH", 18),
        "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f": ("WBTC", 8),
        "0x5979d7b546e38e414f7e9822514be443a4800529": ("wstETH", 18),
        "0xec70dcb4a1efa46b8f2d97c310c9c4790ba5ffa8": ("rETH", 18),
        "0x1debd73e752beaf79865fd6446b0c970eae7732f": ("cbETH", 18),
        "0xb0C7a3Ba49C7a6EaBa6cD4a96C55a1391070Ac9A": ("MAGIC", 18),
        "0x912ce59144191c1204e64559fe8253a0e49e6548": ("ARB", 18),
        "0xf929de51d91c77e42f5090069e0ad7a09e513c73": ("ACX", 18),
        "0xcf934e2402a5e072928a39a956964eb8f2b5b79c": ("POOL", 18),
    },
    
    # =========================================================================
    # BASE (Chain ID: 8453)
    # =========================================================================
    8453: {
        "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": ("USDC", 6),
        "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca": ("USDbC", 6),  # Bridged USDC
        "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2": ("USDT", 6),
        "0x50c5725949a6f0c72e6c4a641f24049a917db0cb": ("DAI", 18),
        "0x4200000000000000000000000000000000000006": ("WETH", 18),
        "0xc1cba3fcea344f92d9239c08c0568f6f2f0ee452": ("wstETH", 18),
        "0xb6fe221fe9eef5aba221c348ba20a1bf5e73624c": ("rETH", 18),
        "0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22": ("cbETH", 18),
        "0xba0dda8762c24da9487f5fa026a9b64b695a07ea": ("ACX", 18),
        "0xd652c40fbb3f06d6b58cb9aa9cff063ee63d465d": ("POOL", 18),
        "0x78a087d713be963bf307b18f2ff8122ef9a63ae9": ("BSWAP", 18),
    },
    
    # =========================================================================
    # LINEA (Chain ID: 59144)
    # =========================================================================
    59144: {
        "0x176211869ca2b568f2a7d4ee941e073a821ee1ff": ("USDC", 6),
        "0xa219439258ca9da29e9cc4ce5596924745e12b93": ("USDT", 6),
        "0x4af15ec2a0bd43db75dd04e62faa3b8ef36b00d5": ("DAI", 18),
        "0xe5d7c2a44ffddf6b295a15c148167daaaf5cf34f": ("WETH", 18),
        "0xb5bedd42000b71fdde22d3ee8a79bd49a568fc8f": ("wstETH", 18),
        "0x3aab2285ddcddad8edf438c1bab47e1a9d05a9b4": ("WBTC", 8),
        "0x7d43aabc515c356145049227cee54b608342c0ad": ("BUSD", 18),
    },
    
    # =========================================================================
    # ZKSYNC ERA (Chain ID: 324)
    # =========================================================================
    324: {
        "0x3355df6d4c9c3035724fd0e3914de96a5a83aaf4": ("USDC", 6),
        "0x493257fd37edb34451f62edf8d2a0c418852ba4c": ("USDT", 6),
        "0x4b9eb6c0b6ea15176bbf62841c6b2a8a398cb656": ("DAI", 18),
        "0x5aea5775959fbc2557cc8789bc1bf90a239d9a91": ("WETH", 18),
        "0xbbeb516fb02a01611cbbe0453fe3c580d7281011": ("WBTC", 8),
    },
    
    # =========================================================================
    # HYPEREVM (Chain ID: 999)
    # =========================================================================
    999: {
        "0xb88339cb7199b77e23db6e890353e22632ba630f": ("USDC", 6),
        "0x111111a1a0667d36bd57c0a9f569b98057111111": ("ETH", 18),  # Placeholder ETH
        "0x4200000000000000000000000000000000000006": ("WETH", 18),
    },

    # =========================================================================
    # ZORA (Chain ID: 7777777)
    # =========================================================================
    7777777: {
        "0xcccccccc7021b32ebb4e8c08314bd62f7c653ec4": ("USDC", 6),
        "0x4200000000000000000000000000000000000006": ("WETH", 18),
    }

    # =========================================================================
    # BLAST (Chain ID: 81457)
    # =========================================================================
    81457: {
        "0x4300000000000000000000000000000000000003": ("USDB", 18),  # Blast native stablecoin
        "0x4300000000000000000000000000000000000004": ("WETH", 18),
    },
    
    # =========================================================================
    # MODE (Chain ID: 34443)
    # =========================================================================
    34443: {
        "0xd988097fb8612cc24eec14542bc03424c656005f": ("USDC", 6),
        "0xf0f161fda2712db8b566946122a5af183995e2ed": ("USDT", 6),
        "0x4200000000000000000000000000000000000006": ("WETH", 18),
    },
    
    # =========================================================================
    # MONAD (Chain ID: 143)
    # =========================================================================
    143: {
        "0x754704bc059f8c67012fed69bc8a327a5aafb603": ("USDC", 6),
        "0x4200000000000000000000000000000000000006": ("WETH", 18),
    }
}


def get_chain_name(chain_id: int) -> str:
    """
    Convert a chain ID to its human-readable name.
    
    Args:
        chain_id: The numeric blockchain network identifier.
    
    Returns:
        Human-readable chain name, or "Unknown (chain_id)" if not found.
    
    Example:
        >>> get_chain_name(1)
        'Ethereum'
        >>> get_chain_name(42161)
        'Arbitrum'
    """
    return CHAIN_NAMES.get(chain_id, f"Unknown ({chain_id})")


def get_token_info(chain_id: int, token_address: str) -> Tuple[str, int]:
    """
    Get token symbol and decimals for a token address on a specific chain.
    
    The function handles case-insensitivity for addresses (Ethereum addresses
    are case-insensitive except for checksum validation).
    
    Args:
        chain_id: The blockchain network identifier.
        token_address: The token's contract address (0x...).
    
    Returns:
        Tuple of (symbol, decimals). Returns ("Unknown", 18) if not found.
        We default to 18 decimals as it's the most common.
    
    Example:
        >>> get_token_info(1, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
        ('USDC', 6)
    """
    # Normalize address to lowercase for lookup
    # Ethereum addresses are hex and case-insensitive
    normalized_address = token_address.lower() if token_address else ""
    
    # Try to find token in the chain's token list
    chain_tokens = TOKEN_METADATA.get(chain_id, {})
    
    # Check if token exists in chain's token map
    if normalized_address in chain_tokens:
        return chain_tokens[normalized_address]
    
    # Fallback: search all chains (token might be on a different chain context)
    # This handles edge cases where origin/destination chain differs
    for cid, tokens in TOKEN_METADATA.items():
        if normalized_address in tokens:
            return tokens[normalized_address]
    
    # Unknown token - return abbreviated address for identification
    short_addr = f"0x{token_address[2:6]}...{token_address[-4:]}" if len(token_address) >= 10 else token_address
    return (f"Unknown ({short_addr})", 18)


def convert_amount(raw_amount: int, decimals: int) -> float:
    """
    Convert raw token amount to human-readable decimal value.
    
    Blockchain tokens store amounts as integers in their smallest unit.
    For example, USDC has 6 decimals, so 1 USDC = 1,000,000 raw units.
    ETH has 18 decimals, so 1 ETH = 1,000,000,000,000,000,000 raw units (1e18).
    
    Args:
        raw_amount: The raw integer amount from the blockchain.
        decimals: The number of decimal places for this token.
    
    Returns:
        Human-readable decimal amount.
    
    Example:
        >>> convert_amount(1000000, 6)  # 1 USDC
        1.0
        >>> convert_amount(1000000000000000000, 18)  # 1 ETH
        1.0
    """
    try:
        # Use Decimal for precision, then convert to float for DataFrame compatibility
        # This avoids floating-point precision issues with large numbers
        return float(Decimal(str(raw_amount)) / Decimal(10 ** decimals))
    except (TypeError, ValueError, InvalidOperation):
        # Return 0 if conversion fails (e.g., null values)
        return 0.0


def process_csv_file(input_path: str, output_path: Optional[str] = None) -> pd.DataFrame:
    """
    Process a single CSV file and convert blockchain data to human-readable format.
    
    This function:
    1. Detects which columns exist in the CSV (handles different file structures)
    2. Converts chainId columns to chainName
    3. Converts token addresses to token names
    4. Converts raw amounts to human-readable amounts using token decimals
    
    Args:
        input_path: Path to the input CSV file.
        output_path: Path for the output CSV. If None, generates automatically
                    by appending "_converted" to the original filename.
    
    Returns:
        The processed DataFrame.
    """
    print(f"\n{'='*60}")
    print(f"Processing: {os.path.basename(input_path)}")
    print(f"{'='*60}")
    
    # Read the CSV file
    df = pd.read_csv(input_path)
    original_rows = len(df)
    print(f"Loaded {original_rows:,} rows")
    
    # Identify chain ID columns (could be 'chainId', 'originChainId', 'destinationChainId', etc.)
    # We look for any column containing 'chainid' (case-insensitive)
    chain_columns = [col for col in df.columns if 'chainid' in col.lower()]
    print(f"Found chain columns: {chain_columns}")
    
    # Process each chain ID column → add corresponding chainName column
    for chain_col in chain_columns:
        # Create new column name: originChainId → originChainName
        name_col = chain_col.replace('ChainId', 'ChainName').replace('chainId', 'chainName')
        if name_col == chain_col:  # If no replacement happened
            name_col = f"{chain_col}_name"
        
        # Convert chain IDs to names using vectorized apply
        df[name_col] = df[chain_col].apply(lambda x: get_chain_name(int(x)) if pd.notna(x) else "Unknown")
        print(f"  Added: {name_col}")
    
    # Identify token address columns
    # Common patterns: inputToken, outputToken, l2TokenAddress, tokenAddress
    token_columns = [col for col in df.columns if 'token' in col.lower() and 'name' not in col.lower()]
    print(f"Found token columns: {token_columns}")
    
    # Identify amount columns
    # Common patterns: inputAmount, outputAmount, refundAmount, amountToReturn
    amount_columns = [col for col in df.columns if 'amount' in col.lower() and 'converted' not in col.lower()]
    print(f"Found amount columns: {amount_columns}")
    
    # For each token column, we need to determine which chain to use for token lookup
    # and which amount columns are associated with this token
    
    # Map token columns to their corresponding chain columns
    # inputToken → originChainId (deposit) or the chain where token exists
    # outputToken → destinationChainId or repaymentChainId
    # l2TokenAddress → chainId
    
    def get_chain_for_token(token_col: str, row: pd.Series) -> int:
        """
        Determine which chain ID to use for looking up a token.
        
        Logic:
        - inputToken uses originChainId (if exists), else destinationChainId's source
        - outputToken uses destinationChainId or repaymentChainId
        - l2TokenAddress uses chainId
        """
        token_lower = token_col.lower()
        
        if 'input' in token_lower:
            # Input token is on the origin/source chain
            if 'originChainId' in row.index:
                return int(row['originChainId']) if pd.notna(row.get('originChainId')) else 0
            elif 'chainId' in row.index:
                return int(row['chainId']) if pd.notna(row.get('chainId')) else 0
        elif 'output' in token_lower:
            # Output token is on the destination chain
            if 'destinationChainId' in row.index:
                return int(row['destinationChainId']) if pd.notna(row.get('destinationChainId')) else 0
            elif 'repaymentChainId' in row.index:
                return int(row['repaymentChainId']) if pd.notna(row.get('repaymentChainId')) else 0
            elif 'chainId' in row.index:
                return int(row['chainId']) if pd.notna(row.get('chainId')) else 0
        elif 'l2token' in token_lower or 'token' in token_lower:
            # Generic token uses the row's chainId
            if 'chainId' in row.index:
                return int(row['chainId']) if pd.notna(row.get('chainId')) else 0
        
        # Default: try any chain column
        for col in row.index:
            if 'chainid' in col.lower() and pd.notna(row.get(col)):
                return int(row[col])
        return 0
    
    # Process token columns: add token name columns
    for token_col in token_columns:
        name_col = token_col.replace('Token', 'TokenName').replace('token', 'TokenName')
        if name_col == token_col:
            name_col = f"{token_col}_name"
        
        # Apply token name lookup row by row
        # This is necessary because chain ID varies per row
        def lookup_token_name(row):
            token_addr = row.get(token_col)
            if pd.isna(token_addr):
                return "Unknown"
            chain_id = get_chain_for_token(token_col, row)
            symbol, _ = get_token_info(chain_id, str(token_addr))
            return symbol
        
        df[name_col] = df.apply(lookup_token_name, axis=1)
        print(f"  Added: {name_col}")
    
    # Process amount columns: add converted amount columns
    # We need to determine which token's decimals to use for each amount
    for amount_col in amount_columns:
        converted_col = amount_col.replace('Amount', 'AmountConverted').replace('amount', 'AmountConverted')
        if converted_col == amount_col:
            converted_col = f"{amount_col}_converted"
        
        # Determine which token column corresponds to this amount
        # inputAmount → inputToken, outputAmount → outputToken
        # refundAmount/amountToReturn → l2TokenAddress or generic token
        amount_lower = amount_col.lower()
        
        # Find matching token column
        matching_token = None
        if 'input' in amount_lower:
            matching_token = next((c for c in token_columns if 'input' in c.lower()), None)
        elif 'output' in amount_lower:
            matching_token = next((c for c in token_columns if 'output' in c.lower()), None)
        elif 'refund' in amount_lower or 'return' in amount_lower:
            matching_token = next((c for c in token_columns if 'l2token' in c.lower() or 'token' in c.lower()), None)
        
        if not matching_token:
            # Try to find any token column
            matching_token = token_columns[0] if token_columns else None
        
        def convert_row_amount(row):
            """Convert a single row's amount using the appropriate token's decimals."""
            raw_amount = row.get(amount_col)
            if pd.isna(raw_amount):
                return 0.0
            
            # Get decimals from the matching token
            if matching_token:
                token_addr = row.get(matching_token)
                if pd.notna(token_addr):
                    chain_id = get_chain_for_token(matching_token, row)
                    _, decimals = get_token_info(chain_id, str(token_addr))
                    return convert_amount(int(float(raw_amount)), decimals)
            
            # Default to 18 decimals if no token info available
            return convert_amount(int(float(raw_amount)), 18)
        
        df[converted_col] = df.apply(convert_row_amount, axis=1)
        print(f"  Added: {converted_col}")
    
    # Reorder columns to place new columns next to their originals
    # This makes the output more readable
    new_columns = []
    added_new = set()
    
    for col in df.columns:
        if col not in added_new:
            new_columns.append(col)
            added_new.add(col)
            
            # Check for related new columns to add right after
            related_suffixes = ['Name', '_name', 'Converted', '_converted']
            for suffix in related_suffixes:
                potential_new = col + suffix
                if potential_new in df.columns and potential_new not in added_new:
                    new_columns.append(potential_new)
                    added_new.add(potential_new)
                
                # Also check for pattern replacements
                if suffix in ['Name', 'Converted']:
                    potential_patterns = [
                        col.replace('ChainId', 'ChainName'),
                        col.replace('chainId', 'chainName'),
                        col.replace('Token', 'TokenName'),
                        col.replace('token', 'TokenName'),
                        col.replace('Amount', 'AmountConverted'),
                        col.replace('amount', 'AmountConverted'),
                    ]
                    for potential in potential_patterns:
                        if potential in df.columns and potential not in added_new:
                            new_columns.append(potential)
                            added_new.add(potential)
    
    # Add any remaining columns that weren't captured
    for col in df.columns:
        if col not in added_new:
            new_columns.append(col)
    
    df = df[new_columns]
    
    # Generate output path if not provided
    if output_path is None:
        base_name = os.path.splitext(input_path)[0]
        output_path = f"{base_name}_converted.csv"
    
    # Save the processed DataFrame
    df.to_csv(output_path, index=False)
    print(f"\nSaved to: {output_path}")
    print(f"Output has {len(df.columns)} columns")
    
    return df


def process_all_decoded_events(input_folder: str, output_folder: Optional[str] = None) -> Dict[str, pd.DataFrame]:
    """
    Process all CSV files in the decoded_events folder.
    
    Args:
        input_folder: Path to the folder containing decoded event CSVs.
        output_folder: Path for output files. If None, uses same folder with "_converted" suffix.
    
    Returns:
        Dictionary mapping filename to processed DataFrame.
    """
    # Use same folder for output if not specified
    if output_folder is None:
        output_folder = input_folder
    
    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    results = {}
    
    # Find all CSV files in the input folder
    csv_files = [f for f in os.listdir(input_folder) if f.endswith('.csv') and '_converted' not in f]
    
    print(f"\n{'#'*60}")
    print(f"# BLOCKCHAIN METADATA CONVERTER")
    print(f"# Processing {len(csv_files)} files from: {input_folder}")
    print(f"{'#'*60}")
    
    for csv_file in csv_files:
        input_path = os.path.join(input_folder, csv_file)
        output_name = csv_file.replace('.csv', '_converted.csv')
        output_path = os.path.join(output_folder, output_name)
        
        try:
            df = process_csv_file(input_path, output_path)
            results[csv_file] = df
        except Exception as e:
            print(f"\nError processing {csv_file}: {e}")
            continue
    
    print(f"\n{'#'*60}")
    print(f"# PROCESSING COMPLETE")
    print(f"# Successfully processed {len(results)}/{len(csv_files)} files")
    print(f"{'#'*60}\n")
    
    return results


# =============================================================================
# MAIN EXECUTION
# =============================================================================
if __name__ == "__main__":
    # Import for handling potential Decimal conversion errors
    from decimal import InvalidOperation
    
    # Define the folder containing decoded events
    # Using relative path from script location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    decoded_events_folder = os.path.join(script_dir, "data", "decoded_events")
    
    # Process all files
    results = process_all_decoded_events(decoded_events_folder)
    
    # Print summary of conversions
    print("\n" + "="*60)
    print("CONVERSION SUMMARY")
    print("="*60)
    
    for filename, df in results.items():
        print(f"\n{filename}:")
        
        # Show sample of chain conversions
        chain_name_cols = [c for c in df.columns if 'chainname' in c.lower()]
        for col in chain_name_cols:
            unique_chains = df[col].unique()[:5]
            print(f"  {col}: {list(unique_chains)}")
        
        # Show sample of token conversions
        token_name_cols = [c for c in df.columns if 'tokenname' in c.lower()]
        for col in token_name_cols:
            unique_tokens = df[col].unique()[:5]
            print(f"  {col}: {list(unique_tokens)}")
        
        # Show sample of amount conversions
        amount_cols = [c for c in df.columns if 'converted' in c.lower()]
        for col in amount_cols:
            sample_values = df[col].head(3).tolist()
            print(f"  {col}: {[round(v, 4) for v in sample_values]}")

