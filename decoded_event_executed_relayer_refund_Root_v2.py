"""
Decoder for Across Protocol ExecutedRelayerRefundRoot V2 Events.

This script reads raw event log data from a CSV file and decodes the 
hexadecimal data into human-readable fields, creating a new transformed table.

IMPORTANT: ExecutedRelayerRefundRoot is a BATCH REFUND event that repays
multiple relayers in a single transaction. It does NOT contain individual
deposit information like depositId, depositor, or recipient.

DATA STRUCTURE:
===============
The ExecutedRelayerRefundRoot event has both INDEXED and NON-INDEXED parameters:

INDEXED (stored in event topics):
- topic_1 → chainId (uint256)        - The chain where refunds are executed
- topic_2 → rootBundleId (uint32)    - ID of the root bundle being executed
- topic_3 → leafId (uint32)          - ID of the leaf in the Merkle tree

NON-INDEXED (stored in raw_data_hex):
- amountToReturn (uint256)           - Amount returned to HubPool (if any)
- refundAmounts (uint256[])          - Array of amounts to refund each relayer
- l2TokenAddress (address)           - Token being refunded (e.g., WETH, USDC)
- refundAddresses (address[])        - Array of relayer addresses receiving refunds
- deferredRefunds (bool)             - Whether refunds are deferred
- caller (address)                   - Address that called this function

COLUMN MAPPING:
===============
Since the requested columns differ from actual event fields, here's the mapping:

Requested Column    → Actual Source Field
------------------  → ----------------------
block_timestamp     → block_timestamp (CSV)
transaction_hash    → transaction_hash (CSV)
chainId             → topic_1 (the refund chain, NOT origin)
rootBundleId        → topic_2
leafId              → topic_3
l2TokenAddress      → raw_data position 2 (token being refunded)
refundAddress       → raw_data array (relayer receiving refund)
refundAmount        → raw_data array (amount refunded)
amountToReturn      → raw_data position 0
deferredRefunds     → raw_data position 4
caller              → raw_data position 5
"""

import pandas as pd
from typing import Dict, Any, Optional, List, Tuple


# =============================================================================
# CONFIGURATION
# =============================================================================

# Path to input CSV file containing raw event data
INPUT_CSV_PATH = "data/bigquery_events_input/across_spokepool_ExecutedRelayerRefundRootV2.csv"

# Path to output CSV file with decoded data
OUTPUT_CSV_PATH = "data/decoded_events/decoded_executed_relayer_refund_root_v2.csv"


# =============================================================================
# TOPIC DECODER FUNCTIONS (for indexed event parameters)
# =============================================================================

def decode_topic_uint256(topic_hex: str) -> Optional[int]:
    """
    Decode a topic (32-byte hex) to an unsigned integer.
    
    Topics in Ethereum events are always 32 bytes. For uint256 values,
    the number is stored directly in hex format.
    
    Example:
        topic_1 = "0x000000000000000000000000000000000000000000000000000000000000a4b1"
        Result  = 42161 (Arbitrum chain ID)
    
    Args:
        topic_hex: 32-byte hex string (with or without '0x' prefix).
        
    Returns:
        Integer value, or None if invalid/empty.
    """
    # Handle empty or NaN values (pandas may read empty cells as NaN)
    if not topic_hex or (isinstance(topic_hex, float) and pd.isna(topic_hex)):
        return None
    
    # Convert to string if needed and strip whitespace
    topic_str = str(topic_hex).strip()
    
    # Remove '0x' prefix if present
    hex_data = topic_str[2:] if topic_str.startswith('0x') else topic_str
    
    # Validate we have hex data
    if not hex_data:
        return None
    
    try:
        return int(hex_data, 16)  # Convert hex string to integer
    except ValueError:
        return None


# =============================================================================
# HEX DATA DECODER FUNCTIONS (for non-indexed event parameters)
# =============================================================================

def decode_raw_data_hex(raw_data_hex: str) -> Dict[str, Any]:
    """
    Decode the raw_data_hex from an ExecutedRelayerRefundRoot event.
    
    The ABI encoding layout is:
    
    Position | Field              | Type       | Description
    ---------|--------------------|------------|----------------------------------
    0        | amountToReturn     | uint256    | Amount returned to HubPool
    1        | refundAmounts_off  | uint256    | Offset to refundAmounts array
    2        | l2TokenAddress     | address    | Token address (20 bytes, left-padded)
    3        | refundAddresses_off| uint256    | Offset to refundAddresses array
    4        | deferredRefunds    | bool       | Whether refunds are deferred
    5        | caller             | address    | Function caller address
    ...      | (dynamic arrays)   |            | refundAmounts[], refundAddresses[]
    
    Dynamic arrays are stored at their respective offsets:
    - First 32 bytes at offset = array length
    - Following bytes = array elements (each 32 bytes)
    
    Args:
        raw_data_hex: Hexadecimal string (with '0x' prefix) containing
                      the concatenated event data fields.
    
    Returns:
        Dictionary with decoded field names and their values.
    """
    
    # Handle empty or invalid input
    if not raw_data_hex or len(raw_data_hex) < 10:
        return {
            'amountToReturn': None,
            'l2TokenAddress': None,
            'deferredRefunds': None,
            'caller': None,
            'refundAmounts': [],
            'refundAddresses': []
        }
    
    # Remove '0x' prefix for easier string slicing
    # Each byte = 2 hex characters
    hex_data = raw_data_hex[2:] if raw_data_hex.startswith('0x') else raw_data_hex
    
    # Each field occupies 32 bytes = 64 hexadecimal characters
    # This is the standard ABI encoding size for all fixed-size types
    CHUNK_SIZE = 64  # 64 hex chars = 32 bytes
    
    def get_chunk(index: int) -> Optional[str]:
        """
        Extract a 32-byte chunk from hex data at the given index.
        
        Think of the hex data as an array of 32-byte slots.
        Index 0 = first 32 bytes, Index 1 = next 32 bytes, etc.
        
        Args:
            index: Zero-based index of the chunk to extract.
            
        Returns:
            64-character hex string, or None if out of bounds.
        """
        start = index * CHUNK_SIZE  # Starting position in hex string
        end = start + CHUNK_SIZE    # Ending position
        
        # Check if we have enough data
        if end > len(hex_data):
            return None
            
        return hex_data[start:end]
    
    def decode_address(hex_chunk: Optional[str]) -> Optional[str]:
        """
        Decode a 32-byte padded address to standard 20-byte format.
        
        Ethereum addresses are 20 bytes (40 hex chars), but in ABI encoding
        they're LEFT-PADDED with zeros to fill 32 bytes.
        
        Example: 
        32-byte: 000000000000000000000000af88d065e77c8cc2239327c5edb3a432268e5831
        20-byte:                         af88d065e77c8cc2239327c5edb3a432268e5831
        
        We extract the last 40 characters (20 bytes = actual address).
        
        Args:
            hex_chunk: 64-character padded hex string.
            
        Returns:
            Address with '0x' prefix in lowercase, or None if invalid.
        """
        if not hex_chunk or len(hex_chunk) < 40:
            return None
            
        # Last 40 chars = the actual 20-byte address
        address_hex = hex_chunk[-40:]
        return '0x' + address_hex.lower()
    
    def decode_uint256(hex_chunk: Optional[str]) -> Optional[int]:
        """
        Decode a 32-byte hex chunk to an unsigned 256-bit integer.
        
        Python's int() can handle arbitrarily large numbers,
        so we simply convert the hex string to decimal.
        
        Args:
            hex_chunk: 64-character hex string representing uint256.
            
        Returns:
            Python integer value, or None if invalid.
        """
        if not hex_chunk:
            return None
            
        try:
            return int(hex_chunk, 16)  # Base 16 = hexadecimal
        except ValueError:
            return None
    
    def decode_bool(hex_chunk: Optional[str]) -> Optional[bool]:
        """
        Decode a 32-byte hex chunk to a boolean.
        
        In ABI encoding, booleans are stored as uint8:
        - 0 = False
        - 1 = True
        But they occupy a full 32-byte slot (left-padded with zeros).
        
        Args:
            hex_chunk: 64-character hex string.
            
        Returns:
            Boolean value, or None if invalid.
        """
        if not hex_chunk:
            return None
            
        try:
            # The value is stored in the last byte, but we can just check
            # if the integer value is non-zero
            return int(hex_chunk, 16) != 0
        except ValueError:
            return None
    
    def decode_uint256_array_at_offset(offset_bytes: int) -> List[int]:
        """
        Decode a dynamic uint256 array starting at the given byte offset.
        
        Dynamic arrays in ABI encoding are structured as:
        1. First 32 bytes at offset = array length (number of elements)
        2. Following 32-byte chunks = array elements
        
        Example:
            offset = 192 (0xc0 in hex)
            At byte 192: length = 9
            At byte 224-480: 9 uint256 values
        
        Args:
            offset_bytes: Byte offset from start of raw_data where array begins.
            
        Returns:
            List of integer values, or empty list if invalid.
        """
        # Convert byte offset to hex character position
        # (2 hex chars per byte)
        offset_chars = offset_bytes * 2
        
        # Check if offset is within bounds
        if offset_chars >= len(hex_data):
            return []
        
        # Read array length from first 32 bytes at offset
        length_start = offset_chars
        length_end = offset_chars + CHUNK_SIZE
        
        if length_end > len(hex_data):
            return []
        
        try:
            array_length = int(hex_data[length_start:length_end], 16)
        except ValueError:
            return []
        
        # Read each array element
        elements = []
        for i in range(array_length):
            elem_start = length_end + (i * CHUNK_SIZE)
            elem_end = elem_start + CHUNK_SIZE
            
            if elem_end > len(hex_data):
                break  # Not enough data for remaining elements
                
            try:
                value = int(hex_data[elem_start:elem_end], 16)
                elements.append(value)
            except ValueError:
                elements.append(0)  # Use 0 for invalid values
        
        return elements
    
    def decode_address_array_at_offset(offset_bytes: int) -> List[str]:
        """
        Decode a dynamic address array starting at the given byte offset.
        
        Similar to uint256 arrays, but each element is an address
        (left-padded to 32 bytes).
        
        Args:
            offset_bytes: Byte offset from start of raw_data where array begins.
            
        Returns:
            List of address strings, or empty list if invalid.
        """
        # Convert byte offset to hex character position
        offset_chars = offset_bytes * 2
        
        # Check if offset is within bounds
        if offset_chars >= len(hex_data):
            return []
        
        # Read array length from first 32 bytes at offset
        length_start = offset_chars
        length_end = offset_chars + CHUNK_SIZE
        
        if length_end > len(hex_data):
            return []
        
        try:
            array_length = int(hex_data[length_start:length_end], 16)
        except ValueError:
            return []
        
        # Read each address element
        addresses = []
        for i in range(array_length):
            elem_start = length_end + (i * CHUNK_SIZE)
            elem_end = elem_start + CHUNK_SIZE
            
            if elem_end > len(hex_data):
                break  # Not enough data for remaining elements
            
            # Extract last 40 hex chars (20 bytes) as address
            addr_hex = hex_data[elem_start:elem_end][-40:]
            addresses.append('0x' + addr_hex.lower())
        
        return addresses
    
    # ==========================================================================
    # DECODE FIXED FIELDS FROM THEIR RESPECTIVE POSITIONS
    # ==========================================================================
    
    # Position 0: amountToReturn (how much is sent back to HubPool)
    amount_to_return = decode_uint256(get_chunk(0))
    
    # Position 1: Offset to refundAmounts array
    refund_amounts_offset_chunk = get_chunk(1)
    refund_amounts_offset = decode_uint256(refund_amounts_offset_chunk) or 0
    
    # Position 2: l2TokenAddress (the token being refunded)
    l2_token_address = decode_address(get_chunk(2))
    
    # Position 3: Offset to refundAddresses array
    refund_addresses_offset_chunk = get_chunk(3)
    refund_addresses_offset = decode_uint256(refund_addresses_offset_chunk) or 0
    
    # Position 4: deferredRefunds (boolean flag)
    deferred_refunds = decode_bool(get_chunk(4))
    
    # Position 5: caller (address that initiated this refund execution)
    caller = decode_address(get_chunk(5))
    
    # ==========================================================================
    # DECODE DYNAMIC ARRAYS AT THEIR RESPECTIVE OFFSETS
    # ==========================================================================
    
    # Decode refundAmounts array (list of amounts for each relayer)
    refund_amounts = decode_uint256_array_at_offset(refund_amounts_offset)
    
    # Decode refundAddresses array (list of relayer addresses)
    refund_addresses = decode_address_array_at_offset(refund_addresses_offset)
    
    return {
        'amountToReturn': amount_to_return,
        'l2TokenAddress': l2_token_address,
        'deferredRefunds': deferred_refunds,
        'caller': caller,
        'refundAmounts': refund_amounts,
        'refundAddresses': refund_addresses
    }


# =============================================================================
# MAIN DATA TRANSFORMATION FUNCTION
# =============================================================================

def transform_csv(input_path: str, output_path: str) -> pd.DataFrame:
    """
    Read raw event CSV, decode hex data, and create transformed table.
    
    This function:
    1. Reads the input CSV with raw event data
    2. Decodes each row's topic and raw_data_hex fields
    3. EXPANDS arrays so each refund becomes its own row
    4. Saves the result to a new CSV file
    
    Since ExecutedRelayerRefundRoot contains arrays (refundAmounts[],
    refundAddresses[]), one event log produces MULTIPLE output rows -
    one for each (relayer, amount) pair.
    
    Args:
        input_path: Path to input CSV file.
        output_path: Path to output CSV file.
        
    Returns:
        DataFrame with the transformed data.
    """
    
    print(f"Reading input file: {input_path}")
    
    # Read the source CSV file
    # Columns: block_timestamp, transaction_hash, topic_0, topic_1, topic_2, topic_3, event_type, raw_data_hex
    df = pd.read_csv(input_path)
    
    print(f"Found {len(df)} event rows to process")
    
    # ==========================================================================
    # PROCESS EACH ROW AND EXPAND ARRAYS INTO MULTIPLE ROWS
    # ==========================================================================
    
    # List to collect all expanded rows
    expanded_rows: List[Dict[str, Any]] = []
    
    for idx, row in df.iterrows():
        # ------------------------------------------------------------------
        # DECODE INDEXED FIELDS FROM TOPICS
        # ------------------------------------------------------------------
        
        # topic_1 contains chainId (uint256)
        # This is the chain where refunds are executed (e.g., 42161 = Arbitrum)
        chain_id = decode_topic_uint256(row['topic_1'])
        
        # topic_2 contains rootBundleId (uint32)
        # Identifier for the root bundle being executed
        root_bundle_id = decode_topic_uint256(row['topic_2'])
        
        # topic_3 contains leafId (uint32)
        # Identifier for this specific leaf in the Merkle tree
        leaf_id = decode_topic_uint256(row['topic_3'])
        
        # ------------------------------------------------------------------
        # DECODE NON-INDEXED FIELDS FROM raw_data_hex
        # ------------------------------------------------------------------
        
        decoded = decode_raw_data_hex(row['raw_data_hex'])
        
        refund_amounts = decoded['refundAmounts']
        refund_addresses = decoded['refundAddresses']
        
        # ------------------------------------------------------------------
        # EXPAND ARRAYS: Create one row per (relayer, amount) pair
        # ------------------------------------------------------------------
        
        # The refundAmounts and refundAddresses arrays should have the same length
        # Each pair represents one relayer getting one refund
        num_refunds = min(len(refund_amounts), len(refund_addresses))
        
        if num_refunds == 0:
            # Even if no refunds, create one row to preserve event metadata
            expanded_rows.append({
                'block_timestamp': row['block_timestamp'],
                'transaction_hash': row['transaction_hash'],
                'chainId': chain_id,                           # The refund chain
                'rootBundleId': root_bundle_id,
                'leafId': leaf_id,
                'l2TokenAddress': decoded['l2TokenAddress'],   # Token being refunded
                'refundAddress': None,                         # Relayer address
                'refundAmount': None,                          # Amount refunded
                'amountToReturn': decoded['amountToReturn'],
                'deferredRefunds': decoded['deferredRefunds'],
                'caller': decoded['caller']
            })
        else:
            # Create one row for each refund in the batch
            for i in range(num_refunds):
                expanded_rows.append({
                    'block_timestamp': row['block_timestamp'],
                    'transaction_hash': row['transaction_hash'],
                    'chainId': chain_id,                              # The refund chain
                    'rootBundleId': root_bundle_id,
                    'leafId': leaf_id,
                    'l2TokenAddress': decoded['l2TokenAddress'],      # Token being refunded
                    'refundAddress': refund_addresses[i],             # This relayer's address
                    'refundAmount': refund_amounts[i],                # This relayer's refund
                    'amountToReturn': decoded['amountToReturn'],
                    'deferredRefunds': decoded['deferredRefunds'],
                    'caller': decoded['caller']
                })
    
    # ==========================================================================
    # CREATE OUTPUT DATAFRAME
    # ==========================================================================
    
    output_df = pd.DataFrame(expanded_rows)
    
    # Reorder columns for better readability
    column_order = [
        'block_timestamp',
        'transaction_hash',
        'chainId',
        'rootBundleId',
        'leafId',
        'l2TokenAddress',
        'refundAddress',
        'refundAmount',
        'amountToReturn',
        'deferredRefunds',
        'caller'
    ]
    
    output_df = output_df[column_order]
    
    # ==========================================================================
    # SAVE TO OUTPUT FILE
    # ==========================================================================
    
    print(f"Saving decoded data to: {output_path}")
    output_df.to_csv(output_path, index=False)
    
    # Count statistics
    original_events = len(df)
    expanded_rows_count = len(output_df)
    
    print(f"\nSuccessfully processed:")
    print(f"  - Input events: {original_events}")
    print(f"  - Output rows (expanded): {expanded_rows_count}")
    print(f"  - Average refunds per event: {expanded_rows_count / original_events:.1f}")
    
    print("\n" + "="*70)
    print("DECODED FIELDS:")
    print("  FROM TOPICS (indexed):")
    print("    - chainId (topic_1) - Chain where refunds executed")
    print("    - rootBundleId (topic_2) - Bundle identifier")
    print("    - leafId (topic_3) - Merkle leaf identifier")
    print("  FROM RAW_DATA_HEX (non-indexed):")
    print("    - l2TokenAddress - Token being refunded")
    print("    - refundAddress - Relayer receiving refund (from array)")
    print("    - refundAmount - Amount refunded (from array)")
    print("    - amountToReturn - Amount sent back to HubPool")
    print("    - deferredRefunds - Whether refunds were deferred")
    print("    - caller - Address that executed this refund")
    print("="*70 + "\n")
    
    return output_df


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    """
    Main execution block - runs when script is executed directly.
    
    Usage:
        python across_spokepool_executed_relayer_refund_Root_v2.py
    
    This will:
    1. Read from: data/across_spokepool_ExecutedRelayerRefundRootV2.csv
    2. Decode all topic and raw_data_hex fields
    3. Expand arrays (one row per relayer refund)
    4. Save to: data/decoded_executed_relayer_refund_root_v2.csv
    """
    
    # Run the transformation
    result_df = transform_csv(INPUT_CSV_PATH, OUTPUT_CSV_PATH)
    
    # Display preview of the results
    print("Preview of decoded data (first 10 rows):")
    print("-" * 90)
    
    # Show subset of columns for cleaner display
    preview_cols = [
        'chainId', 'rootBundleId', 'leafId',
        'l2TokenAddress', 'refundAddress', 'refundAmount'
    ]
    
    # Format large numbers for readability
    preview_df = result_df[preview_cols].head(10).copy()
    print(preview_df.to_string(index=False))
    
    print("\n" + "-" * 90)
    print(f"Full output saved to: {OUTPUT_CSV_PATH}")

