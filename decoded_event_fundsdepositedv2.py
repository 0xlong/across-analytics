"""
Decoder for Across Protocol FundsDeposited V2 Events.

This script reads raw event log data from a CSV file and decodes the 
hexadecimal data into human-readable fields, creating a new transformed table.

DATA STRUCTURE:
===============
The FundsDeposited event has both INDEXED and NON-INDEXED parameters:

INDEXED (stored in event topics):
- topic_1 → destinationChainId (uint256) - Target blockchain chain ID
- topic_2 → depositId (uint256) - Unique identifier for this deposit
- topic_3 → depositor (address) - Address that initiated the deposit

NON-INDEXED (stored in raw_data_hex):
- inputToken, outputToken, inputAmount, outputAmount
- quoteTimestamp, fillDeadline, exclusivityDeadline
- recipient, exclusiveRelayer, message
"""

import pandas as pd
from typing import Dict, Any, Optional


# =============================================================================
# CONFIGURATION
# =============================================================================

# Path to input CSV file containing raw event data
INPUT_CSV_PATH = "data/bigquery_events_input/across_spokepool_fundsdepositedv2.csv"

# Path to output CSV file with decoded data
OUTPUT_CSV_PATH = "data/decoded_events/decoded_funds_deposited_v2.csv"


# =============================================================================
# TOPIC DECODER FUNCTIONS (for indexed event parameters)
# =============================================================================

def decode_topic_uint256(topic_hex: str) -> Optional[int]:
    """
    Decode a topic (32-byte hex) to an unsigned integer.
    
    Topics in Ethereum events are always 32 bytes. For uint256 values,
    the number is stored directly in hex format.
    
    Example:
        topic_1 = "0x0000000000000000000000000000000000000000000000000000000000000089"
        Result  = 137 (Polygon chain ID)
    
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


def decode_topic_address(topic_hex: str) -> Optional[str]:
    """
    Decode a topic (32-byte hex) to an Ethereum address.
    
    Addresses are 20 bytes, but topics are 32 bytes. The address is
    LEFT-PADDED with zeros to fill the 32-byte slot.
    
    Example:
        topic_3 = "0x000000000000000000000000e5f55822f713a2e3b2028c561a779b66c866167d"
        Result  = "0xe5f55822f713a2e3b2028c561a779b66c866167d"
    
    Args:
        topic_hex: 32-byte hex string (with or without '0x' prefix).
        
    Returns:
        20-byte address with '0x' prefix, or None if invalid/empty.
    """
    # Handle empty or NaN values
    if not topic_hex or (isinstance(topic_hex, float) and pd.isna(topic_hex)):
        return None
    
    # Convert to string and strip whitespace
    topic_str = str(topic_hex).strip()
    
    # Remove '0x' prefix if present
    hex_data = topic_str[2:] if topic_str.startswith('0x') else topic_str
    
    # Address needs at least 40 chars (20 bytes)
    if len(hex_data) < 40:
        return None
    
    # Extract last 40 characters (the actual 20-byte address)
    address_hex = hex_data[-40:]
    
    return '0x' + address_hex.lower()


# =============================================================================
# HEX DATA DECODER FUNCTIONS (for non-indexed event parameters)
# =============================================================================

def decode_raw_data_hex(raw_data_hex: str) -> Dict[str, Any]:
    """
    Decode the raw_data_hex from a FundsDeposited (v1_bytes32) event.
    
    The event data is structured as consecutive 32-byte (64 hex char) chunks:
    
    Position | Field              | Type     | Description
    ---------|--------------------|----------|---------------------------------
    0        | inputToken         | address  | Token deposited on source chain
    1        | outputToken        | address  | Token to receive on dest chain
    2        | inputAmount        | uint256  | Amount of input tokens
    3        | outputAmount       | uint256  | Amount of output tokens
    4        | quoteTimestamp     | uint32   | When quote was generated
    5        | fillDeadline       | uint32   | Deadline for fill
    6        | exclusivityDeadline| uint32   | Exclusive relayer deadline
    7        | recipient          | address  | Receiver on destination chain
    8        | exclusiveRelayer   | address  | Exclusive relayer (or zero)
    9        | message_offset     | uint256  | Pointer to message bytes
    10+      | message            | bytes    | Dynamic message data
    
    Args:
        raw_data_hex: Hexadecimal string (with '0x' prefix) containing
                      the concatenated event data fields.
    
    Returns:
        Dictionary with decoded field names and their values.
    """
    
    # Handle empty or invalid input
    # If the hex string is too short, return empty values
    if not raw_data_hex or len(raw_data_hex) < 10:
        return {
            'inputToken': None,
            'outputToken': None,
            'inputAmount': None,
            'outputAmount': None,
            'recipient': None,
            'message': None
        }
    
    # Remove '0x' prefix for easier string slicing
    # We work with pure hex characters (each byte = 2 hex chars)
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
        they're LEFT-PADDED with zeros to fill 32 bytes:
        
        Example: 
        32-byte: 000000000000000000000000af88d065e77c8cc2239327c5edb3a432268e5831
        20-byte:                         af88d065e77c8cc2239327c5edb3a432268e5831
        
        We extract the last 40 characters (20 bytes = actual address).
        
        Args:
            hex_chunk: 64-character padded hex string.
            
        Returns:
            Checksummed address with '0x' prefix, or None if invalid.
        """
        if not hex_chunk or len(hex_chunk) < 40:
            return None
            
        # Last 40 chars = the actual 20-byte address
        address_hex = hex_chunk[-40:]
        return '0x' + address_hex.lower()  # Lowercase for consistency
    
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
    
    def decode_message() -> Optional[str]:
        """
        Decode the dynamic 'message' bytes field from event data.
        
        Dynamic types in Solidity ABI encoding work like this:
        1. At position 9, there's an OFFSET (in bytes) pointing to where
           the actual message data starts
        2. At that offset location: first 32 bytes = LENGTH of message
        3. After length: the actual message bytes
        
        For example, if offset = 0x140 (320 decimal):
        - Position 320 bytes (640 hex chars) from start = length
        - After length = actual message content
        
        Returns:
            Hex string of message data with '0x' prefix, or empty string.
        """
        # Get the offset value from position 9
        offset_chunk = get_chunk(9)
        if not offset_chunk:
            return ''
            
        # Convert offset from hex to bytes (then to hex char position)
        try:
            offset_bytes = int(offset_chunk, 16)  # Offset in bytes
            offset_chars = offset_bytes * 2        # Convert to hex chars
        except ValueError:
            return ''
        
        # Check if offset points within our data
        if offset_chars >= len(hex_data):
            return ''
            
        # First 32 bytes at offset = message length
        length_hex = hex_data[offset_chars:offset_chars + CHUNK_SIZE]
        if len(length_hex) < CHUNK_SIZE:
            return ''
            
        try:
            message_length_bytes = int(length_hex, 16)
        except ValueError:
            return ''
        
        # If length is 0, message is empty
        if message_length_bytes == 0:
            return 'no_message_data'
        
        # Extract actual message content after the length field
        message_start = offset_chars + CHUNK_SIZE
        message_end = message_start + (message_length_bytes * 2)  # 2 hex chars per byte
        
        message_hex = hex_data[message_start:message_end]
        
        return '0x' + message_hex if message_hex else 'no_message_data'
    
    # ==========================================================================
    # DECODE ALL FIELDS FROM THEIR RESPECTIVE POSITIONS
    # ==========================================================================
    
    return {
        # Position 0: Input token address (what user deposits)
        'inputToken': decode_address(get_chunk(0)),
        
        # Position 1: Output token address (what user receives)
        'outputToken': decode_address(get_chunk(1)),
        
        # Position 2: Amount of input tokens (in smallest unit, like wei)
        'inputAmount': decode_uint256(get_chunk(2)),
        
        # Position 3: Amount of output tokens user will receive
        'outputAmount': decode_uint256(get_chunk(3)),
        
        # Position 7: Recipient address on destination chain
        'recipient': decode_address(get_chunk(7)),
        
        # Dynamic: Message data (optional cross-chain message)
        'message': decode_message()
    }


# =============================================================================
# MAIN DATA TRANSFORMATION FUNCTION
# =============================================================================

def transform_csv(input_path: str, output_path: str) -> pd.DataFrame:
    """
    Read raw event CSV, decode hex data, and create transformed table.
    
    This function:
    1. Reads the input CSV with raw event data
    2. Decodes each row's raw_data_hex field
    3. Creates new columns with decoded values
    4. Saves the result to a new CSV file
    
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
    
    print(f"Found {len(df)} rows to process")
    
    # ==========================================================================
    # DECODE INDEXED FIELDS FROM TOPICS
    # ==========================================================================
    
    # topic_1 contains destinationChainId (uint256)
    # This is the chain ID where funds will be received (e.g., 137 = Polygon)
    destination_chain_ids = df['topic_1'].apply(decode_topic_uint256)
    
    # topic_2 contains depositId (uint256)
    # This is a unique identifier for each deposit transaction
    deposit_ids = df['topic_2'].apply(decode_topic_uint256)
    
    # topic_3 contains depositor address
    # This is the wallet address that initiated the cross-chain transfer
    depositors = df['topic_3'].apply(decode_topic_address)
    
    # ==========================================================================
    # DECODE NON-INDEXED FIELDS FROM raw_data_hex
    # ==========================================================================
    
    # Apply the decoder function to each row's raw_data_hex
    # This creates a Series of dictionaries, one per row
    decoded_series = df['raw_data_hex'].apply(decode_raw_data_hex)
    
    # Convert the Series of dicts into a DataFrame
    # Each dictionary key becomes a column
    decoded_df = pd.DataFrame(decoded_series.tolist())
    
    # ==========================================================================
    # BUILD THE OUTPUT DATAFRAME WITH REQUESTED COLUMNS
    # ==========================================================================
    
    # Create the final DataFrame with the user's requested column order:
    # block_timestamp, transaction_hash, destinationChainId, depositId,
    # depositor, recipient, inputToken, outputToken, inputAmount, outputAmount, message
    
    output_df = pd.DataFrame({
        # These columns come directly from the source CSV
        'block_timestamp': df['block_timestamp'],
        'transaction_hash': df['transaction_hash'],
        
        # INDEXED FIELDS - Decoded from topic_1, topic_2, topic_3
        'destinationChainId': destination_chain_ids,  # From topic_1
        'depositId': deposit_ids,                     # From topic_2
        'depositor': depositors,                      # From topic_3
        
        # NON-INDEXED FIELDS - Decoded from raw_data_hex
        'recipient': decoded_df['recipient'],
        'inputToken': decoded_df['inputToken'],
        'outputToken': decoded_df['outputToken'],
        'inputAmount': decoded_df['inputAmount'],
        'outputAmount': decoded_df['outputAmount'],
        'message': decoded_df['message']
    })
    
    # ==========================================================================
    # SAVE TO OUTPUT FILE
    # ==========================================================================
    
    print(f"Saving decoded data to: {output_path}")
    output_df.to_csv(output_path, index=False)
    
    print(f"Successfully processed {len(output_df)} rows")
    print("\n" + "="*60)
    print("DECODED FIELDS:")
    print("  FROM TOPICS (indexed):")
    print("    - destinationChainId (topic_1)")
    print("    - depositId (topic_2)")
    print("    - depositor (topic_3)")
    print("  FROM RAW_DATA_HEX (non-indexed):")
    print("    - recipient, inputToken, outputToken")
    print("    - inputAmount, outputAmount, message")
    print("="*60 + "\n")
    
    return output_df


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    """
    Main execution block - runs when script is executed directly.
    
    Usage:
        python decoded_event_fundsdepositedv2.py
    
    This will:
    1. Read from: data/across_spokepool_fundsdepositedv2.csv
    2. Decode all raw_data_hex fields
    3. Save to: data/decoded_fundsdepositedv2.csv
    """
    
    # Run the transformation
    result_df = transform_csv(INPUT_CSV_PATH, OUTPUT_CSV_PATH)
    
    # Display preview of the results
    print("Preview of decoded data (first 5 rows):")
    print("-" * 80)
    
    # Show subset of columns for cleaner display
    preview_cols = [
        'destinationChainId', 'depositId', 
        'inputAmount', 'outputAmount'
    ]
    print(result_df[preview_cols].head().to_string(index=False))
    
    print("\n" + "-" * 80)
    print(f"Full output saved to: {OUTPUT_CSV_PATH}")

