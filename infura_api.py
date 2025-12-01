import requests
import json
from eth_abi import decode  # ABI decoder for Ethereum calldata

# Infura API key - replace with your key
INFURA_API_KEY = 'aa58aa901dc640c785e25f239d13e17f'

# Infura endpoint for Base mainnet
INFURA_URL = f"https://base-mainnet.infura.io/v3/{INFURA_API_KEY}"


# Spoke Pool contract addresses
SPOKE_POOL_CONTRACT_ADDRESSES = {
    "Base": "0x09aea4b2242a3bD9a4F7fF57a9F4c8B1eE4f1D8",
    "Ethereum": "0x5c7BCd6E7De5423a257D81B442095A1a6ced35C5",
    "Arbitrum": "0xe35e9842fceaca96570b734083f4a58e8f7c5f2a",
    "Optimism": "0x6f26Bf09B1C792e3228e5467807a900A503c0281",
    "Polygon": "0x0330E9b4D0325cCfF515E81DFbc7754F2a02ac57",
    "BNBChain": "0x4e8E101924eDE233C13e2D8622DC8aED2872d505",
}


# ============================================================================
# FILL V3 RELAY DECODER
# ============================================================================
# Decodes the "data" field from fillV3Relay transaction logs.
# 
# Function signature:
# fillV3Relay(
#     (address,address,address,address,address,uint256,uint256,uint256,uint32,uint32,uint32,bytes) relayData,
#     uint256 repaymentChainId
# )
#
# Tuple fields (in order):
#   1. depositor       - address who initiated the bridge on origin chain
#   2. recipient       - address receiving funds on destination chain
#   3. exclusiveRelayer - relayer with exclusive rights (0x0 = open to all)
#   4. inputToken      - token deposited on origin chain
#   5. outputToken     - token to receive on destination chain
#   6. inputAmount     - amount deposited on origin (in token's smallest unit)
#   7. outputAmount    - amount to receive on destination (after fees)
#   8. originChainId   - chain ID where deposit originated
#   9. depositId       - unique identifier for this deposit
#  10. fillDeadline    - unix timestamp by which fill must complete
#  11. exclusivityDeadline - timestamp when exclusivity ends
#  12. message         - optional message/calldata for recipient
# ============================================================================

# Method ID = first 4 bytes of keccak256("fillV3Relay((address,address,address,address,address,uint256,uint256,uint256,uint32,uint32,uint32,bytes),uint256)")
FILL_V3_RELAY_METHOD_ID = "0x2e378115"

# ============================================================================
# FILLED V3 RELAY EVENT DECODER
# ============================================================================
# eth_getLogs returns EVENT LOGS, not function call data!
# The FilledV3Relay event is emitted when a relay is filled.
#
# Event signature (topics[0]):
# keccak256("FilledV3Relay(address,address,uint256,uint256,uint256,uint32,uint32,uint32,uint32,address,address,address,address,bytes,(address,bytes,uint256,uint8))")
# = 0x571749edf1d5c9599318cdbc4e28a6475d65e87fd3b2ddbe1e9a8d5e7a0f0ff7
#
# Indexed parameters (in topics):
#   topics[1] = originChainId (uint32, padded to 32 bytes)
#   topics[2] = depositId (uint32, padded to 32 bytes)  
#   topics[3] = relayer address (who filled the relay)
#
# Non-indexed parameters (in data field):
#   inputToken, outputToken, inputAmount, outputAmount, repaymentChainId,
#   fillDeadline, exclusivityDeadline, exclusiveRelayer, depositor, recipient,
#   message, updatableRelayData struct
# ============================================================================

FILLED_V3_RELAY_EVENT_SIGNATURE = "0x571749edf1d5c9599318cdbc4e28a6475d65e87fd3b2ddbe1e9a8d5e7a0f0ff7"


def decode_filled_v3_relay_event(log: dict) -> dict:
    """
    Decode a FilledV3Relay EVENT LOG from eth_getLogs.
    
    Event logs have a different structure than function calls:
    - topics[0] = event signature hash
    - topics[1-3] = indexed parameters (originChainId, depositId, relayer)
    - data = ABI-encoded non-indexed parameters
    
    Args:
        log: A single log entry from eth_getLogs result (dict with 'topics' and 'data')
    
    Returns:
        Dictionary with all decoded event fields
    
    Example:
        >>> logs = get_logs(...)['result']
        >>> decoded = decode_filled_v3_relay_event(logs[0])
        >>> print(decoded['inputAmount'])
    """
    topics = log.get("topics", [])
    data_hex = log.get("data", "")
    
    # Step 1: Validate this is a FilledV3Relay event by checking topic[0]
    if len(topics) < 4:
        raise ValueError(f"Expected 4 topics, got {len(topics)}")
    
    if topics[0].lower() != FILLED_V3_RELAY_EVENT_SIGNATURE:
        raise ValueError(
            f"Not a FilledV3Relay event. Expected topic[0]={FILLED_V3_RELAY_EVENT_SIGNATURE}, "
            f"got {topics[0]}"
        )
    
    # Step 2: Extract INDEXED parameters from topics
    # topics are 32-byte hex strings, need to parse appropriately
    origin_chain_id = int(topics[1], 16)  # uint32 stored as 32 bytes
    deposit_id = int(topics[2], 16)        # uint32 stored as 32 bytes
    # Relayer address is in last 20 bytes of the 32-byte topic
    relayer = "0x" + topics[3][-40:]       # Extract last 40 hex chars (20 bytes)
    
    # Step 3: Decode NON-INDEXED parameters from data field
    if data_hex.startswith("0x") or data_hex.startswith("0X"):
        data_hex = data_hex[2:]
    
    data_bytes = bytes.fromhex(data_hex)
    
    # Step 4: Define ABI types for non-indexed event parameters
    # Order matches the event definition (excluding indexed params)
    # The last param is a tuple: (address recipient, bytes message, uint256 updatedOutputAmount, uint8 fillType)
    abi_types = [
        "address",   # inputToken
        "address",   # outputToken
        "uint256",   # inputAmount
        "uint256",   # outputAmount
        "uint256",   # repaymentChainId
        "uint32",    # fillDeadline
        "uint32",    # exclusivityDeadline
        "address",   # exclusiveRelayer
        "address",   # depositor
        "address",   # recipient
        "bytes",     # message
        "(address,bytes,uint256,uint8)",  # V3RelayExecutionEventInfo struct
    ]
    
    # Step 5: Decode the data field
    decoded = decode(abi_types, data_bytes)
    
    # Step 6: Helper for address conversion
    def to_hex_address(addr) -> str:
        """Convert address (bytes or str) to lowercase hex string."""
        if isinstance(addr, bytes):
            return "0x" + addr.hex()
        return addr.lower()
    
    # Step 7: Unpack the V3RelayExecutionEventInfo struct
    # (recipient, message, updatedOutputAmount, fillType)
    updatable_relay_data = decoded[11]
    
    # Step 8: Build comprehensive result dictionary
    result = {
        # From topics (indexed)
        "originChainId": origin_chain_id,
        "depositId": deposit_id,
        "relayer": relayer.lower(),
        
        # From data (non-indexed)
        "inputToken": to_hex_address(decoded[0]),
        "outputToken": to_hex_address(decoded[1]),
        "inputAmount": decoded[2],
        "outputAmount": decoded[3],
        "repaymentChainId": decoded[4],
        "fillDeadline": decoded[5],
        "exclusivityDeadline": decoded[6],
        "exclusiveRelayer": to_hex_address(decoded[7]),
        "depositor": to_hex_address(decoded[8]),
        "recipient": to_hex_address(decoded[9]),
        "message": "0x" + decoded[10].hex() if decoded[10] else "0x",
        
        # Nested struct: V3RelayExecutionEventInfo
        "updatableRelayData": {
            "recipient": to_hex_address(updatable_relay_data[0]),
            "message": "0x" + updatable_relay_data[1].hex() if updatable_relay_data[1] else "0x",
            "updatedOutputAmount": updatable_relay_data[2],
            "fillType": updatable_relay_data[3],  # 0=FastFill, 1=ReplacedSlowFill, 2=SlowFill
        },
        
        # Metadata from log
        "transactionHash": log.get("transactionHash", ""),
        "blockNumber": int(log.get("blockNumber", "0x0"), 16),
        "logIndex": int(log.get("logIndex", "0x0"), 16),
    }
    
    return result


def format_filled_v3_relay_event(decoded: dict) -> str:
    """
    Format decoded FilledV3Relay event into human-readable table.
    
    Args:
        decoded: Output from decode_filled_v3_relay_event()
    
    Returns:
        Formatted string for console output
    """
    # Map fillType int to readable string
    fill_type_names = {0: "FastFill", 1: "ReplacedSlowFill", 2: "SlowFill"}
    fill_type = fill_type_names.get(decoded["updatableRelayData"]["fillType"], "Unknown")
    
    lines = [
        "=" * 75,
        "DECODED FilledV3Relay EVENT",
        "=" * 75,
        "",
        f"{'Parameter':<30} | {'Value'}",
        "-" * 75,
        "--- INDEXED (from topics) ---",
        f"{'originChainId':<30} | {decoded['originChainId']}",
        f"{'depositId':<30} | {decoded['depositId']}",
        f"{'relayer':<30} | {decoded['relayer']}",
        "-" * 75,
        "--- NON-INDEXED (from data) ---",
        f"{'inputToken':<30} | {decoded['inputToken']}",
        f"{'outputToken':<30} | {decoded['outputToken']}",
        f"{'inputAmount (wei)':<30} | {decoded['inputAmount']:,}",
        f"{'outputAmount (wei)':<30} | {decoded['outputAmount']:,}",
        f"{'repaymentChainId':<30} | {decoded['repaymentChainId']}",
        f"{'fillDeadline':<30} | {decoded['fillDeadline']}",
        f"{'exclusivityDeadline':<30} | {decoded['exclusivityDeadline']}",
        f"{'exclusiveRelayer':<30} | {decoded['exclusiveRelayer']}",
        f"{'depositor':<30} | {decoded['depositor']}",
        f"{'recipient':<30} | {decoded['recipient']}",
        f"{'message':<30} | {decoded['message']}",
        "-" * 75,
        "--- UPDATABLE RELAY DATA ---",
        f"{'updatedRecipient':<30} | {decoded['updatableRelayData']['recipient']}",
        f"{'updatedOutputAmount (wei)':<30} | {decoded['updatableRelayData']['updatedOutputAmount']:,}",
        f"{'fillType':<30} | {fill_type}",
        "-" * 75,
        "--- METADATA ---",
        f"{'transactionHash':<30} | {decoded['transactionHash']}",
        f"{'blockNumber':<30} | {decoded['blockNumber']}",
        "=" * 75,
    ]
    
    return "\n".join(lines)


def decode_fill_v3_relay(data_hex: str) -> dict:
    """
    Decode the 'data' field from a fillV3Relay transaction.
    
    The data field structure:
    - First 4 bytes: Method selector (function ID)
    - Remaining bytes: ABI-encoded parameters
    
    Args:
        data_hex: Hex string from log's 'data' or tx 'input' field (with or without 0x prefix)
    
    Returns:
        Dictionary with decoded relayData fields and repaymentChainId
    
    Example:
        >>> decoded = decode_fill_v3_relay(log['data'])
        >>> print(decoded['relayData']['depositor'])
        '0x1231deb6f5749ef6ce6943a275a1d3e7486f4eae'
    """
    # Step 1: Normalize hex string - remove 0x prefix for consistent slicing
    if data_hex.startswith("0x") or data_hex.startswith("0X"):
        data_hex = data_hex[2:]
    
    # Step 2: Extract method ID (first 8 hex chars = 4 bytes)
    method_id = "0x" + data_hex[:8].lower()
    
    # Validate this is a fillV3Relay call
    if method_id != FILL_V3_RELAY_METHOD_ID:
        raise ValueError(
            f"Invalid method ID. Expected {FILL_V3_RELAY_METHOD_ID}, got {method_id}. "
            "This data is not from a fillV3Relay transaction."
        )
    
    # Step 3: Extract parameters (everything after 4-byte method selector)
    params_hex = data_hex[8:]
    
    # Step 4: Convert hex to bytes for ABI decoding
    params_bytes = bytes.fromhex(params_hex)
    
    # Step 5: Define ABI types matching the function signature
    # The tuple contains all relayData fields, followed by repaymentChainId
    abi_types = [
        "(address,address,address,address,address,uint256,uint256,uint256,uint32,uint32,uint32,bytes)",
        "uint256"
    ]
    
    # Step 6: Decode using eth_abi - returns tuple matching abi_types order
    decoded = decode(abi_types, params_bytes)
    
    # Step 7: Unpack decoded values
    relay_data_tuple = decoded[0]  # 12-element tuple
    repayment_chain_id = decoded[1]  # uint256
    
    # Step 8: Helper to convert address bytes to hex string
    def to_hex_address(addr_bytes: bytes) -> str:
        """Convert 20-byte address to lowercase hex string with 0x prefix."""
        return "0x" + addr_bytes.hex()
    
    # Step 9: Build structured response matching Etherscan's decoded view
    result = {
        "relayData": {
            "depositor": to_hex_address(relay_data_tuple[0]),
            "recipient": to_hex_address(relay_data_tuple[1]),
            "exclusiveRelayer": to_hex_address(relay_data_tuple[2]),
            "inputToken": to_hex_address(relay_data_tuple[3]),
            "outputToken": to_hex_address(relay_data_tuple[4]),
            "inputAmount": relay_data_tuple[5],           # uint256 in wei
            "outputAmount": relay_data_tuple[6],          # uint256 in wei
            "originChainId": relay_data_tuple[7],         # uint32 as int
            "depositId": relay_data_tuple[8],             # uint32 as int
            "fillDeadline": relay_data_tuple[9],          # uint32 as int
            "exclusivityDeadline": relay_data_tuple[10],  # uint32 as int
            "message": "0x" + relay_data_tuple[11].hex() if relay_data_tuple[11] else "0x",
        },
        "repaymentChainId": repayment_chain_id
    }
    
    return result


def format_relay_data(decoded: dict) -> str:
    """
    Format decoded relay data into human-readable table string.
    
    Args:
        decoded: Output from decode_fill_v3_relay()
    
    Returns:
        Formatted string matching Etherscan's decoded data display
    """
    relay = decoded["relayData"]
    
    lines = [
        "=" * 70,
        "DECODED fillV3Relay DATA",
        "=" * 70,
        "",
        f"{'Parameter':<30} | {'Value'}",
        "-" * 70,
        f"{'depositor':<30} | {relay['depositor']}",
        f"{'recipient':<30} | {relay['recipient']}",
        f"{'exclusiveRelayer':<30} | {relay['exclusiveRelayer']}",
        f"{'inputToken':<30} | {relay['inputToken']}",
        f"{'outputToken':<30} | {relay['outputToken']}",
        f"{'inputAmount (wei)':<30} | {relay['inputAmount']:,}",
        f"{'outputAmount (wei)':<30} | {relay['outputAmount']:,}",
        f"{'originChainId':<30} | {relay['originChainId']}",
        f"{'depositId':<30} | {relay['depositId']}",
        f"{'fillDeadline':<30} | {relay['fillDeadline']}",
        f"{'exclusivityDeadline':<30} | {relay['exclusivityDeadline']}",
        f"{'message':<30} | {relay['message']}",
        "-" * 70,
        f"{'repaymentChainId':<30} | {decoded['repaymentChainId']}",
        "=" * 70,
    ]
    
    return "\n".join(lines)


def get_logs(
    address: str = None,
    topics: list = None,
    from_block: str = "latest",
    to_block: str = "latest",
    blockhash: str = None
) -> dict:
    """
    Fetch event logs using eth_getLogs.
    
    Args:
        address: Contract address to filter (optional)
        topics: List of topic filters - topics[0] is event signature hash
        from_block: Starting block (hex or "latest")
        to_block: Ending block (hex or "latest")
        blockhash: Specific block hash (cannot use with from/to block)
    
    Returns:
        dict with 'result' array of log entries
    """
    # Build filter object - blockhash is MUTUALLY EXCLUSIVE with fromBlock/toBlock
    # Ethereum RPC spec: you can only use ONE approach - either blockhash OR block range
    if blockhash:
        # When using blockhash, we ONLY include blockhash (no from/to blocks allowed)
        filter_params = {"blockHash": blockhash}
    else:
        # When using block range, include from/to blocks
        filter_params = {
            "fromBlock": from_block,
            "toBlock": to_block,
        }
    
    # Add optional address filter
    if address:
        filter_params["address"] = address
    # Add optional topics filter
    if topics:
        filter_params["topics"] = topics
    
    # JSON-RPC payload
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [filter_params]
    }
    
    response = requests.post(
        INFURA_URL,
        json=payload,
        headers={"Content-Type": "application/json"}
    )
    
    return response.json()


if __name__ == "__main__":
    # Example: Fetch logs from a contract for a specific block NUMBER
    CONTRACT = "0x09aea4b2242abc8bb4bb78d537a67a245a7bec64"  # Base SpokePool
    
    # Convert block number to hex format (required by Ethereum JSON-RPC)
    # Example: block 19000000 -> "0x121eac0"
    BLOCK_NUMBER = 10874922  # <-- PUT YOUR BLOCK NUMBER HERE
    BLOCK_HEX = hex(BLOCK_NUMBER)  # Python's hex() converts int to "0x..." string
    
    result = get_logs(
        address=CONTRACT,
        from_block=BLOCK_HEX,  # Same block for both = query single block
        to_block=BLOCK_HEX
    )
    
    # ALWAYS print raw response first for debugging - shows exactly what API returned
    print("Raw API response:")
    print(json.dumps(result, indent=2))
    
    # Process results if they exist
    if "result" in result:
        logs = result["result"]
        print(f"\nFound {len(logs)} log(s)")
        
        # Iterate through logs and decode based on event type
        for i, log in enumerate(logs):
            topics = log.get("topics", [])
            
            # Check if this is a FilledV3Relay EVENT (topic[0] = event signature)
            if topics and topics[0].lower() == FILLED_V3_RELAY_EVENT_SIGNATURE:
                print(f"\n--- Log #{i+1}: FilledV3Relay EVENT detected ---")
                try:
                    # Decode the event log and print formatted output
                    decoded = decode_filled_v3_relay_event(log)
                    print(format_filled_v3_relay_event(decoded))
                except Exception as e:
                    print(f"Failed to decode: {e}")
            else:
                # Unknown event - show topic[0] for identification
                topic0 = topics[0][:18] + "..." if topics else "no topics"
                print(f"\n--- Log #{i+1}: Unknown event (topic[0]: {topic0}) ---")
                
    elif "error" in result:
        # Handle API errors explicitly - shows error code and message
        print(f"\nAPI Error: {result['error']}")