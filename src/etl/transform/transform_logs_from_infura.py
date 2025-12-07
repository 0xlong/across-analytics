from datetime import datetime, UTC


def get_datetime_from_blocknumber_in_hex(blocknumber_in_hex: str) -> str:
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