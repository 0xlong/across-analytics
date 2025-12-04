"""
Event Signature Decoder using 4byte.directory API

This script decodes Ethereum event signature hashes (topic[0]) to human-readable
event names using the 4byte.directory public API.

How it works:
1. Parse event hashes from the BigQuery frequency table
2. Query 4byte.directory API for each unique hash
3. Display results with event names and full signatures
"""

import requests
import time

# =============================================================================
# INPUT DATA: BigQuery Results - Event Hash Frequency Table
# SQL Query:
"""
SELECT
    topics[SAFE_OFFSET(0)] AS topic_hash,
    COUNT(*) AS frequency
FROM
    `bigquery-public-data.goog_blockchain_arbitrum_one_us.logs`
WHERE
    address = '0xe35e9842fceaca96570b734083f4a58e8f7c5f2a'
GROUP BY
    topic_hash
ORDER BY
    frequency DESC
"""
# =============================================================================
# Format: Row Number | Event Signature Hash (topic[0]) | Occurrence Count
data = """
1	0x571749edf1d5c9599318cdbc4e28a6475d65e87fd3b2ddbe1e9a8d5e7a0f0ff7	1792689
2	0xa123dc29aebf7d0c3322c8eeb5b999e859f39937950ed31056532713d0de396f	1644290
3	0x44b559f101f8fbcc8a0ea43fa91a05a729a5ea6e14a7c75aa750374690137208	1145619
4	0x32ed1a409ef04c7b0227189c3a103dc5ac10e775a15b785dcc510201f7c25ad3	1061217
5	0x8ab9dc6c19fe88e69bc70221b339c84332752fdd49591b7c51e66bae3947b73c	485048
6	0xafc4df6845a4ab948b492800d3d8a25d538a102a2bc07cd01f1cfa097fddcff6	398554
7	0xf8bd640004bcec1b89657020f561d0b070cbdf662d0b158db9dccb0a8301bfab	38310
8	0xf4ad92585b1bc117fbdd644990adf0827bc4c95baeae8a23322af807b6d0020e	24607
9	0xc86ba04c55bc5eb2f2876b91c438849a296dbec7b08751c3074d92e04f0a77af	15855
10	0x923794976d026d6b119735adc163cb71decfc903e17c3dc226c00789593c04e1	7788
11	0x828fc203220356df8f072a91681caee7d5c75095e2a95e80ed5a14b384697f71	4410
12	0x997d81a0a8415d688a6c319736602098252bf6445e0e879326f682f11928e317	2443
13	0x3cee3e290f36226751cd0b3321b213890fe9c768e922f267fa6111836ce05c32	2136
14	0xfa7fa7cf6d7dde5f9be65a67e6a1a747e7aa864dcd2d793353c722d80fbbb357	1047
15	0xa6aa57bd282fc82378458de27be4eadfa791a0c7321c49562eeba8b2643dd566	176
16	0x0a21fdd43d0ad0c62689ee7230a47309a050755bcc52eba00310add65297692a	123
17	0xb0a29aed3d389a1041194255878b423f7780be3ed2324d4693508c6ff189845e	35
18	0x8d7f294eaa476236fe8cb5629376a12cd37dace3d21e6a7b98f1641c4ed5f09e	12
19	0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b	12
20	0x3569b846531b754c99cb80df3f49cd72fa6fe106aaee5ab8e0caf35a9d7ce88d	2
21	0xe88463c2f254e2b070013a2dc7ee1e099f9bc00534cbdf03af551dc26ae49219	2
22	0x7f26b83ff96e1f2b6a682f133852f6798a09c465da95921460cefb3847402498	1
23	0xdc4a5f4c066ad14c1306e624550b42395e08f992a76b416cc7b1ad11503d376c	1
24	0x1f17a88f67b0f49060a34bec1a4723a563620e6aa265eb640b5046dcee0759a0	1
25	0x323983f5343e25b2c1396361b1b791be31484841fdfb95b8615cd02d910b1e08	1
26	0xa9e8c42c9e7fca7f62755189a16b2f5314d43d8fb24e91ba54e6d65f9314e849	1
"""


def query_4byte_api(hex_signature: str) -> dict:
    """
    Query the 4byte.directory API to decode an event signature hash.
    
    The 4byte.directory is a public database that maps:
    - Function selectors (first 4 bytes of keccak256 hash) to function signatures
    - Event signature hashes (full 32 bytes keccak256) to event signatures
    
    Args:
        hex_signature: The 0x-prefixed event signature hash (topic[0])
    
    Returns:
        dict with 'name' (just event name) and 'signature' (full signature with params)
        Returns None values if not found in the database
    """
    # 4byte.directory API endpoint for event signatures
    # Note: Use hex_signature param (not bytes_signature which is for the web UI)
    api_url = "https://www.4byte.directory/api/v1/event-signatures/"
    
    try:
        # Query with the hex signature - API expects the full 32-byte hash
        response = requests.get(
            api_url,
            params={"hex_signature": hex_signature},
            timeout=10  # 10 second timeout to avoid hanging
        )
        response.raise_for_status()  # Raise exception for HTTP errors (4xx, 5xx)
        
        data = response.json()
        results = data.get("results", [])
        
        if results:
            # Return the first match (usually the most common/canonical one)
            # The 'text_signature' contains the full signature like "Transfer(address,address,uint256)"
            full_signature = results[0]["text_signature"]
            
            # Extract just the event name (everything before the first parenthesis)
            event_name = full_signature.split("(")[0]
            
            return {
                "name": event_name,
                "signature": full_signature,
                "matches": len(results)  # Number of potential matches (hash collisions possible)
            }
        else:
            # No results found in the database
            return {"name": None, "signature": None, "matches": 0}
            
    except requests.exceptions.RequestException as e:
        # Handle network errors, timeouts, etc.
        print(f"  ⚠️ API Error for {hex_signature[:18]}...: {e}")
        return {"name": "API_ERROR", "signature": str(e), "matches": 0}


def parse_event_data(raw_data: str) -> list:
    """
    Parse the tab-separated event frequency data into structured records.
    
    Args:
        raw_data: Multi-line string with format "row_num\thash\tfrequency"
    
    Returns:
        List of dicts with row_num, hash, and frequency fields
    """
    events = []
    
    for line in raw_data.strip().split('\n'):
        if not line.strip():
            continue  # Skip empty lines
            
        parts = line.split('\t')
        if len(parts) >= 3:
            events.append({
                "row": int(parts[0]),
                "hash": parts[1].strip().lower(),  # Normalize to lowercase
                "frequency": int(parts[2])
            })
    
    return events


def main():
    """
    Main execution: Decode all event signatures and display results.
    """
    print("=" * 100)
    print("🔍 Event Signature Decoder - Using 4byte.directory API")
    print("📊 Arbitrum SpokePool (Across Protocol) Event Analysis")
    print("=" * 100)
    print()
    
    # Parse the input data into structured format
    events = parse_event_data(data)
    
    print(f"📋 Processing {len(events)} unique event signatures...")
    print()
    
    # Store results for final summary table
    decoded_results = []
    
    # Query each event signature against the 4byte API
    for i, event in enumerate(events, 1):
        print(f"[{i:02d}/{len(events)}] Querying: {event['hash'][:20]}...")
        
        # Query the API
        result = query_4byte_api(event['hash'])
        
        # Store combined result
        decoded_results.append({
            **event,  # Spread original data (row, hash, frequency)
            **result  # Spread API result (name, signature, matches)
        })
        
        # Brief status update
        status = result['name'] if result['name'] else "NOT FOUND"
        print(f"         → {status}")
        
        # Polite rate limiting: 0.5 second delay between requests
        # This prevents overwhelming the free API service
        if i < len(events):
            time.sleep(0.5)
    
    print()
    print("=" * 100)
    print("📊 DECODED EVENT SIGNATURES - RESULTS TABLE")
    print("=" * 100)
    print()
    
    # Print formatted results table
    print(f"{'#':<3} {'Frequency':<12} {'Event Name':<35} {'Topic Hash'}")
    print("-" * 100)
    
    for r in decoded_results:
        # Format the event name or show "UNKNOWN" if not found
        event_name = r['name'] if r['name'] else "❌ UNKNOWN"
        
        # Truncate hash for display (show first and last parts)
        short_hash = f"{r['hash'][:10]}...{r['hash'][-8:]}"
        
        print(f"{r['row']:<3} {r['frequency']:>10,}  {event_name:<35} {short_hash}")
    
    print("-" * 100)
    print()
    
    # Print detailed signatures for reference
    print("=" * 100)
    print("📝 FULL EVENT SIGNATURES (for ABI reference)")
    print("=" * 100)
    print()
    
    for r in decoded_results:
        if r['signature']:
            # Show matches count if there are potential hash collisions
            collision_note = f" ({r['matches']} matches)" if r['matches'] > 1 else ""
            print(f"✅ {r['name']}{collision_note}")
            print(f"   Hash: {r['hash']}")
            print(f"   Sig:  {r['signature']}")
            print()
        else:
            print(f"❌ UNKNOWN - Not in 4byte.directory")
            print(f"   Hash: {r['hash']}")
            print(f"   This is likely a custom/proprietary event from the Across protocol")
            print()
    
    # Summary statistics
    found = sum(1 for r in decoded_results if r['name'] and r['name'] != "API_ERROR")
    not_found = len(decoded_results) - found
    
    print("=" * 100)
    print(f"📈 SUMMARY: {found}/{len(decoded_results)} signatures decoded, {not_found} unknown")
    print("=" * 100)


# =============================================================================
# SCRIPT ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    main()