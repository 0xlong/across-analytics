{#
    Macro: get_token_decimals
    
    Purpose: Returns a CTE query that fetches token decimals for a specific chain.
    This enables efficient amount rescaling by joining with token metadata.
    
    Usage:
        WITH token_decimals AS (
            {{ get_token_decimals('arbitrum') }}
        ),
        ...
        LEFT JOIN token_decimals input_tok 
            ON LOWER(c.input_token_address) = input_tok.token_address
    
    Args:
        chain_name: The blockchain name (e.g., 'ethereum', 'arbitrum', 'polygon')
    
    Returns:
        SQL query selecting token_address (lowercase) and decimals for the specified chain
#}

{% macro get_token_decimals(chain_name) %}
    SELECT 
        LOWER(token_address) AS token_address,
        token_symbol,
        decimals
    FROM {{ ref('token_metadata') }}
    WHERE LOWER(chain) = LOWER('{{ chain_name }}')
{% endmacro %}
