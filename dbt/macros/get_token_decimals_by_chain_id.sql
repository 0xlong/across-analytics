{#
    Macro: get_token_decimals_by_chain_id
    
    Purpose: Returns token metadata with chain_id for joining 
    output tokens based on destination_chain_id in deposits.
    
    Usage:
        WITH output_token_meta AS (
            {{ get_token_decimals_by_chain_id() }}
        ),
        ...
        LEFT JOIN output_token_meta output_tok 
            ON c.output_token_address = output_tok.token_address
            AND c.destination_chain_id = output_tok.chain_id
    
    Returns:
        SQL query selecting token_address (lowercase), token_symbol, 
        decimals, and chain_id from all chains
#}

{% macro get_token_decimals_by_chain_id() %}
    SELECT 
        LOWER(token_address) AS token_address,
        token_symbol,
        decimals,
        chain_id
    FROM {{ ref('token_metadata') }}
{% endmacro %}
