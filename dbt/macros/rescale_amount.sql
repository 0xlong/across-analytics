{#
    Macro: rescale_amount
    
    Purpose: Divides a raw token amount by 10^decimals to get human-readable amount.
    Uses COALESCE to default to 18 decimals if token not found in metadata.
    
    Usage:
        {{ rescale_amount('input_amount_raw', 'input_tok.decimals') }} AS input_amount
    
    Args:
        amount_column: The column containing the raw amount (e.g., 'input_amount_raw')
        decimals_column: The column containing decimals from token_decimals join (e.g., 'input_tok.decimals')
    
    Returns:
        SQL expression that divides amount by POWER(10, decimals)
#}

{% macro rescale_amount(amount_column, decimals_column) %}
    {{ amount_column }} / POWER(10, COALESCE({{ decimals_column }}, 18))
{% endmacro %}
