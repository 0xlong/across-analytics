{#
    Macro: rescale_output_amount
    
    Purpose: Rescales output_amount_raw with conditional logic:
    - If output token is native (zero address): use INPUT token decimals
    - If output token is regular ERC-20: use OUTPUT token decimals
    
    Why: In Across Protocol, when bridging to native tokens (ETH, POL, etc.),
    the output_amount_raw is denominated in the input token's decimal scale.
    For regular ERC-20 outputs, it uses the output token's decimals.
    
    Usage:
        {{ rescale_output_amount('c.output_token_address', 'c.output_amount_raw', 'input_tok.decimals', 'output_tok.decimals') }} AS output_amount
    
    Args:
        output_token_address: Column with output token address
        amount_column: Column with raw output amount
        input_decimals_column: Column with input token decimals
        output_decimals_column: Column with output token decimals
#}

{% macro rescale_output_amount(output_token_address, amount_column, input_decimals_column, output_decimals_column) %}
    CASE 
        WHEN {{ output_token_address }} = '0x0000000000000000000000000000000000000000' 
        THEN {{ amount_column }} / POWER(10, COALESCE({{ input_decimals_column }}, 18))
        ELSE {{ amount_column }} / POWER(10, COALESCE({{ output_decimals_column }}, 18))
    END
{% endmacro %}
