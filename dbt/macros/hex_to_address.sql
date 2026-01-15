-- Macro: Validate and format addresses
-- Usage: {{ hex_to_address('address_column') }}

# code
{% macro hex_to_address(hex_column) %}
    {# 
      Convert a 32-byte hex string (topic/data slot) into a standard EVM
      address string. Addresses are right-aligned within 32-byte topics, so
      we strip any leading "0x", keep the last 40 hex characters (20 bytes),
      and add "0x" back to produce the familiar address shape.
    #}
    {% set expr %}
    (
        '0x' ||
        SUBSTR(
            REGEXP_REPLACE({{ hex_column }}, r'^0x', ''), -- drop leading 0x if present
            -40                                            -- take the least-significant 20 bytes
        )
    )
    {% endset %}
    {{ return(expr) }}
{% endmacro %}