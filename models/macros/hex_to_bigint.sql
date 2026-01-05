-- Macro: Convert hex TEXT to NUMERIC(78,0)
-- Usage: {{ hex_to_bigint('hex_column') }}

{% macro hex_to_bigint(hex_column) %}
    {#
      Convert a hex string (optionally prefixed with "0x") into a wide
      BIGNUMERIC so we can safely handle uint256-sized values without
      overflow. We strip any leading prefix, re-add 0x so BigQuery can
      interpret the literal, and use SAFE_CAST to return NULL instead of
      erroring on bad input.
    #}
    {% set expr %}
    SAFE_CAST(
        CONCAT('0x', REGEXP_REPLACE({{ hex_column }}, r'^0x', ''))
        AS BIGNUMERIC
    )
    {% endset %}
    {{ return(expr) }}
{% endmacro %}