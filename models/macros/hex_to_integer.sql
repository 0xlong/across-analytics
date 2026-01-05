-- Macro: Convert hex TEXT to INTEGER
-- Usage: {{ hex_to_integer('hex_column') }}

{% macro hex_to_integer(hex_column) %}
    {#
      Convert a hex string into an INT64. This is the right fit for fields
      like chain_id, block_number, log_index, and other values that fit in
      64 bits. We remove any "0x" prefix, prepend it back for casting, and
      use SAFE_CAST to avoid breaking the query if the text is malformed.
    #}
    {% set expr %}
    SAFE_CAST(
        CONCAT('0x', REGEXP_REPLACE({{ hex_column }}, r'^0x', ''))
        AS INT64
    )
    {% endset %}
    {{ return(expr) }}
{% endmacro %}