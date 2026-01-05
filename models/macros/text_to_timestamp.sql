-- Macro: Convert TEXT to TIMESTAMP
-- Usage: {{ text_to_timestamp('text_column') }}

{% macro text_to_timestamp(text_column) %}
    {#
      Normalize timestamp-like strings into TIMESTAMP.
      - If the value is purely numeric, we treat it as Unix seconds.
      - SAFE_CAST functions return NULL instead of errors on unexpected inputs.
    #}
    {% set expr %}
    CASE
        WHEN {{ text_column }} IS NULL THEN NULL
        WHEN REGEXP_CONTAINS({{ text_column }}, r'^\d+$') THEN TIMESTAMP_SECONDS(SAFE_CAST({{ text_column }} AS INT64))
        ELSE SAFE_CAST({{ text_column }} AS TIMESTAMP)
    END
    {% endset %}
    {{ return(expr) }}
{% endmacro %}