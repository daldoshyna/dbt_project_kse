{% macro standardize_text(column_name) %}
    trim(
        upper(substring({{ column_name }}, 1, 1)) ||
        lower(substring({{ column_name }}, 2))
    )
{% endmacro %}