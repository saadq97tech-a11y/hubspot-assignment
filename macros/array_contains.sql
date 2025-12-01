{% macro array_contains(array_column, value) %}
    -- Check if array contains a specific value
    -- Warehouse-agnostic wrapper
    {{ warehouse_array_contains(array_column, value) }}
{% endmacro %}

