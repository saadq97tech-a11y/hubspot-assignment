{% macro parse_json_amenity(column_name, amenity_name) %}
    -- Check if JSON array contains a specific amenity
    -- Warehouse-agnostic using string matching
    {{ warehouse_string_contains(column_name, amenity_name) }}
{% endmacro %}

