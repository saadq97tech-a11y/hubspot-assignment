{% macro parse_json_array(column_name) %}
    -- Parse JSON array string to array type
    -- This is a simplified version - adjust based on your warehouse
    {{ column_name }}::json
{% endmacro %}

