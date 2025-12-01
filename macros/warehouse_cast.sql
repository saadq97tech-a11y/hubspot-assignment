{% macro warehouse_cast(column_name, data_type) %}
    -- Warehouse-agnostic type casting
    -- Works on Snowflake and BigQuery
    
    {% if target.type == 'snowflake' %}
        {{ column_name }}::{{ data_type }}
    {% elif target.type == 'bigquery' %}
        cast({{ column_name }} as {{ data_type }})
    {% else %}
        -- Default to Snowflake syntax
        {{ column_name }}::{{ data_type }}
    {% endif %}
{% endmacro %}

