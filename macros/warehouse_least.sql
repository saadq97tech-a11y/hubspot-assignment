{% macro warehouse_least(*args) %}
    -- Warehouse-agnostic least function
    -- Works on Snowflake and BigQuery
    
    {% if target.type == 'snowflake' %}
        least({{ args | join(", ") }})
    {% elif target.type == 'bigquery' %}
        least({{ args | join(", ") }})
    {% else %}
        -- Both use same syntax
        least({{ args | join(", ") }})
    {% endif %}
{% endmacro %}

