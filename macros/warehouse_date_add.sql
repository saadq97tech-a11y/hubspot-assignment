{% macro warehouse_date_add(date_column, interval_value, interval_unit) %}
    -- Warehouse-agnostic date addition
    -- interval_unit: 'day', 'week', 'month', 'year'
    -- Works on Snowflake and BigQuery
    
    {% if target.type == 'snowflake' %}
        dateadd('{{ interval_unit }}', {{ interval_value }}, {{ date_column }})
    {% elif target.type == 'bigquery' %}
        date_add({{ date_column }}, interval {{ interval_value }} {{ interval_unit }})
    {% else %}
        -- Default to Snowflake syntax
        dateadd('{{ interval_unit }}', {{ interval_value }}, {{ date_column }})
    {% endif %}
{% endmacro %}

