{% macro warehouse_date_diff(datepart, start_date, end_date) %}
    -- Warehouse-agnostic date_diff function
    -- datepart: 'day', 'week', 'month', 'year'
    -- Works on Snowflake and BigQuery
    
    {% if target.type == 'snowflake' %}
        datediff('{{ datepart }}', {{ start_date }}, {{ end_date }})
    {% elif target.type == 'bigquery' %}
        date_diff({{ end_date }}, {{ start_date }}, {{ datepart }})
    {% else %}
        -- Default to Snowflake syntax
        datediff('{{ datepart }}', {{ start_date }}, {{ end_date }})
    {% endif %}
{% endmacro %}

