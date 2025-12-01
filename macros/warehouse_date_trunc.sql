{% macro warehouse_date_trunc(datepart, date_column) %}
    -- Warehouse-agnostic date_trunc function
    -- datepart: 'day', 'week', 'month', 'quarter', 'year'
    -- Works on Snowflake and BigQuery
    
    {% if target.type == 'snowflake' %}
        date_trunc('{{ datepart }}', {{ date_column }})
    {% elif target.type == 'bigquery' %}
        date_trunc({{ date_column }}, {{ datepart | upper }})
    {% else %}
        -- Default to Snowflake syntax
        date_trunc('{{ datepart }}', {{ date_column }})
    {% endif %}
{% endmacro %}

