{% macro warehouse_extract(datepart, date_column) %}
    -- Warehouse-agnostic extract function
    -- datepart: 'day', 'month', 'year', 'dayofweek', 'dayofyear', 'week', 'quarter'
    -- Works on Snowflake and BigQuery
    -- Note: dayofweek is normalized to 0-6 (0=Sunday, 6=Saturday) for consistency
    
    {% if datepart == 'dayofweek' %}
        -- Normalize dayofweek to 0-6 (0=Sunday, 6=Saturday) for both warehouses
        {% if target.type == 'snowflake' %}
            extract(dow from {{ date_column }})
        {% elif target.type == 'bigquery' %}
            extract(DAYOFWEEK from {{ date_column }}) - 1  -- Convert 1-7 to 0-6
        {% else %}
            extract(dow from {{ date_column }})
        {% endif %}
    {% elif datepart == 'dayofyear' %}
        {% if target.type == 'snowflake' %}
            extract(doy from {{ date_column }})
        {% elif target.type == 'bigquery' %}
            extract(DAYOFYEAR from {{ date_column }})
        {% else %}
            extract(doy from {{ date_column }})
        {% endif %}
    {% else %}
        {% if target.type == 'snowflake' %}
            extract({{ datepart }} from {{ date_column }})
        {% elif target.type == 'bigquery' %}
            extract({{ datepart | upper }} from {{ date_column }})
        {% else %}
            extract({{ datepart }} from {{ date_column }})
        {% endif %}
    {% endif %}
{% endmacro %}

