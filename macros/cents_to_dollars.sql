{% macro cents_to_dollars(column_name) %}
    -- Convert price strings like "$125.00" to decimal 125.00 (warehouse-agnostic)
    {% if target.type == 'bigquery' %}
        cast(
            replace(
                replace({{ column_name }}, '$', ''),
                ',',
                ''
            ) as numeric
        )
    {% else %}
        -- Snowflake and others
        cast(
            replace(
                replace({{ column_name }}, '$', ''),
                ',',
                ''
            ) as decimal(10, 2)
        )
    {% endif %}
{% endmacro %}

