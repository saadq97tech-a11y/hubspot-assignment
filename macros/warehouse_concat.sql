{% macro warehouse_concat(column1, separator, column2) %}
    -- Warehouse-agnostic string concatenation
    -- Concatenates column1 + separator + column2
    
    {% if target.type == 'snowflake' %}
        {{ column1 }} || {{ separator }} || cast({{ column2 }} as string)
    {% elif target.type == 'bigquery' %}
        concat(cast({{ column1 }} as string), {{ separator }}, cast({{ column2 }} as string))
    {% else %}
        -- Default to Snowflake syntax
        {{ column1 }} || {{ separator }} || cast({{ column2 }} as string)
    {% endif %}
{% endmacro %}

