{% macro warehouse_string_contains(column_name, search_value) %}
    -- Warehouse-agnostic string contains check (case-insensitive)
    -- Works on Snowflake and BigQuery
    -- search_value should be passed WITH quotes, e.g., "'Air conditioning'"
    
    {% set clean_value = search_value | replace("'", "") | replace('"', '') %}
    
    {% if target.type == 'snowflake' %}
        case
            when {{ column_name }} is null then false
            when upper(cast({{ column_name }} as string)) like '%' || upper('{{ clean_value }}') || '%' then true
            else false
        end
    {% elif target.type == 'bigquery' %}
        case
            when {{ column_name }} is null then false
            when upper(cast({{ column_name }} as string)) like concat('%', upper('{{ clean_value }}'), '%') then true
            else false
        end
    {% else %}
        -- Default to Snowflake syntax
        case
            when {{ column_name }} is null then false
            when upper(cast({{ column_name }} as string)) like '%' || upper('{{ clean_value }}') || '%' then true
            else false
        end
    {% endif %}
{% endmacro %}

