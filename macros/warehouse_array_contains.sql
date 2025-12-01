{% macro warehouse_array_contains(array_column, value) %}
    -- Warehouse-agnostic array contains check
    -- Works on Snowflake and BigQuery
    -- For JSON arrays stored as strings, uses string matching
    -- value should be passed WITH quotes, e.g., "'Air conditioning'"
    
    {% set clean_value = value | replace("'", "") | replace('"', '') %}
    
    {% if target.type == 'snowflake' %}
        case
            when {{ array_column }} is null then false
            when upper(cast({{ array_column }} as string)) like '%' || upper('{{ clean_value }}') || '%' then true
            else false
        end
    {% elif target.type == 'bigquery' %}
        case
            when {{ array_column }} is null then false
            when upper(cast({{ array_column }} as string)) like concat('%', upper('{{ clean_value }}'), '%') then true
            else false
        end
    {% else %}
        -- Default fallback
        case
            when {{ array_column }} is null then false
            when upper(cast({{ array_column }} as string)) like '%' || upper('{{ clean_value }}') || '%' then true
            else false
        end
    {% endif %}
{% endmacro %}

