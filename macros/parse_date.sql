{% macro parse_date(column_name) %}
    -- Parse date string to date type (warehouse-agnostic)
    case
        when {{ column_name }} is null then null
        when {{ warehouse_cast(column_name, 'string') }} = '' then null
        else {{ warehouse_cast(column_name, 'date') }}
    end
{% endmacro %}

