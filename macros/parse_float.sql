{% macro parse_float(column_name) %}
    -- Parse string to float, handling nulls and empty strings (warehouse-agnostic)
    case
        when {{ column_name }} is null then null
        when {{ warehouse_cast(column_name, 'string') }} = '' then null
        else {{ warehouse_cast(column_name, 'float64') }}
    end
{% endmacro %}

