{{
  config(
    materialized='table',
    unique_key='date_key'
  )
}}

with
    date_spine as (
        select
            date_day
        from {{ dbt_utils.date_spine(
            datepart="day",
            start_date="'" ~ var('analysis_start_date') ~ "'",
            end_date="'" ~ var('analysis_end_date') ~ "'"
        ) }}
    ),

    dim_dates as (
        select
            -- surrogate key
            {{ dbt_utils.surrogate_key(['date_day']) }} as date_key,
            
            -- natural key
            date_day as date,
            
            -- date attributes
            {{ warehouse_date_trunc('year', 'date_day') }} as year,
            {{ warehouse_date_trunc('quarter', 'date_day') }} as quarter,
            {{ warehouse_date_trunc('month', 'date_day') }} as month,
            {{ warehouse_date_trunc('week', 'date_day') }} as week,
            
            -- day attributes (warehouse-agnostic)
            {{ warehouse_extract('dayofweek', 'date_day') }} as day_of_week,
            {{ warehouse_extract('day', 'date_day') }} as day_of_month,
            {{ warehouse_extract('dayofyear', 'date_day') }} as day_of_year,
            
            -- month attributes
            {{ warehouse_extract('month', 'date_day') }} as month_number,
            {{ warehouse_extract('quarter', 'date_day') }} as quarter_number,
            {{ warehouse_extract('year', 'date_day') }} as year_number,
            
            -- week attributes
            {{ warehouse_extract('week', 'date_day') }} as week_number,
            
            -- flags (warehouse-agnostic)
            -- day_of_week is normalized to 0-6 (0=Sunday, 6=Saturday) by warehouse_extract macro
            -- is_weekday can be derived as: NOT is_weekend
            case
                when {{ warehouse_extract('dayofweek', 'date_day') }} in (0, 6) then true
                else false
            end as is_weekend,
            
            -- metadata
            current_timestamp() as dbt_updated_at
            
        from date_spine
    )

select * from dim_dates

