{{
  config(
    materialized='table',
    unique_key='month_amenity_key'
  )
}}

-- Business Mart: Monthly revenue aggregated by amenity flags
-- Answers: Business Question #1 - Amenity Revenue Analysis

with fact_data as (
    select * from {{ ref('fct_listings_daily') }}
),

monthly_revenue as (
    select
        {{ warehouse_date_trunc('month', 'date') }} as month,
        has_air_conditioning,
        sum(revenue_usd) as total_revenue_usd,
        count(*) as total_days,
        sum(case when has_reservation then 1 else 0 end) as reserved_days
    from fact_data
    group by 1, 2
),

monthly_totals as (
    select
        month,
        sum(total_revenue_usd) as month_total_revenue_usd
    from monthly_revenue
    group by 1
)

select
    {{ dbt_utils.surrogate_key(['mr.month', 'mr.has_air_conditioning']) }} as month_amenity_key,
    mr.month,
    mr.has_air_conditioning,
    mr.total_revenue_usd,
    mt.month_total_revenue_usd,
    round(
        (mr.total_revenue_usd / nullif(mt.month_total_revenue_usd, 0)) * 100,
        1
    ) as revenue_percentage,
    mr.total_days,
    mr.reserved_days,
    current_timestamp() as dbt_updated_at
from monthly_revenue as mr
left join monthly_totals as mt
    on mr.month = mt.month
order by mr.month, mr.has_air_conditioning

