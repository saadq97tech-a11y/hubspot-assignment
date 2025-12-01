-- Business Problem #1: Amenity Revenue
-- Find the total revenue and percentage of revenue by month segmented by whether or not 
-- air conditioning exists on the listing.

with monthly_revenue as (
    select
        {{ warehouse_date_trunc('month', 'date') }} as month,
        has_air_conditioning,
        sum(revenue_usd) as total_revenue_usd
    from {{ ref('fct_listings_daily') }}
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
    mr.month,
    mr.has_air_conditioning,
    mr.total_revenue_usd,
    mt.month_total_revenue_usd,
    round(
        (mr.total_revenue_usd / nullif(mt.month_total_revenue_usd, 0)) * 100,
        1
    ) as revenue_percentage
from monthly_revenue as mr
left join monthly_totals as mt
    on mr.month = mt.month
order by mr.month, mr.has_air_conditioning

