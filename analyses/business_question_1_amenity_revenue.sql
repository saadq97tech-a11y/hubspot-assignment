-- Business Problem #1: Amenity Revenue
-- Uses pre-computed mart: mart_monthly_revenue_by_amenity

select
    month,
    has_air_conditioning,
    total_revenue_usd,
    month_total_revenue_usd,
    revenue_percentage,
    total_days,
    reserved_days
from {{ ref('mart_monthly_revenue_by_amenity') }}
order by month, has_air_conditioning

