-- Business Problem #3A: Long Stay / Picky Renter
-- Uses pre-computed mart: mart_listing_max_stay_duration

select
    listing_id,
    max_possible_stay_days
from {{ ref('mart_listing_max_stay_duration') }}
order by max_possible_stay_days desc

