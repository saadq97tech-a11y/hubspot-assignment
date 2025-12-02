-- Business Problem #3B: Long Stay / Picky Renter (with amenities filter)
-- Uses pre-computed mart: mart_listing_max_stay_duration
-- Filters for listings with both lockbox AND first aid kit

with filtered_listings as (
    select distinct listing_id
    from {{ ref('fct_listings_daily') }}
    where has_lockbox = true
        and has_first_aid_kit = true
)

select
    msmd.listing_id,
    msmd.max_possible_stay_days
from {{ ref('mart_listing_max_stay_duration') }} as msmd
inner join filtered_listings as fl
    on msmd.listing_id = fl.listing_id
order by msmd.max_possible_stay_days desc

