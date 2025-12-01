-- Business Problem #3A: Long Stay / Picky Renter
-- Find the maximum duration one could stay in each listing, based on availability 
-- and what the owner allows.

with calendar_availability as (
    select
        fld.listing_id,
        fld.date,
        dl.minimum_nights,
        dl.maximum_nights,
        -- Create groups of consecutive available dates using date difference
        -- Warehouse-agnostic: subtract row_number from date to create groups
        {% if target.type == 'bigquery' %}
            date_sub(fld.date, interval cast(row_number() over (partition by fld.listing_id order by fld.date) as int64) day) as availability_group
        {% else %}
            dateadd('day', -1 * cast(row_number() over (partition by fld.listing_id order by fld.date) as integer), fld.date) as availability_group
        {% endif %}
    from {{ ref('fct_listings_daily') }} as fld
    inner join {{ ref('dim_listings') }} as dl
        on fld.listing_id = dl.listing_id
    where fld.has_reservation = false
),

consecutive_availability as (
    select
        listing_id,
        availability_group,
        min(date) as start_date,
        max(date) as end_date,
        count(*) as consecutive_days_available,
        min(minimum_nights) as min_nights_required,
        min(maximum_nights) as max_nights_allowed
    from calendar_availability
    group by 1, 2
),

-- Calculate possible stay duration considering constraints
possible_stays as (
    select
        listing_id,
        availability_group,
        consecutive_days_available,
        min_nights_required,
        max_nights_allowed,
        -- Stay duration is limited by: consecutive days available AND max nights allowed
        -- But must meet minimum nights requirement
        case
            when consecutive_days_available < min_nights_required then 0
            when consecutive_days_available <= max_nights_allowed then consecutive_days_available
            else max_nights_allowed
        end as possible_stay_days
    from consecutive_availability
),

max_stay_per_listing as (
    select
        listing_id,
        max(possible_stay_days) as max_possible_stay_days
    from possible_stays
    where possible_stay_days > 0
    group by 1
)

select
    listing_id,
    max_possible_stay_days
from max_stay_per_listing
order by max_possible_stay_days desc

