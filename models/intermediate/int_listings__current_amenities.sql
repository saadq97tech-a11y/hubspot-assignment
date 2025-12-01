{{
  config(
    materialized='ephemeral'
  )
}}

with
    -- Get all unique listing-date combinations from calendar
    listing_dates as (
        select distinct
            listing_id,
            date
        from {{ ref('stg_calendar') }}
    ),

    -- Get amenities changelog with date cast for comparison
    amenities_changes as (
        select
            listing_id,
            {{ warehouse_cast('change_at', 'date') }} as change_date,
            change_at,
            amenities_array
        from {{ ref('stg_amenities_changelog') }}
        where change_at is not null
    ),

    -- For each listing-date, find the most recent amenities change before or on that date
    listing_amenities_with_changes as (
        select
            ld.listing_id,
            ld.date,
            ac.amenities_array,
            ac.change_at as amenities_effective_at,
            row_number() over (
                partition by ld.listing_id, ld.date
                order by ac.change_at desc
            ) as change_rank
        from listing_dates as ld
        left join amenities_changes as ac
            on ld.listing_id = ac.listing_id
            and ac.change_date <= ld.date
    ),

    -- Get the most recent change for each listing-date
    most_recent_amenities as (
        select
            listing_id,
            date,
            amenities_array,
            amenities_effective_at
        from listing_amenities_with_changes
        where change_rank = 1
    ),

    -- Fallback to listings table amenities if no changelog entry exists
    fallback_amenities as (
        select
            listing_id,
            amenities_json
        from {{ ref('stg_listings') }}
    ),

    -- Combine changelog amenities with fallback from listings table
    final as (
        select
            ld.listing_id,
            ld.date,
            coalesce(
                mra.amenities_array,
                {{ warehouse_cast('fa.amenities_json', 'string') }}
            ) as amenities_array,
            mra.amenities_effective_at
        from listing_dates as ld
        left join most_recent_amenities as mra
            on ld.listing_id = mra.listing_id
            and ld.date = mra.date
        left join fallback_amenities as fa
            on ld.listing_id = fa.listing_id
    )

select * from final

