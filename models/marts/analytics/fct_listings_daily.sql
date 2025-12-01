{{
  config(
    materialized='table',
    unique_key='listing_daily_key'
  )
}}

with
    calendar_metrics as (
        select * from {{ ref('int_calendar__daily_metrics') }}
    ),

    current_amenities as (
        select * from {{ ref('int_listings__current_amenities') }}
    ),

    dim_listings as (
        select * from {{ ref('dim_listings') }}
    ),

    dim_dates as (
        select * from {{ ref('dim_dates') }}
    ),

    dim_neighborhoods as (
        select * from {{ ref('dim_neighborhoods') }}
    ),

    -- Fact table with foreign keys to dimensions
    fct_listings_daily as (
        select
            -- surrogate key
            {{ dbt_utils.surrogate_key(['cm.listing_id', 'cm.date']) }} as listing_daily_key,
            
            -- foreign keys to dimensions
            dl.listing_key,
            dd.date_key,
            dl.host_key,
            dn.neighborhood_key,
            
            -- degenerate dimensions (natural keys for convenience)
            cm.listing_id,
            cm.date,
            
            -- measures (facts)
            cm.revenue_usd,
            cm.price_usd,
            cm.has_reservation,
            
            -- amenity flags (from current amenities on this date)
            -- These are time-varying attributes, so included in fact table
            {{ array_contains('ca.amenities_array', "'Air conditioning'") }} as has_air_conditioning,
            {{ array_contains('ca.amenities_array', "'Lockbox'") }} as has_lockbox,
            {{ array_contains('ca.amenities_array', "'Keypad'") }} as has_keypad,
            {{ array_contains('ca.amenities_array', "'First aid kit'") }} as has_first_aid_kit,
            {{ array_contains('ca.amenities_array', "'Wifi'") }} as has_wifi,
            {{ array_contains('ca.amenities_array', "'Heating'") }} as has_heating,
            {{ array_contains('ca.amenities_array', "'Kitchen'") }} as has_kitchen,
            
            -- metadata
            current_timestamp() as dbt_updated_at
            
        from calendar_metrics as cm
        inner join dim_listings as dl
            on cm.listing_id = dl.listing_id
        inner join dim_dates as dd
            on cm.date = dd.date
        left join dim_neighborhoods as dn
            on dl.neighborhood = dn.neighborhood
        left join current_amenities as ca
            on cm.listing_id = ca.listing_id
            and cm.date = ca.date
    )

select * from fct_listings_daily
