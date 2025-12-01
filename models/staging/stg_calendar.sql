{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ source('rental_data', 'calendar') }}
),

renamed as (
    select
        -- ids
        listing_id,
        reservation_id,
        
        -- dates
        {{ parse_date('date') }} as date,
        
        -- booleans
        case
            when lower(available) = 't' then true
            when lower(available) = 'f' then false
            else null
        end as is_available,
        case
            when reservation_id is not null then true
            else false
        end as has_reservation,
        
        -- numerics
        {{ cents_to_dollars('price') }} as price_usd,
        minimum_nights,
        maximum_nights,
        
        -- composite key
        {{ warehouse_concat('listing_id', "'_'", 'date') }} as primary_key
        
    from source
)

select * from renamed

