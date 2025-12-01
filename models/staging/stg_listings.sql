{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ source('rental_data', 'listings') }}
),

renamed as (
    select
        -- ids
        id as listing_id,
        host_id,
        
        -- strings
        name as listing_name,
        host_name,
        host_location,
        neighborhood,
        property_type,
        room_type,
        bathrooms_text,
        
        -- numerics
        accommodates,
        bedrooms,
        beds,
        {{ cents_to_dollars('price') }} as price_usd,
        number_of_reviews,
        {{ parse_float('review_scores_rating') }} as review_scores_rating,
        
        -- dates
        {{ parse_date('host_since') }} as host_since_date,
        {{ parse_date('first_review') }} as first_review_date,
        {{ parse_date('last_review') }} as last_review_date,
        
        -- raw data for intermediate processing
        amenities as amenities_json,
        host_verifications as host_verifications_json
        
    from source
)

select * from renamed
