{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ source('rental_data', 'generated_reviews') }}
),

renamed as (
    select
        -- ids
        id as review_id,
        listing_id,
        
        -- numerics
        review_score,
        
        -- dates
        {{ parse_date('review_date') }} as review_date
        
    from source
)

select * from renamed

