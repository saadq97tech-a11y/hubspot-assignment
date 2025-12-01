{{
  config(
    materialized='view'
  )
}}

with source as (
    select * from {{ source('rental_data', 'amenities_changelog') }}
),

renamed as (
    select
        -- ids
        listing_id,
        
        -- timestamps (warehouse-agnostic)
        {{ warehouse_cast('change_at', 'timestamp_ltz') }} as change_at,
        
        -- keep as text for string matching (works across all warehouses)
        amenities as amenities_json,
        amenities as amenities_array
        
    from source
)

select * from renamed

