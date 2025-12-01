{{
  config(
    materialized='table',
    unique_key='neighborhood_key'
  )
}}

with
    listings as (
        select * from {{ ref('stg_listings') }}
    ),

    dim_neighborhoods as (
        select
            -- surrogate key
            {{ dbt_utils.surrogate_key(['neighborhood']) }} as neighborhood_key,
            
            -- natural key
            neighborhood,
            
            -- neighborhood attributes (could be enriched with external data)
            count(distinct listing_id) as total_listings,
            avg(accommodates) as avg_accommodates,
            
            -- metadata
            current_timestamp() as dbt_updated_at
            
        from listings
        where neighborhood is not null
        group by neighborhood
    )

select * from dim_neighborhoods

