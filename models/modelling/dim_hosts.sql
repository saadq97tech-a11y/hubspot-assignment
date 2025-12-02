{{
  config(
    materialized='table',
    unique_key='host_key'
  )
}}

with
    listings as (
        select * from {{ ref('stg_listings') }}
    ),

    -- Get most recent listing per host (all attributes from same record)
    ranked_listings as (
        select
            host_id,
            host_name,
            host_location,
            host_since_date,
            row_number() over (
                partition by host_id 
                order by host_since_date desc nulls last, listing_id desc
            ) as rn
        from listings
        where host_id is not null
    ),

    dim_hosts as (
        select
            -- surrogate key
            {{ dbt_utils.surrogate_key(['host_id']) }} as host_key,
            
            -- natural key
            host_id,
            
            -- host attributes (from most recent record)
            host_name,
            host_location,
            host_since_date,
            
            -- metadata
            current_timestamp() as dbt_updated_at
            
        from ranked_listings
        where rn = 1
    )

select * from dim_hosts

