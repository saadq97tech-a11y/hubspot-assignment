{{
  config(
    materialized='table',
    unique_key='listing_key'
  )
}}

with
    listings as (
        select * from {{ ref('stg_listings') }}
    ),

    calendar as (
        select * from {{ ref('stg_calendar') }}
    ),

    hosts as (
        select
            host_id,
            host_key
        from {{ ref('dim_hosts') }}
    ),

    review_metrics as (
        select * from {{ ref('int_listings__review_metrics') }}
    ),

    -- Get listing-level minimum_nights and maximum_nights (most common value per listing)
    min_nights_counts as (
        select
            listing_id,
            minimum_nights,
            count(*) as nights_count
        from calendar
        where minimum_nights is not null
        group by listing_id, minimum_nights
    ),
    max_nights_counts as (
        select
            listing_id,
            maximum_nights,
            count(*) as nights_count
        from calendar
        where maximum_nights is not null
        group by listing_id, maximum_nights
    ),
    min_nights_ranked as (
        select
            listing_id,
            minimum_nights,
            row_number() over (partition by listing_id order by nights_count desc, minimum_nights) as rn
        from min_nights_counts
    ),
    max_nights_ranked as (
        select
            listing_id,
            maximum_nights,
            row_number() over (partition by listing_id order by nights_count desc, maximum_nights) as rn
        from max_nights_counts
    ),
    listing_nights as (
        select
            mn.listing_id,
            mn.minimum_nights,
            mx.maximum_nights
        from min_nights_ranked as mn
        inner join max_nights_ranked as mx
            on mn.listing_id = mx.listing_id
        where mn.rn = 1
            and mx.rn = 1
    ),

    dim_listings as (
        select
            -- surrogate key
            {{ dbt_utils.surrogate_key(['l.listing_id']) }} as listing_key,
            
            -- natural key
            l.listing_id,
            
            -- foreign keys
            h.host_key,
            
            -- listing attributes (static/slowly changing)
            l.listing_name,
            l.property_type,
            l.room_type,
            l.neighborhood,
            l.accommodates,
            l.bedrooms,
            l.beds,
            l.bathrooms_text,
            
            -- night constraints (listing-level)
            ln.minimum_nights,
            ln.maximum_nights,
            
            -- review metrics (listing-level aggregates)
            coalesce(rm.avg_review_score, l.review_scores_rating) as avg_review_score,
            coalesce(rm.total_reviews, l.number_of_reviews, 0) as total_reviews,
            rm.first_review_date,
            rm.last_review_date,
            
            -- natural key reference (for convenience)
            l.host_id,
            
            -- metadata
            current_timestamp() as dbt_updated_at
            
        from listings as l
        left join hosts as h
            on l.host_id = h.host_id
        left join review_metrics as rm
            on l.listing_id = rm.listing_id
        left join listing_nights as ln
            on l.listing_id = ln.listing_id
        where l.listing_id is not null
    )

select * from dim_listings

