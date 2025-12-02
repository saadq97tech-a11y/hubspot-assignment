{{
  config(
    materialized='ephemeral'
  )
}}

with reviews as (
    select * from {{ ref('stg_generated_reviews') }}
),

review_metrics as (
    select
        listing_id,
        count(*) as total_reviews,
        avg(review_score) as avg_review_score,
        min(review_date) as first_review_date,
        max(review_date) as last_review_date
    from reviews
    group by 1
)

select * from review_metrics

