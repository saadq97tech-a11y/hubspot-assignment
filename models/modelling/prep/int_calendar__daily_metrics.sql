{{
  config(
    materialized='ephemeral'
  )
}}

with calendar as (
    select * from {{ ref('stg_calendar') }}
),

daily_metrics as (
    select
        listing_id,
        date,
        has_reservation,
        price_usd,
        -- Calculate revenue: if reserved, revenue = price, else 0
        case
            when has_reservation then price_usd
            else 0
        end as revenue_usd
    from calendar
)

select * from daily_metrics

