-- Business Problem #2: Neighborhood Pricing
-- Find the average price increase for each neighborhood from July 12th 2021 to July 11th 2022.

with july_2021_prices as (
    select
        n.neighborhood,
        f.listing_id,
        f.price_usd as price_july_2021
    from {{ ref('fct_listings_daily') }} as f
    left join {{ ref('dim_neighborhoods') }} as n
        on f.neighborhood_key = n.neighborhood_key
    where f.date = '2021-07-12'
        and n.neighborhood is not null
),

july_2022_prices as (
    select
        n.neighborhood,
        f.listing_id,
        f.price_usd as price_july_2022
    from {{ ref('fct_listings_daily') }} as f
    left join {{ ref('dim_neighborhoods') }} as n
        on f.neighborhood_key = n.neighborhood_key
    where f.date = '2022-07-11'
        and n.neighborhood is not null
),

listing_price_changes as (
    select
        coalesce(p2021.neighborhood, p2022.neighborhood) as neighborhood,
        coalesce(p2021.listing_id, p2022.listing_id) as listing_id,
        p2021.price_july_2021,
        p2022.price_july_2022,
        p2022.price_july_2022 - p2021.price_july_2021 as price_change
    from july_2021_prices as p2021
    full outer join july_2022_prices as p2022
        on p2021.neighborhood = p2022.neighborhood
        and p2021.listing_id = p2022.listing_id
    where p2021.price_july_2021 is not null
        and p2022.price_july_2022 is not null
)

select
    neighborhood,
    avg(price_change) as avg_price_increase,
    count(distinct listing_id) as listing_count
from listing_price_changes
group by 1
order by 2 desc

