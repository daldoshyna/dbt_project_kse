with guest_stats as (
    select
        g.guest_id,
        c.country_name,
        count(b.booking_id) as lifetime_bookings,
        sum(b.total_total) as lifetime_spend
    from {{ ref('dim_guests') }} g
    left join {{ ref('fct_bookings') }} b on g.guest_id = b.guest_id
    left join {{ ref('dim_countries') }} c on g.country_id = c.country_id
    group by 1, 2
),

review_stats as (
    select
        guest_id,
        avg(rating_overall) as avg_guest_rating
    from {{ ref('fct_reviews') }}
    group by 1
)

select
    gs.country_name,
    count(gs.guest_id) as num_guests,
    sum(gs.lifetime_bookings) as num_bookings,
    sum(gs.lifetime_spend) as total_revenue,
    coalesce(round(avg(rs.avg_guest_rating), 2), 0) as avg_guest_rating
from guest_stats gs
left join review_stats rs on gs.guest_id = rs.guest_id
group by 1