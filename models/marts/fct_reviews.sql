{{
    config(
        materialized='incremental',
        unique_key='review_id'
    )
}}

with stg_reviews as (
    select * from {{ ref('stg_reviews') }}

    {% if is_incremental() %}
    where reviewed_at > (select max(reviewed_at) from {{ this }})
    {% endif %}
),

stg_bookings as (
    select
        booking_id,
        guest_id,
        room_id
    from {{ ref('stg_bookings') }}
)

select
    r.review_id,
    r.booking_id,
    b.guest_id,
    b.room_id,
    r.reviewed_at,
    r.rating_overall,
    r.rating_cleanliness,
    r.rating_service,
    r.average_rating_score,
    r.review_text

from stg_reviews r
left join stg_bookings b
    on r.booking_id = b.booking_id