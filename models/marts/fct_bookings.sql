{{
    config(
        materialized='incremental',
        unique_key='booking_id'
    )
}}

with bookings as (
    select * from {{ ref('stg_bookings') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

rooms as (
    select room_id, base_price_per_night from {{ ref('stg_rooms') }}
),

status_dim as (
    select * from {{ ref('dim_booking_statuses') }}
)

select
    b.booking_id,
    b.guest_id,
    b.room_id,
    s.status_id,
    b.booking_at,
    b.check_in_date,
    b.check_out_date,
    b.stay_duration_nights,
    r.base_price_per_night as price_per_night,
    cast(b.stay_duration_nights * r.base_price_per_night as decimal(10,2)) as total_total,

    b.updated_at

from bookings b
left join rooms r on b.room_id = r.room_id
left join status_dim s on b.booking_status = s.booking_status