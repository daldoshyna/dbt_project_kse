{{
    config(
        materialized='incremental',
        unique_key='booking_id',
        incremental_strategy='delete+insert',
        incremental_predicates=["booking_date >= date_trunc('month', current_date - interval '3 month')"]
    )
}}

with raw_bookings as (
    select * from {{ ref('bookings_raw') }}

    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

latest_booking_updates as (
    select
        cast(booking_id as string) as booking_id,
        cast(guest_id as string) as guest_id,
        cast(room_id as string) as room_id,

        cast(booking_date as timestamp) as booking_at,
        cast(check_in_date as date) as check_in_date,
        cast(check_out_date as date) as check_out_date,

        status as booking_status,

        cast(updated_at as timestamp) as updated_at,

        row_number() over (
            partition by booking_id
            order by updated_at desc
        ) as latest_update_rank

    from raw_bookings
)

select
    booking_id,
    guest_id,
    room_id,
    booking_at,
    check_in_date,
    check_out_date,
    date_diff('day', check_in_date, check_out_date) as stay_duration_nights,
    booking_status,
    updated_at

from latest_booking_updates
where latest_update_rank = 1