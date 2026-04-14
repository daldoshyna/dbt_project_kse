with statuses as (
    select distinct
        booking_status
    from {{ ref('stg_bookings') }}
)

select
    row_number() over (order by booking_status) as status_id,
    booking_status
from statuses