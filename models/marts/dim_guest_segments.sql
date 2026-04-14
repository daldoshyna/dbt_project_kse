with segments as (
    select distinct
        guest_segment
    from {{ ref('stg_guests') }}
)

select
    row_number() over (order by guest_segment) as segment_id,
    guest_segment
from segments