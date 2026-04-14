with categories as (
    select distinct
        room_category
    from {{ ref('stg_room_types') }}
)

select
    row_number() over (order by room_category) as room_category_id,
    room_category
from categories