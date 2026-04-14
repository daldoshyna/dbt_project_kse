with rooms as (
    select * from {{ ref('stg_rooms') }}
),

room_types as (
    select * from {{ ref('stg_room_types') }}
),

categories as (
    select * from {{ ref('dim_room_categories') }}
)

select
    r.room_id,
    r.floor_number,
    r.base_price_per_night,
    r.room_status,

    c.room_category_id,
    rt.room_category,

    rt.bed_count,
    rt.max_occupancy,

    rt.has_balcony,
    rt.has_view,
    rt.has_air_conditioning,
    rt.has_minibar

from rooms r
left join room_types rt
    on r.room_type_id = rt.room_type_id
left join categories c
    on rt.room_category = c.room_category