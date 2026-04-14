with raw_room_types as (
    select * from {{ ref('seed_room_types') }}
)

select
    cast(room_type_id as integer) as room_type_id,
    {{ standardize_text('category') }} as room_category,
    cast(bed_count as integer) as bed_count,
    cast(max_occupancy as integer) as max_occupancy,

    cast(has_balcony as boolean) as has_balcony,
    cast(has_view as boolean) as has_view,
    cast(has_air_conditioning as boolean) as has_air_conditioning,
    cast(has_minibar as boolean) as has_minibar

from raw_room_types