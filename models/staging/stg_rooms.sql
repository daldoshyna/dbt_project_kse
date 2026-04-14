{{
    config(
        materialized='incremental',
        unique_key='room_id'
    )
}}

with raw_rooms as (
    select * from {{ ref('rooms_raw') }}

    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

latest_room_status as (
    select
        cast(room_id as string) as room_id,
        cast(room_type_id as integer) as room_type_id,

        cast(floor_number as integer) as floor_number,

        cast(base_price as decimal(10,2)) as base_price_per_night,

        {{ standardize_text('status') }} as room_status,
        cast(updated_at as timestamp) as updated_at,

        row_number() over (
            partition by room_id
            order by updated_at desc
        ) as latest_room_rank

    from raw_rooms
)

select
    room_id,
    room_type_id,
    floor_number,
    base_price_per_night,
    room_status,
    updated_at

from latest_room_status
where latest_room_rank = 1