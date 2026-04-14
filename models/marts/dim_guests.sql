with guests as (
    select * from {{ ref('stg_guests') }}
),

     segments as (
         select * from {{ ref('dim_guest_segments') }}
     ),

     countries as (
         select * from {{ ref('dim_countries') }}
     )

select
    g.guest_id,
    g.first_name,
    g.last_name,
    g.email,

    s.segment_id,
    c.country_id,

    g.is_loyalty_member,

    g.updated_at

from guests g
left join segments s on g.guest_segment = s.guest_segment
left join countries c on g.country_code = c.country_name