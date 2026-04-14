with countries as (
    select distinct
        country_code
    from {{ ref('stg_guests') }}
)

select
    row_number() over (order by country_code) as country_id,
    country_code as country_name
from countries