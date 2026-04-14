with methods as (
    select distinct
        payment_method
    from {{ ref('stg_transactions') }}
)

select
    row_number() over (order by payment_method) as payment_method_id,
    payment_method
from methods