with categories as (
    select distinct
        transaction_category
    from {{ ref('stg_transactions') }}
)

select
    row_number() over (order by transaction_category) as transaction_category_id,
    transaction_category
from categories