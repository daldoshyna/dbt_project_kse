with transactions as (
    select * from {{ ref('stg_transactions') }}
),

pm as (
    select * from {{ ref('dim_payment_methods') }}
),

cat as (
    select * from {{ ref('dim_transaction_categories') }}
)

select
    t.transaction_id,
    t.booking_id,
    pm.payment_method_id,
    cat.transaction_category_id,
    t.amount_usd,
    t.transaction_at
from transactions t
left join pm on t.payment_method = pm.payment_method
left join cat on t.transaction_category = cat.transaction_category