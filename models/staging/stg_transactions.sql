{{
    config(
        materialized='incremental',
        unique_key='transaction_id'
    )
}}

with raw_transactions as (
    select * from {{ ref('transactions_raw') }}

    {% if is_incremental() %}
    where transaction_date > (select max(transaction_date) from {{ this }})
    {% endif %}
),

renamed_and_cast as (
    select
        cast(transaction_id as integer) as transaction_id,

        cast(booking_id as string) as booking_id,

        cast(transaction_date as timestamp) as transaction_at,

        lower(category) as transaction_category,
        lower(payment_method) as payment_method,

        cast(amount as decimal(10,2)) as amount,
        upper(currency) as currency_code,

        case
            when upper(currency) = 'EUR' then cast(amount * 1.08 as decimal(10,2))
            else cast(amount as decimal(10,2))
        end as amount_usd

    from raw_transactions
)

select * from renamed_and_cast