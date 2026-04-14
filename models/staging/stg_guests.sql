{{
    config(
        materialized='incremental',
        unique_key='guest_id'
    )
}}

with raw_guests as (
    select * from {{ ref('guests_raw') }}

    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

deduplicated_updates as (
    select
        cast(guest_id as string) as guest_id,

        {{ standardize_text('first_name') }} as first_name,
        {{ standardize_text('last_name') }} as last_name,

        cast(email as string) as email,

        {{ standardize_text('country') }} as country_code,
        {{ standardize_text('guest_segment') }} as guest_segment,

        cast(is_loyalty_member as boolean) as is_loyalty_member,

        cast(updated_at as timestamp) as updated_at

    from raw_guests
)

select * from deduplicated_updates