{{
    config(
        materialized='incremental',
        unique_key='review_id'
    )
}}

with raw_reviews as (
    select * from {{ ref('reviews_raw') }}

    {% if is_incremental() %}
    where review_date > (select max(reviewed_at) from {{ this }})
    {% endif %}
),

renamed_and_cast as (
    select
        cast(review_id as integer) as review_id,
        cast(booking_id as string) as booking_id,

        cast(review_date as timestamp) as reviewed_at,

        cast(rating_overall as integer) as rating_overall,
        cast(rating_cleanliness as integer) as rating_cleanliness,
        cast(rating_service as integer) as rating_service,

        {{ standardize_text('review_text') }} as review_text,

        cast(
            (rating_overall + rating_cleanliness + rating_service) / 3.0
            as decimal(10,2)
        ) as average_rating_score

    from raw_reviews
)

select * from renamed_and_cast