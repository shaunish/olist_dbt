with source as (
    select * from {{ source('olist_raw', 'order_reviews') }}
),

renamed as (
    select
        review_id,
        order_id,
        review_score,
        nullif(review_comment_title, '')    as review_title,
        nullif(review_comment_message, '')  as review_text,
        cast(review_creation_date as timestamp)     as reviewed_at,
        cast(review_answer_timestamp as timestamp)  as answered_at
    from source
)

select * from renamed