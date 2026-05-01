with source as (
    select * from {{ source('olist_raw', 'order_items') }}
),

renamed as (
    select
        order_id,
        order_item_id                               as item_sequence,
        product_id,
        seller_id,
        cast(shipping_limit_date as timestamp)      as shipping_limit_at,
        price                                       as item_price,
        freight_value                               as freight_price
    from source
)

select * from renamed