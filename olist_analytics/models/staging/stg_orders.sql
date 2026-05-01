with source as (
    select * from {{ source('olist_raw', 'orders') }}
),

renamed as (
    select
        order_id,
        customer_id,

        -- normalize status to lowercase
        lower(order_status)                             as order_status,

        -- cast all timestamps
        cast(order_purchase_timestamp as timestamp)     as ordered_at,
        cast(order_approved_at as timestamp)            as approved_at,
        cast(order_delivered_carrier_date as timestamp) as shipped_at,
        cast(order_delivered_customer_date as timestamp) as delivered_at,
        cast(order_estimated_delivery_date as timestamp) as estimated_delivery_at

    from source
)

select * from renamed