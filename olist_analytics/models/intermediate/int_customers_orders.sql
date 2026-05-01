with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select
        order_id,
        sum(item_price)     as items_subtotal,
        sum(freight_price)  as freight_subtotal,
        count(*)            as item_count
    from {{ ref('stg_order_items') }}
    group by order_id
),

order_payments as (
    select
        order_id,
        sum(payment_amount) as total_payment
    from {{ ref('stg_order_payments') }}
    group by order_id
),

customers as (
    select * from {{ ref('stg_customers') }}
),

joined as (
    select
        o.order_id,
        o.customer_id,
        c.customer_unique_id,
        c.city,
        c.state,
        o.order_status,
        o.ordered_at,
        o.approved_at,
        o.shipped_at,
        o.delivered_at,
        o.estimated_delivery_at,
        coalesce(i.item_count, 0)       as item_count,
        i.items_subtotal,
        i.freight_subtotal,
        p.total_payment
    from orders o
    left join customers c       on o.customer_id = c.customer_id
    left join order_items i     on o.order_id = i.order_id
    left join order_payments p  on o.order_id = p.order_id
)

select * from joined