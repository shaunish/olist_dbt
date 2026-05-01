with customer_orders as (
    select * from {{ ref('int_customers_orders') }}
),

reviews as (
    select * from {{ ref('stg_order_reviews') }}
),

-- calculate order sequence per customer
with_order_sequence as (
    select
        *,
        row_number() over (
            partition by customer_unique_id
            order by ordered_at
        ) as customer_order_sequence

    from customer_orders
),

final as (
    select
        o.order_id,
        o.customer_unique_id,
        o.city,
        o.state,
        o.order_status,
        o.ordered_at,
        o.approved_at,
        o.shipped_at,
        o.delivered_at,
        o.estimated_delivery_at,
        o.item_count,
        o.items_subtotal,
        o.freight_subtotal,
        o.total_payment,
        o.customer_order_sequence,
        r.review_score,
        r.review_title,
        r.review_text,
        r.reviewed_at,

        -- is this a repeat purchase?
        case
            when o.customer_order_sequence = 1 then false
            else true
        end                                             as is_repeat_purchase,

        -- days since previous order (null for first order)
        datediff('day',
            lag(o.ordered_at) over (
                partition by o.customer_unique_id
                order by o.ordered_at
            ),
            o.ordered_at
        )                                               as days_since_previous_order,

        -- delivery timeliness
        case
            when o.delivered_at is null then null
            when o.delivered_at <= o.estimated_delivery_at then true
            else false
        end                                             as delivered_on_time

    from with_order_sequence o
    left join reviews r on o.order_id = r.order_id
)

select * from final