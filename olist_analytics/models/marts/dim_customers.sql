with customer_orders as (
    select * from {{ ref('int_customers_orders') }}
),

aggregated as (
    select
        customer_unique_id,

        -- use the most recent city/state in case they've moved
        last(city order by ordered_at)          as city,
        last(state order by ordered_at)         as state,

        count(order_id)                         as total_orders,
        sum(total_payment)                      as lifetime_value,
        avg(total_payment)                      as avg_order_value,

        min(ordered_at)                         as first_ordered_at,
        max(ordered_at)                         as last_ordered_at,

        -- days between first and last order (null if only one order)
        case
            when count(order_id) > 1
            then datediff('day', min(ordered_at), max(ordered_at))
        end                                     as days_between_first_and_last_order,

        -- customer type
        case
            when count(order_id) = 1 then 'one_time'
            when count(order_id) between 2 and 3 then 'repeat'
            when count(order_id) > 3 then 'loyal'
        end                                     as customer_segment

    from customer_orders
    where order_status = 'delivered'  -- only count completed orders
    group by customer_unique_id
)

select * from aggregated