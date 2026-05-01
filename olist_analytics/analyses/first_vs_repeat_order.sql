-- SQL query to compare first-time purchases vs. returning purchases
select
    is_repeat_purchase,
    round(avg(total_payment), 2)    as avg_order_value,
    round(avg(item_count), 1)       as avg_items_per_order,
    count(*)                        as order_count
from fct_customer_orders
where order_status = 'delivered'
group by is_repeat_purchase