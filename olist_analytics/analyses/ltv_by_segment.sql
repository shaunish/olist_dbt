-- SQL query for retrieving average lifetime value, average order value, and average number of orders by segment
select
    customer_segment,
    round(avg(lifetime_value), 2)   as avg_ltv,
    round(avg(avg_order_value), 2)  as avg_order_value,
    round(avg(total_orders), 1)     as avg_orders
from dim_customers
group by customer_segment
order by avg_ltv desc