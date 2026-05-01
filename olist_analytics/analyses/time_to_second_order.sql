-- SQL query to retrieve time distribution of 2nd orders - i.e. how long does it take for a customer to order again?
select
    days_since_previous_order,
    count(*)    as order_count
from fct_customer_orders
where customer_order_sequence = 2  -- second orders only
    and days_since_previous_order is not null
group by days_since_previous_order
order by days_since_previous_order