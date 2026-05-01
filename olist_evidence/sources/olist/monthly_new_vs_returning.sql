-- SQL query to retrieve number of new vs. returning customers for each month
select
    date_trunc('month', ordered_at)     as month,
    count(*) filter (
        where customer_order_sequence = 1
    )                                   as new_customers,
    count(*) filter (
        where customer_order_sequence > 1
    )                                   as returning_customers
from fct_customer_orders
where order_status = 'delivered'
group by 1
order by 1