-- SQL query to retrieve number of new vs. returning customers for each month
select
    date_trunc('month', ordered_at)::date   as month,
    sum(case when is_repeat_purchase = false then 1 else 0 end) as first_orders,
    sum(case when is_repeat_purchase = true then 1 else 0 end)  as repeat_orders
from main.fct_customer_orders
where order_status = 'delivered'
group by 1
order by 1