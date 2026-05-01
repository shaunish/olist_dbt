-- SQL query to retrieve customer location by state. Useful for a chloropleth.
select
    state,
    count(distinct customer_unique_id)  as unique_customers,
    round(avg(lifetime_value), 2)       as avg_ltv
from dim_customers
group by state
order by unique_customers desc