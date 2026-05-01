select
    customer_segment,
    count(*)                                        as customer_count,
    round(100.0 * count(*) / sum(count(*)) over (), 1) as pct_of_customers
from dim_customers
group by customer_segment
order by customer_count desc