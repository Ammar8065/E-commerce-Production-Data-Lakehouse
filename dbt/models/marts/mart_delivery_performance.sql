-- Delivery performance by month and customer state: on-time rate and average
-- days-to-deliver. Only orders that actually reached the customer contribute to
-- the delivery-time averages; the order counts include all non-canceled orders.
select
    order_purchase_year_month                                  as year_month,
    customer_state,
    count(*)                                                   as n_orders,
    count(order_delivered_customer_date)                       as n_delivered,
    avg(days_to_deliver)                                       as avg_days_to_deliver,
    avg(case when delivered_on_time then 1.0 else 0.0 end)     as on_time_rate
from {{ ref('fct_orders') }}
where order_status not in ('canceled', 'unavailable')
group by 1, 2
