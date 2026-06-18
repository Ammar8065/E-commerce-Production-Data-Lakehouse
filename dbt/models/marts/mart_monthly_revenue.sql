-- Monthly revenue sliced by product category and customer state.
-- (Customer *segment* revenue is provided by mart_customer_rfm, which assigns
--  each customer an RFM segment; joining that to fct_order_items yields revenue
--  by segment when needed.)
--
-- Canceled / unavailable orders are excluded from revenue — they did not result
-- in a fulfilled sale.
select
    order_purchase_year_month as year_month,
    product_category,
    customer_state,
    count(distinct order_id)  as n_orders,
    sum(item_revenue)         as revenue,
    sum(price)                as product_revenue,
    sum(freight_value)        as freight_revenue
from {{ ref('fct_order_items') }}
where order_status not in ('canceled', 'unavailable')
group by 1, 2, 3
