-- Item-grain fact: one row per (order_id, order_item_id), the lowest-level money
-- table. Enriched with order context, product category, and customer state so
-- revenue marts can slice without re-joining.
select
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    o.customer_id,
    o.customer_state,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_purchase_year_month,
    pr.product_category,
    oi.price,
    oi.freight_value,
    oi.item_revenue
from {{ ref('stg_order_items') }} oi
inner join {{ ref('fct_orders') }}   o  on oi.order_id   = o.order_id
left  join {{ ref('dim_products') }}  pr on oi.product_id = pr.product_id
