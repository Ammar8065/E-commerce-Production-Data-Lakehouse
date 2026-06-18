-- Seller performance scorecard. One row per seller: volume, revenue, review
-- quality, and delivery speed/reliability.
with sales as (
    select
        seller_id,
        count(distinct order_id) as n_orders,
        count(*)                 as n_items,
        sum(item_revenue)        as total_revenue
    from {{ ref('fct_order_items') }}
    group by 1
),

reviews as (
    -- a seller's review score = avg score of the orders containing their items
    select
        oi.seller_id,
        avg(r.review_score) as avg_review_score
    from {{ ref('stg_order_items') }} oi
    inner join {{ ref('stg_order_reviews') }} r on oi.order_id = r.order_id
    group by 1
),

delivery as (
    select
        oi.seller_id,
        avg(o.days_to_deliver)                                      as avg_days_to_deliver,
        avg(case when o.delivered_on_time then 1.0 else 0.0 end)    as on_time_rate
    from {{ ref('stg_order_items') }} oi
    inner join {{ ref('fct_orders') }} o on oi.order_id = o.order_id
    where o.order_delivered_customer_date is not null
    group by 1
)

select
    s.seller_id,
    s.state                  as seller_state,
    sales.n_orders,
    sales.n_items,
    sales.total_revenue,
    reviews.avg_review_score,
    delivery.avg_days_to_deliver,
    delivery.on_time_rate
from {{ ref('dim_sellers') }} s
left join sales    on s.seller_id = sales.seller_id
left join reviews  on s.seller_id = reviews.seller_id
left join delivery on s.seller_id = delivery.seller_id
