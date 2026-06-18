-- Order-grain fact: one row per order, enriched with customer, revenue, payment,
-- and delivery metrics.
--
-- ORPHANED-ORDER EDGE CASE (from the plan): an order whose customer_id has no
-- matching customer is *kept*, not dropped — the revenue is real and dropping it
-- would understate the business. We LEFT JOIN the customer and expose an
-- `is_orphaned_customer` flag so analysts can include or exclude such orders
-- deliberately. A dbt relationships test (severity: warn) surfaces the count
-- without failing the build, because orphans are an expected data-quality fact,
-- not a pipeline error.
with items as (
    select
        order_id,
        sum(price)        as total_price,
        sum(freight_value) as total_freight,
        sum(item_revenue)  as order_revenue,
        count(*)           as n_items
    from {{ ref('stg_order_items') }}
    group by 1
),

payments as (
    select
        order_id,
        sum(payment_value) as total_payment,
        count(*)           as n_payments
    from {{ ref('stg_order_payments') }}
    group by 1
)

select
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.state                              as customer_state,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_purchase_year_month,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    -- delivery metrics (null until the order is actually delivered)
    date_diff('day', o.order_purchase_timestamp, o.order_delivered_customer_date)
        as days_to_deliver,
    case
        when o.order_delivered_customer_date is not null
        then o.order_delivered_customer_date <= o.order_estimated_delivery_date
    end                                  as delivered_on_time,

    -- data-quality flag, not a filter
    (c.customer_id is null)              as is_orphaned_customer,

    coalesce(i.order_revenue, 0)         as order_revenue,
    coalesce(i.total_price, 0)           as total_price,
    coalesce(i.total_freight, 0)         as total_freight,
    coalesce(i.n_items, 0)               as n_items,
    p.total_payment,
    p.n_payments
from {{ ref('stg_orders') }} o
left join {{ ref('dim_customers') }} c on o.customer_id = c.customer_id
left join items i    on o.order_id = i.order_id
left join payments p on o.order_id = p.order_id
