-- One row per order. Silver already typed/cleaned these, so staging just selects
-- and documents the columns the marts use.
select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    order_purchase_year_month
from {{ source('silver', 'orders') }}
