-- Item grain. item_revenue = product price + freight, the basis for all revenue
-- aggregation downstream.
select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    price + freight_value as item_revenue
from {{ source('silver', 'order_items') }}
