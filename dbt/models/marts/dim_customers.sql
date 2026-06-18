-- Customer dimension. Grain = customer_id (Olist issues a new customer_id per
-- order; customer_unique_id identifies the actual person across orders).
select
    customer_id,
    customer_unique_id,
    zip_code_prefix,
    city,
    state
from {{ ref('stg_customers') }}
