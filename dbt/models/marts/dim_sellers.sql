-- Seller dimension. Grain = seller_id.
select
    seller_id,
    zip_code_prefix,
    city,
    state
from {{ ref('stg_sellers') }}
