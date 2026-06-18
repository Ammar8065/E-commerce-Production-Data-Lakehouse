select
    product_category_name,
    product_category_name_english
from {{ source('silver', 'category_translation') }}
