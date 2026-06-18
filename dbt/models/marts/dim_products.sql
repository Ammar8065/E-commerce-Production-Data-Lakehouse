-- Product dimension with the English category name resolved. Falls back to the
-- Portuguese name, then 'unknown', so revenue never silently disappears just
-- because a category translation is missing.
select
    p.product_id,
    p.product_category_name,
    coalesce(t.product_category_name_english, p.product_category_name, 'unknown')
        as product_category,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
from {{ ref('stg_products') }} p
left join {{ ref('stg_category_translation') }} t
    on p.product_category_name = t.product_category_name
