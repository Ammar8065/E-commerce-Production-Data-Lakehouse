select
    review_id,
    order_id,
    review_score,
    review_creation_date,
    review_answer_timestamp
from {{ source('silver', 'order_reviews') }}
