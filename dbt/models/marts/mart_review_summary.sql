-- Review-score distribution: the customer-satisfaction signal. One row per
-- 1–5 star score with its share of all reviews.
select
    review_score,
    count(*)                                              as n_reviews,
    round(100.0 * count(*) / sum(count(*)) over (), 2)    as pct
from {{ ref('stg_order_reviews') }}
where review_score is not null
group by 1
order by review_score
