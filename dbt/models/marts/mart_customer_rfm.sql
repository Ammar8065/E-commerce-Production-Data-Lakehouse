-- Customer RFM (Recency, Frequency, Monetary) scoring, per person
-- (customer_unique_id).
--
-- The dataset is historical (2016–2018), so "today" is the latest purchase in
-- the data (the snapshot date), not the wall clock — otherwise every customer
-- would look equally ancient.
--
-- Scores are 1–5 quintiles via NTILE:
--   * Recency: fewer days since last order = more recent = higher score, so we
--     order by recency_days DESC (largest gap -> tile 1, smallest gap -> tile 5).
--   * Frequency / Monetary: more = higher score (order ASC).
with orders as (
    select
        c.customer_unique_id,
        o.order_id,
        o.order_purchase_timestamp,
        o.order_revenue
    from {{ ref('fct_orders') }} o
    inner join {{ ref('dim_customers') }} c on o.customer_id = c.customer_id
    where o.order_status not in ('canceled', 'unavailable')
),

snapshot as (
    select max(order_purchase_timestamp) as snapshot_date from orders
),

agg as (
    select
        o.customer_unique_id,
        date_diff('day', max(o.order_purchase_timestamp), s.snapshot_date) as recency_days,
        count(distinct o.order_id)                                         as frequency,
        sum(o.order_revenue)                                               as monetary
    from orders o
    cross join snapshot s
    group by o.customer_unique_id, s.snapshot_date
),

scored as (
    select
        *,
        ntile(5) over (order by recency_days desc) as r_score,
        ntile(5) over (order by frequency asc)     as f_score,
        ntile(5) over (order by monetary asc)      as m_score
    from agg
)

select
    customer_unique_id,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    (r_score * 100 + f_score * 10 + m_score) as rfm_cell,
    case
        when r_score >= 4 and f_score >= 4 then 'Champions'
        when f_score >= 4                  then 'Loyal'
        when r_score >= 4                  then 'Recent'
        when r_score <= 2 and f_score >= 3 then 'At Risk'
        when r_score <= 2                  then 'Hibernating'
        else 'Needs Attention'
    end as segment
from scored
