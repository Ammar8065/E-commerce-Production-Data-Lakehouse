-- Singular (data) test: Gold must contain NO order whose delivery dates are
-- logically impossible. These rows are quarantined upstream in the Silver
-- business-rule layer, so this test asserts the quarantine actually worked
-- end-to-end. dbt fails the build if this query returns any rows.
select
    order_id,
    order_status,
    order_purchase_timestamp,
    order_delivered_customer_date
from {{ ref('fct_orders') }}
where
    -- delivered, but no delivery date recorded
    (order_status = 'delivered' and order_delivered_customer_date is null)
    -- delivered before it was purchased (impossible)
    or (order_delivered_customer_date is not null
        and order_delivered_customer_date < order_purchase_timestamp)
