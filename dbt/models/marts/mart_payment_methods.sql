-- Payment-method mix: how customers actually pay. Olist's payment data is rich
-- (credit card, boleto, voucher, debit) and installment behavior is a real
-- business signal, so it earns its own mart.
select
    payment_type,
    count(*)                                                          as n_payments,
    count(distinct order_id)                                          as n_orders,
    sum(payment_value)                                               as total_value,
    round(avg(payment_installments), 2)                              as avg_installments,
    round(100.0 * sum(payment_value) / sum(sum(payment_value)) over (), 2) as pct_value
from {{ ref('stg_order_payments') }}
group by 1
order by total_value desc
