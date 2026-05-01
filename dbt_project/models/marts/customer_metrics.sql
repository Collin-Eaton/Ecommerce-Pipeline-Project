SELECT
    customer_id,
    COUNT(order_id) AS total_orders,
    SUM(total_amount) AS total_spent,
    AVG(total_amount) AS avg_order_value,
    SUM(is_returned) AS total_returns,
    SUM(is_returned) * 1.0 / COUNT(order_id) AS return_rate

FROM {{ ref('fct_orders') }}

GROUP BY customer_id