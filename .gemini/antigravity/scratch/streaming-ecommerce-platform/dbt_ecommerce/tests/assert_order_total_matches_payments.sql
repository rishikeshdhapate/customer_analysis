-- Fails if an order's completed payments do not match order total amount
WITH order_totals AS (
    SELECT
        order_id,
        total_amount
    FROM {{ ref('stg_orders') }}
    WHERE order_status = 'COMPLETED'
),
payment_totals AS (
    SELECT
        order_id,
        SUM(amount) AS total_paid
    FROM {{ ref('stg_payments') }}
    WHERE payment_status = 'SUCCESS'
    GROUP BY order_id
)
SELECT
    o.order_id,
    o.total_amount,
    p.total_paid,
    ABS(o.total_amount - COALESCE(p.total_paid, 0)) AS diff
FROM order_totals o
LEFT JOIN payment_totals p ON o.order_id = p.order_id
WHERE ABS(o.total_amount - COALESCE(p.total_paid, 0)) > 0.01
