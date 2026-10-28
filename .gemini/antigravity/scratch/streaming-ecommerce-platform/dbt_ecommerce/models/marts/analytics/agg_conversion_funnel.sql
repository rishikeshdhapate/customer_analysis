{{ config(materialized='table') }}

WITH click_metrics AS (
    SELECT
        d.date,
        COUNT(DISTINCT CASE WHEN c.event_type = 'view' THEN c.session_id END) AS view_sessions,
        COUNT(DISTINCT CASE WHEN c.event_type = 'add_to_cart' THEN c.session_id END) AS cart_sessions,
        COUNT(DISTINCT CASE WHEN c.event_type = 'checkout' THEN c.session_id END) AS checkout_sessions
    FROM {{ ref('fct_clickstream') }} c
    JOIN {{ ref('dim_date') }} d ON c.date_key = d.date_key
    GROUP BY d.date
),
order_metrics AS (
    SELECT
        d.date,
        COUNT(DISTINCT f.order_id) AS purchase_orders
    FROM {{ ref('fct_orders') }} f
    JOIN {{ ref('dim_date') }} d ON f.date_key = d.date_key
    WHERE f.order_status = 'COMPLETED'
    GROUP BY d.date
)
SELECT
    c.date,
    c.view_sessions,
    c.cart_sessions,
    ROUND(c.cart_sessions * 100.0 / NULLIF(c.view_sessions, 0), 2) AS view_to_cart_rate_pct,
    c.checkout_sessions,
    ROUND(c.checkout_sessions * 100.0 / NULLIF(c.cart_sessions, 0), 2) AS cart_to_checkout_rate_pct,
    COALESCE(o.purchase_orders, 0) AS purchase_orders,
    ROUND(COALESCE(o.purchase_orders, 0) * 100.0 / NULLIF(c.checkout_sessions, 0), 2) AS checkout_to_purchase_rate_pct,
    ROUND(COALESCE(o.purchase_orders, 0) * 100.0 / NULLIF(c.view_sessions, 0), 2) AS overall_conversion_rate_pct
FROM click_metrics c
LEFT JOIN order_metrics o ON c.date = o.date
