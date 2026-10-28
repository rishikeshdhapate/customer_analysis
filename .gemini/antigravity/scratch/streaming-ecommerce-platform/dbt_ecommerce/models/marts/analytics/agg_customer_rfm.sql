{{ config(materialized='table') }}

WITH customer_orders AS (
    SELECT
        c.customer_key,
        c.customer_id,
        c.name,
        c.city,
        c.state,
        COUNT(DISTINCT f.order_id) AS total_orders,
        SUM(f.quantity) AS total_units_purchased,
        ROUND(SUM(f.revenue), 2) AS total_spend,
        ROUND(AVG(f.revenue), 2) AS avg_order_spend,
        MAX(f.order_timestamp) AS last_order_timestamp
    FROM {{ ref('dim_customers') }} c
    JOIN {{ ref('fct_orders') }} f ON c.customer_key = f.customer_key
    WHERE f.order_status = 'COMPLETED'
    GROUP BY c.customer_key, c.customer_id, c.name, c.city, c.state
)
SELECT
    customer_key,
    customer_id,
    name,
    city,
    state,
    total_orders,
    total_units_purchased,
    total_spend,
    avg_order_spend,
    last_order_timestamp,
    DENSE_RANK() OVER (ORDER BY total_spend DESC) AS spend_rank
FROM customer_orders
