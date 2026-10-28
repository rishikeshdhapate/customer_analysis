{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
customers AS (
    SELECT customer_key, customer_id FROM {{ ref('dim_customers') }}
),
products AS (
    SELECT product_key, product_id FROM {{ ref('dim_products') }}
),
dates AS (
    SELECT date_key, date FROM {{ ref('dim_date') }}
)
SELECT
    o.order_id,
    COALESCE(c.customer_key, -1) AS customer_key,
    COALESCE(p.product_key, -1) AS product_key,
    COALESCE(d.date_key, TO_CHAR(o.order_date, 'YYYYMMDD')::INT) AS date_key,
    o.quantity,
    o.total_amount AS revenue,
    o.order_status,
    o.order_timestamp
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN products p ON o.product_id = p.product_id
LEFT JOIN dates d ON o.order_date = d.date
