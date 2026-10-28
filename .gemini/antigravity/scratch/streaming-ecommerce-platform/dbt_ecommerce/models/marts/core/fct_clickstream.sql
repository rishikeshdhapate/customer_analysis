{{ config(materialized='table') }}

WITH clicks AS (
    SELECT * FROM {{ ref('stg_clickstream') }}
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
    clk.event_id,
    COALESCE(c.customer_key, -1) AS customer_key,
    COALESCE(p.product_key, -1) AS product_key,
    COALESCE(d.date_key, TO_CHAR(clk.event_date, 'YYYYMMDD')::INT) AS date_key,
    clk.event_type,
    clk.session_id,
    clk.device,
    clk.referrer,
    clk.event_timestamp
FROM clicks clk
LEFT JOIN customers c ON clk.customer_id = c.customer_id
LEFT JOIN products p ON clk.product_id = p.product_id
LEFT JOIN dates d ON clk.event_date = d.date
