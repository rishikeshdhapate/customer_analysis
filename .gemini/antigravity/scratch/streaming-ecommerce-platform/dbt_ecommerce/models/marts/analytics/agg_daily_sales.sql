{{ config(materialized='table') }}

SELECT
    d.date,
    d.day_name,
    d.month_name,
    d.year,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS total_items_sold,
    ROUND(SUM(f.revenue), 2) AS total_revenue,
    ROUND(SUM(f.revenue) / NULLIF(COUNT(DISTINCT f.order_id), 0), 2) AS average_order_value
FROM {{ ref('fct_orders') }} f
JOIN {{ ref('dim_date') }} d ON f.date_key = d.date_key
WHERE f.order_status = 'COMPLETED'
GROUP BY d.date, d.day_name, d.month_name, d.year
