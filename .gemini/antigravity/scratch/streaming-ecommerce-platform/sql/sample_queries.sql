-- ==============================================================================
-- Streaming E-Commerce Platform - Analytical Queries & Metric Dashboards
-- Star-Schema business metrics: Revenue, Funnels, AOV, Top Spenders, DAU
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. Total Revenue and Orders Per Day
-- ------------------------------------------------------------------------------
SELECT
    d.date,
    d.day_name,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS total_units_sold,
    ROUND(SUM(f.revenue), 2) AS total_revenue,
    ROUND(AVG(f.revenue), 2) AS avg_revenue_per_order
FROM warehouse.fct_orders f
JOIN warehouse.dim_date d ON f.date_key = d.date_key
GROUP BY d.date, d.day_name
ORDER BY d.date DESC;

-- ------------------------------------------------------------------------------
-- 2. Revenue by Product Category
-- ------------------------------------------------------------------------------
SELECT
    p.category,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS total_quantity_sold,
    ROUND(SUM(f.revenue), 2) AS total_revenue,
    ROUND(SUM(f.revenue) * 100.0 / SUM(SUM(f.revenue)) OVER (), 2) AS revenue_share_pct
FROM warehouse.fct_orders f
JOIN warehouse.dim_products p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;

-- ------------------------------------------------------------------------------
-- 3. E-Commerce Conversion Funnel (view -> add_to_cart -> checkout -> purchase)
-- ------------------------------------------------------------------------------
WITH funnel_steps AS (
    SELECT
        COUNT(DISTINCT CASE WHEN c.event_type = 'view' THEN c.session_id END) AS view_sessions,
        COUNT(DISTINCT CASE WHEN c.event_type = 'add_to_cart' THEN c.session_id END) AS cart_sessions,
        COUNT(DISTINCT CASE WHEN c.event_type = 'checkout' THEN c.session_id END) AS checkout_sessions
    FROM warehouse.fct_clickstream c
),
purchase_steps AS (
    -- Completed order sessions
    SELECT COUNT(DISTINCT order_id) AS purchase_orders
    FROM warehouse.fct_orders
    WHERE order_status = 'COMPLETED'
)
SELECT
    f.view_sessions,
    f.cart_sessions,
    ROUND(f.cart_sessions * 100.0 / NULLIF(f.view_sessions, 0), 2) AS view_to_cart_pct,
    f.checkout_sessions,
    ROUND(f.checkout_sessions * 100.0 / NULLIF(f.cart_sessions, 0), 2) AS cart_to_checkout_pct,
    p.purchase_orders,
    ROUND(p.purchase_orders * 100.0 / NULLIF(f.checkout_sessions, 0), 2) AS checkout_to_purchase_pct,
    ROUND(p.purchase_orders * 100.0 / NULLIF(f.view_sessions, 0), 2) AS overall_conversion_rate_pct
FROM funnel_steps f
CROSS JOIN purchase_steps p;

-- ------------------------------------------------------------------------------
-- 4. Top 10 Customers by Total Spend
-- ------------------------------------------------------------------------------
SELECT
    c.customer_id,
    c.name,
    c.city,
    c.state,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS total_items_bought,
    ROUND(SUM(f.revenue), 2) AS total_spend,
    ROUND(AVG(f.revenue), 2) AS avg_spend_per_order
FROM warehouse.fct_orders f
JOIN warehouse.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.customer_id, c.name, c.city, c.state
ORDER BY total_spend DESC
LIMIT 10;

-- ------------------------------------------------------------------------------
-- 5. Average Order Value (AOV) Overall and by Customer Segment
-- ------------------------------------------------------------------------------
SELECT
    ROUND(AVG(revenue), 2) AS overall_aov,
    ROUND(SUM(revenue) / COUNT(DISTINCT order_id), 2) AS computed_aov,
    ROUND(MIN(revenue), 2) AS min_order_value,
    ROUND(MAX(revenue), 2) AS max_order_value
FROM warehouse.fct_orders
WHERE order_status = 'COMPLETED';

-- ------------------------------------------------------------------------------
-- 6. Daily Active Users (DAU) and Session Count
-- ------------------------------------------------------------------------------
SELECT
    d.date,
    d.day_name,
    COUNT(DISTINCT c.customer_key) AS daily_active_users,
    COUNT(DISTINCT c.session_id) AS total_sessions,
    COUNT(c.event_id) AS total_events,
    ROUND(COUNT(c.event_id)::NUMERIC / NULLIF(COUNT(DISTINCT c.session_id), 0), 1) AS avg_events_per_session
FROM warehouse.fct_clickstream c
JOIN warehouse.dim_date d ON c.date_key = d.date_key
GROUP BY d.date, d.day_name
ORDER BY d.date DESC;

-- ------------------------------------------------------------------------------
-- 7. Top 10 Best-Selling Products by Revenue
-- ------------------------------------------------------------------------------
SELECT
    p.product_id,
    p.name,
    p.category,
    p.price,
    SUM(f.quantity) AS units_sold,
    ROUND(SUM(f.revenue), 2) AS total_revenue
FROM warehouse.fct_orders f
JOIN warehouse.dim_products p ON f.product_key = p.product_key
GROUP BY p.product_id, p.name, p.category, p.price
ORDER BY total_revenue DESC
LIMIT 10;
