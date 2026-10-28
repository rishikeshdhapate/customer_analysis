-- ==============================================================================
-- Streaming E-Commerce Platform - PostgreSQL Warehouse Initialization DDL
-- Creates schemas, staging tables, star-schema dimensions, facts, and seed data.
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS staging;

-- ------------------------------------------------------------------------------
-- 1. STAGING TABLES (Ingested from Spark Silver Layer)
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.stg_raw_orders (
    order_id VARCHAR(64) PRIMARY KEY,
    session_id VARCHAR(64),
    customer_id VARCHAR(64) NOT NULL,
    product_id VARCHAR(64) NOT NULL,
    quantity INT NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    total_amount NUMERIC(10, 2) NOT NULL,
    order_status VARCHAR(32),
    order_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    order_date DATE,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.stg_raw_clickstream (
    event_id VARCHAR(64) PRIMARY KEY,
    session_id VARCHAR(64) NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    product_id VARCHAR(64),
    event_type VARCHAR(32) NOT NULL,
    page_url TEXT,
    referrer TEXT,
    device VARCHAR(32),
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    event_date DATE,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.stg_raw_payments (
    payment_id VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL,
    customer_id VARCHAR(64) NOT NULL,
    payment_method VARCHAR(32) NOT NULL,
    payment_status VARCHAR(32) NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    payment_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    payment_date DATE,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.stg_raw_customers (
    customer_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    email VARCHAR(128),
    city VARCHAR(64),
    state VARCHAR(32),
    country VARCHAR(32) DEFAULT 'USA',
    signup_date TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS public.stg_raw_products (
    product_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    category VARCHAR(64) NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    stock INT DEFAULT 100
);

-- ------------------------------------------------------------------------------
-- 2. STAR SCHEMA DIMENSIONS & FACTS (Warehouse Marts)
-- ------------------------------------------------------------------------------

-- Date Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key INT PRIMARY KEY,              -- YYYYMMDD
    date DATE NOT NULL UNIQUE,
    day_of_month INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(16) NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(16) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Customer Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL UNIQUE,
    name VARCHAR(128) NOT NULL,
    email VARCHAR(128),
    city VARCHAR(64),
    state VARCHAR(32),
    country VARCHAR(32),
    signup_date DATE
);

-- Product Dimension
CREATE TABLE IF NOT EXISTS warehouse.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(64) NOT NULL UNIQUE,
    name VARCHAR(128) NOT NULL,
    category VARCHAR(64) NOT NULL,
    price NUMERIC(10, 2) NOT NULL
);

-- Orders Fact Table
CREATE TABLE IF NOT EXISTS warehouse.fct_orders (
    order_id VARCHAR(64) PRIMARY KEY,
    customer_key INT NOT NULL REFERENCES warehouse.dim_customers(customer_key),
    product_key INT NOT NULL REFERENCES warehouse.dim_products(product_key),
    date_key INT NOT NULL REFERENCES warehouse.dim_date(date_key),
    quantity INT NOT NULL,
    revenue NUMERIC(10, 2) NOT NULL,
    order_status VARCHAR(32),
    order_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Clickstream Fact Table
CREATE TABLE IF NOT EXISTS warehouse.fct_clickstream (
    event_id VARCHAR(64) PRIMARY KEY,
    customer_key INT NOT NULL REFERENCES warehouse.dim_customers(customer_key),
    product_key INT REFERENCES warehouse.dim_products(product_key),
    date_key INT NOT NULL REFERENCES warehouse.dim_date(date_key),
    event_type VARCHAR(32) NOT NULL,
    session_id VARCHAR(64) NOT NULL,
    device VARCHAR(32),
    referrer TEXT,
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Indexes for Fast Analytics & BI Joins
CREATE INDEX IF NOT EXISTS idx_fct_orders_customer ON warehouse.fct_orders(customer_key);
CREATE INDEX IF NOT EXISTS idx_fct_orders_product ON warehouse.fct_orders(product_key);
CREATE INDEX IF NOT EXISTS idx_fct_orders_date ON warehouse.fct_orders(date_key);
CREATE INDEX IF NOT EXISTS idx_fct_clickstream_customer ON warehouse.fct_clickstream(customer_key);
CREATE INDEX IF NOT EXISTS idx_fct_clickstream_product ON warehouse.fct_clickstream(product_key);
CREATE INDEX IF NOT EXISTS idx_fct_clickstream_event_type ON warehouse.fct_clickstream(event_type);
CREATE INDEX IF NOT EXISTS idx_fct_clickstream_session ON warehouse.fct_clickstream(session_id);

-- ------------------------------------------------------------------------------
-- 3. SEED DATE DIMENSION (2024 to 2027)
-- ------------------------------------------------------------------------------
INSERT INTO warehouse.dim_date (date_key, date, day_of_month, month, month_name, quarter, year, day_of_week, day_name, is_weekend)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
    d::DATE AS date,
    EXTRACT(DAY FROM d)::INT AS day_of_month,
    EXTRACT(MONTH FROM d)::INT AS month,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(QUARTER FROM d)::INT AS quarter,
    EXTRACT(YEAR FROM d)::INT AS year,
    EXTRACT(ISODOW FROM d)::INT AS day_of_week,
    TO_CHAR(d, 'Day') AS day_name,
    CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM GENERATE_SERIES('2024-01-01'::DATE, '2027-12-31'::DATE, '1 day'::INTERVAL) d
ON CONFLICT (date_key) DO NOTHING;

-- ------------------------------------------------------------------------------
-- 4. SEED INITIAL PRODUCTS CATALOG
-- ------------------------------------------------------------------------------
INSERT INTO public.stg_raw_products (product_id, name, category, price, stock) VALUES
('prod_1001', 'Space Gray Wireless Headphones', 'Electronics', 199.99, 120),
('prod_1002', 'Midnight Black Smartwatch Pro', 'Electronics', 299.99, 80),
('prod_1003', 'Silver 4K OLED Monitor', 'Electronics', 449.99, 45),
('prod_1004', 'RGB Mechanical Keyboard', 'Electronics', 129.99, 150),
('prod_1005', 'Navy Organic Cotton T-Shirt', 'Apparel', 29.99, 300),
('prod_1006', 'Indigo Slim Fit Denim Jeans', 'Apparel', 79.99, 200),
('prod_1007', 'Charcoal Fleece Zip Hoodie', 'Apparel', 69.99, 180),
('prod_1008', 'Stainless Steel Espresso Machine', 'Home & Kitchen', 249.99, 60),
('prod_1009', 'Pro High-Speed Blender', 'Home & Kitchen', 119.99, 90),
('prod_1010', 'Designing Data-Intensive Applications', 'Books', 49.99, 400),
('prod_1011', 'Building Microservices', 'Books', 44.99, 350),
('prod_1012', 'Eco Non-Slip Yoga Mat', 'Sports & Outdoors', 39.99, 250),
('prod_1013', 'Adjustable Dumbbells Set', 'Sports & Outdoors', 189.99, 75)
ON CONFLICT (product_id) DO NOTHING;

-- Populate dim_products from seed
INSERT INTO warehouse.dim_products (product_id, name, category, price)
SELECT product_id, name, category, price FROM public.stg_raw_products
ON CONFLICT (product_id) DO UPDATE SET
    name = EXCLUDED.name,
    category = EXCLUDED.category,
    price = EXCLUDED.price;
