# Power BI Integration Guide

This guide details how to connect Power BI Desktop to the E-Commerce Data Warehouse and build executive sales, conversion funnel, and customer retention dashboards.

---

## 1. Connecting to the PostgreSQL Warehouse

1. Open **Power BI Desktop**.
2. Click **Get Data** ➔ **More...** ➔ Select **PostgreSQL database**.
3. Enter connection parameters:
   - **Server**: `localhost:5432` (or your remote PostgreSQL host)
   - **Database**: `ecommerce_dw`
   - **Data Connectivity mode**: **Import** (recommended for snappy caching) or **DirectQuery** (for live real-time sync).
4. Provide database credentials:
   - **User**: `postgres`
   - **Password**: `postgres` (or your configured password)
5. In the Navigator window, select tables from the `warehouse` schema:
   - `fct_orders`
   - `fct_clickstream`
   - `dim_customers`
   - `dim_products`
   - `dim_date`
6. Click **Load**.

---

## 2. Configuring Star-Schema Relationships

Switch to the **Model View** (left navigation tab) and verify the 1-to-Many relationships:
- `dim_customers[customer_key]` `1` ➔ `*` `fct_orders[customer_key]`
- `dim_products[product_key]` `1` ➔ `*` `fct_orders[product_key]`
- `dim_date[date_key]` `1` ➔ `*` `fct_orders[date_key]`
- `dim_customers[customer_key]` `1` ➔ `*` `fct_clickstream[customer_key]`
- `dim_products[product_key]` `1` ➔ `*` `fct_clickstream[product_key]`
- `dim_date[date_key]` `1` ➔ `*` `fct_clickstream[date_key]`

Set **Cross filter direction** to **Single** (Dimensions filter Facts).

---

## 3. Importing DAX Measures

1. In the Fields pane, right-click `fct_orders` and select **New Measure**.
2. Copy and paste the DAX definitions from [`measures_and_dax.dax`](./measures_and_dax.dax):
   - `Total Revenue`
   - `Total Orders`
   - `Average Order Value (AOV)`
   - `Conversion Funnel` measures (`View to Cart Rate`, `Checkout to Purchase Rate`, `Overall Conversion Rate`)
   - `Daily Active Users (DAU)`

---

## 4. Recommended Visual Layouts

| Visual Type | Fields / Dimensions | Measures |
| :--- | :--- | :--- |
| **KPI Card Row** | N/A | `Total Revenue`, `Total Orders`, `AOV`, `Overall Conversion Rate` |
| **Area Chart** | `dim_date[date]` | `Total Revenue`, `Total Orders` |
| **Funnel Chart** | Static Stages | `Total Product Views`, `Total Add to Carts`, `Total Checkouts Initiated`, `Total Purchases` |
| **Donut Chart** | `dim_products[category]` | `Total Revenue` |
| **Matrix Table** | `dim_customers[name]`, `dim_customers[city]` | `Total Orders`, `Total Revenue`, `AOV` (Top 10 filter) |
| **Bar Chart** | `fct_clickstream[device]` | `Total Sessions`, `Events Per Session` |
