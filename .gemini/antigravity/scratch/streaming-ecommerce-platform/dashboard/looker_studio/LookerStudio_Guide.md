# Looker Studio Integration Guide

Connect Google Looker Studio directly to your PostgreSQL Data Warehouse or Snowflake instance.

---

## 1. Connecting Looker Studio to PostgreSQL

1. Open [Google Looker Studio](https://lookerstudio.google.com).
2. Click **Create** ➔ **Data Source**.
3. Select the **PostgreSQL** connector.
4. If testing locally:
   - Use an SSH tunnel, ngrok, or cloud-hosted PostgreSQL instance (AWS RDS, Cloud SQL).
   - Enter Host, Port (`5432`), Database (`ecommerce_dw`), Username (`postgres`), Password.
5. Choose **Custom Query** to utilize star-schema analytical rollups:

```sql
SELECT
    f.order_id,
    f.order_timestamp,
    d.date,
    d.month_name,
    d.year,
    c.customer_id,
    c.name AS customer_name,
    c.city,
    c.state,
    p.product_id,
    p.name AS product_name,
    p.category,
    f.quantity,
    f.revenue
FROM warehouse.fct_orders f
JOIN warehouse.dim_customers c ON f.customer_key = c.customer_key
JOIN warehouse.dim_products p ON f.product_key = p.product_key
JOIN warehouse.dim_date d ON f.date_key = d.date_key;
```

---

## 2. Calculated Fields & Metrics

In the Looker Studio data source editor:
- **Conversion Rate %**:
  `COUNT_DISTINCT(order_id) / COUNT_DISTINCT(session_id)`
- **Average Order Value (AOV)**:
  `SUM(revenue) / COUNT_DISTINCT(order_id)`
- **Average Basket Size**:
  `SUM(quantity) / COUNT_DISTINCT(order_id)`

---

## 3. Recommended Visualizations

1. **Scorecards**: Total Revenue, Total Transactions, Average Order Value.
2. **Time Series Chart**: Daily Revenue vs Orders.
3. **Bar Chart**: Revenue by Product Category (sorted descending).
4. **Geo Map**: Customer Revenue by State/City.
5. **Table with Heatmap**: Top 10 Customers by Total Spend.
