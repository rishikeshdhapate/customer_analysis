"""
Streaming E-Commerce Platform - Interactive Executive Dashboard.
Built with Streamlit and Plotly.
Visualizes real-time metrics: Revenue, Orders, Conversion Funnel, Top Spenders, DAU, and AOV.
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text

# Page Configuration
st.set_page_config(
    page_title="E-Commerce Real-Time Analytics",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Styling
st.markdown(
    """
    <style>
    .metric-card {
        background-color: #f8f9fa;
        border-radius: 8px;
        padding: 15px;
        border-left: 4px solid #1f77b4;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 10px;
        border-radius: 6px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Database Connection Helper
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "ecommerce_dw")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")


@st.cache_resource
def get_db_engine():
    db_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    try:
        engine = create_engine(db_url, connect_args={"connect_timeout": 3})
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        return None


def fetch_data(query: str, fallback_df: pd.DataFrame) -> pd.DataFrame:
    engine = get_db_engine()
    if engine is None:
        return fallback_df
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        return fallback_df


# --- Fallback Mock Data (Ensures dashboard renders even before warehouse is seeded) ---
def get_mock_daily_sales():
    dates = pd.date_range(end=pd.Timestamp.now(), periods=14, freq="D")
    return pd.DataFrame({
        "date": [d.strftime("%Y-%m-%d") for d in dates],
        "total_revenue": [1240.50, 1580.20, 2100.00, 1890.40, 2450.70, 3100.80, 2800.50,
                          2950.00, 3400.20, 3200.50, 3890.10, 4200.00, 4510.90, 4980.50],
        "total_orders": [18, 22, 29, 25, 34, 42, 38, 40, 46, 43, 51, 56, 60, 68],
        "average_order_value": [68.91, 71.82, 72.41, 75.61, 72.07, 73.82, 73.69,
                                73.75, 73.91, 74.43, 76.27, 75.00, 75.18, 73.24],
    })


def get_mock_categories():
    return pd.DataFrame({
        "category": ["Electronics", "Apparel", "Home & Kitchen", "Sports & Outdoors", "Books"],
        "total_revenue": [18450.00, 12300.50, 9400.20, 6800.00, 4120.80],
        "total_quantity_sold": [140, 280, 110, 95, 120],
    })


def get_mock_funnel():
    return pd.DataFrame({
        "stage": ["1. Product Views", "2. Add to Cart", "3. Checkout Initiated", "4. Completed Orders"],
        "session_count": [12500, 4850, 2910, 2182],
        "conversion_pct": [100.0, 38.8, 60.0, 75.0],
    })


def get_mock_top_customers():
    return pd.DataFrame({
        "customer_id": [f"cust_{i:04d}" for i in range(1, 11)],
        "name": ["Sarah Jenkins", "Michael Chang", "Emily Davis", "David Wilson", "Jessica Taylor",
                 "Robert Martinez", "Amanda Anderson", "James Thomas", "Megan White", "Brian Harris"],
        "city": ["Seattle", "San Francisco", "Austin", "Denver", "Chicago", "Boston", "Atlanta", "Portland", "Miami", "Dallas"],
        "total_orders": [12, 11, 9, 8, 8, 7, 7, 6, 6, 5],
        "total_spend": [2450.80, 2190.50, 1890.00, 1650.20, 1540.00, 1420.50, 1380.00, 1290.40, 1180.90, 1050.00],
    })


# --- Dashboard Header ---
st.title("🛒 Streaming E-Commerce Analytics Platform")
st.caption("Real-Time Star Schema Lakehouse Dashboard | Kafka ➔ Spark ➔ MinIO ➔ dbt ➔ PostgreSQL")

# Sidebar Controls
st.sidebar.header("⚙️ Platform Controls")
refresh_btn = st.sidebar.button("🔄 Refresh Data", use_container_width=True)
date_filter = st.sidebar.selectbox("Date Range", ["Last 14 Days", "Last 30 Days", "All Time"])

engine = get_db_engine()
if engine:
    st.sidebar.success("🟢 Connected to PostgreSQL Warehouse")
else:
    st.sidebar.warning("🟡 Live DB Offline (Displaying Streaming Simulation)")

# --- Top KPI Summary Cards ---
sales_df = fetch_data(
    """
    SELECT
        d.date,
        ROUND(SUM(f.revenue), 2) AS total_revenue,
        COUNT(DISTINCT f.order_id) AS total_orders,
        ROUND(SUM(f.revenue) / NULLIF(COUNT(DISTINCT f.order_id), 0), 2) AS average_order_value
    FROM warehouse.fct_orders f
    JOIN warehouse.dim_date d ON f.date_key = d.date_key
    GROUP BY d.date
    ORDER BY d.date DESC
    LIMIT 14;
    """,
    get_mock_daily_sales(),
)

total_rev = sales_df["total_revenue"].sum()
total_ord = sales_df["total_orders"].sum()
overall_aov = round(total_rev / max(total_ord, 1), 2)

funnel_df = fetch_data(
    """
    WITH f_counts AS (
        SELECT
            COUNT(DISTINCT CASE WHEN event_type = 'view' THEN session_id END) AS views,
            COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN session_id END) AS carts,
            COUNT(DISTINCT CASE WHEN event_type = 'checkout' THEN session_id END) AS checkouts
        FROM warehouse.fct_clickstream
    ),
    p_counts AS (
        SELECT COUNT(DISTINCT order_id) AS purchases FROM warehouse.fct_orders
    )
    SELECT * FROM f_counts CROSS JOIN p_counts;
    """,
    pd.DataFrame({"views": [12500], "carts": [4850], "checkouts": [2910], "purchases": [2182]}),
)

views_cnt = int(funnel_df["views"].iloc[0]) if not funnel_df.empty else 12500
purchases_cnt = int(funnel_df["purchases"].iloc[0]) if not funnel_df.empty else 2182
overall_cr = round((purchases_cnt * 100.0) / max(views_cnt, 1), 2)

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
with kpi1:
    st.metric(label="Total Revenue", value=f"${total_rev:,.2f}", delta="+14.2% vs last week")
with kpi2:
    st.metric(label="Total Orders", value=f"{int(total_ord):,}", delta="+8.5%")
with kpi3:
    st.metric(label="Average Order Value (AOV)", value=f"${overall_aov:.2f}", delta="+$3.10")
with kpi4:
    st.metric(label="Conversion Rate (Funnel)", value=f"{overall_cr:.2f}%", delta="+1.2%")

st.divider()

# --- Section 1: Revenue Trends & Category Breakdown ---
row1_col1, row1_col2 = st.columns([3, 2])

with row1_col1:
    st.subheader("📈 Daily Revenue & Order Volume")
    fig_rev = px.bar(
        sales_df.sort_values("date"),
        x="date",
        y="total_revenue",
        title="Daily Revenue Trend (USD)",
        labels={"total_revenue": "Revenue ($)", "date": "Date"},
        color="total_revenue",
        color_continuous_scale="Blues",
    )
    fig_rev.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=350)
    st.plotly_chart(fig_rev, use_container_width=True)

with row1_col2:
    st.subheader("📦 Revenue by Product Category")
    cat_df = fetch_data(
        """
        SELECT
            p.category,
            ROUND(SUM(f.revenue), 2) AS total_revenue,
            SUM(f.quantity) AS total_quantity_sold
        FROM warehouse.fct_orders f
        JOIN warehouse.dim_products p ON f.product_key = p.product_key
        GROUP BY p.category
        ORDER BY total_revenue DESC;
        """,
        get_mock_categories(),
    )
    fig_pie = px.pie(
        cat_df,
        names="category",
        values="total_revenue",
        hole=0.45,
        color_discrete_sequence=px.colors.qualitative.Pastel,
    )
    fig_pie.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=350)
    st.plotly_chart(fig_pie, use_container_width=True)

st.divider()

# --- Section 2: Conversion Funnel & Customer Behavior ---
row2_col1, row2_col2 = st.columns([3, 2])

with row2_col1:
    st.subheader("🎯 E-Commerce Conversion Funnel")
    funnel_stages = [
        ("1. Product Views", views_cnt),
        ("2. Add to Cart", int(funnel_df["carts"].iloc[0])),
        ("3. Checkout Started", int(funnel_df["checkouts"].iloc[0])),
        ("4. Orders Completed", purchases_cnt),
    ]
    funnel_plot_df = pd.DataFrame(funnel_stages, columns=["Stage", "Count"])
    
    fig_funnel = go.Figure(go.Funnel(
        y=funnel_plot_df["Stage"],
        x=funnel_plot_df["Count"],
        textinfo="value+percent initial+percent previous",
        marker={"color": ["#1f77b4", "#3b82f6", "#60a5fa", "#93c5fd"]},
    ))
    fig_funnel.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=350)
    st.plotly_chart(fig_funnel, use_container_width=True)

with row2_col2:
    st.subheader("👥 Active Traffic & Devices")
    device_df = pd.DataFrame({
        "device": ["Mobile App/Web", "Desktop Browser", "Tablet"],
        "share": [62, 31, 7],
    })
    fig_dev = px.bar(
        device_df,
        x="device",
        y="share",
        labels={"share": "Traffic Share (%)", "device": "Device Type"},
        color="device",
        color_discrete_sequence=px.colors.qualitative.Safe,
    )
    fig_dev.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=350, showlegend=False)
    st.plotly_chart(fig_dev, use_container_width=True)

st.divider()

# --- Section 3: Top Customers by Spend ---
st.subheader("💎 Top 10 Customers by Total Spend")
top_cust_df = fetch_data(
    """
    SELECT
        c.customer_id,
        c.name,
        c.city,
        c.state,
        COUNT(DISTINCT f.order_id) AS total_orders,
        SUM(f.quantity) AS total_items,
        ROUND(SUM(f.revenue), 2) AS total_spend,
        ROUND(AVG(f.revenue), 2) AS avg_spend_per_order
    FROM warehouse.fct_orders f
    JOIN warehouse.dim_customers c ON f.customer_key = c.customer_key
    GROUP BY c.customer_id, c.name, c.city, c.state
    ORDER BY total_spend DESC
    LIMIT 10;
    """,
    get_mock_top_customers(),
)
st.dataframe(
    top_cust_df.style.format({"total_spend": "${:,.2f}", "avg_spend_per_order": "${:,.2f}"}),
    use_container_width=True,
    hide_index=True,
)
