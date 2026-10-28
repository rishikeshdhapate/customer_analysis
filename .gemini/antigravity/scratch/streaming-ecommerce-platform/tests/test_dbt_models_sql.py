"""
Unit tests validating dbt SQL models, schema definitions, and star-schema keys.
"""

import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODELS_DIR = os.path.join(BASE_DIR, "dbt_ecommerce", "models")


def test_fct_orders_model_columns():
    fct_orders_path = os.path.join(MODELS_DIR, "marts", "core", "fct_orders.sql")
    assert os.path.exists(fct_orders_path)
    
    with open(fct_orders_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Must contain star-schema foreign keys and measures
    assert "order_id" in content
    assert "customer_key" in content
    assert "product_key" in content
    assert "date_key" in content
    assert "quantity" in content
    assert "revenue" in content


def test_dim_customers_model_columns():
    dim_cust_path = os.path.join(MODELS_DIR, "marts", "core", "dim_customers.sql")
    assert os.path.exists(dim_cust_path)

    with open(dim_cust_path, "r", encoding="utf-8") as f:
        content = f.read()

    assert "customer_key" in content
    assert "customer_id" in content
    assert "signup_date" in content


def test_fct_clickstream_model_columns():
    fct_clicks_path = os.path.join(MODELS_DIR, "marts", "core", "fct_clickstream.sql")
    assert os.path.exists(fct_clicks_path)

    with open(fct_clicks_path, "r", encoding="utf-8") as f:
        content = f.read()

    assert "event_id" in content
    assert "customer_key" in content
    assert "event_type" in content
    assert "event_timestamp" in content
