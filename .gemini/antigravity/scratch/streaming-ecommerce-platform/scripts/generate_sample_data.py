"""
Generates static offline sample datasets (CSV, JSON, and Parquet if pyarrow installed) for local exploration.
"""

import os
import sys
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from producers.generator import EcommerceSimulator

DATA_DIR = os.path.join(BASE_DIR, "data", "sample")


def generate_sample_files(count: int = 500):
    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"Generating {count} sample e-commerce journeys into {DATA_DIR}...")

    simulator = EcommerceSimulator(num_customers=100, num_products=40, seed=42)

    clicks = []
    orders = []
    payments = []

    for _ in range(count):
        c_list, o, p = simulator.generate_journey_step()
        for c in c_list:
            clicks.append(c.model_dump())
        if o:
            orders.append(o.model_dump())
        if p:
            payments.append(p.model_dump())

    df_clicks = pd.DataFrame(clicks)
    df_orders = pd.DataFrame(orders)
    df_payments = pd.DataFrame(payments)
    df_customers = pd.DataFrame([c.model_dump() for c in simulator.customers])
    df_products = pd.DataFrame([p.model_dump() for p in simulator.products])

    # Save as CSV & JSON
    df_orders.to_csv(os.path.join(DATA_DIR, "orders.csv"), index=False)
    df_orders.to_json(os.path.join(DATA_DIR, "orders.json"), orient="records", indent=2)

    df_clicks.to_csv(os.path.join(DATA_DIR, "clickstream.csv"), index=False)
    df_clicks.to_json(os.path.join(DATA_DIR, "clickstream.json"), orient="records", indent=2)

    df_payments.to_csv(os.path.join(DATA_DIR, "payments.csv"), index=False)
    df_payments.to_json(os.path.join(DATA_DIR, "payments.json"), orient="records", indent=2)

    df_customers.to_csv(os.path.join(DATA_DIR, "customers.csv"), index=False)
    df_products.to_csv(os.path.join(DATA_DIR, "products.csv"), index=False)

    # If pyarrow or fastparquet is available, also write parquet
    try:
        df_orders.to_parquet(os.path.join(DATA_DIR, "orders.parquet"), index=False)
        df_clicks.to_parquet(os.path.join(DATA_DIR, "clickstream.parquet"), index=False)
        df_payments.to_parquet(os.path.join(DATA_DIR, "payments.parquet"), index=False)
        print("Parquet files exported successfully.")
    except Exception as e:
        print(f"Parquet export skipped (install pyarrow for parquet support): {e}")

    print(f"Sample data generation complete:")
    print(f"  - Orders:      {len(df_orders)} records")
    print(f"  - Clickstream: {len(df_clicks)} records")
    print(f"  - Payments:    {len(df_payments)} records")
    print(f"  - Customers:   {len(df_customers)} records")
    print(f"  - Products:    {len(df_products)} records")


if __name__ == "__main__":
    generate_sample_files()
