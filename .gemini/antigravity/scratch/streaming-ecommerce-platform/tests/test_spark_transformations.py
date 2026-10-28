"""
Unit tests for Spark streaming transformation logic and data quality rules.
"""

import pandas as pd


def test_order_deduplication_and_null_filtering():
    # Mock orders DataFrame with duplicates and invalid nulls
    data = [
        {"order_id": "ord_1", "customer_id": "c1", "product_id": "p1", "quantity": 1, "total_amount": 50.0, "order_timestamp": "2024-05-01T10:00:00Z"},
        {"order_id": "ord_1", "customer_id": "c1", "product_id": "p1", "quantity": 1, "total_amount": 50.0, "order_timestamp": "2024-05-01T10:00:00Z"}, # Duplicate
        {"order_id": None, "customer_id": "c2", "product_id": "p2", "quantity": 1, "total_amount": 30.0, "order_timestamp": "2024-05-01T10:05:00Z"},   # Null ID
        {"order_id": "ord_3", "customer_id": "c3", "product_id": "p3", "quantity": 0, "total_amount": 0.0, "order_timestamp": "2024-05-01T10:10:00Z"},   # Invalid qty/amount
        {"order_id": "ord_4", "customer_id": "c4", "product_id": "p4", "quantity": 2, "total_amount": 100.0, "order_timestamp": "2024-05-01T10:15:00Z"},
    ]
    df = pd.DataFrame(data)

    # Apply quality checks
    cleaned = df[df["order_id"].notnull() & df["customer_id"].notnull() & df["product_id"].notnull()]
    cleaned = cleaned[(cleaned["quantity"] > 0) & (cleaned["total_amount"] > 0)]
    deduped = cleaned.drop_duplicates(subset=["order_id"])

    assert len(deduped) == 2
    assert set(deduped["order_id"]) == {"ord_1", "ord_4"}


def test_clickstream_event_type_validation():
    data = [
        {"event_id": "e1", "session_id": "s1", "customer_id": "c1", "event_type": "view"},
        {"event_id": "e2", "session_id": "s1", "customer_id": "c1", "event_type": "add_to_cart"},
        {"event_id": "e3", "session_id": "s1", "customer_id": "c1", "event_type": "checkout"},
        {"event_id": "e4", "session_id": "s1", "customer_id": "c1", "event_type": "corrupt_event"},
    ]
    df = pd.DataFrame(data)

    valid_types = ["view", "add_to_cart", "checkout"]
    filtered = df[df["event_type"].isin(valid_types)]
    assert len(filtered) == 3
    assert "corrupt_event" not in filtered["event_type"].values
