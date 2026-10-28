"""
Unit tests for Pydantic event schemas and data validation rules.
"""

import pytest
from pydantic import ValidationError
from producers.schema import (
    Customer,
    Product,
    ClickstreamEvent,
    ClickEventType,
    OrderEvent,
    OrderStatus,
    PaymentEvent,
    PaymentMethod,
    PaymentStatus,
)


def test_customer_valid():
    cust = Customer(
        customer_id="cust_0001",
        name="Alice Smith",
        email="alice@example.com",
        city="Seattle",
        state="WA",
        country="USA",
        signup_date="2024-01-15T10:00:00+00:00",
    )
    assert cust.customer_id == "cust_0001"
    assert cust.country == "USA"


def test_product_positive_price():
    prod = Product(
        product_id="prod_1001",
        name="Wireless Headphones",
        category="Electronics",
        price=199.99,
        stock=50,
    )
    assert prod.price > 0

    with pytest.raises(ValidationError):
        # Negative or zero price must raise ValidationError
        Product(
            product_id="prod_1002",
            name="Free Item",
            category="Electronics",
            price=-10.0,
            stock=10,
        )


def test_order_schema_validation():
    order = OrderEvent(
        order_id="ord_12345",
        session_id="sess_abcde",
        customer_id="cust_0001",
        product_id="prod_1001",
        quantity=2,
        unit_price=50.0,
        total_amount=100.0,
        order_status=OrderStatus.COMPLETED,
        order_timestamp="2024-05-01T12:00:00+00:00",
    )
    assert order.total_amount == 100.0
    assert order.quantity == 2

    # Zero or negative quantity should fail
    with pytest.raises(ValidationError):
        OrderEvent(
            order_id="ord_invalid",
            session_id="sess_abcde",
            customer_id="cust_0001",
            product_id="prod_1001",
            quantity=0,
            unit_price=50.0,
            total_amount=0.0,
            order_status=OrderStatus.COMPLETED,
            order_timestamp="2024-05-01T12:00:00+00:00",
        )


def test_clickstream_event_types():
    click = ClickstreamEvent(
        event_id="evt_001",
        session_id="sess_001",
        customer_id="cust_0001",
        product_id="prod_1001",
        event_type=ClickEventType.VIEW,
        page_url="https://store.ecommerce.com/products/prod_1001",
        referrer="https://google.com",
        device="mobile",
        event_timestamp="2024-05-01T12:00:00+00:00",
    )
    assert click.event_type == ClickEventType.VIEW

    # Invalid event type should fail
    with pytest.raises(ValidationError):
        ClickstreamEvent(
            event_id="evt_002",
            session_id="sess_001",
            customer_id="cust_0001",
            event_type="invalid_action",  # Not in view, add_to_cart, checkout
            page_url="https://store.com",
            referrer="direct",
            device="desktop",
            event_timestamp="2024-05-01T12:00:00+00:00",
        )


def test_payment_schema():
    payment = PaymentEvent(
        payment_id="pay_999",
        order_id="ord_12345",
        customer_id="cust_0001",
        payment_method=PaymentMethod.CREDIT_CARD,
        payment_status=PaymentStatus.SUCCESS,
        amount=100.0,
        payment_timestamp="2024-05-01T12:01:00+00:00",
    )
    assert payment.payment_status == PaymentStatus.SUCCESS
    assert payment.amount == 100.0
