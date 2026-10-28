"""
Unit tests for EcommerceSimulator and journey generator.
"""

from producers.generator import EcommerceSimulator
from producers.schema import ClickEventType, OrderStatus


def test_simulator_catalogs():
    simulator = EcommerceSimulator(num_customers=20, num_products=15, seed=123)
    assert len(simulator.customers) == 20
    assert len(simulator.products) == 15

    cust = simulator.get_random_customer()
    assert cust.customer_id.startswith("cust_")

    prod = simulator.get_random_product()
    assert prod.product_id.startswith("prod_")
    assert prod.price > 0


def test_journey_generation():
    simulator = EcommerceSimulator(num_customers=10, num_products=10, seed=999, duplicate_prob=0.0)
    
    view_count = 0
    orders_generated = 0
    payments_generated = 0

    for _ in range(50):
        clicks, order, payment = simulator.generate_journey_step()
        assert len(clicks) >= 1
        assert clicks[0].event_type == ClickEventType.VIEW
        view_count += 1

        if order:
            orders_generated += 1
            assert order.order_id.startswith("ord_")
            assert order.quantity > 0
            assert order.total_amount > 0
            assert order.order_status == OrderStatus.COMPLETED

        if payment:
            payments_generated += 1
            assert payment.payment_id.startswith("pay_")
            assert payment.order_id == order.order_id
            assert payment.amount == order.total_amount

    # In 50 journeys, there should be some orders and matching payments
    assert orders_generated > 0
    assert payments_generated == orders_generated


def test_duplicate_injection():
    # Set 100% duplicate probability to verify injection
    simulator = EcommerceSimulator(num_customers=5, num_products=5, seed=1, duplicate_prob=1.0)
    clicks, _, _ = simulator.generate_journey_step()
    
    # Check if duplicate event_id is present
    event_ids = [c.event_id for c in clicks]
    assert len(event_ids) != len(set(event_ids))
