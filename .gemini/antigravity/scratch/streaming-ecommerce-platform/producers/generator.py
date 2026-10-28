"""
Synthetic E-Commerce Event Generator using Python Faker.
Simulates realistic user browsing journeys, conversion funnels, orders, and payments.
"""

import random
import uuid
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Optional, Dict, Any
from faker import Faker

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


CATEGORIES = [
    ("Electronics", ["Wireless Headphones", "Smartwatch Pro", "4K OLED Monitor", "Mechanical Keyboard", "USB-C Hub", "Bluetooth Speaker"]),
    ("Apparel", ["Organic Cotton T-Shirt", "Slim Fit Denim Jeans", "Fleece Zip Hoodie", "Waterproof Rain Jacket", "Running Shoes"]),
    ("Home & Kitchen", ["Espresso Coffee Machine", "Stainless Steel Blender", "Cast Iron Skillet", "Air Purifier HEPA", "Smart LED Bulb 4-Pack"]),
    ("Books", ["Designing Data-Intensive Applications", "Building Microservices", "Data Engineering Cookbook", "Python Fluent Guide", "Clean Architecture"]),
    ("Sports & Outdoors", ["Yoga Mat Non-Slip", "Adjustable Dumbbells Set", "Hydration Daypack", "Trail Running Poles", "Camping Tent 2-Person"]),
]

REFERRERS = ["https://google.com", "https://instagram.com", "https://facebook.com", "direct", "https://tiktok.com", "https://youtube.com"]
DEVICES = ["mobile", "desktop", "tablet"]


class EcommerceSimulator:
    """Simulates an active e-commerce store with catalog, customer base, and user behavior."""

    def __init__(
        self,
        num_customers: int = 500,
        num_products: int = 80,
        seed: int = 42,
        duplicate_prob: float = 0.03,
    ):
        self.fake = Faker()
        Faker.seed(seed)
        random.seed(seed)

        self.duplicate_prob = duplicate_prob
        self.customers: List[Customer] = self._generate_customers(num_customers)
        self.products: List[Product] = self._generate_products(num_products)
        self.active_sessions: Dict[str, Dict[str, Any]] = {}
        self.recent_events_cache: List[Dict[str, Any]] = []

    def _generate_customers(self, count: int) -> List[Customer]:
        """Pre-generate a fixed customer directory."""
        customers = []
        for i in range(1, count + 1):
            cust_id = f"cust_{i:04d}"
            signup_dt = self.fake.date_time_between(
                start_date="-2y", end_date="now", tzinfo=timezone.utc
            )
            customers.append(
                Customer(
                    customer_id=cust_id,
                    name=self.fake.name(),
                    email=self.fake.email(),
                    city=self.fake.city(),
                    state=self.fake.state_abbr(),
                    country="USA",
                    signup_date=signup_dt.isoformat(),
                )
            )
        return customers

    def _generate_products(self, count: int) -> List[Product]:
        """Pre-generate a realistic product catalog."""
        products = []
        sku_counter = 1001
        while len(products) < count:
            cat_name, item_names = random.choice(CATEGORIES)
            base_name = random.choice(item_names)
            variant = self.fake.color_name()
            full_name = f"{variant} {base_name}"
            
            # Category-based pricing
            if cat_name == "Electronics":
                price = round(random.uniform(49.99, 599.99), 2)
            elif cat_name == "Books":
                price = round(random.uniform(19.99, 79.99), 2)
            elif cat_name == "Apparel":
                price = round(random.uniform(24.99, 149.99), 2)
            elif cat_name == "Home & Kitchen":
                price = round(random.uniform(29.99, 299.99), 2)
            else:
                price = round(random.uniform(15.99, 199.99), 2)

            products.append(
                Product(
                    product_id=f"prod_{sku_counter}",
                    name=full_name,
                    category=cat_name,
                    price=price,
                    stock=random.randint(20, 500),
                )
            )
            sku_counter += 1
        return products

    def get_random_customer(self) -> Customer:
        return random.choice(self.customers)

    def get_random_product(self) -> Product:
        return random.choice(self.products)

    def generate_journey_step(
        self,
    ) -> Tuple[List[ClickstreamEvent], Optional[OrderEvent], Optional[PaymentEvent]]:
        """
        Simulates a step in the user conversion funnel:
        view (100%) -> add_to_cart (40%) -> checkout (60% of cart) -> purchase (75% of checkout)
        Returns:
            - List of ClickstreamEvent(s) generated in this step
            - Optional OrderEvent if converted to purchase
            - Optional PaymentEvent if converted to purchase
        """
        now = datetime.now(timezone.utc)
        customer = self.get_random_customer()
        product = self.get_random_product()
        session_id = f"sess_{uuid.uuid4().hex[:12]}"
        device = random.choice(DEVICES)
        referrer = random.choice(REFERRERS)

        clicks: List[ClickstreamEvent] = []
        order: Optional[OrderEvent] = None
        payment: Optional[PaymentEvent] = None

        # 1. Step 1: Page View (Always)
        t_view = now.isoformat()
        view_event = ClickstreamEvent(
            event_id=f"evt_{uuid.uuid4().hex[:16]}",
            session_id=session_id,
            customer_id=customer.customer_id,
            product_id=product.product_id,
            event_type=ClickEventType.VIEW,
            page_url=f"https://store.ecommerce.com/products/{product.product_id}",
            referrer=referrer,
            device=device,
            event_timestamp=t_view,
        )
        clicks.append(view_event)

        # 2. Step 2: Add to Cart (40% probability)
        if random.random() < 0.40:
            t_cart = (now + timedelta(seconds=random.randint(5, 45))).isoformat()
            cart_event = ClickstreamEvent(
                event_id=f"evt_{uuid.uuid4().hex[:16]}",
                session_id=session_id,
                customer_id=customer.customer_id,
                product_id=product.product_id,
                event_type=ClickEventType.ADD_TO_CART,
                page_url=f"https://store.ecommerce.com/cart",
                referrer=referrer,
                device=device,
                event_timestamp=t_cart,
            )
            clicks.append(cart_event)

            # 3. Step 3: Checkout Initiation (60% of cart)
            if random.random() < 0.60:
                t_chk = (now + timedelta(seconds=random.randint(50, 120))).isoformat()
                checkout_event = ClickstreamEvent(
                    event_id=f"evt_{uuid.uuid4().hex[:16]}",
                    session_id=session_id,
                    customer_id=customer.customer_id,
                    product_id=product.product_id,
                    event_type=ClickEventType.CHECKOUT,
                    page_url=f"https://store.ecommerce.com/checkout",
                    referrer=referrer,
                    device=device,
                    event_timestamp=t_chk,
                )
                clicks.append(checkout_event)

                # 4. Step 4: Final Purchase (75% of checkout)
                if random.random() < 0.75:
                    t_order = (now + timedelta(seconds=random.randint(125, 200))).isoformat()
                    order_id = f"ord_{uuid.uuid4().hex[:14]}"
                    qty = random.choices([1, 2, 3, 4, 5], weights=[0.65, 0.20, 0.08, 0.05, 0.02])[0]
                    total_amt = round(qty * product.price, 2)

                    order = OrderEvent(
                        order_id=order_id,
                        session_id=session_id,
                        customer_id=customer.customer_id,
                        product_id=product.product_id,
                        quantity=qty,
                        unit_price=product.price,
                        total_amount=total_amt,
                        order_status=OrderStatus.COMPLETED,
                        order_timestamp=t_order,
                    )

                    # Payment settled 2-5 seconds later
                    t_pay = (now + timedelta(seconds=random.randint(201, 206))).isoformat()
                    # 95% success rate, 5% failed payment
                    p_status = PaymentStatus.SUCCESS if random.random() < 0.95 else PaymentStatus.FAILED

                    payment = PaymentEvent(
                        payment_id=f"pay_{uuid.uuid4().hex[:14]}",
                        order_id=order_id,
                        customer_id=customer.customer_id,
                        payment_method=random.choice(list(PaymentMethod)),
                        payment_status=p_status,
                        amount=total_amt,
                        payment_timestamp=t_pay,
                    )

        # Inject simulated duplicate event for testing Spark deduplication
        if self.duplicate_prob > 0 and random.random() < self.duplicate_prob:
            if clicks:
                # Duplicate one click event
                dup_click = clicks[0].model_copy()
                clicks.append(dup_click)

        return clicks, order, payment
