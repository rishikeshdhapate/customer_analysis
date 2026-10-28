"""
Configuration settings for event producers.
Loads parameters from environment variables with sensible defaults.
"""

import os
from typing import List

# Kafka Broker Configuration
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)

# Kafka Topics
TOPIC_ORDERS: str = os.getenv("KAFKA_TOPIC_ORDERS", "ecommerce.orders")
TOPIC_CLICKSTREAM: str = os.getenv("KAFKA_TOPIC_CLICKSTREAM", "ecommerce.clickstream")
TOPIC_PAYMENTS: str = os.getenv("KAFKA_TOPIC_PAYMENTS", "ecommerce.payments")
TOPIC_CUSTOMERS: str = os.getenv("KAFKA_TOPIC_CUSTOMERS", "ecommerce.customers")

ALL_TOPICS: List[str] = [
    TOPIC_ORDERS,
    TOPIC_CLICKSTREAM,
    TOPIC_PAYMENTS,
    TOPIC_CUSTOMERS,
]

# Producer Run Settings
EMIT_RATE_HZ: float = float(os.getenv("PRODUCER_EMIT_RATE_HZ", "5.0"))
SIMULATION_MAX_CUSTOMERS: int = int(os.getenv("SIMULATION_MAX_CUSTOMERS", "500"))
SIMULATION_MAX_PRODUCTS: int = int(os.getenv("SIMULATION_MAX_PRODUCTS", "80"))
DUPLICATE_PROBABILITY: float = float(os.getenv("DUPLICATE_PROBABILITY", "0.03"))
RANDOM_SEED: int = int(os.getenv("PRODUCER_SEED", "42"))
