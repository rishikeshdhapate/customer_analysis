"""
Resilient Kafka Producer for E-Commerce Events.
Handles connection retries, JSON serialization, delivery confirmation, and fallback simulation.
"""

import json
import logging
import time
from typing import Dict, Any, Optional

try:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable, KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

from producers.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_ORDERS,
    TOPIC_CLICKSTREAM,
    TOPIC_PAYMENTS,
    TOPIC_CUSTOMERS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("KafkaProducer")


class ResilientEventPublisher:
    """Wrapper around KafkaProducer with automatic retry, reconnection, and graceful fallback."""

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        retries: int = 5,
        retry_backoff_sec: float = 2.0,
        dry_run: bool = False,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.dry_run = dry_run
        self.producer: Optional[Any] = None
        self.published_counts: Dict[str, int] = {
            TOPIC_ORDERS: 0,
            TOPIC_CLICKSTREAM: 0,
            TOPIC_PAYMENTS: 0,
            TOPIC_CUSTOMERS: 0,
        }

        if not dry_run and KAFKA_AVAILABLE:
            self._connect(retries, retry_backoff_sec)
        else:
            if not KAFKA_AVAILABLE and not dry_run:
                logger.warning("kafka-python not installed. Running in DRY-RUN mode.")
            self.dry_run = True

    def _connect(self, retries: int, backoff: float) -> None:
        """Attempt connection to Kafka cluster with backoff."""
        for attempt in range(1, retries + 1):
            try:
                logger.info(f"Connecting to Kafka brokers at {self.bootstrap_servers} (attempt {attempt}/{retries})...")
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers.split(","),
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks="all",
                    retries=3,
                    linger_ms=10,
                    batch_size=16384,
                )
                logger.info("Successfully connected to Kafka cluster.")
                return
            except NoBrokersAvailable as e:
                logger.warning(f"Kafka brokers unavailable: {e}. Retrying in {backoff} seconds...")
                time.sleep(backoff)
            except Exception as e:
                logger.error(f"Unexpected error connecting to Kafka: {e}")
                time.sleep(backoff)

        logger.warning("Could not establish connection to Kafka. Falling back to DRY-RUN mode.")
        self.dry_run = True

    def publish_event(self, topic: str, key: str, payload: Dict[str, Any]) -> bool:
        """Send event payload to specified Kafka topic."""
        if self.dry_run or not self.producer:
            self.published_counts[topic] = self.published_counts.get(topic, 0) + 1
            if self.published_counts[topic] % 50 == 0:
                logger.info(f"[DRY-RUN] Emitted to {topic} (Key={key}): Total={self.published_counts[topic]}")
            return True

        try:
            future = self.producer.send(topic, key=key, value=payload)
            # Register callbacks for tracking
            future.add_errback(lambda exc: logger.error(f"Failed to produce message to {topic}: {exc}"))
            self.published_counts[topic] = self.published_counts.get(topic, 0) + 1
            return True
        except KafkaError as e:
            logger.error(f"Error publishing to {topic}: {e}")
            return False

    def flush(self) -> None:
        if self.producer:
            self.producer.flush()

    def close(self) -> None:
        if self.producer:
            logger.info("Flushing and closing Kafka producer connection...")
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed.")
        logger.info(f"Summary of published events: {self.published_counts}")
