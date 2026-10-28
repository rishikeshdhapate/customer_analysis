"""
Main entry point for running the e-commerce event stream generator.
Generates clickstreams, orders, and payments continuously or in batches.
"""

import argparse
import signal
import sys
import time
import logging

from producers.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_ORDERS,
    TOPIC_CLICKSTREAM,
    TOPIC_PAYMENTS,
    TOPIC_CUSTOMERS,
    EMIT_RATE_HZ,
    SIMULATION_MAX_CUSTOMERS,
    SIMULATION_MAX_PRODUCTS,
    DUPLICATE_PROBABILITY,
    RANDOM_SEED,
)
from producers.generator import EcommerceSimulator
from producers.kafka_producer import ResilientEventPublisher

logger = logging.getLogger("RunProducer")
RUNNING = True


def signal_handler(sig, frame):
    global RUNNING
    logger.info("Shutdown signal received. Stopping event generator...")
    RUNNING = False


def main():
    parser = argparse.ArgumentParser(description="Simulated E-Commerce Event Stream Producer")
    parser.add_argument("--rate", type=float, default=EMIT_RATE_HZ, help="Emission rate in journeys per second")
    parser.add_argument("--duration", type=int, default=0, help="Duration to run in seconds (0 = infinite)")
    parser.add_argument("--dry-run", action="store_true", help="Run without Kafka connection (logs only)")
    parser.add_argument("--bootstrap-servers", type=str, default=KAFKA_BOOTSTRAP_SERVERS, help="Kafka bootstrap servers")
    parser.add_argument("--customers", type=int, default=SIMULATION_MAX_CUSTOMERS, help="Number of customers to seed")
    parser.add_argument("--products", type=int, default=SIMULATION_MAX_PRODUCTS, help="Number of products to seed")
    parser.add_argument("--seed", type=int, default=RANDOM_SEED, help="Random seed for reproducibility")
    parser.add_argument("--duplicates", type=float, default=DUPLICATE_PROBABILITY, help="Duplicate injection rate (0.0-1.0)")

    args = parser.parse_args()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Initializing E-Commerce Simulator...")
    simulator = EcommerceSimulator(
        num_customers=args.customers,
        num_products=args.products,
        seed=args.seed,
        duplicate_prob=args.duplicates,
    )

    publisher = ResilientEventPublisher(
        bootstrap_servers=args.bootstrap_servers,
        dry_run=args.dry_run,
    )

    # First, publish customer master records to customers topic
    logger.info(f"Publishing {len(simulator.customers)} customer master records to {TOPIC_CUSTOMERS}...")
    for cust in simulator.customers:
        publisher.publish_event(TOPIC_CUSTOMERS, key=cust.customer_id, payload=cust.model_dump())
    publisher.flush()
    logger.info("Customer master catalog published.")

    interval_sec = 1.0 / max(args.rate, 0.1)
    start_time = time.time()
    steps_emitted = 0

    logger.info(f"Starting real-time event generation at {args.rate:.2f} Hz (Interval: {interval_sec*1000:.1f}ms)...")

    try:
        while RUNNING:
            if args.duration > 0 and (time.time() - start_time) >= args.duration:
                logger.info(f"Target duration of {args.duration}s reached. Stopping.")
                break

            # Generate a realistic user journey step
            clicks, order, payment = simulator.generate_journey_step()

            # Publish Clickstream events
            for click in clicks:
                publisher.publish_event(
                    topic=TOPIC_CLICKSTREAM,
                    key=click.customer_id,
                    payload=click.model_dump(),
                )

            # Publish Order event if journey converted
            if order:
                publisher.publish_event(
                    topic=TOPIC_ORDERS,
                    key=order.order_id,
                    payload=order.model_dump(),
                )

            # Publish Payment event
            if payment:
                publisher.publish_event(
                    topic=TOPIC_PAYMENTS,
                    key=payment.payment_id,
                    payload=payment.model_dump(),
                )

            steps_emitted += 1
            if steps_emitted % 25 == 0:
                elapsed = time.time() - start_time
                logger.info(
                    f"Progress: {steps_emitted} journeys processed in {elapsed:.1f}s | "
                    f"Orders: {publisher.published_counts[TOPIC_ORDERS]} | "
                    f"Clicks: {publisher.published_counts[TOPIC_CLICKSTREAM]} | "
                    f"Payments: {publisher.published_counts[TOPIC_PAYMENTS]}"
                )

            time.sleep(interval_sec)

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received.")
    finally:
        publisher.close()
        logger.info("Producer terminated successfully.")


if __name__ == "__main__":
    main()
