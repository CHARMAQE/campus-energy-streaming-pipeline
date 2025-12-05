import json
import random
import time
import argparse
import logging
import signal
import sys
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

log = logging.getLogger("producer")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:29092")  # Changed to 29092 (works for you)
    p.add_argument("--topic", default="university_consumption")
    p.add_argument("--interval", type=float, default=0.2, help="Seconds between batches")  # Changed to 0.2s (5x faster)
    p.add_argument("--floors", type=int, default=5)
    p.add_argument("--buildings", nargs="*", default=["Building A", "Building B", "Building C"])
    p.add_argument("--seed", type=int, default=None)
    return p.parse_args()

def make_producer(bootstrap, max_retries=5, retry_delay=2):
    """Create Kafka producer with retry logic"""
    for attempt in range(1, max_retries + 1):
        try:
            log.info("Connecting to Kafka at %s (attempt %d/%d)...", bootstrap, attempt, max_retries)
            producer = KafkaProducer(
                bootstrap_servers=bootstrap,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=50,
                acks="all",
                retries=3,
                api_version_auto_timeout_ms=10000,
                request_timeout_ms=30000,
                metadata_max_age_ms=5000,
            )
            log.info("✅ Connected to Kafka successfully!")
            return producer
        except NoBrokersAvailable as e:
            if attempt < max_retries:
                log.warning("⚠️  Kafka not ready yet. Retrying in %ds... (%s)", retry_delay, str(e))
                time.sleep(retry_delay)
            else:
                log.error("❌ Failed to connect to Kafka after %d attempts", max_retries)
                log.error("Please ensure Kafka is running: docker ps | grep kafka")
                raise
        except Exception as e:
            log.error("❌ Unexpected error connecting to Kafka: %s", e)
            raise

def base_profile(hour: int):
    # Example diurnal pattern modifiers
    # Higher consumption 7–19, peak at 12–14
    daylight_factor = 1.2 if 7 <= hour <= 19 else 0.8
    peak_factor = 1.3 if 12 <= hour <= 14 else 1.0
    return daylight_factor * peak_factor

def compute_status(electricity: float, water: float) -> str:
    if electricity > 180 or water > 350:
        return "High"
    if electricity > 140 or water > 275:
        return "Warning"
    return "Normal"

def generate_data(building: str, floor: int):
    hour = datetime.utcnow().hour
    profile = base_profile(hour)

    # Building / floor modifiers
    building_factor = 1.0 + 0.05 * (hash(building) % 5)
    floor_factor = 1.0 + (floor - 1) * 0.02

    # Electricity (kWh) and water (liters) simulation
    base_electricity = 95 * profile * building_factor * floor_factor
    electricity = round(random.uniform(base_electricity, base_electricity + 85), 2)

    base_water = 180 * profile * building_factor * floor_factor
    water = round(random.uniform(base_water, base_water + 170), 2)

    status = compute_status(electricity, water)

    return {
        "building": building,
        "floor": floor,
        "electricity": electricity,
        "water": water,
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat(),  # ✅ USE CURRENT TIME
    }

_running = True
def _handle_stop(signum, frame):
    global _running
    log.info("Shutdown signal received (%s). Stopping...", signum)
    _running = False

def main():
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    # Create producer with retry logic
    producer = make_producer(args.bootstrap)
    
    log.info("🚀 Starting data producer:")
    log.info("   • Topic: %s", args.topic)
    log.info("   • Buildings: %s", args.buildings)
    log.info("   • Floors per building: %d", args.floors)
    log.info("   • Interval: %.2fs (%.0f records/sec)", args.interval, len(args.buildings) * args.floors / args.interval)
    log.info("Press Ctrl+C to stop.")

    try:
        batch_count = 0
        records_sent = 0
        while _running:
            for building in args.buildings:
                for floor in range(1, args.floors + 1):
                    payload = generate_data(building, floor)
                    try:
                        future = producer.send(
                            args.topic,
                            value=payload,
                            key=building.encode("utf-8")
                        )
                        future.add_errback(lambda e: log.error("Send failed: %s", e))
                        records_sent += 1
                        log.debug("Queued: %s", payload)
                    except KafkaError as e:
                        log.error("Immediate send error: %s", e)
            
            batch_count += 1
            if batch_count % 20 == 0:
                producer.flush()
                log.info("✓ Batch %d completed (%d records sent)", batch_count, records_sent)
            
            time.sleep(args.interval)
    
    except KeyboardInterrupt:
        log.info("Interrupted by user")
    finally:
        log.info("Flushing & closing producer...")
        producer.flush()
        producer.close()
        log.info("✅ Stopped cleanly. Total records sent: %d", records_sent)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)
    main()
