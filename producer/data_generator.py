import json
import random
import time
import argparse
import logging
import signal
import sys
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

log = logging.getLogger("producer")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="university_consumption")
    p.add_argument("--interval", type=float, default=1.0, help="Seconds between batches")
    p.add_argument("--floors", type=int, default=5)
    p.add_argument("--buildings", nargs="*", default=["Building A", "Building B", "Building C"])
    p.add_argument("--seed", type=int, default=None)
    return p.parse_args()

def make_producer(bootstrap):
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        acks="all",
        retries=3,
    )

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
    building_factor = 1.0 + 0.05 * (hash(building) % 5)  # deterministic slight variation
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
        "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
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

    producer = make_producer(args.bootstrap)
    log.info("Starting data producer. Topic=%s Buildings=%s Floors=%d Interval=%.2fs",
             args.topic, args.buildings, args.floors, args.interval)

    try:
        batch_count = 0
        while _running:
            for building in args.buildings:
                for floor in range(1, args.floors + 1):
                    payload = generate_data(building, floor)
                    try:
                        future = producer.send(
                            args.topic,
                            value=payload,
                            key=building.encode("utf-8")  # partition by building
                        )
                        future.add_errback(lambda e: log.error("Send failed: %s", e))
                        log.debug("Queued: %s", payload)
                    except KafkaError as e:
                        log.error("Immediate send error: %s", e)
            batch_count += 1
            if batch_count % 10 == 0:
                producer.flush()
                log.info("Flushed batch_count=%d", batch_count)
            time.sleep(args.interval)
    finally:
        log.info("Flushing & closing producer...")
        producer.flush()
        producer.close()
        log.info("Stopped cleanly.")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)
    main()
