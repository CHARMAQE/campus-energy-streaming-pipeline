"""
Stream real-time energy data to Kafka.
NO LABELS - simulates real production sensors.
"""

import json
import time
import argparse
from datetime import datetime
from kafka import KafkaProducer
from energy_data_core import generate_reading, BUILDINGS

def main():
    parser = argparse.ArgumentParser(description="Stream energy data to Kafka")
    parser.add_argument("--bootstrap", default="localhost:29092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="university_consumption", help="Kafka topic")
    parser.add_argument("--interval", type=float, default=1.0, help="Send interval (seconds)")
    args = parser.parse_args()
    
    # Connect to Kafka
    print(f"Connecting to Kafka at {args.bootstrap}...")
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f"✅ Connected! Streaming to topic: {args.topic}")
    print(f"📡 Sending {len(BUILDINGS) * 5} readings every {args.interval}s")
    print("Press Ctrl+C to stop\n")
    
    batch_count = 0
    total_sent = 0
    
    try:
        while True:
            batch_count += 1
            batch_size = 0
            
            # Generate readings for all buildings and floors
            for building in BUILDINGS:
                for floor in range(1, 6):
                    # Generate reading WITHOUT label (real production scenario)
                    reading = generate_reading(
                        building=building,
                        floor=floor,
                        timestamp=datetime.now(),
                        include_label=False  # ← NO STATUS in production!
                    )
                    
                    producer.send(args.topic, value=reading)
                    batch_size += 1
            
            total_sent += batch_size
            print(f"Batch {batch_count} | Sent: {batch_size} readings | Total: {total_sent}")
            
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        print(f"\n\n🛑 Stopped. Total sent: {total_sent} readings")
        producer.close()

if __name__ == "__main__":
    main()
