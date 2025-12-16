"""
Stream real-time energy data to Kafka.
Optimized for low-resource environments.
"""

import json
import time
import argparse
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError
from energy_data_core import generate_reading, BUILDINGS

def create_producer(bootstrap_servers, max_retries=3):
    """Create Kafka producer with retry logic."""
    for attempt in range(max_retries):
        try:
            print(f"Connecting to Kafka at {bootstrap_servers}... (attempt {attempt + 1}/{max_retries})")
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=30000,
                metadata_max_age_ms=5000,
                # Optimize for low memory
                buffer_memory=8388608,  # 8MB (reduced from 32MB default)
                batch_size=8192,  # 8KB (reduced from 16KB default)
                linger_ms=100  # Batch messages for efficiency
            )
            print("✅ Connected!")
            return producer
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️ Connection failed: {e}")
                print(f"Retrying in 5 seconds...")
                time.sleep(5)
            else:
                raise

def main():
    parser = argparse.ArgumentParser(description="Stream energy data to Kafka")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="university_consumption", help="Kafka topic")
    parser.add_argument("--interval", type=float, default=2.0, help="Send interval (seconds)")  # Increased from 1.0
    args = parser.parse_args()
    
    # Connect to Kafka with retry
    producer = create_producer(args.bootstrap)
    
    print(f"Streaming to topic: {args.topic}")
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
                    reading = generate_reading(
                        building=building,
                        floor=floor,
                        timestamp=datetime.now(),
                        include_label=False
                    )
                    
                    try:
                        producer.send(args.topic, value=reading)
                        batch_size += 1
                    except KafkaTimeoutError:
                        print(f"⚠️ Timeout sending message, retrying...")
                        time.sleep(1)
                        producer.send(args.topic, value=reading)
                        batch_size += 1
            
            total_sent += batch_size
            print(f"Batch {batch_count} | Sent: {batch_size} readings | Total: {total_sent}")
            
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        print(f"\n\n🛑 Stopped. Total sent: {total_sent} readings")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
