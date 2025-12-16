"""
Generate labeled training data for ML model.
Creates historical dataset with known anomalies.
"""

import csv
import random
import argparse
from datetime import datetime, timedelta, timezone
from tqdm import trange
from energy_data_core import (
    generate_reading,
    BUILDINGS,
    FLOORS_PER_BUILDING
)

def generate_training_dataset(
    num_records: int = 200000,
    anomaly_rate: float = 0.05,
    days_back: int = 30,
    output_file: str = "../spark/training_energy.csv"
):
    """
    Generate training dataset with labeled anomalies.
    
    Args:
        num_records: Total number of records to generate
        anomaly_rate: Fraction of records that should be anomalies (0.05 = 5%)
        days_back: Generate data from this many days in the past
        output_file: Output CSV filename
    
    Why these defaults?
        - 200k records: Enough for good ML training (not too small, not too big)
        - 5% anomaly rate: Realistic for university buildings (1-2 events per day)
        - 30 days back: One month of historical data
    """
    print("=" * 60)
    print("GENERATING LABELED TRAINING DATA")
    print("=" * 60)
    print(f"Records: {num_records:,}")
    print(f"Anomaly rate: {anomaly_rate*100:.1f}%")
    print(f"Time range: {days_back} days ago to now")
    print(f"Buildings: {len(BUILDINGS)} ({', '.join(BUILDINGS)})")
    print(f"Floors per building: {FLOORS_PER_BUILDING}")
    
    # Calculate expected anomalies
    expected_anomalies = int(num_records * anomaly_rate)
    print(f"Expected anomalies: ~{expected_anomalies:,}")
    print("=" * 60)
    
    records = []
    actual_anomalies = 0
    
    # Generate records with progress bar
    now = datetime.now(timezone.utc)
    start_time = now - timedelta(days=days_back)
    
    for i in trange(num_records, desc="Generating"):
        # Random building and floor
        building = random.choice(BUILDINGS)
        floor = random.randint(1, FLOORS_PER_BUILDING)
        
        # Random timestamp in the past (uniform distribution)
        seconds_offset = random.randint(0, days_back * 86400)
        timestamp = start_time + timedelta(seconds=seconds_offset)
        
        # Force some anomalies to meet target rate
        remaining_records = num_records - i
        remaining_anomalies = expected_anomalies - actual_anomalies
        force_anomaly = False
        
        if remaining_records > 0:
            # Probability to force anomaly to reach target rate
            force_probability = max(0, remaining_anomalies / remaining_records)
            force_anomaly = random.random() < force_probability
        
        # Generate reading using unified core logic
        reading = generate_reading(building, floor, timestamp, force_anomaly)
        
        if reading["status"] == "anomaly":
            actual_anomalies += 1
        
        records.append(reading)
    
    # Write to CSV
    print(f"\n💾 Writing to {output_file}...")
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "building", "floor", "electricity", "water", "status", "timestamp"
        ])
        writer.writeheader()
        writer.writerows(records)
    
    # Statistics
    normal_count = num_records - actual_anomalies
    actual_rate = actual_anomalies / num_records * 100
    
    print("\n" + "=" * 60)
    print("✅ TRAINING DATA GENERATED SUCCESSFULLY!")
    print("=" * 60)
    print(f"Total records: {num_records:,}")
    print(f"Normal: {normal_count:,} ({normal_count/num_records*100:.1f}%)")
    print(f"Anomalies: {actual_anomalies:,} ({actual_rate:.1f}%)")
    print(f"File: {output_file}")
    print(f"Size: {len(records) * 80 / 1024 / 1024:.1f} MB (approx)")
    print("=" * 60)

def main():
    parser = argparse.ArgumentParser(
        description="Generate labeled training data for energy anomaly detection"
    )
    parser.add_argument(
        "--output",
        default="../spark/training_energy.csv",
        help="Output CSV file path"
    )
    parser.add_argument(
        "--records",
        type=int,
        default=200000,
        help="Number of records to generate"
    )
    parser.add_argument(
        "--anomaly-rate",
        type=float,
        default=0.05,
        help="Fraction of anomalous records (0.0-1.0)"
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=30,
        help="Generate data from this many days in the past"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducibility"
    )
    
    args = parser.parse_args()
    
    if args.seed is not None:
        random.seed(args.seed)
        print(f"🎲 Using random seed: {args.seed}")
    
    generate_training_dataset(
        num_records=args.records,
        anomaly_rate=args.anomaly_rate,
        days_back=args.days_back,
        output_file=args.output
    )

if __name__ == "__main__":
    main()
