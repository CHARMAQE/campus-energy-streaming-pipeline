# producer/data_generator_training.py
import csv
import random
import datetime
import argparse
from tqdm import trange

def build_row(building, floor, is_anomaly=False, hour=None):
    # hour influences profile: daytime heavier
    if hour is None:
        hour = random.randint(0, 23)
    daylight = 1.2 if 7 <= hour <= 19 else 0.8
    peak = 1.3 if 12 <= hour <= 14 else 1.0
    profile = daylight * peak

    building_factor = 1.0 + 0.05 * (hash(building) % 5)
    floor_factor = 1.0 + (floor - 1) * 0.02

    base_elec = 95 * profile * building_factor * floor_factor
    base_water = 180 * profile * building_factor * floor_factor

    if is_anomaly:
        electricity = round(random.uniform(base_elec + 120, base_elec + 350), 2)
        water = round(random.uniform(base_water + 150, base_water + 600), 2)
        status = "anomaly"
    else:
        electricity = round(random.uniform(base_elec * 0.6, base_elec + 85), 2)
        water = round(random.uniform(base_water * 0.6, base_water + 170), 2)
        status = "normal"

    ts = (datetime.datetime.utcnow() - datetime.timedelta(seconds=random.randint(0, 86400))).isoformat() + "Z"
    return [building, floor, electricity, water, status, ts]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="training_energy.csv")
    parser.add_argument("--rows", type=int, default=200000)
    parser.add_argument("--buildings", nargs="*", default=["Building A", "Building B", "Building C", "Building D"])
    parser.add_argument("--floors", type=int, default=5)
    parser.add_argument("--anomaly_rate", type=float, default=0.05)
    args = parser.parse_args()

    with open(args.out, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["building", "floor", "electricity", "water", "status", "timestamp"])
        for _ in trange(args.rows):
            b = random.choice(args.buildings)
            fl = random.randint(1, args.floors)
            is_an = random.random() < args.anomaly_rate
            row = build_row(b, fl, is_an)
            writer.writerow(row)

    print(f"Saved {args.out}")

if __name__ == "__main__":
    main()
