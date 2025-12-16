# producer/energy_data_core.py
"""
Core energy data generation logic.
Used by both training and real-time producers.
"""

import random
from datetime import datetime
from typing import Dict

# Configuration
BUILDINGS = ["Building A", "Building B", "Building C"]
FLOORS_PER_BUILDING = 5
BASE_ELECTRICITY = 95  # kWh
BASE_WATER = 180  # Liters

# Anomaly thresholds (for training data only)
ELECTRICITY_ANOMALY_THRESHOLD = 250
WATER_ANOMALY_THRESHOLD = 450


def time_profile(hour: int) -> float:
    """Consumption multiplier based on hour."""
    if 8 <= hour <= 18:  # Work hours
        return 1.3
    elif 19 <= hour <= 21:  # Evening
        return 1.1
    elif 22 <= hour <= 6:  # Night
        return 0.7
    return 1.0


def building_factor(building: str) -> float:
    """Different buildings have different efficiency."""
    return {"Building A": 1.0, "Building B": 1.15, "Building C": 0.95}.get(building, 1.0)


def floor_factor(floor: int) -> float:
    """Higher floors consume slightly more."""
    return 1.0 + (floor - 1) * 0.03


def generate_reading(building: str, floor: int, timestamp: datetime, 
                    force_anomaly: bool = False, include_label: bool = True) -> Dict:
    """
    Generate energy reading.
    
    Args:
        building: Building name
        floor: Floor number
        timestamp: Reading timestamp
        force_anomaly: Force anomaly (training only)
        include_label: Include status label (training only)
    """
    hour = timestamp.hour
    
    # Calculate base consumption
    time_mult = time_profile(hour)
    building_mult = building_factor(building)
    floor_mult = floor_factor(floor)
    
    base_elec = BASE_ELECTRICITY * time_mult * building_mult * floor_mult
    base_water = BASE_WATER * time_mult * building_mult * floor_mult
    
    # Determine if anomaly
    is_anomaly = force_anomaly or (random.random() < 0.05)
    
    if is_anomaly:
        electricity = round(random.uniform(ELECTRICITY_ANOMALY_THRESHOLD, 400), 2)
        water = round(random.uniform(WATER_ANOMALY_THRESHOLD, 650), 2)
        status = "anomaly"
    else:
        electricity = round(random.uniform(base_elec * 0.7, base_elec * 1.3), 2)
        water = round(random.uniform(base_water * 0.7, base_water * 1.3), 2)
        status = "normal"
    
    # Build result
    reading = {
        "building": building,
        "floor": floor,
        "electricity": electricity,
        "water": water,
        "timestamp": timestamp.isoformat()
    }
    
    # Only include label for training data
    if include_label:
        reading["status"] = status
    
    return reading