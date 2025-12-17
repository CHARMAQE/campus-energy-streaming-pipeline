"""
Core energy data generation logic.
✅ SIMPLIFIED: Rare anomalies, no complex statistics
"""

import random
from datetime import datetime, time

# Building configuration
BUILDINGS = ["Building A", "Building B", "Building C"]
FLOORS_PER_BUILDING = 5

# Normal consumption ranges (kWh)
ELEC_NORMAL_MEAN = 100.0
ELEC_NORMAL_STD = 15.0
WATER_NORMAL_MEAN = 120.0
WATER_NORMAL_STD = 20.0

# Working hours
WORKING_HOURS_START = time(8, 0)   # 8:00 AM
WORKING_HOURS_END = time(18, 0)    # 6:00 PM

# ✅ SIMPLIFIED: Very low anomaly rates (1-2 per day)
# Calculation: 15 readings/2sec = 450/min = 27,000/hour = 648,000/day
# For 1 anomaly per day: 1/648,000 = 0.00000154
ANOMALY_PROBABILITY_WORKING_HOURS = 0.000002   # ~1-2 anomalies per day
ANOMALY_PROBABILITY_OFF_HOURS = 0.0000001      # Almost never at night

# Simple anomaly types
ANOMALY_TYPES = {
    'high_consumption': 3,    # Most common: unusual high usage
    'very_high': 1,          # Rare: equipment failure
    'leak': 2                # Water leak
}

def is_working_hours(timestamp):
    """Check if timestamp is during working hours (Mon-Fri 8am-6pm)"""
    if timestamp.weekday() >= 5:  # Weekend
        return False
    current_time = timestamp.time()
    return WORKING_HOURS_START <= current_time <= WORKING_HOURS_END

def should_generate_anomaly(timestamp, force_anomaly=False):
    """
    Simple anomaly probability check.
    - Training: force_anomaly=True generates balanced dataset
    - Production: Very rare (1-2 per day during work hours)
    """
    if force_anomaly:
        return True
    
    probability = (ANOMALY_PROBABILITY_WORKING_HOURS if is_working_hours(timestamp) 
                   else ANOMALY_PROBABILITY_OFF_HOURS)
    return random.random() < probability

def generate_reading(building, floor, timestamp, force_anomaly=False, include_label=True):
    """
    Generate energy reading.
    ✅ SIMPLIFIED: Clear anomaly patterns, always positive values
    """
    # Normal values
    electricity = random.gauss(ELEC_NORMAL_MEAN, ELEC_NORMAL_STD)
    water = random.gauss(WATER_NORMAL_MEAN, WATER_NORMAL_STD)
    
    is_anomaly = should_generate_anomaly(timestamp, force_anomaly)
    status = "normal"
    
    if is_anomaly:
        anomaly_type = random.choices(
            list(ANOMALY_TYPES.keys()),
            weights=list(ANOMALY_TYPES.values())
        )[0]
        
        if anomaly_type == 'high_consumption':
            # 2-3x normal usage
            electricity *= random.uniform(2.0, 3.0)
            water *= random.uniform(1.5, 2.5)
            status = "anomaly"
        
        elif anomaly_type == 'very_high':
            # 3-5x normal usage (equipment failure)
            electricity *= random.uniform(3.0, 5.0)
            water *= random.uniform(2.0, 3.5)
            status = "anomaly"
        
        elif anomaly_type == 'leak':
            # Water leak: high water, normal electricity
            water *= random.uniform(3.0, 5.0)
            status = "anomaly"
    
    # Always positive values in production
    if not include_label:
        electricity = max(0.1, electricity)
        water = max(0.1, water)
    
    reading = {
        "building": building,
        "floor": floor,
        "electricity": round(electricity, 2),
        "water": round(water, 2),
        "timestamp": timestamp.isoformat()
    }
    
    if include_label:
        reading["status"] = status
    
    return reading