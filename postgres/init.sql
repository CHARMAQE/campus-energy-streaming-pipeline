-- Initialize energy_monitoring database
-- This script runs automatically when PostgreSQL container starts with empty volume

-- Drop tables if they exist (for clean initialization)
DROP TABLE IF EXISTS anomalies CASCADE;
DROP TABLE IF EXISTS aggregations_floor CASCADE;
DROP TABLE IF EXISTS aggregations CASCADE;

-- Table for building-level aggregations (30-second windows)
CREATE TABLE aggregations (
    id SERIAL PRIMARY KEY,
    building VARCHAR(50) NOT NULL,
    avg_electricity DOUBLE PRECISION NOT NULL,
    avg_water DOUBLE PRECISION NOT NULL,
    max_elec DOUBLE PRECISION NOT NULL,
    min_elec DOUBLE PRECISION NOT NULL,
    avg_anomaly_prob DOUBLE PRECISION NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for floor-level aggregations (30-second windows)
CREATE TABLE aggregations_floor (
    id SERIAL PRIMARY KEY,
    building VARCHAR(50) NOT NULL,
    floor INTEGER NOT NULL,
    avg_electricity DOUBLE PRECISION NOT NULL,
    avg_water DOUBLE PRECISION NOT NULL,
    max_elec DOUBLE PRECISION NOT NULL,
    min_elec DOUBLE PRECISION NOT NULL,
    avg_anomaly_prob DOUBLE PRECISION NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for detected anomalies with classification
CREATE TABLE anomalies (
    id SERIAL PRIMARY KEY,
    building VARCHAR(50) NOT NULL,
    floor INTEGER NOT NULL,
    electricity DOUBLE PRECISION NOT NULL,
    water DOUBLE PRECISION NOT NULL,
    anomaly_probability DOUBLE PRECISION NOT NULL,
    anomaly_type VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_aggregations_building ON aggregations(building);
CREATE INDEX idx_aggregations_window ON aggregations(window_start, window_end);
CREATE INDEX idx_aggregations_floor_building_floor ON aggregations_floor(building, floor);
CREATE INDEX idx_aggregations_floor_window ON aggregations_floor(window_start, window_end);
CREATE INDEX idx_anomalies_building_floor ON anomalies(building, floor);
CREATE INDEX idx_anomalies_type ON anomalies(anomaly_type);
CREATE INDEX idx_anomalies_timestamp ON anomalies(timestamp);
CREATE INDEX idx_anomalies_probability ON anomalies(anomaly_probability);

-- Grant privileges
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO admin;

-- ✅ NEW: Create Airflow database for orchestration
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO admin;

-- Success message
\echo 'Database initialization completed successfully!'
\echo 'Created: energy_monitoring (for streaming data)'
\echo 'Created: airflow (for workflow orchestration)'