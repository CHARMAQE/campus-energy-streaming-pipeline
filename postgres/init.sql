-- Create aggregations table for time-windowed energy consumption data
CREATE TABLE IF NOT EXISTS aggregations (
    id SERIAL PRIMARY KEY,
    building VARCHAR(50) NOT NULL,
    avg_electricity DOUBLE PRECISION,
    avg_water DOUBLE PRECISION,
    max_elec DOUBLE PRECISION,
    min_elec DOUBLE PRECISION,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create anomalies table for ML-detected anomalies
CREATE TABLE IF NOT EXISTS anomalies (
    id SERIAL PRIMARY KEY,
    building VARCHAR(50) NOT NULL,
    floor INTEGER,
    electricity DOUBLE PRECISION,
    water DOUBLE PRECISION,
    status VARCHAR(20),
    timestamp TIMESTAMP,
    cluster INTEGER,
    distance DOUBLE PRECISION,
    is_anomaly BOOLEAN DEFAULT TRUE,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_aggregations_building ON aggregations(building);
CREATE INDEX IF NOT EXISTS idx_aggregations_window_start ON aggregations(window_start);
CREATE INDEX IF NOT EXISTS idx_aggregations_window_end ON aggregations(window_end);
CREATE INDEX IF NOT EXISTS idx_aggregations_processed_at ON aggregations(processed_at);

CREATE INDEX IF NOT EXISTS idx_anomalies_building ON anomalies(building);
CREATE INDEX IF NOT EXISTS idx_anomalies_timestamp ON anomalies(timestamp);
CREATE INDEX IF NOT EXISTS idx_anomalies_detected_at ON anomalies(detected_at);
CREATE INDEX IF NOT EXISTS idx_anomalies_is_anomaly ON anomalies(is_anomaly);

-- Grant permissions
GRANT ALL PRIVILEGES ON TABLE aggregations TO admin;
GRANT ALL PRIVILEGES ON TABLE anomalies TO admin;
GRANT ALL PRIVILEGES ON SEQUENCE aggregations_id_seq TO admin;
GRANT ALL PRIVILEGES ON SEQUENCE anomalies_id_seq TO admin;

-- Display table info
\dt