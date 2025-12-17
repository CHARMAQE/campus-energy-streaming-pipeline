-- ✅ SIMPLIFIED Database Schema

-- Aggregations table (30-second windows)
CREATE TABLE IF NOT EXISTS aggregations (
    id SERIAL PRIMARY KEY,
    building VARCHAR(50),
    avg_electricity DOUBLE PRECISION,
    avg_water DOUBLE PRECISION,
    max_electricity DOUBLE PRECISION,
    avg_anomaly_prob DOUBLE PRECISION,
    window_start TIMESTAMP,
    window_end TIMESTAMP
);

CREATE INDEX idx_agg_time ON aggregations(window_start);
CREATE INDEX idx_agg_building ON aggregations(building);

-- ✅ SIMPLIFIED: Anomalies table (only essential fields)
CREATE TABLE IF NOT EXISTS anomalies (
    id SERIAL PRIMARY KEY,
    building VARCHAR(50),
    floor INTEGER,
    electricity DOUBLE PRECISION,
    water DOUBLE PRECISION,
    anomaly_probability DOUBLE PRECISION,
    timestamp TIMESTAMP
);

CREATE INDEX idx_anomaly_time ON anomalies(timestamp);
CREATE INDEX idx_anomaly_building ON anomalies(building);
CREATE INDEX idx_anomaly_probability ON anomalies(anomaly_probability DESC);

-- Grant permissions
GRANT ALL PRIVILEGES ON TABLE aggregations TO admin;
GRANT ALL PRIVILEGES ON TABLE anomalies TO admin;
GRANT ALL PRIVILEGES ON SEQUENCE aggregations_id_seq TO admin;
GRANT ALL PRIVILEGES ON SEQUENCE anomalies_id_seq TO admin;