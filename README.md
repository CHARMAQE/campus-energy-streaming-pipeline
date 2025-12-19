# ⚡ Real-Time Energy Monitoring System

A **big data streaming pipeline** for university energy monitoring with **ML-based anomaly detection**.

[![Kafka](https://img.shields.io/badge/Kafka-3.4.1-black?logo=apachekafka)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/Spark-3.5.1-orange?logo=apachespark)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)](https://www.docker.com/)

---

## 🏗️ Architecture

```
📊 Data Generator (Kafka Producer)
    ↓
🔄 Apache Kafka (3 partitions)
    ↓
⚡ Spark Streaming (Random Forest ML Model)
    ↓
💾 PostgreSQL (Real-time storage)
    ↓
📈 Grafana (Visualization)
```

---

## 🎯 Key Features

- ✅ **Real-time streaming** with Apache Kafka
- ✅ **ML anomaly detection** using Random Forest (98.6% accuracy)
- ✅ **Distributed processing** with Spark Structured Streaming
- ✅ **PostgreSQL** for analytics and storage
- ✅ **Grafana dashboards** for real-time monitoring
- ✅ **Docker Compose** for one-command deployment

---

## 📁 Project Structure

```
real_time_energy_project/
├── producer/
│   ├── data_generator.py              # Real-time Kafka producer
│   ├── data_generator_training.py     # Generate 200k training records
│   └── energy_data_core.py            # Shared data generation logic
├── spark/
│   ├── train_random_forest.py         # Train ML model (offline)
│   ├── streaming_with_rf.py           # Real-time ML inference
│   └── training_energy.csv            # 200k labeled records
├── models/
│   └── random_forest_energy/          # Trained Random Forest model
├── postgres/
│   └── init.sql                       # Database schema
├── docker-compose.yml                 # Infrastructure setup
└── README.md
```

---

## 🚀 Quick Start

### **Prerequisites**
- Docker & Docker Compose installed
- Python 3.9+

### **1️⃣ Start Infrastructure**
```bash
# Start all services
docker-compose up -d

# Create Kafka topic
docker exec -it kafka kafka-topics \
  --create \
  --bootstrap-server localhost:9092 \
  --topic university_consumption \
  --partitions 3 \
  --replication-factor 1
```

### **2️⃣ Train ML Model (One-time)**
```bash
# Generate training data (200k records)
python producer/data_generator_training.py

# Train Random Forest model
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master local[*] \
  /opt/spark/work-dir/train_random_forest.py
```

### **3️⃣ Start Real-Time Pipeline**
```bash
# Terminal 1: Start data producer (15 buildings × 3 floors)
python producer/data_generator.py --bootstrap localhost:9092

# Terminal 2: Start Spark streaming with ML inference
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.1 \
  /opt/spark/work-dir/streaming_with_rf.py
```

### **4️⃣ Access Dashboards**
- **Grafana**: http://localhost:3000 (admin/admin123)
- **Spark UI**: http://localhost:8080
- **PostgreSQL**: localhost:5432 (admin/admin123)

---

## 🧠 Machine Learning Model

### **Random Forest Classifier**
- **Accuracy**: 98.6%
- **Features**: Electricity consumption, Water consumption
- **Training Data**: 200,000 labeled records (3% anomalies)
- **Anomaly Types**: 
  - 🔴 High Consumption (equipment malfunction)
  - 🔴 Very High (critical failure)
  - 💧 Leak (water system issue)

### **Anomaly Detection Process**
```
New Reading → ML Model → Prediction (0.0-1.0) → Anomaly if > 0.5
                      ↓
            Classify Type (high_consumption / very_high / leak)
                      ↓
            Save to PostgreSQL with metadata
```

---

## 📊 Database Schema

### **Table: `aggregations`** (30-second windows)
```sql
CREATE TABLE aggregations (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    building VARCHAR(50),
    avg_electricity DOUBLE PRECISION,
    avg_water DOUBLE PRECISION,
    max_electricity DOUBLE PRECISION,
    max_water DOUBLE PRECISION
);
```

### **Table: `aggregations_floor`** (by building + floor)
```sql
CREATE TABLE aggregations_floor (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    building VARCHAR(50),
    floor INTEGER,
    avg_electricity DOUBLE PRECISION,
    avg_water DOUBLE PRECISION
);
```

### **Table: `anomalies`** (ML-detected)
```sql
CREATE TABLE anomalies (
    timestamp TIMESTAMP,
    building VARCHAR(50),
    floor INTEGER,
    electricity DOUBLE PRECISION,
    water DOUBLE PRECISION,
    anomaly_probability DOUBLE PRECISION,
    anomaly_type VARCHAR(50)  -- 'high_consumption', 'very_high', 'leak'
);
```

---

## 📈 Grafana Dashboard Setup

### **Add PostgreSQL Data Source**
1. Configuration → Data Sources → Add PostgreSQL
2. Settings:
   - **Host**: `postgres:5432`
   - **Database**: `energy_monitoring`
   - **User**: `admin`
   - **Password**: `admin123`

### **Sample Queries**

**Total Electricity (Stat Panel)**
```sql
SELECT ROUND(SUM(avg_electricity)::numeric, 2) as "Total kWh"
FROM aggregations
WHERE window_end >= NOW() - INTERVAL '5 minutes';
```

**Real-Time Consumption (Time Series)**
```sql
SELECT 
  window_end as time,
  building,
  avg_electricity as value
FROM aggregations
WHERE $__timeFilter(window_end)
ORDER BY window_end;
```

**Recent Anomalies (Table)**
```sql
SELECT 
  timestamp,
  building,
  floor,
  ROUND(electricity::numeric, 2) as "Electricity (kWh)",
  ROUND((anomaly_probability * 100)::numeric, 1) as "Confidence %",
  anomaly_type as "Type"
FROM anomalies
WHERE $__timeFilter(timestamp)
ORDER BY timestamp DESC
LIMIT 20;
```

---

## 🛠️ Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Streaming** | Apache Kafka 3.4.1 | Message broker |
| **Processing** | Apache Spark 3.5.1 | Distributed computing |
| **ML** | Random Forest (Spark MLlib) | Anomaly detection |
| **Database** | PostgreSQL 15 | Data storage |
| **Visualization** | Grafana 10.2 | Real-time dashboards |
| **Orchestration** | Docker Compose | Container management |

---

## 📊 Example Output

### **Console (Spark Streaming)**
```
✅ Batch 42: Saved consumption data
🚨 ANOMALY DETECTED!
   Building: Building A, Floor 2
   Electricity: 271.5 kWh (2.7x normal)
   Water: 456.8 L
   Probability: 0.92 (92% confident)
   Type: high_consumption
   Timestamp: 2025-12-20 14:32:15
```

---

## 🧪 Testing & Validation

### **Check Data Pipeline**
```bash
# PostgreSQL: Count records
docker exec -it postgres psql -U admin -d energy_monitoring \
  -c "SELECT COUNT(*) FROM aggregations;"

# PostgreSQL: View anomalies
docker exec -it postgres psql -U admin -d energy_monitoring \
  -c "SELECT * FROM anomalies ORDER BY timestamp DESC LIMIT 5;"
```

### **Generate More Anomalies (Testing)**
```bash
# Speed up data generation (10x faster)
python producer/data_generator.py --bootstrap localhost:9092 --interval 0.2
```

---

## 🛑 Cleanup

```bash
# Stop all services
docker-compose down

# Remove all data (volumes)
docker-compose down -v
```

---

## 📝 Key Metrics

- **Throughput**: 15 records/sec (45 buildings × floors)
- **Latency**: <1 minute (from data generation to Grafana)
- **Anomaly Rate**: ~1-2 per day (0.0002% probability during work hours)
- **Model Accuracy**: 98.6%
- **Data Volume**: ~1.3M records/day

---

## 🎓 Author

**CHARMAQE Hamza**  
Big Data & Machine Learning Project  
December 2025

---

## 📚 References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Grafana Dashboards](https://grafana.com/docs/grafana/latest/)
- [Random Forest Algorithm](https://spark.apache.org/docs/latest/ml-classification-regression.html#random-forest-classifier)

---

**🚀 Ready to monitor energy consumption in real-time!**