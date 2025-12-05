# ⚡ Real-Time Energy Consumption Monitoring System

A distributed **big data streaming pipeline** for university energy monitoring with **ML-based anomaly detection** using Apache Kafka, Spark Streaming, and K-Means clustering.

[![Kafka](https://img.shields.io/badge/Kafka-3.4.1-black?logo=apachekafka)](https://kafka.apache.org/)
[![Spark](https://img.shields.io/badge/Spark-3.5.1-orange?logo=apachespark)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.9+-blue?logo=python)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)](https://www.docker.com/)

---

## 🏗️ Architecture

```
📊 Data Generator (15 records/sec)
    ↓
🔄 Apache Kafka (university_consumption topic)
    ↓
⚡ Spark Streaming (1-min batches, 30-sec windows)
    ├─ Aggregations (avg/min/max electricity & water)
    └─ ML Anomaly Detection (K-Means + Distance Threshold)
    ↓
💾 Dual Storage:
    ├─ MongoDB (aggregations + anomalies)
    └─ PostgreSQL (aggregations + anomalies)
    ↓
📈 Grafana Dashboards (Real-time visualization)
```

---

## 🎯 Key Features

- ✅ **Real-time streaming** with Apache Kafka 
- ✅ **Distributed processing** with Spark Structured Streaming
- ✅ **ML anomaly detection** using K-Means clustering 
- ✅ **Dual persistence** (MongoDB for flexibility + PostgreSQL for analytics)
- ✅ **Docker Compose** orchestration (one-command deployment)

---

## 📁 Project Structure

```
real_time_energy_project/
├── producer/
│   ├── data_generator.py              # Kafka producer
│   └── data_generator_training.py     # Generate 200k training records
├── ml/
│   └── kmeans.py                      # Train K-Means (k=3) + StandardScaler
├── models/
│   ├── kmeans_energy/
│   │   ├── kmeans_model/              # Trained K-Means model
│   │   └── scaler_model/              # StandardScaler
│   └── kmeans_threshold.json          # Anomaly threshold (1.86)
├── spark/
│   ├── streaming_with_ml.py           # Real-time streaming + ML inference
│   └── Dockerfile.spark               # Custom Spark image
├── docker-compose.yml                 # Full stack (Kafka, Spark, MongoDB, PostgreSQL)
├── postgres
│     └── init.sql                         # PostgreSQL schema initialization
└── README.md
```

---

## 🚀 Quick Start (3 Steps)

### **Prerequisites**
```bash
# Required: Docker, Docker Compose, Python 3.9+
docker --version
python3 --version
```

### **1️⃣ Start Infrastructure**
```bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/real_time_energy_project.git
cd real_time_energy_project

# Start all services (Kafka, Spark, MongoDB, PostgreSQL, Grafana)
docker-compose up -d

# Create Kafka topic
docker exec -it kafka kafka-topics \
  --create \
  --bootstrap-server localhost:9092 \
  --topic university_consumption \
  --partitions 3 \
  --replication-factor 1
```

### **2️⃣ Start Data Producer**
```bash
# Generate real-time data (15 records/sec)
python producer/data_generator.py --bootstrap localhost:29092 --interval 1.0
```

### **3️⃣ Start Spark Streaming**
```bash
# Launch Spark job with ML inference
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 512m \
  --executor-memory 512m \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,org.postgresql:postgresql:42.7.1 \
  /opt/spark/work-dir/streaming_with_ml.py
```

**✅ System is now running!** Check outputs:
- **Console**: Real-time aggregations & anomaly alerts
- **MongoDB**: `docker exec -it mongodb mongosh -u admin -p **********`
- **PostgreSQL**: `localhost:5432` (user: `admin`, password: `**********`)
- **Grafana**: http://localhost:3000 (admin/admin)

---

## 📊 Example Output

### **Aggregations (30-sec windows)**
```
Building A: avg_elec=171.4 kWh, avg_water=328.1 L, max_elec=220 kWh
Building B: avg_elec=174.2 kWh, avg_water=336.5 L, max_elec=230 kWh
Building C: avg_elec=165.8 kWh, avg_water=315.2 L, max_elec=210 kWh
```

### **Anomalies (when detected)**
```
🚨 ANOMALY DETECTED:
   Building: Building A, Floor 4
   Electricity: 271 kWh (⚠️ 180% of average)
   Water: 456 L
   Distance from cluster: 1.87 (threshold: 1.86)
   Timestamp: 2025-12-04 16:02:00
```

---

## 🧠 Machine Learning: K-Means Anomaly Detection

### **How It Works**

1. **Training Phase** (offline):
   ```bash
   # Generate 200k training records
   python producer/data_generator_training.py --rows 200000
   
   # Train K-Means (k=3) + StandardScaler
   docker exec -it spark-master /opt/spark/bin/spark-submit \
     --master local[*] /opt/spark/work-dir/ml/kmeans.py
   ```
   - **Output**: 3 clusters (Low/Medium/High consumption)
   - **Threshold**: 1.86 (95th percentile of distances)

2. **Inference Phase** (real-time):
   - Each record → **scaled features** (electricity, water)
   - Assigned to **nearest cluster**
   - **Distance** calculated: `√[(elec - center_elec)² + (water - center_water)²]`
   - **Anomaly flagged** if `distance > 1.86`

### **Distance Metric Explained**

| Distance | Interpretation | Action |
|----------|----------------|--------|
| **0.0 - 1.0** | 🟢 Very normal | No action |
| **1.0 - 1.86** | 🟡 Normal variation | No action |
| **1.87 - 2.5** | 🟠 Minor anomaly | Monitor |
| **2.5 - 3.5** | 🔴 Anomaly | Investigate |
| **> 3.5** | 🔴🔴 Critical | Immediate action |

**Example**: 
- Normal consumption: `(150 kWh, 300 L)` → cluster center
- Anomalous consumption: `(248 kWh, 504 L)` → distance `1.88` → **ANOMALY**

---

## 🛠️ Technologies

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Streaming** | Apache Kafka | 3.4.1 | Message broker (3 partitions) |
| **Processing** | Apache Spark | 3.5.1 | Distributed streaming + ML |
| **ML** | K-Means Clustering | Spark MLlib | Unsupervised anomaly detection |
| **Storage** | MongoDB | 7.0 | Flexible JSON storage |
| **Storage** | PostgreSQL | 16 | Relational analytics |
| **Visualization** | Grafana | 10.2 | Real-time dashboards |
| **Orchestration** | Docker Compose | - | Container management |

---

## 📈 Grafana Dashboard Setup

### **1. Add PostgreSQL Data Source**
```
Configuration → Data Sources → PostgreSQL
Host: postgres:5432
Database: energy_monitoring
User: admin / Password: **********
```

### **2. Create Panels**

**Panel 1: Real-Time Electricity Trend**
```sql
SELECT
  window_start as time,
  building,
  avg_electricity as value
FROM aggregations
WHERE $__timeFilter(window_start)
ORDER BY window_start
```

**Panel 2: Anomaly Alerts**
```sql
SELECT
  detected_at as time,
  building,
  floor,
  electricity,
  distance
FROM anomalies
WHERE $__timeFilter(detected_at)
ORDER BY detected_at DESC
LIMIT 10
```

---


### **Access UIs**
- **Spark Master**: http://localhost:8080
- **Spark Application**: http://localhost:4040 (while streaming)
- **Grafana**: http://localhost:3000

---

## 🧪 Testing

### **Generate High-Speed Data**
```bash
# 10x faster (150 records/sec)
python producer/data_generator.py --bootstrap localhost:29092 --interval 0.1
```

### **Verify Anomaly Detection**
```sql
-- In PostgreSQL (pgAdmin or psql)
SELECT 
    building,
    COUNT(*) as anomaly_count,
    AVG(distance) as avg_distance
FROM anomalies
GROUP BY building
ORDER BY anomaly_count DESC;
```

---

## 🛑 Cleanup

```bash
# Stop all services
docker-compose down

# Remove volumes (data + checkpoints)
docker-compose down -v
```

---

## 📝 Project Highlights

- **Scalable**: Kafka partitions + Spark workers scale horizontally
- **Fault-tolerant**: Checkpointing ensures exactly-once processing
- **Production-ready**: Docker Compose, health checks, resource limits
- **ML-driven**: Unsupervised learning adapts to consumption patterns
- **Real-time**: <1 minute latency from data generation to visualization

---

## 🎓 Author

**CHARMAQE Hamza**  
Big Data & Machine Learning Project
---

## 🤝 Acknowledgments

- Apache Spark & Kafka communities
- Grafana Labs for visualization tools
- Docker for containerization platform