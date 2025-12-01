# Real-Time Energy Consumption Monitoring System

A distributed big data pipeline for real-time university energy consumption monitoring with ML-based anomaly detection using Kafka, Spark Streaming, and K-Means clustering.

## 🏗️ Architecture

```
Training: CSV → K-Means Model → Saved Models
Streaming: Data Generator → Kafka → Spark → ML Inference → Anomaly Detection
```

## 📁 Project Structure

```
real_time_energy_project/
├── data_generation/
│   ├── data_generator_training.py    # Generate historical training data
│   └── data_generator.py             # Real-time Kafka producer
├── training_energy/
│   └── university_energy.csv         # Historical dataset (10k+ records)
├── ml/
│   └── kmeans.py                     # Train K-Means clustering model
├── models/
│   ├── kmeans_energy/
│   │   ├── kmeans_model/            # Trained K-Means (k=3)
│   │   └── scaler_model/            # StandardScaler
│   └── kmeans_threshold.json        # Anomaly threshold (95th percentile)
├── spark/
│   ├── Dockerfile.spark             # Custom Spark image + dependencies
│   └── streaming_with_ml.py         # Real-time streaming + ML inference
├── checkpoints/                     # Spark streaming state (fault tolerance)
└── docker-compose.yml               # Kafka + Spark cluster
```

## 🔄 Complete Workflow

### 1. Generate Training Data
```bash
python data_generation/data_generator_training.py
```
**Output:** `training_energy/university_energy.csv` (3 buildings, 5 floors, electricity + water)

### 2. Train ML Model
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master local[*] /opt/spark/work-dir/ml/kmeans.py
```
**Output:** K-Means model + scaler + threshold in `models/`

### 3. Start Real-Time Pipeline
```bash
# Start infrastructure
docker-compose up -d

# Start data producer
python data_generation/data_generator.py

# Start streaming analytics
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark/work-dir/streaming_with_ml.py
```

## 🎯 Key Components

### **streaming_with_ml.py** - Real-Time Pipeline
1. Load pre-trained models (K-Means + Scaler)
2. Consume Kafka stream (`university_consumption`)
3. Transform & scale features
4. Predict cluster + calculate distance to center
5. Flag anomalies (distance > threshold)
6. Compute 30-second windowed aggregations
7. Output to console (aggregations + anomalies)

### **Checkpoints** - Fault Tolerance
- Stores Kafka offsets + streaming state
- Enables exactly-once processing
- Recovers from failures without data loss

### **Dockerfile.spark** - Custom Image
- Spark 3.5.1 + Kafka connector
- Python ML libraries (scikit-learn, pandas)
- Mounted volumes for code & models

## 📊 Example Output

**Aggregations (30-sec windows):**
```
Building A: avg_elec=171.4 kWh, avg_water=328.1 L
Building B: avg_elec=174.2 kWh, avg_water=336.5 L
```

**Anomalies (when detected):**
```
Building A, Floor 3: electricity=450 kWh, distance=5.23 → ANOMALY
```

## 🛠️ Technologies

- **Apache Kafka**: Message streaming
- **Apache Spark**: Distributed processing + ML
- **K-Means Clustering**: Unsupervised anomaly detection
- **Docker**: Containerization

## 📝 Quick Start

```bash
# 1. Generate training data
python data_generation/data_generator_training.py

# 2. Train model
docker-compose up -d
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master local[*] /opt/spark/work-dir/ml/kmeans.py

# 3. Start streaming
python data_generation/data_generator.py &
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /opt/spark/work-dir/streaming_with_ml.py
```

## 🎓 Project By
**CHARMAQE Hamza** - Big Data Project 2025