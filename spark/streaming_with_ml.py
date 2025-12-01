# spark/streaming_with_ml.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, max as spark_max, min as spark_min,
    to_timestamp, udf
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml.feature import VectorAssembler, StandardScalerModel
from pyspark.ml import PipelineModel
from pyspark.ml.clustering import KMeansModel
import json
import math
import os
import logging

# Verify model paths exist before starting Spark
MODEL_BASE = "/opt/spark/work-dir/models/kmeans_energy"
KMEANS_PATH = MODEL_BASE + "/kmeans_model"
SCALER_PATH = MODEL_BASE + "/scaler_model"
THRESHOLD_PATH = "/opt/spark/work-dir/models/kmeans_threshold.json"

print("=" * 60)
print("Checking model paths...")
print(f"KMeans model: {KMEANS_PATH} - Exists: {os.path.exists(KMEANS_PATH)}")
print(f"Scaler model: {SCALER_PATH} - Exists: {os.path.exists(SCALER_PATH)}")
print(f"Threshold file: {THRESHOLD_PATH} - Exists: {os.path.exists(THRESHOLD_PATH)}")
print("=" * 60)

if not os.path.exists(KMEANS_PATH):
    raise FileNotFoundError(f"KMeans model not found at {KMEANS_PATH}")
if not os.path.exists(SCALER_PATH):
    raise FileNotFoundError(f"Scaler model not found at {SCALER_PATH}")
if not os.path.exists(THRESHOLD_PATH):
    raise FileNotFoundError(f"Threshold file not found at {THRESHOLD_PATH}")

# 1 Spark session
print("Creating Spark session...")
spark = SparkSession.builder \
    .appName("RealTimeEnergyStreamingWithML") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.streaming.checkpointLocation", "/opt/spark/work-dir/checkpoints") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Suppress Kafka AdminClient warnings
logging.getLogger("org.apache.kafka.clients.admin.AdminClientConfig").setLevel(logging.ERROR)
logging.getLogger("org.apache.kafka.clients.consumer.ConsumerConfig").setLevel(logging.ERROR)

print("Spark session created successfully!")

# 2 Schema
schema = StructType([
    StructField("building", StringType(), True),
    StructField("floor", IntegerType(), True),
    StructField("electricity", DoubleType(), True),
    StructField("water", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# 3 Read stream from Kafka
print("Connecting to Kafka...")
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "university_consumption") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

df = df_raw.selectExpr("CAST(value AS STRING)")

parsed = df.select(from_json(col("value"), schema).alias("data")).select("data.*") \
    .withColumn("timestamp", to_timestamp(col("timestamp")))
print("Kafka connection established!")

# 4 load models & threshold
print("Loading ML models...")
try:
    # Load KMeansModel directly (not as PipelineModel)
    km = KMeansModel.load(KMEANS_PATH)
    print(f"✓ KMeans model loaded from {KMEANS_PATH}")
    
    # Load scaler - try as PipelineModel first, then as StandardScalerModel
    try:
        scaler_model = PipelineModel.load(SCALER_PATH)
        print(f"✓ Scaler (Pipeline) loaded from {SCALER_PATH}")
    except:
        scaler_model = StandardScalerModel.load(SCALER_PATH)
        print(f"✓ Scaler (StandardScaler) loaded from {SCALER_PATH}")
    
    with open(THRESHOLD_PATH, "r") as f:
        th = json.load(f)
    THRESHOLD = float(th.get("threshold", 3.0))
    print(f"✓ Threshold loaded: {THRESHOLD}")
    
    centers = km.clusterCenters()
    print(f"✓ K-Means model has {len(centers)} clusters")
    
    # Get the prediction column name from the model
    prediction_col = km.getPredictionCol()
    print(f"✓ Prediction column: {prediction_col}")
except Exception as e:
    print(f"ERROR loading models: {e}")
    raise

# 5 prepare features
assembler = VectorAssembler(inputCols=["electricity", "water"], outputCol="features_raw")
clean = assembler.transform(parsed)

# Apply scaler
clean = scaler_model.transform(clean)

# 6 predict cluster and compute distance to center
# udf to compute distance to center
def compute_dist(features, cluster):
    # features is DenseVector
    if features is None or cluster is None:
        return None
    c = centers[int(cluster)]
    s = 0.0
    for a, b in zip(features, c):
        s += (float(a) - float(b)) ** 2
    return float(s ** 0.5)

dist_udf = udf(compute_dist, DoubleType())

pred = km.transform(clean)
# Use the prediction column name from the model (either "prediction" or "cluster")
pred = pred.withColumn("dist", dist_udf(col("features"), col(prediction_col)))
pred = pred.withColumn("is_anomaly", (col("dist") > THRESHOLD).cast("boolean"))

# 7 Aggregations + anomalies output
agg_building = pred.groupBy(window(col("timestamp"), "30 seconds"), col("building")).agg(
    avg("electricity").alias("avg_electricity"),
    avg("water").alias("avg_water"),
    spark_max("electricity").alias("max_elec"),
    spark_min("electricity").alias("min_elec")
)

anomalies = pred.filter(col("is_anomaly") == True)

# 8 outputs (console for debug)
print("=" * 60)
print("Starting streaming queries...")
print("=" * 60)

q1 = agg_building.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/opt/spark/work-dir/checkpoints/agg") \
    .start()

q2 = anomalies.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/opt/spark/work-dir/checkpoints/anomalies") \
    .start()

print("Streaming queries started! Waiting for data...")
print("Press Ctrl+C to stop...")

q1.awaitTermination()
q2.awaitTermination()
