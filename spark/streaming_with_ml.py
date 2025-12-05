# spark/streaming_with_ml.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, max as spark_max, min as spark_min,
    to_timestamp, udf, current_timestamp, date_format
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

# 1 Spark session with MongoDB connector
print("Creating Spark session...")
spark = SparkSession.builder \
    .appName("RealTimeEnergyStreamingWithML") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.streaming.checkpointLocation", "/opt/spark/work-dir/checkpoints") \
    .config("spark.mongodb.write.connection.uri", "mongodb://admin:admin123@mongodb:27017/") \
    .config("spark.mongodb.write.database", "energy_monitoring") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,org.postgresql:postgresql:42.7.1") \
    .config("spark.driver.extraClassPath", "/home/spark/.ivy2/jars/*") \
    .config("spark.executor.extraClassPath", "/home/spark/.ivy2/jars/*") \
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

# 3. Read stream from Kafka with proper options
print("Connecting to Kafka...")
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "university_consumption")
    .option("startingOffsets", "earliest")  # Changed to read all available data
    .option("failOnDataLoss", "false")      # Don't fail on data loss
    .option("maxOffsetsPerTrigger", "1000") # Limit records per batch
    .load()
)

print("Kafka connection established!")

# Parse JSON value
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# 4. Load models & threshold (with cleaner error handling)
print("Loading ML models...")
try:
    # Load KMeansModel
    km = KMeansModel.load(KMEANS_PATH)
    print(f"✓ KMeans model loaded from {KMEANS_PATH}")
    
    # Load scaler directly as StandardScalerModel (avoids warning)
    from pyspark.ml.feature import StandardScalerModel
    scaler_model = StandardScalerModel.load(SCALER_PATH)
    print(f"✓ Scaler (StandardScaler) loaded from {SCALER_PATH}")
    
    # Load threshold
    with open(THRESHOLD_PATH, "r") as f:
        th = json.load(f)
    THRESHOLD = float(th.get("threshold", 3.0))
    print(f"✓ Threshold loaded: {THRESHOLD}")
    
    # Model info
    centers = km.clusterCenters()
    print(f"✓ K-Means model has {len(centers)} clusters")
    prediction_col = km.getPredictionCol()
    print(f"✓ Prediction column: {prediction_col}")
    
except Exception as e:
    print(f"ERROR loading models: {e}")
    raise

# 5 prepare features
assembler = VectorAssembler(inputCols=["electricity", "water"], outputCol="features_raw")
clean = assembler.transform(df_parsed)  # Fixed: use df_parsed instead of parsed

# Apply scaler
clean = scaler_model.transform(clean)

# 6 predict cluster and compute distance to center
def compute_dist(features, cluster):
    if features is None or cluster is None:
        return None
    c = centers[int(cluster)]
    s = 0.0
    for a, b in zip(features, c):
        s += (float(a) - float(b)) ** 2
    return float(s ** 0.5)

dist_udf = udf(compute_dist, DoubleType())

pred = km.transform(clean)
pred = pred.withColumn("dist", dist_udf(col("features"), col(prediction_col)))
pred = pred.withColumn("is_anomaly", (col("dist") > THRESHOLD).cast("boolean"))

# 7 Aggregations + anomalies output
agg_building = pred.groupBy(window(col("timestamp"), "30 seconds"), col("building")).agg(
    avg("electricity").alias("avg_electricity"),
    avg("water").alias("avg_water"),
    spark_max("electricity").alias("max_elec"),
    spark_min("electricity").alias("min_elec")
).withColumn("window_start", col("window.start")) \
 .withColumn("window_end", col("window.end")) \
 .withColumn("processed_at", current_timestamp()) \
 .drop("window")

anomalies = pred.filter(col("is_anomaly") == True) \
    .withColumn("detected_at", current_timestamp()) \
    .select("building", "floor", "electricity", "water", "cluster", "dist", "timestamp", "detected_at")

# 8 outputs - Console + MongoDB with improved logging
from pyspark.sql.streaming import StreamingQueryListener

class MongoWriteListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"🚀 Query {event.name} started (ID: {event.id})")
    
    def onQueryProgress(self, event):
        progress = event.progress
        if progress.numInputRows > 0:
            print(f"📊 Query: {progress.name} | Batch: {progress.batchId} | "
                  f"Input Rows: {progress.numInputRows} | "
                  f"Processed Rows: {progress.processedRowsPerSecond:.0f} rows/sec")
    
    def onQueryTerminated(self, event):
        print(f"🛑 Query terminated: {event.id}")

# Register listener
spark.streams.addListener(MongoWriteListener())

print("=" * 60)
print("Starting streaming queries (1-minute batch intervals)...")
print("=" * 60)

# NEW: Write aggregations to MongoDB with timestamp conversion
def write_to_mongo_agg(batch_df, batch_id):
    """Write aggregation batch to MongoDB"""
    try:
        if not batch_df.isEmpty():
            count = batch_df.count()
            
            # Convert timestamp columns to string for MongoDB compatibility
            batch_df_str = batch_df \
                .withColumn("window_start", col("window_start").cast("string")) \
                .withColumn("window_end", col("window_end").cast("string")) \
                .withColumn("processed_at", col("processed_at").cast("string"))
            
            batch_df_str.write \
                .format("mongodb") \
                .mode("append") \
                .option("collection", "aggregations") \
                .save()
            
            print(f"\n{'='*60}")
            print(f"✅ AGGREGATIONS BATCH {batch_id}")
            print(f"   Records written: {count}")
            print(f"{'='*60}\n")
        else:
            print(f"⚠️  Batch {batch_id}: No aggregation data to write")
    except Exception as e:
        print(f"❌ Batch {batch_id} ERROR writing aggregations: {str(e)}")

# NEW: Write anomalies to MongoDB with timestamp conversion
def write_to_mongo_anomalies(batch_df, batch_id):
    """Write anomaly batch to MongoDB"""
    try:
        if not batch_df.isEmpty():
            count = batch_df.count()
            
            # Convert timestamp columns to string
            batch_df_str = batch_df \
                .withColumn("timestamp", col("timestamp").cast("string")) \
                .withColumn("detected_at", col("detected_at").cast("string"))
            
            batch_df_str.write \
                .format("mongodb") \
                .mode("append") \
                .option("collection", "anomalies") \
                .save()
            
            print(f"\n{'='*60}")
            print(f"🚨 ANOMALIES BATCH {batch_id}")
            print(f"   Anomalies detected: {count}")
            print(f"{'='*60}\n")
        else:
            print(f"✓ Batch {batch_id}: No anomalies detected")
    except Exception as e:
        print(f"❌ Batch {batch_id} ERROR writing anomalies: {str(e)}")

# PostgreSQL connection properties
postgres_url = "jdbc:postgresql://postgres:5432/energy_monitoring"
postgres_properties = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

# Write to PostgreSQL for Grafana
def write_to_postgres_agg(batch_df, batch_id):
    """Write aggregation batch to PostgreSQL"""
    try:
        if not batch_df.isEmpty():
            count = batch_df.count()
            
            # Select and rename columns for PostgreSQL
            batch_df_pg = batch_df.select(
                col("building"),
                col("avg_electricity"),
                col("avg_water"),
                col("max_elec"),
                col("min_elec"),
                col("window_start").cast("timestamp"),
                col("window_end").cast("timestamp"),
                col("processed_at").cast("timestamp")
            )
            
            # Write to PostgreSQL
            batch_df_pg.write \
                .jdbc(url=postgres_url, 
                      table="aggregations", 
                      mode="append", 
                      properties=postgres_properties)
            
            print(f"\n{'='*60}")
            print(f"✅ POSTGRES AGGREGATIONS BATCH {batch_id}")
            print(f"   Records written: {count}")
            print(f"{'='*60}\n")
        else:
            print(f"⚠️  Batch {batch_id}: No aggregation data to write")
    except Exception as e:
        print(f"❌ Batch {batch_id} ERROR writing to PostgreSQL: {str(e)}")

def write_to_postgres_anomalies(batch_df, batch_id):
    """Write anomaly batch to PostgreSQL"""
    try:
        if not batch_df.isEmpty():
            count = batch_df.count()
            
            # Select columns for PostgreSQL - FIX: 'dist' not 'distance'
            batch_df_pg = batch_df.select(
                col("building"),
                col("floor"),
                col("electricity"),
                col("water"),
                col("dist").alias("distance"),  # Fixed: use 'dist' from DataFrame
                col("timestamp").cast("timestamp"),
                col("detected_at").cast("timestamp")
            )
            
            # Write to PostgreSQL
            batch_df_pg.write \
                .jdbc(url=postgres_url, 
                      table="anomalies", 
                      mode="append", 
                      properties=postgres_properties)
            
            print(f"\n{'='*60}")
            print(f"🚨 POSTGRES ANOMALIES BATCH {batch_id}")
            print(f"   Anomalies detected: {count}")
            print(f"{'='*60}\n")
        else:
            print(f"✓ Batch {batch_id}: No anomalies detected")
    except Exception as e:
        print(f"❌ Batch {batch_id} ERROR writing anomalies to PostgreSQL: {str(e)}")

# Query 1: Aggregations to MongoDB (1-minute trigger)
q1 = agg_building.writeStream \
    .queryName("aggregations_to_mongo") \
    .outputMode("update") \
    .foreachBatch(write_to_mongo_agg) \
    .trigger(processingTime='1 minute') \
    .option("checkpointLocation", "/opt/spark/work-dir/checkpoints/agg") \
    .start()

# Query 2: Anomalies to MongoDB (1-minute trigger)
q2 = anomalies.writeStream \
    .queryName("anomalies_to_mongo") \
    .outputMode("append") \
    .foreachBatch(write_to_mongo_anomalies) \
    .trigger(processingTime='1 minute') \
    .option("checkpointLocation", "/opt/spark/work-dir/checkpoints/anomalies") \
    .start()

# Query 3: Console output (1-minute trigger)
q3 = agg_building.writeStream \
    .queryName("aggregations_console") \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='1 minute') \
    .option("checkpointLocation", "/opt/spark/work-dir/checkpoints/agg_console") \
    .start()

# Update queries to write to BOTH MongoDB and PostgreSQL
q1_postgres = agg_building.writeStream \
    .queryName("aggregations_to_postgres") \
    .outputMode("update") \
    .foreachBatch(write_to_postgres_agg) \
    .trigger(processingTime='1 minute') \
    .option("checkpointLocation", "/opt/spark/work-dir/checkpoints/agg_pg") \
    .start()

q2_postgres = anomalies.writeStream \
    .queryName("anomalies_to_postgres") \
    .outputMode("append") \
    .foreachBatch(write_to_postgres_anomalies) \
    .trigger(processingTime='1 minute') \
    .option("checkpointLocation", "/opt/spark/work-dir/checkpoints/anomalies_pg") \
    .start()

print("\n✅ All streaming queries started!")
print(f"   • Batch interval: 1 minute")
print(f"   • Aggregation window: 30 seconds")
print(f"   • CPU-optimized configuration")
print(f"   • Writing to MongoDB collections: aggregations, anomalies")
print(f"   • Writing to PostgreSQL tables: aggregations, anomalies")
print("\nPress Ctrl+C to stop...\n")

try:
    q1.awaitTermination()
except KeyboardInterrupt:
    print("\n🛑 Stopping all queries...")
    q1.stop()
    q2.stop()
    q3.stop()
    q1_postgres.stop()
    q2_postgres.stop()
    print("✅ All queries stopped successfully!")
