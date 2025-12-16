"""
Real-time anomaly detection on Kafka stream.
Optimized for low-memory environments (8GB RAM).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, max as spark_max, udf, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml import PipelineModel
import json

MODEL_DIR = "/opt/spark/work-dir/models/random_forest_energy"

# Load metadata
with open(f"{MODEL_DIR}/metadata.json", "r") as f:
    metadata = json.load(f)

print("=" * 60)
print("REAL-TIME ANOMALY DETECTION")
print("=" * 60)
print(f"Model accuracy: {metadata['accuracy']*100:.1f}%")
print("=" * 60)

# Create Spark session with minimum required memory
spark = SparkSession.builder \
    .appName("RealTimeAnomalyDetection") \
    .config("spark.executor.memory", "512m") \
    .config("spark.driver.memory", "512m") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.default.parallelism", "4") \
    .config("spark.sql.streaming.minBatchesToRetain", "2") \
    .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Load model
print("\nLoading trained model...")
model = PipelineModel.load(MODEL_DIR)
print("✅ Model loaded")

# Schema
schema = StructType([
    StructField("building", StringType(), True),
    StructField("floor", IntegerType(), True),
    StructField("electricity", DoubleType(), True),
    StructField("water", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Connect to Kafka
print("\nConnecting to Kafka...")
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "university_consumption") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", "100") \
    .load()

# Parse JSON
df = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", current_timestamp())

print("✅ Connected to Kafka")

# Apply model
print("\nApplying model to stream...")
predictions = model.transform(df)

# Extract anomaly probability
get_prob = udf(lambda p: float(p[1]) if p else 0.0, DoubleType())
predictions = predictions \
    .withColumn("is_anomaly", (col("prediction") == 1.0)) \
    .withColumn("anomaly_probability", get_prob(col("probability")))

# Aggregations with coalesce to reduce partitions
agg_building = predictions.groupBy(
    window(col("timestamp"), "30 seconds"),
    col("building")
).agg(
    avg("electricity").alias("avg_electricity"),
    avg("water").alias("avg_water"),
    spark_max("electricity").alias("max_elec"),
    avg("anomaly_probability").alias("avg_anomaly_prob")
).select(
    col("building"),
    col("avg_electricity"),
    col("avg_water"),
    col("max_elec"),
    col("avg_anomaly_prob"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end")
).coalesce(1)

# Filter anomalies
anomalies = predictions.filter(col("is_anomaly") == True) \
    .select("building", "floor", "electricity", "water", "anomaly_probability", "timestamp") \
    .coalesce(1)

# PostgreSQL config
postgres_url = "jdbc:postgresql://postgres:5432/energy_monitoring"
postgres_props = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

# Write functions
def write_aggregations(batch_df, batch_id):
    if not batch_df.isEmpty():
        try:
            batch_df.write.jdbc(postgres_url, "aggregations", "append", postgres_props)
            count = batch_df.count()
            print(f"✅ Batch {batch_id}: Wrote {count} aggregations")
        except Exception as e:
            print(f"❌ Error writing aggregations: {e}")

def write_anomalies(batch_df, batch_id):
    if not batch_df.isEmpty():
        try:
            batch_df.write.jdbc(postgres_url, "anomalies", "append", postgres_props)
            count = batch_df.count()
            print(f"🚨 Batch {batch_id}: {count} ANOMALIES detected!")
        except Exception as e:
            print(f"❌ Error writing anomalies: {e}")

# Start queries
print("\nStarting streaming queries...")
print("✅ Streaming queries started")
print("Press Ctrl+C to stop\n")

# Longer trigger interval to reduce overhead
q1 = agg_building.writeStream \
    .foreachBatch(write_aggregations) \
    .outputMode("update") \
    .trigger(processingTime='1 minute') \
    .option("checkpointLocation", "/opt/spark/work-dir/checkpoints/agg") \
    .start()

q2 = anomalies.writeStream \
    .foreachBatch(write_anomalies) \
    .outputMode("append") \
    .trigger(processingTime='1 minute') \
    .option("checkpointLocation", "/opt/spark/work-dir/checkpoints/anomalies") \
    .start()

try:
    q1.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping...")
    q1.stop()
    q2.stop()
    spark.stop()