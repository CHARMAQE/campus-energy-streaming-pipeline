"""
Real-time anomaly detection using ONLY Machine Learning.
✅ SIMPLIFIED: No complex statistics, clear messages
✅ FIXED: Column names match database schema
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, max as spark_max, min as spark_min, udf, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.ml import PipelineModel
import json

MODEL_DIR = "/opt/spark/work-dir/models/random_forest_energy"

# Load model metadata
with open(f"{MODEL_DIR}/metadata.json", "r") as f:
    metadata = json.load(f)

print("=" * 60)
print("🔍 REAL-TIME ANOMALY DETECTION")
print("=" * 60)
print(f"✅ Model Accuracy: {metadata['accuracy']*100:.1f}%")
print(f"✅ Detection Method: Random Forest (Machine Learning)")
print(f"✅ Expected Rate: 1-2 anomalies per day")
print("=" * 60)

# Create Spark session (optimized for 8GB RAM)
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

# Load trained model
print("\n📦 Loading ML model...")
model = PipelineModel.load(MODEL_DIR)
print("✅ Model loaded successfully\n")

# Schema
schema = StructType([
    StructField("building", StringType(), True),
    StructField("floor", IntegerType(), True),
    StructField("electricity", DoubleType(), True),
    StructField("water", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Connect to Kafka
print("🔌 Connecting to Kafka...")
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

print("✅ Connected to Kafka\n")

# Apply ML model
print("🤖 Applying machine learning model...")
predictions = model.transform(df)

# Extract anomaly probability (0-100%)
get_prob = udf(lambda p: float(p[1]) if p else 0.0, DoubleType())

predictions = predictions \
    .withColumn("is_anomaly", (col("prediction") == 1.0)) \
    .withColumn("anomaly_probability", get_prob(col("probability")))

# ✅ FIXED: Match database column names exactly
agg_building = predictions.groupBy(
    window(col("timestamp"), "30 seconds"),
    col("building")
).agg(
    avg("electricity").alias("avg_electricity"),
    avg("water").alias("avg_water"),
    spark_max("electricity").alias("max_elec"),           # ✅ Changed from max_electricity
    spark_min("electricity").alias("min_elec"),           # ✅ Added min_elec
    avg("anomaly_probability").alias("avg_anomaly_prob")
).select(
    col("building"),
    col("avg_electricity"),
    col("avg_water"),
    col("max_elec"),           # ✅ Matches database schema
    col("min_elec"),           # ✅ Matches database schema
    col("avg_anomaly_prob"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end")
).coalesce(1)

# ✅ SIMPLIFIED: Just filter anomalies (ML decides, no manual rules)
anomalies = predictions.filter(col("is_anomaly") == True) \
    .select(
        "building", 
        "floor", 
        "electricity", 
        "water", 
        "anomaly_probability", 
        "timestamp"
    ).coalesce(1)

# PostgreSQL config
postgres_url = "jdbc:postgresql://postgres:5432/energy_monitoring"
postgres_props = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

# ✅ SIMPLIFIED: Clear, readable output
def write_aggregations(batch_df, batch_id):
    if not batch_df.isEmpty():
        try:
            batch_df.write.jdbc(postgres_url, "aggregations", "append", postgres_props)
            print(f"✅ Batch {batch_id}: Saved consumption data")
        except Exception as e:
            print(f"❌ Error saving data: {e}")

def write_anomalies(batch_df, batch_id):
    if not batch_df.isEmpty():
        try:
            anomalies_list = batch_df.collect()
            count = len(anomalies_list)
            
            print(f"\n{'='*60}")
            print(f"🚨 ANOMALY DETECTED! (Batch {batch_id})")
            print(f"{'='*60}")
            
            for row in anomalies_list:
                confidence = row['anomaly_probability'] * 100
                
                # Simple classification based on values
                if row['electricity'] > 250:
                    reason = "Very high electricity (equipment failure?)"
                elif row['water'] > 250:
                    reason = "Very high water consumption (leak?)"
                elif row['electricity'] > 180:
                    reason = "High electricity consumption"
                elif row['water'] > 180:
                    reason = "High water consumption"
                else:
                    reason = "Unusual consumption pattern"
                
                print(f"\n📍 Location: {row['building']}, Floor {row['floor']}")
                print(f"⚡ Electricity: {row['electricity']:.1f} kWh")
                print(f"💧 Water: {row['water']:.1f} L")
                print(f"🎯 ML Confidence: {confidence:.1f}%")
                print(f"📝 Reason: {reason}")
                print(f"🕐 Time: {row['timestamp']}")
            
            print(f"{'='*60}\n")
            
            # Save to database
            batch_df.write.jdbc(postgres_url, "anomalies", "append", postgres_props)
            
        except Exception as e:
            print(f"❌ Error processing anomaly: {e}")

# Start streaming
print("🚀 Starting real-time monitoring...")
print("⏱️  Processing every 1 minute")
print("📊 Expected: 1-2 anomalies per day (very rare!)")
print("Press Ctrl+C to stop\n")

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
    print("\n⏹️  Stopping monitoring...")
    q1.stop()
    q2.stop()
    spark.stop()
    print("✅ Stopped successfully")