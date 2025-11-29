from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, max as spark_max, min as spark_min
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# -------------------------------
# 1. Spark Session
# -------------------------------
spark = SparkSession.builder \
    .appName("RealTimeEnergyStreaming") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------
# 2. Kafka Schema
# -------------------------------
schema = StructType([
    StructField("building", StringType(), True),
    StructField("floor", IntegerType(), True),
    StructField("electricity", DoubleType(), True),
    StructField("water", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# -------------------------------
# 3. Read Kafka Stream
# -------------------------------
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "university_consumption") \
    .option("startingOffsets", "latest") \
    .load()

df = df_raw.selectExpr("CAST(value AS STRING)")

parsed = df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# -------------------------------
# 4. Basic Aggregations (per building)
# -------------------------------
agg_building = parsed.groupBy(
    window(col("timestamp"), "30 seconds"),
    col("building")
).agg(
    avg("electricity").alias("avg_electricity"),
    avg("water").alias("avg_water"),
    spark_max("electricity").alias("max_electricity"),
    spark_max("water").alias("max_water"),
)

# -------------------------------
# 5. Simple anomaly detection
# -------------------------------
anomalies = parsed.filter(
    (col("electricity") > 180) | (col("water") > 350)
)

# -------------------------------
# 6. Output to console (for debugging)
# -------------------------------
query1 = agg_building.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

query2 = anomalies.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

query1.awaitTermination()
query2.awaitTermination()
