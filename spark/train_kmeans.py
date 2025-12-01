# spark/train_kmeans.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
import json
import os
import sys

INPUT = "/opt/spark/work-dir/training_energy.csv"
MODEL_DIR = "/opt/spark/work-dir/models/kmeans_energy"
THRESH_FILE = "/opt/spark/work-dir/models/kmeans_threshold.json"

def main():
    spark = SparkSession.builder.appName("TrainKMeansEnergy").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.csv(INPUT, header=True, inferSchema=True)
    # keep only numeric features for training
    df = df.select("electricity", "water").na.drop()

    assembler = VectorAssembler(inputCols=["electricity", "water"], outputCol="features_raw")
    df_vec = assembler.transform(df)

    # scale features (recommended)
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
    scaler_model = scaler.fit(df_vec)
    df_scaled = scaler_model.transform(df_vec)

    # train KMeans
    k = 3
    kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=k, seed=42)
    model = kmeans.fit(df_scaled)

    # compute distance of each point to its cluster center
    centers = model.clusterCenters()

    from pyspark.ml.linalg import Vectors
    from pyspark.sql.functions import udf
    from pyspark.sql.types import DoubleType

    def dist_to_center_udf_factory(centers):
        def dist_to_center(vec, idx):
            # vec is DenseVector; idx is cluster id
            c = centers[int(idx)]
            s = 0.0
            for a, b in zip(vec, c):
                s += (a - b) ** 2
            return float(s ** 0.5)
        return udf(dist_to_center, DoubleType())

    # add cluster assignment and distance
    df_with_cluster = model.transform(df_scaled)
    dist_udf = dist_to_center_udf_factory(centers)
    df_with_dist = df_with_cluster.withColumn("dist", dist_udf(col("features"), col("cluster")))

    # compute threshold: e.g., mean + 3*std of distances
    stats = df_with_dist.selectExpr("avg(dist) as mean_dist", "stddev(dist) as std_dist").collect()[0]
    mean_dist = stats["mean_dist"]
    std_dist = stats["std_dist"] if stats["std_dist"] is not None else 0.0
    threshold = float(mean_dist + 3.0 * std_dist)

    # Save model + scaler in MODEL_DIR
    if os.path.exists(MODEL_DIR):
        import shutil
        shutil.rmtree(MODEL_DIR)
    model.save(MODEL_DIR + "/kmeans_model")
    scaler_model.save(MODEL_DIR + "/scaler_model")

    # Save threshold
    os.makedirs(os.path.dirname(THRESH_FILE), exist_ok=True)
    with open(THRESH_FILE, "w") as f:
        json.dump({"threshold": threshold, "k": k}, f)

    print("Model saved to:", MODEL_DIR)
    print("Threshold saved to:", THRESH_FILE)
    print("Threshold value:", threshold)
    spark.stop()

if __name__ == "__main__":
    main()
