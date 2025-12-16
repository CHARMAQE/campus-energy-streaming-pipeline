# spark/train_random_forest.py
"""
Train Random Forest model for anomaly detection.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
import json
import os
import shutil

INPUT = "/opt/spark/work-dir/training_energy.csv"
MODEL_DIR = "/opt/spark/work-dir/models/random_forest_energy"

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("TrainRandomForest") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("=" * 60)
    print("TRAINING RANDOM FOREST ANOMALY DETECTOR")
    print("=" * 60)
    
    # Load training data
    print(f"\nLoading data from {INPUT}...")
    df = spark.read.csv(INPUT, header=True, inferSchema=True)
    total = df.count()
    print(f"✅ Loaded {total:,} records")
    
    # Convert status to binary label
    df = df.withColumn("label", when(col("status") == "anomaly", 1.0).otherwise(0.0))
    
    # Check distribution
    print("\nClass distribution:")
    for row in df.groupBy("label").count().collect():
        label_name = "Anomaly" if row['label'] == 1.0 else "Normal"
        pct = row['count'] / total * 100
        print(f"  {label_name}: {row['count']:,} ({pct:.1f}%)")
    
    # Select features
    feature_cols = ["electricity", "water"]
    df_clean = df.select(*feature_cols, "label").na.drop()
    
    # Create pipeline
    print("\nBuilding ML pipeline...")
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features")
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label",
        numTrees=100,
        maxDepth=10,
        seed=42
    )
    pipeline = Pipeline(stages=[assembler, scaler, rf])
    
    # Train/test split
    print("\nSplitting data (80% train, 20% test)...")
    train_df, test_df = df_clean.randomSplit([0.8, 0.2], seed=42)
    train_count = train_df.count()
    test_count = test_df.count()
    print(f"  Train: {train_count:,}")
    print(f"  Test: {test_count:,}")
    
    # Train model
    print("\nTraining model...")
    model = pipeline.fit(train_df)
    print("✅ Training complete!")
    
    # Evaluate
    print("\nEvaluating on test set...")
    predictions = model.transform(test_df)
    
    evaluator_acc = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")
    evaluator_auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    
    accuracy = evaluator_acc.evaluate(predictions)
    f1 = evaluator_f1.evaluate(predictions)
    auc = evaluator_auc.evaluate(predictions)
    
    # Confusion matrix
    tp = predictions.filter((col("label") == 1.0) & (col("prediction") == 1.0)).count()
    fp = predictions.filter((col("label") == 0.0) & (col("prediction") == 1.0)).count()
    tn = predictions.filter((col("label") == 0.0) & (col("prediction") == 0.0)).count()
    fn = predictions.filter((col("label") == 1.0) & (col("prediction") == 0.0)).count()
    
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Accuracy:  {accuracy:.4f} ({accuracy*100:.1f}%)")
    print(f"F1-Score:  {f1:.4f}")
    print(f"AUC-ROC:   {auc:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall:    {recall:.4f}")
    print("\nConfusion Matrix:")
    print(f"  True Positives:  {tp}")
    print(f"  False Positives: {fp}")
    print(f"  True Negatives:  {tn}")
    print(f"  False Negatives: {fn}")
    print("=" * 60)
    
    # Feature importance
    rf_model = model.stages[-1]
    importance = rf_model.featureImportances.toArray()
    print("\nFeature Importance:")
    for feat, imp in zip(feature_cols, importance):
        print(f"  {feat}: {imp:.4f}")
    
    # Save model
    print(f"\nSaving model to {MODEL_DIR}...")
    if os.path.exists(MODEL_DIR):
        shutil.rmtree(MODEL_DIR)
    model.save(MODEL_DIR)
    
    # Save metadata
    metadata = {
        "model_type": "RandomForestClassifier",
        "accuracy": float(accuracy),
        "f1_score": float(f1),
        "auc_roc": float(auc),
        "precision": float(precision),
        "recall": float(recall)
    }
    
    with open(f"{MODEL_DIR}/metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)
    
    print("✅ Model saved successfully!")
    print("\n" + "=" * 60)
    
    spark.stop()

if __name__ == "__main__":
    main()