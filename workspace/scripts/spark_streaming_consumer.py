#!/usr/bin/env python3
"""
spark_streaming_consumer.py
---------------------------
Spark Structured Streaming consumer for real-time IDS.

Pipeline:
  Kafka (network-traffic topic)
    → parse JSON
    → load saved Spark LR PipelineModel
    → classify each micro-batch
    → write ALL predictions to MongoDB (ids_results.predictions)
    → write ATTACK-ONLY to MongoDB (ids_results.alerts)
    → print summary per micro-batch to console

Run from inside spark-jupyter container:
    python3 /opt/work/scripts/spark_streaming_consumer.py
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    DoubleType, IntegerType, StringType
)
from pyspark.ml import PipelineModel

# ── Config ────────────────────────────────────────────────────────────────────
HDFS_BASE      = "hdfs://namenode:8020"
MODEL_PATH     = f"{HDFS_BASE}/user/spark/ids/models/spark_lr"
RF_IMPORT_DIR  = f"{HDFS_BASE}/user/spark/ids/processed/rf_feature_importance"

KAFKA_SERVERS  = "kafka:9092"
KAFKA_TOPIC    = "network-traffic"
MONGO_URI      = "mongodb://mongodb:27017"
MONGO_DB       = "ids_results"
CHECKPOINT_DIR = "/opt/work/output/streaming_checkpoints"
TOP_K          = 30
LABEL_COL      = "label_idx"

# Correct mapping from label_mapping CSV (verified)
ATTACK_NAMES = {
    0:  "Benign",
    1:  "DDOS attack-HOIC",
    2:  "DDoS attacks-LOIC-HTTP",
    3:  "DoS attacks-Hulk",
    4:  "Bot",
    5:  "FTP-BruteForce",
    6:  "SSH-Bruteforce",
    7:  "Infilteration",
    8:  "DoS attacks-SlowHTTPTest",
    9:  "DoS attacks-GoldenEye",
    10: "DoS attacks-Slowloris",
    11: "DDOS attack-LOIC-UDP",
    12: "Brute Force -Web",
    13: "Brute Force -XSS",
    14: "SQL Injection",
}

# High-priority classes — DDoS, DoS variants
HIGH_PRIORITY = {1, 2, 3, 8, 9, 10, 11}


def build_spark():
    return (
        SparkSession.builder
        .appName("IDS_Streaming_Consumer")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.hadoop.fs.defaultFS", HDFS_BASE)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
    )


def load_feature_cols(spark):
    rf_imp_df = (
        spark.read.option("header", True).csv(RF_IMPORT_DIR)
        .withColumn("importance", F.col("importance").cast("double"))
        .orderBy(F.desc("importance"))
    )
    raw = [r["feature"] for r in rf_imp_df.limit(TOP_K).collect()]
    return [c for c in raw if c != "timestamp_unix"]


def build_schema(feature_cols):
    fields = [StructField(c, DoubleType(), True) for c in feature_cols]
    fields.append(StructField(LABEL_COL,    IntegerType(), True))
    fields.append(StructField("label_name", StringType(),  True))
    return StructType(fields)


def write_to_mongo(df, collection):
    if df.empty:
        return
    import pymongo
    client = pymongo.MongoClient(MONGO_URI)
    col    = client[MONGO_DB][collection]
    col.insert_many(df.to_dict(orient="records"), ordered=False)
    client.close()


def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    n_total   = batch_df.count()
    n_attacks = batch_df.filter(F.col("prediction") != 0).count()
    n_benign  = n_total - n_attacks

    # Map prediction integer → name — no UDF, pure Spark SQL
    label_expr = F.lit("Unknown")
    for idx, name in sorted(ATTACK_NAMES.items(), reverse=True):
        label_expr = F.when(F.col("prediction") == idx, F.lit(name)).otherwise(label_expr)

    # Map ground truth label_idx → name for verification
    truth_expr = F.lit("Unknown")
    for idx, name in sorted(ATTACK_NAMES.items(), reverse=True):
        truth_expr = F.when(F.col(LABEL_COL) == idx, F.lit(name)).otherwise(truth_expr)

    batch_df = (
        batch_df
        .withColumn("predicted_label",    label_expr)
        .withColumn("true_label",         truth_expr)
        .withColumn("correct",            F.col("prediction") == F.col(LABEL_COL).cast("double"))
        .withColumn("is_attack",          F.col("prediction") != 0)
        .withColumn("is_high_priority",   F.col("prediction").isin(list(HIGH_PRIORITY)))
        .withColumn("detection_time",     F.lit(time.strftime("%Y-%m-%d %H:%M:%S")))
        .withColumn("batch_id",           F.lit(batch_id))
    )

    # ── Console summary ───────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  Batch {batch_id:>4}  |  Total: {n_total:>6,}  |  "
          f"Attacks: {n_attacks:>5,}  |  Benign: {n_benign:>6,}")
    print(f"{'='*60}")

    if n_attacks > 0:
        attack_dist = (
            batch_df.filter(F.col("is_attack"))
            .groupBy("predicted_label")
            .count()
            .orderBy(F.desc("count"))
            .toPandas()
        )
        for _, row in attack_dist.iterrows():
            print(f"    {row['predicted_label']:30s}  {row['count']:>5,}")

        n_high = batch_df.filter(F.col("is_high_priority")).count()
        if n_high > 0:
            print(f"\n  ⚠  HIGH PRIORITY: {n_high:,}")

    # Batch accuracy (only for rows where label_idx came through correctly)
    valid = batch_df.filter(F.col(LABEL_COL).isNotNull())
    n_valid = valid.count()
    if n_valid > 0:
        n_correct = valid.filter(F.col("correct")).count()
        print(f"\n  Batch accuracy: {n_correct/n_valid*100:.1f}%  ({n_correct}/{n_valid} valid labels)")

    # ── MongoDB ───────────────────────────────────────────────────────────────
    output_cols = [
        "batch_id", "detection_time",
        "predicted_label", "prediction",
        "true_label", LABEL_COL,
        "label_name", "correct",
        "is_attack", "is_high_priority",
    ]
    output_cols = [c for c in output_cols if c in batch_df.columns]
    out_pd = batch_df.select(output_cols).toPandas()

    try:
        write_to_mongo(out_pd, "predictions")
    except Exception as e:
        print(f"  MongoDB predictions write failed: {e}")

    alerts_pd = out_pd[out_pd["is_attack"] == True]
    if not alerts_pd.empty:
        try:
            write_to_mongo(alerts_pd, "alerts")
        except Exception as e:
            print(f"  MongoDB alerts write failed: {e}")


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    feature_cols = load_feature_cols(spark)
    schema       = build_schema(feature_cols)

    print(f"Model        : {MODEL_PATH}")
    print(f"Features     : {len(feature_cols)} (timestamp_unix excluded)")
    print(f"Kafka topic  : {KAFKA_TOPIC}")
    print(f"MongoDB      : {MONGO_URI}/{MONGO_DB}")
    print()

    model = PipelineModel.load(MODEL_PATH)
    print(f"Model loaded. Stages: {[type(s).__name__ for s in model.stages]}")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 10_000)
        .load()
    )

    parsed_df = (
        kafka_df
        .select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
        .select("data.*")
    )

    for c in feature_cols:
        parsed_df = parsed_df.withColumn(c, F.col(c).cast(DoubleType()))

    predictions = model.transform(parsed_df)

    drop_cols = ["features_raw", "features", "probability", "rawPrediction"]
    predictions = predictions.drop(*[c for c in drop_cols if c in predictions.columns])

    query = (
        predictions.writeStream
        .outputMode("append")
        .foreachBatch(lambda df, bid: process_batch(df, bid))
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/main")
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("\nStreaming started. Waiting for data from Kafka...")
    print("Start producer in another terminal:")
    print("  python3 /opt/work/scripts/kafka_producer.py")
    print("\nCtrl+C to stop.\n")

    query.awaitTermination()


if __name__ == "__main__":
    main()