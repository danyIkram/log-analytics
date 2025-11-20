"""
================================================================================
SPARK STREAMING LOG CATEGORIZATION
================================================================================

Description:
    This Spark Structured Streaming script reads raw logs from the Kafka topic
    'raw-logs', parses each log line, categorizes the logs by severity level
    (INFO, WARN, ERROR, CRITICAL), writes the enriched logs to HDFS in Parquet 
    format (partitioned by date), and outputs the processed logs back to Kafka 
    in the 'parsed-logs' topic. Console output is also provided for monitoring.

Error Categorization Rules:
    - INFO: 200–299
    - WARN: 300–399
    - ERROR: 400–499
    - CRITICAL: 500–599

Outputs:
    1. Console stream (debugging / monitoring)
    2. HDFS: hdfs://namenode:9000/logs/processed (partitioned by date)
    3. Kafka topic: parsed-logs
    4. Hive-ready dataset for table 'clean_logs'

Usage:
    Run inside the Spark Docker container:
        spark-submit --master local[*] spark_streaming_categorize.py
================================================================================
"""


# spark_streaming_categorize.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, current_timestamp, when, to_json, struct

# ===== CONFIGURATION =====
KAFKA_BROKER = "kafka:9092"
RAW_TOPIC = "raw-logs"
PARSED_TOPIC = "parsed-logs"
HDFS_NN = "hdfs://namenode:9000"
HDFS_PATH_PROCESSED = f"{HDFS_NN}/logs/processed"
HDFS_PATH_CLASSIFIED = f"{HDFS_NN}/logs/processed_classified"
CHECKPOINT_PATH = f"{HDFS_NN}/tmp/spark-checkpoint"
CHECKPOINT_PATH_CLASSIFIED = f"{HDFS_NN}/tmp/spark-checkpoint-classified"

# ===== SPARK SESSION =====
spark = SparkSession.builder \
    .appName("LogAnalytics-Streaming-ErrorCategorization") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark session created")

# ===== READ FROM KAFKA =====
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", RAW_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

logs_df = raw_stream.selectExpr("CAST(value AS STRING) as log_line")

# ===== PARSE LOG LINES =====
parsed_df = logs_df.select(
    split(col("log_line"), "\t").getItem(0).alias("line_id"),
    split(col("log_line"), "\t").getItem(1).alias("month"),
    split(col("log_line"), "\t").getItem(2).alias("date"),
    split(col("log_line"), "\t").getItem(3).alias("time"),
    split(col("log_line"), "\t").getItem(4).alias("level"),
    split(col("log_line"), "\t").getItem(5).alias("component"),
    split(col("log_line"), "\t").getItem(6).alias("pid"),
    split(col("log_line"), "\t").getItem(7).alias("content"),
    split(col("log_line"), "\t").getItem(8).alias("event_id"),
    split(col("log_line"), "\t").getItem(9).alias("template")
).withColumn("processed_at", current_timestamp())

# ===== ERROR CATEGORIZATION =====
classified_df = parsed_df.withColumn(
    "error_category",
    when(col("event_id").between(200, 299), "INFO")
    .when(col("event_id").between(300, 399), "WARN")
    .when(col("event_id").between(400, 499), "ERROR")
    .when(col("event_id").between(500, 599), "CRITICAL")
    .otherwise("UNKNOWN")
)

# ===== OUTPUT 1: CONSOLE =====
console_query = classified_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 5) \
    .trigger(processingTime="10 seconds") \
    .start()

# ===== OUTPUT 2: HDFS PARQUET (Partitioned by date) =====
hdfs_query = classified_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", HDFS_PATH_CLASSIFIED) \
    .option("checkpointLocation", CHECKPOINT_PATH_CLASSIFIED) \
    .partitionBy("date") \
    .trigger(processingTime="10 seconds") \
    .start()

# ===== OUTPUT 3: KAFKA PARSED TOPIC =====
kafka_output = classified_df.select(to_json(struct("*")).alias("value"))
kafka_query = kafka_output.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", PARSED_TOPIC) \
    .option("checkpointLocation", CHECKPOINT_PATH_CLASSIFIED + "-kafka") \
    .trigger(processingTime="10 seconds") \
    .start()

print("✅ ALL STREAMING QUERIES STARTED")
print("Outputs:\n1. Console\n2. HDFS Parquet (partitioned by date)\n3. Kafka parsed-logs topic")
spark.streams.awaitAnyTermination()
