"""
Step 3: Process logs from Kafka with Spark Structured Streaming
This script reads from Kafka, parses logs, and writes to HDFS
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Configuration
KAFKA_BROKER = "kafka:9092"  # Use container name inside Docker
RAW_TOPIC = "raw-logs"
PARSED_TOPIC = "parsed-logs"
HDFS_NN = "hdfs://namenode:9000"
HDFS_PATH = f"{HDFS_NN}/logs/processed"
CHECKPOINT_PATH = f"{HDFS_NN}/tmp/spark-checkpoint"

print("=" * 70)
print("⚡ SPARK STRUCTURED STREAMING - Log Analytics")
print("=" * 70)
print(f"Kafka Broker: {KAFKA_BROKER}")
print(f"Input Topic: {RAW_TOPIC}")
print(f"Output Topic: {PARSED_TOPIC}")
print(f"HDFS Output: {HDFS_PATH}")
print("=" * 70 + "\n")

# Create Spark Session
spark = SparkSession.builder \
    .appName("LogAnalytics-Streaming") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .getOrCreate()


spark.sparkContext.setLogLevel("WARN")

print("✅ Spark session created\n")

# Read from Kafka
print("📥 Connecting to Kafka topic: raw-logs...\n")

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", RAW_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

print("✅ Connected to Kafka\n")

# Convert Kafka message to string
# Kafka stores data as bytes in 'value' column
logs_df = raw_stream.selectExpr("CAST(value AS STRING) as log_line")

# Parse log line (tab-separated)
# Format: LineID\tMonth\tDate\tTime\tLevel\tComponent\tPID\tContent\tEventID\tTemplate
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

print("✅ Log parsing configured\n")

# Output 1: Write to console (for monitoring)
print("📺 Starting console output (for debugging)...\n")

console_query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 5) \
    .trigger(processingTime="10 seconds") \
    .start()

# Output 2: Write to HDFS as Parquet
print("💾 Starting HDFS output...\n")

hdfs_query = parsed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", HDFS_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .trigger(processingTime="10 seconds") \
    .start()

# Output 3: Write back to Kafka (parsed-logs topic)
print("📤 Starting Kafka output (parsed-logs topic)...\n")

# Convert DataFrame to JSON string for Kafka
from pyspark.sql.functions import to_json, struct

kafka_output = parsed_df.select(
    to_json(struct("*")).alias("value")
)

kafka_query = kafka_output.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", PARSED_TOPIC) \
    .option("checkpointLocation", CHECKPOINT_PATH + "-kafka") \
    .trigger(processingTime="10 seconds") \
    .start()

print("=" * 70)
print("✅ ALL STREAMING QUERIES STARTED!")
print("=" * 70)
print("Outputs:")
print("  1. Console (every 10 seconds)")
print("  2. HDFS: hdfs://namenode:9000/logs/processed")
print("  3. Kafka topic: parsed-logs")
print("=" * 70 + "\n")
print("🔄 Processing logs... (Press Ctrl+C to stop)\n")

# Wait for termination
spark.streams.awaitAnyTermination()