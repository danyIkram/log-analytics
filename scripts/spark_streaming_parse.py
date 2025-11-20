# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, regexp_extract, current_timestamp
# from pyspark.sql.types import StringType, StructType, StructField

# # Configuration
# KAFKA_BROKER = "kafka:9092"
# RAW_TOPIC = "raw-logs"
# PARSED_TOPIC = "parsed-logs"
# HDFS_PATH = "hdfs://namenode:9000/logs/processed"  # Fixed: Full HDFS URI
# CHECKPOINT_HDFS = "hdfs://namenode:9000/tmp/checkpoints"
# CHECKPOINT_KAFKA = "hdfs://namenode:9000/tmp/kafka-checkpoints"

# # Initialize Spark with Kafka package
# spark = SparkSession.builder \
#     .appName("LogAnalyticsStreaming") \
#     .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_HDFS) \
#     .config("spark.sql.shuffle.partitions", 2) \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# print("=" * 60)
# print("Starting Spark Structured Streaming - Log Analytics")
# print(f"Kafka Broker: {KAFKA_BROKER}")
# print(f"Subscribing to topic: {RAW_TOPIC}")
# print("=" * 60)

# # Read streaming from Kafka
# raw_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("subscribe", RAW_TOPIC) \
#     .option("startingOffsets", "earliest") \
#     .option("failOnDataLoss", "false") \
#     .load()

# # Convert value to string and add metadata
# logs_df = raw_df.selectExpr(
#     "CAST(value AS STRING) as log",
#     "timestamp as kafka_timestamp",
#     "partition",
#     "offset"
# )

# # Parse log format: assuming tab-separated log like:
# # day\tmonth\tyear\tlevel\tservice\tmessage
# parsed_df = logs_df.select(
#     col("kafka_timestamp"),
#     col("partition"),
#     col("offset"),
#     regexp_extract("log", r"(\d{1,2})\t", 1).alias("day"),
#     regexp_extract("log", r"\t(\w+)\t", 1).alias("month"),
#     regexp_extract("log", r"\t(\d+)\t", 1).alias("year"),
#     regexp_extract("log", r"\t(INFO|DEBUG|WARN|ERROR|TRACE)\t", 1).alias("level"),
#     regexp_extract("log", r"(INFO|DEBUG|WARN|ERROR|TRACE)\t(\w+)", 2).alias("service"),
#     col("log").alias("raw_message")
# ).filter(col("level") != "")  # Filter out unparsed logs

# # Add processing timestamp
# parsed_df = parsed_df.withColumn("processed_at", current_timestamp())

# # Query 1: Write parsed logs to HDFS (Parquet format)
# print("Starting HDFS write stream...")
# hdfs_query = parsed_df.writeStream \
#     .format("parquet") \
#     .option("path", HDFS_PATH) \
#     .option("checkpointLocation", CHECKPOINT_HDFS) \
#     .outputMode("append") \
#     .trigger(processingTime="10 seconds") \
#     .start()

# # Query 2: Write parsed logs back to Kafka
# print("Starting Kafka write stream...")
# kafka_df = parsed_df.select(
#     col("service").alias("key"),  # Use service as key for partitioning
#     to_json(struct(
#         col("day"),
#         col("month"),
#         col("year"),
#         col("level"),
#         col("service"),
#         col("raw_message"),
#         col("processed_at")
#     )).alias("value")
# )

# kafka_query = kafka_df.writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("topic", PARSED_TOPIC) \
#     .option("checkpointLocation", CHECKPOINT_KAFKA) \
#     .outputMode("append") \
#     .trigger(processingTime="10 seconds") \
#     .start()

# # Query 3: Console output for debugging (optional - comment out in production)
# console_query = parsed_df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .trigger(processingTime="10 seconds") \
#     .option("truncate", False) \
#     .option("numRows", 20) \
#     .start()

# print("\n" + "=" * 60)
# print("All streaming queries started successfully!")
# print("Waiting for data...")
# print("=" * 60 + "\n")

# # Wait for all queries to terminate
# spark.streams.awaitAnyTermination()