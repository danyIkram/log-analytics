from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# -------------------------
# 1. Spark session
# -------------------------
spark = SparkSession.builder \
    .appName("RealTimeAggregationHive") \
    .master("local[*]") \
    .enableHiveSupport() \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------
# 2. Kafka source (parsed logs)
# -------------------------
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("service", StringType()),
    StructField("endpoint", StringType()),
    StructField("category", StringType()),
    StructField("response_time", DoubleType())
])

logs_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "parsed-logs") \
    .option("startingOffsets", "latest") \
    .load()

logs_df = logs_df.selectExpr("CAST(value AS STRING)")
logs_df = logs_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
logs_df = logs_df.withColumn("event_time", to_timestamp("timestamp"))

# -------------------------
# 3. Aggregations
# -------------------------

# Errors per minute
errors_per_minute = (
    logs_df
    .withWatermark("event_time", "2 minutes")
    .groupBy(window("event_time", "1 minute"), col("service"))
    .agg(count(when(col("category") == "CRITICAL", True)).alias("errors"))
    .withColumn("metric_type", lit("errors_per_minute"))
    .withColumn("endpoint", lit(None).cast(StringType()))
    .withColumn("requests", lit(None).cast(IntegerType()))
    .withColumn("avg_response_time", lit(None).cast(DoubleType()))
    .withColumn("hits", lit(None).cast(IntegerType()))
)

# Requests per service
requests_per_service = (
    logs_df
    .withWatermark("event_time", "2 minutes")
    .groupBy(window("event_time", "1 minute"), col("service"))
    .agg(count("*").alias("requests"))
    .withColumn("metric_type", lit("requests_per_service"))
    .withColumn("endpoint", lit(None).cast(StringType()))
    .withColumn("errors", lit(None).cast(IntegerType()))
    .withColumn("avg_response_time", lit(None).cast(DoubleType()))
    .withColumn("hits", lit(None).cast(IntegerType()))
)

# Avg response time
avg_response_time = (
    logs_df
    .withWatermark("event_time", "2 minutes")
    .groupBy(window("event_time", "1 minute"), col("service"))
    .agg(avg("response_time").alias("avg_response_time"))
    .withColumn("metric_type", lit("avg_response_time"))
    .withColumn("endpoint", lit(None).cast(StringType()))
    .withColumn("errors", lit(None).cast(IntegerType()))
    .withColumn("requests", lit(None).cast(IntegerType()))
    .withColumn("hits", lit(None).cast(IntegerType()))
)

# Top endpoints
top_endpoints = (
    logs_df
    .withWatermark("event_time", "2 minutes")
    .groupBy(window("event_time", "1 minute"), col("endpoint"))
    .agg(count("*").alias("hits"))
    .withColumn("metric_type", lit("top_endpoints"))
    .withColumn("service", lit(None).cast(StringType()))
    .withColumn("errors", lit(None).cast(IntegerType()))
    .withColumn("requests", lit(None).cast(IntegerType()))
    .withColumn("avg_response_time", lit(None).cast(DoubleType()))
)

# Union all
all_metrics = errors_per_minute.union(requests_per_service).union(avg_response_time).union(top_endpoints)
all_metrics = all_metrics.withColumn("date", date_format(col("window.start"), "yyyy-MM-dd"))

# -------------------------
# 4. Write to Kafka
# -------------------------
output_df = all_metrics.select(
    to_json(struct(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "service",
        "endpoint",
        "errors",
        "requests",
        "avg_response_time",
        "hits",
        "metric_type",
        "date"
    )).alias("value")
)

kafka_output = (
    output_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "aggregated-logs")
    .option("checkpointLocation", "/tmp/spark-checkpoints/aggregation_kafka")
    .outputMode("update")
    .start()
)

# -------------------------
# 5. Write to HDFS
# -------------------------
hdfs_output = (
    output_df.writeStream
    .format("json")
    .option("path", "/logs/aggregated_daily")
    .option("checkpointLocation", "/tmp/spark-checkpoints/aggregation_hdfs")
    .outputMode("append")
    .start()
)

# -------------------------
# 6. Write to Hive via foreachBatch
# -------------------------
def write_to_hive(batch_df, batch_id):
    batch_df.write.mode("append").partitionBy("date").saveAsTable("aggregated_metrics")

hive_output = all_metrics.writeStream \
    .foreachBatch(write_to_hive) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/spark-checkpoints/aggregation_hive") \
    .start()

# -------------------------
# 7. Console debug
# -------------------------
console_output = all_metrics.writeStream \
    .format("console") \
    .outputMode("update") \
    .start()

spark.streams.awaitAnyTermination()
