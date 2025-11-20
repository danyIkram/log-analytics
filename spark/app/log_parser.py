# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json, udf, to_json, struct
# from pyspark.sql.types import StringType
# from utils import parse_log

# # UDF to convert lines
# parse_udf = udf(lambda line: parse_log(line), StringType())

# # Create Spark session with Kafka support
# spark = SparkSession.builder \
#     .appName("LogParser") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("WARN")

# # Read raw logs from Kafka
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("subscribe", "raw-logs") \
#     .load() \
#     .selectExpr("CAST(value AS STRING) as line")

# # Parse logs using UDF
# parsed_df = df.withColumn("parsed", parse_udf(col("line")))

# # Output schema is JSON string → extract fields
# final_df = parsed_df.select(
#     from_json(col("parsed"), """
#         timestamp STRING,
#         level STRING,
#         service STRING,
#         pid INT,
#         message STRING
#     """).alias("data")
# ).select("data.*")

# # -------------- Write to Kafka (parsed-logs) ------------------
# kafka_sink = final_df.select(
#     to_json(struct("*")).alias("value")
# ).writeStream \
#  .format("kafka") \
#  .option("kafka.bootstrap.servers", "kafka:9092") \
#  .option("topic", "parsed-logs") \
#  .option("checkpointLocation", "/tmp/spark/checkpoints/kafka") \
#  .start()

# # -------------- Write to HDFS (/logs/processed/) --------------
# hdfs_sink = final_df.writeStream \
#     .format("parquet") \
#     .option("path", "/logs/processed/") \
#     .option("checkpointLocation", "/tmp/spark/checkpoints/hdfs") \
#     .partitionBy("level") \
#     .outputMode("append") \
#     .start()

# spark.streams.awaitAnyTermination()
