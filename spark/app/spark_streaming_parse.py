# from pyspark.sql import SparkSession

# # 1️⃣ Create Spark session
# spark = SparkSession.builder \
#     .appName("LogAnalyticsStreaming") \
#     .getOrCreate()

# # 2️⃣ Read from Kafka
# # Make sure Kafka service name matches your Docker container
# df = spark \
#   .readStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "kafka:9092")  # container name
#   .option("subscribe", "raw-logs") \
#   .option("startingOffsets", "latest") \
#   .load()

# # 3️⃣ Convert value from bytes to string
# logs = df.selectExpr("CAST(value AS STRING) as log_line")

# # 4️⃣ Output to console
# query = logs.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# query.awaitTermination()
