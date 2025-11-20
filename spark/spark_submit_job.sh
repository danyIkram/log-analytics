#!/bin/bash

# Submit Spark Streaming Job with Kafka Integration

echo "Submitting Spark Streaming job to cluster..."

docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
  --conf spark.sql.streaming.checkpointLocation=hdfs://namenode:9000/tmp/checkpoints \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.streaming.kafka.maxRatePerPartition=100 \
  --driver-memory 1g \
  --executor-memory 1g \
  --executor-cores 1 \
  --total-executor-cores 2 \
  /opt/spark-apps/spark_streaming_parse_fixed.py

echo "Job submitted!"