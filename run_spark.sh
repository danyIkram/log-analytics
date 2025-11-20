# #!/bin/bash
# # =========================================
# # Run Spark Streaming Job
# # Submits the Spark job to process Kafka logs
# # =========================================

# echo "========================================="
# echo "   STARTING SPARK STREAMING JOB"
# echo "========================================="

# echo -e "\n📋 Configuration:"
# echo "  - Master: spark://spark-master:7077"
# echo "  - Input: Kafka topic 'raw-logs'"
# echo "  - Output: HDFS + Kafka + Console"
# echo "  - Trigger: Every 10 seconds"

# echo -e "\n🚀 Submitting job..."

# docker exec spark-master sh -c "/spark/bin/spark-submit \
#   --master spark://spark-master:7077 \
#   --deploy-mode client \
#   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 \
#   --conf 'spark.sql.shuffle.partitions=2' \
#   --conf 'spark.streaming.kafka.maxRatePerPartition=100' \
#   --driver-memory 1g \
#   --executor-memory 1g \
#   --executor-cores 1 \
#   /opt/spark-apps/spark_consumer.py"


# echo -e "\n========================================="
# echo "   ✅ SPARK JOB COMPLETED/STOPPED"
# echo "========================================="
