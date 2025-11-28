#!/bin/bash

echo "========================================="
echo "BigData Pipeline Verification Commands"
echo "========================================="

echo -e "\n1. Check all containers are running:"
docker-compose ps

echo -e "\n2. Verify Kafka topics:"
docker exec kafka kafka-topics --list --bootstrap-server kafka:9092

echo -e "\n3. Check Kafka topic details (raw-logs):"
docker exec kafka kafka-topics --describe --topic raw-logs --bootstrap-server kafka:9092

echo -e "\n4. Test Kafka connectivity from Spark:"
docker exec spark-master nc -zv kafka 9092

echo -e "\n5. Check HDFS namenode status:"
docker exec namenode hdfs dfsadmin -report

echo -e "\n6. Verify existing HDFS directories:"
#docker exec namenode hdfs dfs -ls /
docker exec -it namenode hdfs dfs -ls hdfs://namenode:9000/logs

echo -e "\n7. Create missing HDFS directories (if needed):"
docker exec namenode hdfs dfs -mkdir -p /tmp/checkpoints 2>/dev/null || echo "Directory /tmp/checkpoints already exists or created"
docker exec namenode hdfs dfs -mkdir -p /tmp/kafka-checkpoints 2>/dev/null || echo "Directory /tmp/kafka-checkpoints already exists or created"

echo -e "\n8. Set HDFS permissions for Spark:"
docker exec namenode hdfs dfs -chmod -R 777 /logs 2>/dev/null || echo "Permissions set or already correct"
docker exec namenode hdfs dfs -chmod -R 777 /tmp 2>/dev/null || echo "Permissions set or already correct"

echo -e "\n9. Verify all HDFS directories:"
#docker exec namenode hdfs://namenode:9000/ dfs -ls -R /logs
#docker exec namenode hdfs://namenode:9000/ dfs -ls /tmp | grep checkpoints || echo "Checkpoint directories will be created by Spark"
docker exec namenode bash -c "hdfs dfs -ls /tmp | grep checkpoints || echo 'Checkpoint directories will be created by Spark'"
docker exec namenode bash -c "hdfs dfs -ls /logs"

echo -e "\n10. Check Spark Master UI:"
echo "Open: http://localhost:8080"

echo -e "\n11. Check Hadoop NameNode UI:"
echo "Open: http://localhost:9870"

echo -e "\n========================================="
echo "✅ Verification complete!"
echo "========================================="