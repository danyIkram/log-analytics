#!/bin/bash

echo "Creating Kafka topics for Log Analytics System..."

# Create RAW logs topic (optionnel si déjà créé)
docker exec kafka kafka-topics \
  --create \
  --topic raw-logs \
  --bootstrap-server kafka:9092 \
  --partitions 1 \
  --replication-factor 1

# Create PARSED logs topic
docker exec kafka kafka-topics \
  --create \
  --topic parsed-logs \
  --bootstrap-server kafka:9092 \
  --partitions 1 \
  --replication-factor 1

# Create AGGREGATED logs topic
docker exec kafka kafka-topics \
  --create \
  --topic aggregated-logs \
  --bootstrap-server kafka:9092 \
  --partitions 1 \
  --replication-factor 1

echo "✔ All topics created!"

echo "Applying 7-day retention policies..."

# Apply retention policies (7 days)
docker exec kafka kafka-configs --alter \
  --bootstrap-server kafka:9092 \
  --entity-type topics \
  --entity-name raw-logs \
  --add-config retention.ms=604800000

docker exec kafka kafka-configs --alter \
  --bootstrap-server kafka:9092 \
  --entity-type topics \
  --entity-name parsed-logs \
  --add-config retention.ms=604800000

docker exec kafka kafka-configs --alter \
  --bootstrap-server kafka:9092 \
  --entity-type topics \
  --entity-name aggregated-logs \
  --add-config retention.ms=604800000

echo "Kafka setup completed successfully!"

echo "ℹ️ Verify topics with:"
echo "winpty docker exec -it kafka kafka-topics --list --bootstrap-server kafka:9092"
