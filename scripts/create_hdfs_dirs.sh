#!/bin/bash
docker exec -it namenode hdfs://namenode:9000/ dfs -mkdir -p /logs/raw
docker exec -it namenode hdfs://namenode:9000 dfs -mkdir -p /logs/processed
docker exec -it namenode hdfs://namenode:9000 dfs -mkdir -p /logs/processed

docker exec -it namenode hdfs://namenode:9000 dfs -ls /logs
