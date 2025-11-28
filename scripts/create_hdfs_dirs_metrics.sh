docker exec -it namenode hdfs dfs -mkdir -p /logs/aggregated_daily
docker exec -it namenode hdfs dfs -mkdir -p /logs/metrics/errors
docker exec -it namenode hdfs dfs -mkdir -p /logs/metrics/services
docker exec -it namenode hdfs dfs -mkdir -p /logs/metrics/response
docker exec -it namenode hdfs dfs -mkdir -p /logs/metrics/endpoints
docker exec -it namenode hdfs dfs -ls /logs
