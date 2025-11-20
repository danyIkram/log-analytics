FROM bitnami/spark:3.5

USER root

RUN pip install pyspark

COPY app/ /opt/spark-apps/
WORKDIR /opt/spark-apps/
