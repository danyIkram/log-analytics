FROM eclipse-temurin:8-jre

ENV FLUME_VERSION=1.9.0

RUN apt-get update && apt-get install -y wget tar && \
    wget https://archive.apache.org/dist/flume/${FLUME_VERSION}/apache-flume-${FLUME_VERSION}-bin.tar.gz && \
    tar -xvzf apache-flume-${FLUME_VERSION}-bin.tar.gz -C /opt && \
    mv /opt/apache-flume-${FLUME_VERSION}-bin /opt/flume && \
    rm apache-flume-${FLUME_VERSION}-bin.tar.gz

ENV PATH=$PATH:/opt/flume/bin

WORKDIR /opt/flume

CMD ["tail", "-f", "/dev/null"]
