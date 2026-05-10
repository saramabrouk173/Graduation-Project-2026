FROM apache/airflow:2.8.4

USER root

RUN apt-get update && \
    apt-get install -y default-jre-headless procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

COPY spark-3.5.1-bin-hadoop3.tgz /tmp/
RUN tar xvf /tmp/spark-3.5.1-bin-hadoop3.tgz -C /opt/ && \
    mv /opt/spark-3.5.1-bin-hadoop3 /opt/spark && \
    rm /tmp/spark-3.5.1-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

USER airflow

RUN pip install --no-cache-dir \
    "sqlalchemy<2.0" \
    "kafka-python" \
    "pandas" \
    "sqlalchemy-pytds"