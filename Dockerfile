FROM apache/spark:3.5.3

USER root

COPY target/cp-pipeline-1.0.0.jar /opt/spark/jars/cp-pipeline.jar

USER spark
