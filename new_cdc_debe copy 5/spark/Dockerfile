# ─────────────────────────────────────────────────────────────────────────────
# Spark 3.5.0 + Delta Lake 3.2.0 + S3A/MinIO
#
# Changes from v1:
#   - Kafka JARs REMOVED (spark-sql-kafka, kafka-clients, commons-pool2,
#     spark-token-provider) — Spark no longer reads from Kafka in v2
#   - pyyaml added — needed to load table_config.yml
#   - local[4] instead of local[2] — 3 concurrent streams need more cores
#
# JAR compatibility (verified):
#   delta-spark_2.12-3.2.0   → only Delta version for Spark 3.5.x
#   delta-storage-3.2.0      → must match delta-spark
#   hadoop-aws-3.3.4          → must match Spark 3.5.0 internal Hadoop exactly
#   aws-java-sdk-bundle-1.12.262 → POM-specified dep of hadoop-aws-3.3.4
# ─────────────────────────────────────────────────────────────────────────────
FROM apache/spark:3.5.0-scala2.12-java11-python3-ubuntu
 
USER root
 
# Python dependencies
# delta-spark: Python bindings for Delta Lake API
# pyyaml:      reads table_config.yml at pipeline startup
RUN pip3 install --no-cache-dir \
    delta-spark==3.2.0 \
    pyyaml==6.0.1
  
# ── Delta Lake JARs ───────────────────────────────────────────────────────────
RUN wget -q -P /opt/spark/jars/ \
    "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar" && \
    wget -q -P /opt/spark/jars/ \
    "https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar"



 
# ── S3A / MinIO JARs ───────────────────────────────────────────────────────── 
RUN wget -q -P /opt/spark/jars/ \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" && \
    wget -q -P /opt/spark/jars/ \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
 
# NOTE: Kafka JARs intentionally NOT included
# spark-sql-kafka, kafka-clients, commons-pool2, spark-token-provider
# are all removed — Spark reads S3 files, not Kafka topics, in v2
 
COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY conf/log4j.properties /opt/spark/conf/log4j.properties
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
 
USER spark
WORKDIR /opt/spark/jobs
 
ENTRYPOINT ["/entrypoint.sh"]
 