# ─────────────────────────────────────────────────────────────────────────────
# Debezium Cassandra 4 Connector — Standalone JVM (UNCHANGED from v1)
#
# This is NOT a Kafka Connect source connector.
# It is a standalone JVM process that reads Cassandra commit logs and
# writes directly to Kafka via an embedded Kafka producer.
#
# Debezium version: 2.7.0.Final
# Java: 11 (required for Cassandra 4.0 interop)
# ─────────────────────────────────────────────────────────────────────────────
FROM eclipse-temurin:11-jre
 
# ─────────────────────────────────────────────────────────────────────────────
# cqlsh is NOT in standard Ubuntu apt repos — must install via pip3.
# python3 + pip3 are needed only for cqlsh (used in wait-for-cassandra.sh
# to probe Cassandra readiness before launching the Debezium JVM).
# ─────────────────────────────────────────────────────────────────────────────
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl python3 python3-pip && \
    pip3 install --no-cache-dir --break-system-packages cqlsh && \
    rm -rf /var/lib/apt/lists/*
 
WORKDIR /opt/debezium
 
# Download Debezium Cassandra 4 Connector — standalone jar with all dependencies
RUN curl -L -o connector.jar \
    "https://repo1.maven.org/maven2/io/debezium/debezium-connector-cassandra-4/2.7.0.Final/debezium-connector-cassandra-4-2.7.0.Final-jar-with-dependencies.jar"
 
# Copy conf files into image.
# NOTE: docker-compose bind-mounts ./debezium/conf → /opt/debezium/conf at runtime,
# which overrides this COPY. ENTRYPOINT points into conf/ so the runtime
# bind-mount version is always used — host file changes take effect without rebuild.
COPY conf/ /opt/debezium/conf/
RUN chmod +x /opt/debezium/conf/wait-for-cassandra.sh
 
# Using explicit bash call — same pattern as cassandra-init and connect-init.
# This means the host file does NOT need chmod +x.
# The bind mount (./debezium/conf → /opt/debezium/conf) makes the file available;
# bash reads it as a script argument, execute bit is irrelevant.
ENTRYPOINT ["/bin/bash", "/opt/debezium/conf/wait-for-cassandra.sh"]