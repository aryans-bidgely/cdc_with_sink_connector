#!/bin/bash
# ═══════════════════════════════════════════════════════════════════════════════
# Debezium Cassandra 4 Connector — Startup Script
#
# IMPORTANT: This file must be executable on the host:
#   chmod +x debezium/conf/wait-for-cassandra.sh
#
# docker-compose bind-mounts ./debezium/conf → /opt/debezium/conf at runtime.
# The bind mount overrides the image layer, so the host file permission matters.
# ═══════════════════════════════════════════════════════════════════════════════
set -e
 
CONFIG_FILE="/opt/debezium/conf/cdc.properties"
if [ ! -f "$CONFIG_FILE" ]; then
    echo "[debezium] ✗ ERROR: $CONFIG_FILE not found!"
    exit 1
fi
 
echo "[debezium] ✓ cdc.properties found"
echo "[debezium]   snapshot.mode   = $(grep '^snapshot.mode' "$CONFIG_FILE" | cut -d= -f2)"
echo "[debezium]   topic.prefix    = $(grep '^topic.prefix'  "$CONFIG_FILE" | cut -d= -f2)"
echo "[debezium]   kafka.bootstrap = $(grep '^kafka.producer.bootstrap.servers' "$CONFIG_FILE" | cut -d= -f2)"
 
echo "[debezium] Waiting for Cassandra CQL on cassandra:9042 ..."
MAX_RETRIES=48
RETRIES=0
until cqlsh cassandra -e "DESCRIBE CLUSTER" > /dev/null 2>&1; do
    RETRIES=$((RETRIES + 1))
    if [ "$RETRIES" -ge "$MAX_RETRIES" ]; then
        echo "[debezium] ✗ TIMEOUT after $((MAX_RETRIES * 5))s"
        exit 1
    fi
    echo "[debezium]   Attempt $RETRIES/$MAX_RETRIES — retrying in 5s..."
    sleep 5
done
echo "[debezium] ✓ Cassandra CQL is ready"
 
echo "[debezium] Verifying bidgely.users has CDC enabled..."
CDC_ENABLED=$(cqlsh cassandra -e \
    "SELECT cdc FROM system_schema.tables WHERE keyspace_name='bidgely' AND table_name='users';" \
    2>/dev/null | grep -ci "true" || true)
 
if [ "$CDC_ENABLED" -eq 0 ]; then
    echo "[debezium] ⚠ bidgely.users cdc=true not confirmed. Waiting 15s for cassandra-init..."
    sleep 15
    CDC_ENABLED=$(cqlsh cassandra -e \
        "SELECT cdc FROM system_schema.tables WHERE keyspace_name='bidgely' AND table_name='users';" \
        2>/dev/null | grep -ci "true" || true)
    if [ "$CDC_ENABLED" -eq 0 ]; then
        echo "[debezium] ✗ ERROR: bidgely.users still does not have cdc=true"
        exit 1
    fi
fi
echo "[debezium] ✓ bidgely.users has cdc=true"
 
CDC_RAW_DIR="/var/lib/cassandra/cdc_raw"
if [ -d "$CDC_RAW_DIR" ]; then
    echo "[debezium] ✓ CDC raw directory exists: $CDC_RAW_DIR"
else
    echo "[debezium] ℹ CDC raw directory not yet created — normal on first run"
fi
 
echo "[debezium] Giving Cassandra 8s to settle CDC state..."
sleep 8
 
echo ""
echo "[debezium] ══════════════════════════════════════════════════"
echo "[debezium]  Starting Debezium Cassandra 4 Connector 2.7.0.Final"
echo "[debezium]  Topics: cdc.bidgely.{users,weather_data,pilot_info,...}"
echo "[debezium] ══════════════════════════════════════════════════"
echo ""
 
exec java \
  -Dcassandra.storagedir=/var/lib/cassandra \
  -Djdk.attach.allowAttachSelf=true \
  --add-exports=java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-exports=java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED \
  --add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED \
  --add-exports=java.rmi/sun.rmi.server=ALL-UNNAMED \
  --add-exports=java.sql/java.sql=ALL-UNNAMED \
  --add-opens=java.base/java.lang.module=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.loader=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.reflect=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.math=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.module=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.util.jar=ALL-UNNAMED \
  --add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED \
  -cp /etc/cassandra:/opt/debezium/connector.jar \
  io.debezium.connector.cassandra.CassandraConnectorTask \
  /opt/debezium/conf/cdc.properties
 