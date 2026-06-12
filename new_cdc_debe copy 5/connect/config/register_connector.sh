#!/bin/sh
# ═══════════════════════════════════════════════════════════════════════════════
# register_connector.sh
# Run by connect-init service after Kafka Connect becomes healthy.
# POSTs the S3 Sink Connector config to the Connect REST API.
# ═══════════════════════════════════════════════════════════════════════════════
set -e
 
CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"
CONFIG_FILE="/connect-config/s3_sink_all_tables.json"
CONNECTOR_NAME="bidgely-s3-sink-all-tables"
MAX_RETRIES=30
 
# ─── Wait for Kafka Connect REST API ─────────────────────────────────────────
echo "[connect-init] Waiting for Kafka Connect at ${CONNECT_URL} ..."
RETRIES=0
until curl -sf "${CONNECT_URL}/connectors" > /dev/null 2>&1; do
    RETRIES=$((RETRIES + 1))
    if [ "$RETRIES" -ge "$MAX_RETRIES" ]; then
        echo "[connect-init] ✗ TIMEOUT: Connect not ready after $((MAX_RETRIES * 5))s"
        exit 1
    fi
    echo "[connect-init]   Attempt $RETRIES/$MAX_RETRIES — retrying in 5s..."
    sleep 5
done
echo "[connect-init] ✓ Kafka Connect is ready"
 
# ─── Register Connector ───────────────────────────────────────────────────────
echo "[connect-init] Registering S3 Sink Connector..."
echo "[connect-init]   Config file : ${CONFIG_FILE}"
echo "[connect-init]   Connector   : ${CONNECTOR_NAME}"
echo "[connect-init]   topics.regex: cdc\\.bidgely\\..+"
echo ""
 
HTTP_CODE=$(curl -s \
    -o /tmp/connect_response.txt \
    -w "%{http_code}" \
    -X POST "${CONNECT_URL}/connectors" \
    -H "Content-Type: application/json" \
    --data-binary @"${CONFIG_FILE}")
 
echo "[connect-init] HTTP Status : ${HTTP_CODE}"
echo "[connect-init] Response    :"
cat /tmp/connect_response.txt
echo ""
 
if [ "${HTTP_CODE}" = "201" ]; then
    echo "[connect-init] ✓ Connector created (201 Created)"
elif [ "${HTTP_CODE}" = "409" ]; then
    echo "[connect-init] ✓ Connector already registered (409 Conflict — OK)"
else
    echo "[connect-init] ✗ Unexpected HTTP ${HTTP_CODE}"
    exit 1
fi
 
# ─── Wait for RUNNING state ───────────────────────────────────────────────────
echo ""
echo "[connect-init] Waiting 15s for connector to reach RUNNING state..."
sleep 15
 
echo "[connect-init] Connector status:"
curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" || true
echo ""
 
echo "[connect-init] ══════════════════════════════════════════════"
echo "[connect-init]  S3 Sink Connector registered"
echo "[connect-init]  Consuming: cdc.bidgely.* (all current + future tables)"
echo "[connect-init]  Writing to: s3://bronze/topics/{topic}/year=.../..."
echo "[connect-init] ══════════════════════════════════════════════"
 