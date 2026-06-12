#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# Spark ETL Entrypoint — Generic CDC Pipeline (v2)
#
# Changes from v1:
#   - Waits for MinIO only (no Kafka wait — Spark reads S3, not Kafka)
#   - Submits generic_pipeline.py instead of cdc_pipeline.py
#   - --py-files includes all supporting modules
# ─────────────────────────────────────────────────────────────────────────────
set -e
 
MINIO_HOST="${MINIO_ENDPOINT#http://}"
MINIO_HOST="${MINIO_HOST%%:*}"
MINIO_PORT="${MINIO_ENDPOINT##*:}"
 
# ─── Wait for MinIO ───────────────────────────────────────────────────────────
echo "[spark-etl] Waiting for MinIO at ${MINIO_HOST}:${MINIO_PORT}..."
RETRIES=0
until curl -sf "http://${MINIO_HOST}:${MINIO_PORT}/minio/health/live" > /dev/null 2>&1; do
    RETRIES=$((RETRIES+1))
    [ "$RETRIES" -ge 20 ] && echo "[spark-etl] ERROR: MinIO not ready after 100s" && exit 1
    echo "[spark-etl]   Attempt $RETRIES/20 — retrying in 5s..."
    sleep 5
done
echo "[spark-etl] ✓ MinIO is ready"
 
echo ""
echo "[spark-etl] ════════════════════════════════════════════════════════"
echo "[spark-etl]  Bidgely Generic CDC Pipeline — v2"
echo "[spark-etl]  Source : S3 Bronze (via Kafka S3 Sink Connector)"
echo "[spark-etl]  Config : ${TABLE_CONFIG_PATH}"
echo "[spark-etl]  Silver : s3a://silver/bidgely/"
echo "[spark-etl]  Types  : EAV | JSON_EMBEDDED | PLAIN"
echo "[spark-etl] ════════════════════════════════════════════════════════"
echo ""
 
exec /opt/spark/bin/spark-submit \
    --master "local[4]" \
    --driver-memory 1g \
    --py-files /opt/spark/jobs/config_validator.py,/opt/spark/jobs/generic_decoder.py,/opt/spark/jobs/generic_merge.py,/opt/spark/jobs/alerting.py \
    /opt/spark/jobs/generic_pipeline.py
 