# ─────────────────────────────────────────────────────────────────────────────
# Kafka Connect — Confluent cp-kafka-connect + S3 Sink Connector
#
# Base image: confluentinc/cp-kafka-connect:7.6.0
#   - Kafka Connect runtime included
#   - confluent-hub CLI included
#   - Java 11
#
# S3 Sink Connector: confluentinc/kafka-connect-s3:10.5.7
#   - Installed at build time (no internet needed at runtime)
#   - Installed to /usr/share/confluent-hub-components/ (in default PLUGIN_PATH)
#   - Compatible with Kafka 3.8.0 (Connect is backward-compatible with newer brokers)
#
# MinIO compatibility:
#   - store.url overrides S3 endpoint → http://minio:9000
#   - AWS SDK uses path-style access automatically when custom endpoint is set
#   - Credentials via AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars
# ─────────────────────────────────────────────────────────────────────────────
FROM confluentinc/cp-kafka-connect:7.6.0
 
USER root
 
# Install S3 Sink Connector at build time
# --no-prompt skips interactive license confirmation
RUN confluent-hub install --no-prompt \
    confluentinc/kafka-connect-s3:10.5.7
 
USER appuser