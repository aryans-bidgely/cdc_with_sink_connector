FROM cassandra:4.0
 
# ─────────────────────────────────────────────────────────────────────────────
# Patch cassandra.yaml for CDC
#
# Strategy: delete any existing line for each setting, then append the correct
# value. This is idempotent regardless of what the base image ships.
#
# cdc_enabled: true                    — activates CDC subsystem
# cdc_raw_directory                    — where commit log segments land for CDC
# cdc_total_space_in_mb: 4096          — if cdc_raw fills past this, Cassandra
#                                        rejects ALL writes to CDC tables
# cdc_free_space_check_interval_ms:250 — how often Cassandra checks cdc_raw space
# commitlog_directory                  — explicit path, matches mounted volume
#
# VOLUME ["/etc/cassandra"] is required because docker-compose mounts
# cassandra_config:/etc/cassandra — Debezium reads cassandra.yaml from there
# ─────────────────────────────────────────────────────────────────────────────
RUN set -eux; \
    YAML=/etc/cassandra/cassandra.yaml; \
    sed -i '/cdc_enabled/d'                      "$YAML"; \
    echo "cdc_enabled: true"                  >> "$YAML"; \
    sed -i '/cdc_raw_directory/d'                "$YAML"; \
    echo "cdc_raw_directory: /var/lib/cassandra/cdc_raw" >> "$YAML"; \
    sed -i '/cdc_total_space_in_mb/d'            "$YAML"; \
    echo "cdc_total_space_in_mb: 4096"        >> "$YAML"; \
    sed -i '/cdc_free_space_check_interval_ms/d' "$YAML"; \
    echo "cdc_free_space_check_interval_ms: 250" >> "$YAML"; \
    sed -i '/commitlog_directory/d'              "$YAML"; \
    echo "commitlog_directory: /var/lib/cassandra/commitlog" >> "$YAML"; \
    echo "--- Patched lines ---"; \
    grep -E "cdc_enabled|cdc_raw|commitlog_directory" "$YAML"
 
VOLUME ["/etc/cassandra"]
 