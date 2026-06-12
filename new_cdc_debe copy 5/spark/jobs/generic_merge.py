"""
generic_merge.py
================
Config-driven Delta Lake MERGE for all three table types.
 
Everything — silver path, merge key, partition columns, data columns —
comes from table_config.yml. No hardcoded column names, paths, or keys.
 
Problems from architecture discussion this solves:
  P4/P5  — Null vs unchanged ambiguity:
             EAV uses has_{col} flag CASE WHEN logic.
             has=1 + val=X    → UPDATE to X (explicit set, even if X is null)
             has=1 + val=None → SET to null (explicit attribute delete/tombstone)
             has=0            → KEEP existing Delta value (not in this batch)
 
  P13    — S3 consumers need complete entity records:
             Pivot (Phase 3) produces one wide row per entity.
             MERGE writes that complete row to Delta.
 
  P14    — Snapshot + CDC consistency:
             MERGE is idempotent. Re-running the same batch produces the
             same result. Snapshot rows + CDC rows both go through MERGE.
 
  P15    — Attribute deletes and entity deletes:
             Attribute-level tombstone → has=1 + val=None → SET column null
             Entity-level tombstone   → handle_*_entity_deletes nulls all columns
 
Functions:
  write_dead_letter           — writes failed batch to dead-letter S3 path
  check_partition_compatibility — blocks partition_by changes on existing tables
  merge_eav                   — EAV upsert with CASE WHEN partial update
  handle_eav_entity_deletes   — EAV full entity soft-delete
  merge_json_embedded         — JSON_EMBEDDED full record replace
  handle_json_embedded_entity_deletes — JSON_EMBEDDED partition-level delete
  merge_plain                 — PLAIN full record replace
  handle_plain_entity_deletes — PLAIN row-level delete
"""
 
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable
 
log = logging.getLogger("generic_merge")
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
 
def write_dead_letter(batch_df: DataFrame, cfg: dict, batch_id: int) -> None:
    """
    Write a failed batch to the dead-letter path for investigation.
    Called by the error-isolation wrapper before sending an alert.
    Non-fatal: logs on failure but does not re-raise.
    """
    try:
        dead_path = cfg["dead_letter_path"]
        (
            batch_df
            .write
            .mode("append")
            .json(f"{dead_path}/batch_{batch_id}")
        )
        log.info(f"Dead letter written → {dead_path}/batch_{batch_id}")
    except Exception as e:
        log.error(f"Dead letter write also failed: {e}")
 
 
def _apply_partition(writer, partition_by):
    """
    Apply partitionBy to a DataFrameWriter from the config value.
 
    partition_by can be:
      null / None → no partitioning (small tables)
      "pilot_id"  → partitionBy("pilot_id")
      ["country", "epoch_month"] → partitionBy("country", "epoch_month")
    """
    if not partition_by:
        return writer
    if isinstance(partition_by, list):
        return writer.partitionBy(*partition_by)
    return writer.partitionBy(partition_by)
 
 
def check_partition_compatibility(
    spark: SparkSession, table_name: str, cfg: dict
) -> None:
    """
    Verify that the config's partition_by matches the existing Delta table.
 
    Delta Lake does not support ALTER PARTITION — changing partition columns
    requires a full table rewrite. This check catches the mismatch at startup
    so the operator can handle it before any data is written.
 
    Called once per table at pipeline startup (before streams start).
    No-op if the Delta table does not exist yet (first run).
    """
    silver_path = cfg["silver_path"]
 
    if not DeltaTable.isDeltaTable(spark, silver_path):
        log.info(f"[{table_name}] Delta table does not exist — partition check skipped")
        return
 
    # Get existing partition columns from Delta metadata
    existing_partitions = sorted(
        DeltaTable.forPath(spark, silver_path)
        .detail()
        .first()["partitionColumns"] or []
    )
 
    # Get requested partition columns from config
    partition_by = cfg.get("partition_by")
    if not partition_by:
        config_partitions = []
    elif isinstance(partition_by, list):
        config_partitions = sorted(partition_by)
    else:
        config_partitions = [partition_by]
 
    if existing_partitions != config_partitions:
        from alerting import alert_partition_change_blocked
        alert_partition_change_blocked(
            table_name,
            str(existing_partitions),
            str(config_partitions)
        )
        raise ValueError(
            f"[{table_name}] partition_by change blocked on existing Delta table.\n"
            f"  Existing : {existing_partitions}\n"
            f"  Config   : {config_partitions}\n"
            f"  Action   : Revert config OR manually rewrite the Delta table."
        )
 
    log.info(f"[{table_name}] Partition check OK → {existing_partitions}")
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# EAV — UPSERT
# ═══════════════════════════════════════════════════════════════════════════════
 
def merge_eav(
    spark: SparkSession,
    typed_df: DataFrame,
    cfg: dict,
    batch_id: int
) -> None:
    """
    MERGE typed EAV wide rows into the Delta Silver table.
 
    Solves P4/P5 (null vs unchanged ambiguity) via CASE WHEN:
      CASE WHEN source.has_{col} = 1 THEN source.{col} ELSE target.{col} END
 
    Column derivation (all from table_config.yml, zero hardcoding):
      merge_key  → cfg["merge_key"]             e.g. "uuid"
      data_cols  → cfg["attributes"][*]["wide"]  e.g. ["pilot_id", "user_status", ...]
      prod_cols  → merge_key + data_cols + meta  e.g. ["uuid", "pilot_id", ..., "event_id", ...]
 
    On first run (Delta table absent):
      Creates the table with config-driven partition columns.
 
    typed_df expected columns:
      {merge_key}        — entity key (e.g. "uuid")
      {wide_col}         — typed production column per attribute
      has_{wide_col}     — presence flag (1=in batch, 0=absent)
      event_id, event_timestamp, event_date, deduptime
    """
    silver_path  = cfg["silver_path"]
    merge_key    = cfg["merge_key"]
    data_cols    = [info["wide"] for info in cfg["attributes"].values()]
    meta_cols    = ["event_id", "event_timestamp", "event_date", "deduptime"]
    prod_cols    = [merge_key] + data_cols + meta_cols
 
    # ── MERGE condition ───────────────────────────────────────────────────────
    merge_condition = f"target.{merge_key} = source.{merge_key}"
 
    # ── Update set: CASE WHEN for every data column ───────────────────────────
    # has=1 + val=X    → use source value (even if null = explicit delete)
    # has=0            → keep existing target value (not touched this batch)
    def case_when(col: str) -> str:
        return (
            f"CASE WHEN source.has_{col} = 1 "
            f"THEN source.{col} "
            f"ELSE target.{col} END"
        )
 
    update_set = {col: case_when(col) for col in data_cols}
    update_set["event_id"]        = "source.event_id"
    update_set["event_timestamp"] = "source.event_timestamp"
    update_set["event_date"]      = "source.event_date"
    update_set["deduptime"]       = "source.deduptime"
 
    # ── Insert values: all columns from source ────────────────────────────────
    insert_values = {col: f"source.{col}" for col in prod_cols}
 
    if DeltaTable.isDeltaTable(spark, silver_path):
        (
            DeltaTable.forPath(spark, silver_path)
            .alias("target")
            .merge(typed_df.alias("source"), merge_condition)
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_values)
            .execute()
        )
        log.info(f"[batch {batch_id}][{cfg['kafka_topic']}] EAV MERGE OK → {silver_path}")
    else:
        log.info(f"[batch {batch_id}] Creating EAV Delta table: {silver_path}")
        writer = (
            typed_df.select(prod_cols)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("compression", "snappy")
        )
        _apply_partition(writer, cfg.get("partition_by")).save(silver_path)
        log.info(f"[batch {batch_id}] EAV Delta table created, partition_by={cfg.get('partition_by')}")
 
 
def handle_eav_entity_deletes(
    spark: SparkSession,
    tombstones: DataFrame,
    cfg: dict,
    batch_id: int
) -> None:
    """
    Soft-delete EAV entities whose full partition was deleted in Cassandra.
 
    Tombstone = Debezium op=d + column2=null (range delete: DELETE WHERE key=X)
    Action: null all attribute columns, preserve the entity row (uuid intact).
 
    tombstones expected columns: entity_key, ts_ms, op
    """
    silver_path = cfg["silver_path"]
    if not DeltaTable.isDeltaTable(spark, silver_path):
        log.info(f"[batch {batch_id}] EAV Delta table absent — entity delete skipped")
        return
 
    data_cols  = [info["wide"] for info in cfg["attributes"].values()]
    merge_key  = cfg["merge_key"]
 
    deleted = (
        tombstones
        .groupBy("entity_key")
        .agg(F.max("ts_ms").alias("event_timestamp"))
        .withColumn("event_date",
                    F.to_date(F.from_unixtime(F.col("event_timestamp") / 1000)))
    )
 
    count = deleted.count()
    log.info(f"[batch {batch_id}] EAV entity delete: nulling {count} entity row(s)")
 
    (
        DeltaTable.forPath(spark, silver_path)
        .alias("target")
        .merge(
            deleted.alias("source"),
            f"target.{merge_key} = source.entity_key"
        )
        .whenMatchedUpdate(set={
            **{col: F.lit(None) for col in data_cols},
            "event_timestamp": "source.event_timestamp",
            "event_date":      "source.event_date",
            "deduptime":       "source.event_timestamp",
        })
        .execute()
    )
    log.info(f"[batch {batch_id}] EAV entity delete MERGE complete")
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# JSON_EMBEDDED — UPSERT
# ═══════════════════════════════════════════════════════════════════════════════
 
def _json_embedded_cols(cfg: dict):
    """
    Derive all column name lists for JSON_EMBEDDED from config.
    Returns (pk_cols, ck_cols, metric_cols, meta_cols, all_key_cols, all_cols)
    """
    pk_cols     = [pk["name"] for pk in cfg["partition_keys"]]
    ck_cols     = [ck["name"] for ck in cfg["clustering_keys"]]
    metric_cols = [
        field_info["col"]
        for source_fields in cfg["json_columns"].values()
        for field_info in source_fields.values()
    ]
    meta_cols     = ["event_id", "event_timestamp", "event_date"]
    all_key_cols  = pk_cols + ck_cols
    all_cols      = all_key_cols + metric_cols + meta_cols
    return pk_cols, ck_cols, metric_cols, meta_cols, all_key_cols, all_cols
 
 
def merge_json_embedded(
    spark: SparkSession,
    typed_df: DataFrame,
    cfg: dict,
    batch_id: int
) -> None:
    """
    MERGE JSON_EMBEDDED wide rows into Delta Silver.
 
    Simple full replace — one CDC event replaces all metric columns.
    No CASE WHEN needed: the json_value column holds all metrics in one update.
 
    Column derivation (all from table_config.yml):
      key cols    → cfg["partition_keys"] + cfg["clustering_keys"]
      metric cols → cfg["json_columns"][*][*]["col"]
      merge cond  → cfg["merge_key"] (list)
      partition   → cfg["partition_by"]
 
    update_set:    metric cols + meta (NOT key cols — they're the merge key)
    insert_values: all cols including key cols
    """
    silver_path = cfg["silver_path"]
    pk_cols, ck_cols, metric_cols, meta_cols, all_key_cols, all_cols = \
        _json_embedded_cols(cfg)
 
    # Build merge condition from config merge_key list
    merge_key  = cfg["merge_key"]    # e.g. ["country", "zipcode", "epoch_month", "minute_since_epoch"]
    merge_cond = " AND ".join(f"target.{k} = source.{k}" for k in merge_key)
 
    # Update: replace metrics + meta on match (key cols not in update — they matched)
    update_set    = {c: f"source.{c}" for c in metric_cols + meta_cols}
 
    # Insert: all columns including keys on new record
    insert_values = {c: f"source.{c}" for c in all_cols}
 
    if DeltaTable.isDeltaTable(spark, silver_path):
        (
            DeltaTable.forPath(spark, silver_path)
            .alias("target")
            .merge(typed_df.alias("source"), merge_cond)
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_values)
            .execute()
        )
        log.info(f"[batch {batch_id}][{cfg['kafka_topic']}] JSON_EMBEDDED MERGE OK → {silver_path}")
    else:
        log.info(f"[batch {batch_id}] Creating JSON_EMBEDDED Delta table: {silver_path}")
        writer = (
            typed_df.select(all_cols)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("compression", "snappy")
        )
        _apply_partition(writer, cfg.get("partition_by")).save(silver_path)
        log.info(f"[batch {batch_id}] JSON_EMBEDDED Delta table created, partition_by={cfg.get('partition_by')}")
 
 
def handle_json_embedded_entity_deletes(
    spark: SparkSession,
    tombstones: DataFrame,
    cfg: dict,
    batch_id: int
) -> None:
    """
    Null all metric columns for a deleted Cassandra partition.
 
    Tombstone = Debezium op=d + last clustering key is null.
    For weather: DELETE WHERE country=X AND zipcode=Y AND epoch_month=Z
    → Debezium: op=d, minute_since_epoch=null
    → Action: null ALL metric cols for every row matching the partition keys.
 
    tombstones expected columns: all partition_keys columns, ts_ms, op
    """
    silver_path = cfg["silver_path"]
    if not DeltaTable.isDeltaTable(spark, silver_path):
        log.info(f"[batch {batch_id}] JSON_EMBEDDED Delta table absent — entity delete skipped")
        return
 
    pk_cols = [pk["name"] for pk in cfg["partition_keys"]]
    metric_cols = [
        field_info["col"]
        for source_fields in cfg["json_columns"].values()
        for field_info in source_fields.values()
    ]
 
    # Group by partition keys to get one row per deleted partition
    deleted = (
        tombstones
        .groupBy(*pk_cols)
        .agg(F.max("ts_ms").alias("event_timestamp"))
        .withColumn("event_date",
                    F.to_date(F.from_unixtime(F.col("event_timestamp") / 1000)))
    )
 
    count = deleted.count()
    log.info(f"[batch {batch_id}] JSON_EMBEDDED partition delete: nulling {count} partition(s)")
 
    # Merge on partition keys only — nulls ALL rows for each matching partition
    merge_cond = " AND ".join(f"target.{pk} = source.{pk}" for pk in pk_cols)
 
    (
        DeltaTable.forPath(spark, silver_path)
        .alias("target")
        .merge(deleted.alias("source"), merge_cond)
        .whenMatchedUpdate(set={
            **{col: F.lit(None) for col in metric_cols},
            "event_timestamp": "source.event_timestamp",
            "event_date":      "source.event_date",
        })
        .execute()
    )
    log.info(f"[batch {batch_id}] JSON_EMBEDDED entity delete MERGE complete")
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# PLAIN — UPSERT
# ═══════════════════════════════════════════════════════════════════════════════
 
def _plain_cols(cfg: dict):
    """
    Derive column name lists for PLAIN from config.
    Returns (all_col_names, key_col_names, non_key_cols, meta_cols, all_cols)
    """
    all_col_names = [c["name"] for c in cfg["columns"]]
    key_col_names = set(
        [pk["name"] for pk in cfg["partition_keys"]]
        + [ck["name"] for ck in cfg.get("clustering_keys", [])]
    )
    non_key_cols = [c for c in all_col_names if c not in key_col_names]
    meta_cols    = ["event_id", "event_timestamp", "event_date"]
    all_cols     = all_col_names + meta_cols
    return all_col_names, key_col_names, non_key_cols, meta_cols, all_cols
 
 
def merge_plain(
    spark: SparkSession,
    typed_df: DataFrame,
    cfg: dict,
    batch_id: int
) -> None:
    """
    MERGE PLAIN table rows into Delta Silver.
 
    CASE WHEN partial update — Cassandra CDC only emits changed cells.
    A partial UPDATE emits null for untouched columns; CASE WHEN IS NOT NULL
    keeps existing Silver values for those columns.
 
    Column derivation (all from table_config.yml):
      all cols  → cfg["columns"][*]["name"]
      key cols  → cfg["partition_keys"] + cfg["clustering_keys"]
      merge key → cfg["merge_key"] (string or list)
      partition → cfg["partition_by"]
 
    update_set:    non-key data cols + meta (key cols are the merge key)
    insert_values: all cols including key cols
 
    typed_df expected columns:
      all column names from cfg["columns"] (typed correctly)
      event_id, event_timestamp, event_date
    """
    silver_path = cfg["silver_path"]
    all_col_names, key_col_names, non_key_cols, meta_cols, all_cols = \
        _plain_cols(cfg)
 
    # Build merge condition from config merge_key (string or list)
    merge_key = cfg["merge_key"]
    if isinstance(merge_key, str):
        merge_cond = f"target.{merge_key} = source.{merge_key}"
    else:
        merge_cond = " AND ".join(f"target.{k} = source.{k}" for k in merge_key)
 
    # Update: CASE WHEN IS NOT NULL for non-key cols, direct for meta cols.
    #
    # WHY: Cassandra CDC only writes changed cells to the commit log.
    # A partial UPDATE (SET region='East') emits null for every column
    # NOT in the UPDATE — pilot_name, utility, created_at arrive as null
    # even though they exist in Cassandra. Direct replace would overwrite
    # good Silver data with those nulls.
    #
    # IS NOT NULL → column was in the UPDATE → use new value
    # IS NULL     → column not touched → keep existing Silver value
    #
    # Edge case: explicit SET col=NULL is indistinguishable from "not in UPDATE".
    # Acceptable for POC. Production fix: add has_{col} flags (same as EAV).
    update_set = {
        **{c: f"CASE WHEN source.{c} IS NOT NULL THEN source.{c} ELSE target.{c} END"
           for c in non_key_cols},
        **{c: f"source.{c}" for c in meta_cols},
    }
 
    # Insert: all cols including keys
    insert_values = {c: f"source.{c}" for c in all_cols}
 
    if DeltaTable.isDeltaTable(spark, silver_path):
        (
            DeltaTable.forPath(spark, silver_path)
            .alias("target")
            .merge(typed_df.alias("source"), merge_cond)
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_values)
            .execute()
        )
        log.info(f"[batch {batch_id}][{cfg['kafka_topic']}] PLAIN MERGE OK → {silver_path}")
    else:
        log.info(f"[batch {batch_id}] Creating PLAIN Delta table: {silver_path}")
        writer = (
            typed_df.select(all_cols)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("compression", "snappy")
        )
        _apply_partition(writer, cfg.get("partition_by")).save(silver_path)
        log.info(f"[batch {batch_id}] PLAIN Delta table created, partition_by={cfg.get('partition_by')}")
 
 
def handle_plain_entity_deletes(
    spark: SparkSession,
    tombstones: DataFrame,
    cfg: dict,
    batch_id: int
) -> None:
    """
    Soft-delete PLAIN table rows deleted in Cassandra.
 
    For a simple PK table (no clustering keys): any op=d = full row delete.
    Action: null all non-key data columns, preserve the PK row for audit.
 
    tombstones expected columns: all partition_key columns, ts_ms, op
    """
    silver_path = cfg["silver_path"]
    if not DeltaTable.isDeltaTable(spark, silver_path):
        log.info(f"[batch {batch_id}] PLAIN Delta table absent — entity delete skipped")
        return
 
    all_col_names, key_col_names, non_key_cols, meta_cols, _ = _plain_cols(cfg)
 
    pk_cols    = [pk["name"] for pk in cfg["partition_keys"]]
    merge_key  = cfg["merge_key"]
    if isinstance(merge_key, str):
        merge_cond = f"target.{merge_key} = source.{merge_key}"
    else:
        merge_cond = " AND ".join(f"target.{k} = source.{k}" for k in merge_key)
 
    deleted = (
        tombstones
        .groupBy(*pk_cols)
        .agg(F.max("ts_ms").alias("event_timestamp"))
        .withColumn("event_date",
                    F.to_date(F.from_unixtime(F.col("event_timestamp") / 1000)))
    )
 
    count = deleted.count()
    log.info(f"[batch {batch_id}] PLAIN entity delete: nulling {count} row(s)")
 
    (
        DeltaTable.forPath(spark, silver_path)
        .alias("target")
        .merge(deleted.alias("source"), merge_cond)
        .whenMatchedUpdate(set={
            **{col: F.lit(None) for col in non_key_cols},
            "event_timestamp": "source.event_timestamp",
            "event_date":      "source.event_date",
        })
        .execute()
    )
    log.info(f"[batch {batch_id}] PLAIN entity delete MERGE complete")