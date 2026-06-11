"""
generic_pipeline.py
===================
Bidgely CDC Generic Pipeline — v2
 
Architecture (from design discussion):
  Source  : S3 Bronze (JSON Lines written by Kafka S3 Sink Connector)
  Read    : readStream.format("text"), recursiveFileLookup=true, pathGlobFilter="*.json"
  Config  : table_config.yml drives ALL logic — zero hardcoded table names
  Output  : S3 Silver (Delta Lake, MERGE/UPSERT)
 
Key design decisions implemented:
  ✓ Reads S3, NOT Kafka (NOTE 1 from architecture discussion)
  ✓ One readStream per table, per-table checkpoints (D4)
  ✓ Three table type handlers: EAV, JSON_EMBEDDED, PLAIN
  ✓ Error isolation: try/except per table → dead letter → alert → continue (D5)
  ✓ Full before/after observability tracing at every step (NOTE 3)
  ✓ Schema evolution: unknown EAV attributes → alert, pass through, don't crash
  ✓ Startup guards: config validate → partition check → Bronze scan → UDF register
  ✓ Corrupt record handling: null ts_ms → dead letter, pipeline continues
  ✓ Config-driven: add new table = add YAML block + restart, zero code changes
 
Problems from architecture discussion this solves (end-to-end):
  P1/P3/P16 — Entity spans multiple rows + mismatch: EAV pivot → one wide row
  P2        — CDC emits row-level, not entity-level: pivot aggregates per entity
  P4/P5     — Null vs unchanged: has_{col} CASE WHEN in MERGE
  P8/P9     — High update rates + ordering: in-batch dedup by (entity, attr, ts_ms)
  P10       — Consumer state management: per-table S3 checkpoints
  P11       — Dynamic schema: column2 values → config attributes dict
  P12       — Blobs: config-driven UDF per EAV table, closure captures type map
  P13       — S3 needs complete records: wide row from pivot
  P14       — Snapshot + CDC consistency: MERGE is idempotent
  P15       — Deletes: tombstone detection per table type
"""
 
import os
import logging
from typing import Optional
 
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, StringType
 
from delta.tables import DeltaTable
 
from config_validator import load_config
from generic_decoder import (
    SPARK_TYPE_MAP,
    make_eav_decoder,
    pivot_eav,
    cast_eav,
    expand_json_columns,
    parse_plain_fields,
)
from generic_merge import (
    write_dead_letter,
    check_partition_compatibility,
    merge_eav,
    handle_eav_entity_deletes,
    merge_json_embedded,
    handle_json_embedded_entity_deletes,
    merge_plain,
    handle_plain_entity_deletes,
)
from alerting import (
    alert_table_failure,
    alert_unknown_bronze_topic,
)
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("generic_pipeline")
 
 
# ─── Environment ──────────────────────────────────────────────────────────────
MINIO_ENDPOINT     = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY   = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY   = os.environ["MINIO_SECRET_KEY"]
TABLE_CONFIG_PATH  = os.environ.get("TABLE_CONFIG_PATH",  "/opt/spark/jobs/table_config.yml")
BRONZE_TOPICS_BASE = os.environ.get("BRONZE_TOPICS_BASE", "s3a://bronze/topics")
SILVER_BASE        = os.environ.get("SILVER_BASE",        "s3a://silver")
 
# ── EAV decoder UDF registry ──────────────────────────────────────────────────
# Populated at startup by register_udfs(config).
# Keyed by table_name → Spark UDF.
# Each UDF has its own closure capturing that table's attr_type_map.
_EAV_DECODER_UDFS: dict = {}
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# SPARKSESSION
# ═══════════════════════════════════════════════════════════════════════════════
 
def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BidgelyGenericCDCPipeline")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.hadoop.fs.s3a.endpoint",            MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",          MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",          MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",   "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# STARTUP FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════
 
def register_udfs(config: dict) -> None:
    """
    Create one EAV decoder UDF per EAV table and store in _EAV_DECODER_UDFS.
 
    Each UDF captures its table's attr_type_map in a closure.
    The dict is serialized with the UDF when Spark sends it to executors
    — no file read on the executor side.
    Runs once at startup before any stream starts.
    """
    for table_name, cfg in config.items():
        if cfg.get("type") == "EAV" and cfg.get("enabled", True):
            attr_type_map = {
                col2: attr_info["type"]
                for col2, attr_info in cfg["attributes"].items()
            }
            _EAV_DECODER_UDFS[table_name] = make_eav_decoder(attr_type_map, table_name)
            log.info(
                f"[{table_name}] EAV decoder UDF registered "
                f"({len(attr_type_map)} attributes: {list(attr_type_map.keys())})"
            )
 
 
def check_unconfigured_bronze_topics(spark: SparkSession, config: dict) -> None:
    """
    Scan s3a://bronze/topics/ at startup for topic directories that have
    no entry in table_config.yml.
 
    If the Kafka S3 Sink Connector has been writing data for a topic but no
    Spark stream exists for it, data accumulates silently.
    This check alerts the team so they can add a config entry.
 
    Non-fatal: logs warning and continues on any S3 access error.
    """
    try:
        sc   = spark.sparkContext
        URI  = sc._jvm.java.net.URI
        Path = sc._jvm.org.apache.hadoop.fs.Path
        FS   = sc._jvm.org.apache.hadoop.fs.FileSystem
 
        base_path = f"{BRONZE_TOPICS_BASE}/"
        fs   = FS.get(URI(base_path), sc._jsc.hadoopConfiguration())
        path = Path(base_path)
 
        if not fs.exists(path):
            log.info("Bronze topics directory does not exist yet — skipping scan")
            return
 
        configured_topics = {cfg["kafka_topic"] for cfg in config.values()}
        found_unconfigured = False
 
        for status in fs.listStatus(path):
            dir_name = status.getPath().getName()
            if dir_name not in configured_topics:
                alert_unknown_bronze_topic(f"{BRONZE_TOPICS_BASE}/{dir_name}")
                found_unconfigured = True
 
        if not found_unconfigured:
            log.info("Bronze topics scan: all topic directories have config entries ✓")
 
    except Exception as e:
        log.warning(f"Bronze topics scan failed (non-fatal): {e}")
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# EAV TABLE HANDLER
# ═══════════════════════════════════════════════════════════════════════════════
 
def make_eav_handler(spark: SparkSession, table_name: str, cfg: dict):
    """
    foreachBatch handler for EAV tables.
 
    Steps (all config-driven, full before/after tracing per step):
      1. Parse Debezium envelope  → ts_ms, op, entity_key, column2, value_cell
      2. Corrupt record filter    → null ts_ms → dead letter
      3. Tombstone check          → op=d AND column2=null → entity delete
      4. Blob decode              → UDF(column2, value_cell) → decoded_value
      5. In-batch dedup           → keep latest per (entity_key, column2)
      6. EAV → wide pivot         → one row per entity, _col + _has_col pairs
      7. Type cast + enrich       → production types, event_id, event_date
      8. Delta MERGE              → CASE WHEN has_{col} partial update
    """
    decoder_udf = _EAV_DECODER_UDFS[table_name]
    key_field   = cfg["cassandra_key_field"]   # e.g. "key" for users table
 
    def _handle(batch_df: DataFrame, batch_id: int) -> None:
        count = batch_df.count()
 
        print("\n" + "═" * 90)
        print(f"  BATCH #{batch_id}  |  TABLE: {table_name}  |  TYPE: EAV  |  {count} raw records")
        print("═" * 90)
 
        if count == 0:
            print("  Empty batch — nothing to process.")
            return
 
        # ── STEP 1: PARSE DEBEZIUM ENVELOPE ───────────────────────────────────
        print("\n▶ [STEP 1] PARSE DEBEZIUM ENVELOPE")
        print("--- BEFORE (raw S3 text line sample) ---")
        batch_df.select("json_str").limit(1).show(truncate=100)
 
        parsed = batch_df.select(
            F.get_json_object("json_str", "$.ts_ms").cast(LongType()).alias("ts_ms"),
            F.get_json_object("json_str", "$.op").alias("op"),
            # entity_key: extract from $.after.{cassandra_key_field}.value
            # cassandra_key_field is "key" for users — driven by table_config.yml
            F.get_json_object("json_str", f"$.after.{key_field}.value").alias("entity_key"),
            F.get_json_object("json_str", "$.after.column2.value").alias("column2"),
            # value_cell: extract FULL Debezium cell {"value":"...","deletion_ts":null}
            # NOT $.after.value.value — that would fail due to "value" name collision
            # The UDF unwraps the inner "value" field using Python json.loads
            F.get_json_object("json_str", "$.after.value").alias("value_cell"),
            F.col("json_str"),
        )
 
        # Pick one entity to trace through every step
        sample_row = parsed.filter(F.col("entity_key").isNotNull()).select("entity_key").first()
        sample_uuid = sample_row[0] if sample_row else None
        print(f"\n  [TRACE ENTITY] → {sample_uuid}")
 
        print("--- AFTER (parsed fields for trace entity) ---")
        parsed.filter(F.col("entity_key") == sample_uuid) \
              .select("ts_ms", "op", "entity_key", "column2", "value_cell") \
              .show(truncate=60)
 
        # ── STEP 2: CORRUPT RECORD FILTER ─────────────────────────────────────
        # Records with null ts_ms failed JSON parsing — route to dead letter
        corrupt = parsed.filter(F.col("ts_ms").isNull())
        corrupt_count = corrupt.count()
        if corrupt_count > 0:
            log.warning(f"[batch {batch_id}][{table_name}] {corrupt_count} corrupt record(s) → dead letter")
            write_dead_letter(corrupt, cfg, batch_id)
 
        parsed = parsed.filter(F.col("ts_ms").isNotNull())
        if parsed.count() == 0:
            print("  No valid records after corrupt filter.")
            return
 
        # ── STEP 3: TOMBSTONE CHECK ────────────────────────────────────────────
        # Range tombstone = op=d AND column2=null (DELETE WHERE key=<uuid>)
        # Action: null all attribute columns in Delta (soft delete, uuid preserved)
        tombstones = parsed.filter(
            F.col("entity_key").isNotNull() &
            F.col("column2").isNull() &
            (F.col("op") == "d")
        )
        tombstone_count = tombstones.count()
        if tombstone_count > 0:
            print(f"\n▶ [STEP 3 — TOMBSTONE DETECTED] {tombstone_count} full entity delete(s)")
            tombstones.filter(F.col("entity_key") == sample_uuid) \
                      .select("ts_ms", "op", "entity_key", "column2") \
                      .show(truncate=False)
            handle_eav_entity_deletes(spark, tombstones, cfg, batch_id)
 
        valid = parsed.filter(
            F.col("entity_key").isNotNull() &
            F.col("column2").isNotNull()
        )
        if valid.count() == 0:
            print("  No mutation events after tombstone filter.")
            return
 
        # ── STEP 4: BLOB DECODE ────────────────────────────────────────────────
        print("\n▶ [STEP 4] BLOB DECODE")
        print("--- BEFORE (raw base64 value_cell for trace entity) ---")
        valid.filter(F.col("entity_key") == sample_uuid) \
             .select("column2", "value_cell") \
             .show(truncate=60)
 
        # UDF is registered at startup with this table's attr_type_map baked in
        # Unknown attributes pass through as raw string + trigger schema evolution alert
        decoded = valid.withColumn(
            "decoded_value",
            decoder_udf(F.col("column2"), F.col("value_cell"))
        )
 
        print("--- AFTER (decoded values for trace entity) ---")
        decoded.filter(F.col("entity_key") == sample_uuid) \
               .select("column2", "value_cell", "decoded_value") \
               .show(truncate=60)
 
        # ── STEP 5: IN-BATCH DEDUPLICATION ────────────────────────────────────
        # Keeps latest event per (entity_key, column2) within this micro-batch
        # Solves P8 (high update rates) and P9 (ordering) from architecture discussion
        before_count = decoded.filter(F.col("entity_key") == sample_uuid).count()
        print(f"\n▶ [STEP 5] IN-BATCH DEDUPLICATION  ({before_count} rows for trace entity)")
        print("--- BEFORE ---")
        decoded.filter(F.col("entity_key") == sample_uuid) \
               .select("ts_ms", "column2", "decoded_value") \
               .orderBy("column2") \
               .show(truncate=False)
 
        w = Window.partitionBy("entity_key", "column2").orderBy(F.desc("ts_ms"))
        deduped = (
            decoded
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
 
        after_count = deduped.filter(F.col("entity_key") == sample_uuid).count()
        print(f"--- AFTER ({after_count} rows — duplicates removed, latest ts_ms wins) ---")
        deduped.filter(F.col("entity_key") == sample_uuid) \
               .select("ts_ms", "column2", "decoded_value") \
               .orderBy("column2") \
               .show(truncate=False)
 
        # ── STEP 6: EAV → WIDE PIVOT ──────────────────────────────────────────
        # Aggregates N vertical EAV rows → 1 wide row per entity
        # Creates _col staging + _has_col presence flag for each attribute
        # Config-driven: cfg["attributes"] replaces hardcoded COLUMN2_TO_WIDE
        print("\n▶ [STEP 6] EAV → WIDE PIVOT")
        print("--- BEFORE (vertical EAV rows for trace entity) ---")
        deduped.filter(F.col("entity_key") == sample_uuid) \
               .select("entity_key", "column2", "decoded_value") \
               .orderBy("column2") \
               .show(truncate=False)
 
        wide = pivot_eav(deduped, cfg, table_name)
 
        # Show first 3 attribute pairs in the wide row
        sample_attrs = list(cfg["attributes"].items())[:3]
        sample_wide_cols = ["entity_key"] + [
            col for attr_name, info in sample_attrs
            for col in [f"_{info['wide']}", f"_has_{info['wide']}"]
        ]
        print("--- AFTER (wide row for trace entity, first 3 attrs shown) ---")
        wide.filter(F.col("entity_key") == sample_uuid) \
            .select(*sample_wide_cols) \
            .show(truncate=False)
 
        # ── STEP 7: TYPE CAST + ENRICH ─────────────────────────────────────────
        # Renames entity_key → cfg["merge_key"] (e.g. "uuid")
        # Casts _col → col with production Spark type from config
        # Keeps has_col flags for MERGE CASE WHEN
        # Adds event_id (UUID), event_date (from ts_ms UTC)
        print("\n▶ [STEP 7] TYPE CAST + ENRICH")
        print("--- BEFORE (staging string types for trace entity) ---")
        first_attr_wide = list(cfg["attributes"].values())[0]["wide"]
        wide.filter(F.col("entity_key") == sample_uuid) \
            .select(f"_{first_attr_wide}", "event_timestamp") \
            .printSchema()
 
        typed = cast_eav(wide, cfg)
 
        print(f"--- AFTER (production types + event_id + event_date for trace entity) ---")
        typed.filter(F.col(cfg["merge_key"]) == sample_uuid) \
             .select(cfg["merge_key"], first_attr_wide, "event_id", "event_date", "event_timestamp") \
             .show(truncate=False, vertical=True)
 
        # ── STEP 8: DELTA MERGE ────────────────────────────────────────────────
        print("\n▶ [STEP 8] DELTA MERGE (CASE WHEN partial update)")
        print("    has=1 + val=X    → UPDATE to X (even if null = explicit delete)")
        print("    has=1 + val=None → SET column to null")
        print("    has=0            → KEEP existing Delta value (not in this batch)")
        print("--- BEFORE (current Delta version) ---")
        if DeltaTable.isDeltaTable(spark, cfg["silver_path"]):
            DeltaTable.forPath(spark, cfg["silver_path"]).history(1) \
                      .select("version", "timestamp", "operation").show(truncate=False)
        else:
            print("  Delta table does not exist yet — will be created on this write.")
 
        entity_count = typed.count()
        log.info(f"[batch {batch_id}][{table_name}] {entity_count} entity(ies) → MERGE")
        merge_eav(spark, typed, cfg, batch_id)
 
        print("--- AFTER ---")
        DeltaTable.forPath(spark, cfg["silver_path"]).history(1) \
                  .select("version", "timestamp", "operation", "operationMetrics") \
                  .show(truncate=80)
 
        print("\n" + "═" * 90)
        print(f"  BATCH #{batch_id} | {table_name} | EAV | COMPLETE ({entity_count} entities merged)")
        print("═" * 90 + "\n")
 
    return _handle
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# JSON_EMBEDDED TABLE HANDLER
# ═══════════════════════════════════════════════════════════════════════════════
 
def make_json_embedded_handler(spark: SparkSession, table_name: str, cfg: dict):
    """
    foreachBatch handler for JSON_EMBEDDED tables.
 
    Steps:
      1. Parse Debezium envelope → PK/CK fields + JSON text column + ts_ms, op
      2. Corrupt record filter   → null ts_ms → dead letter
      3. Tombstone check         → last clustering key null → partition delete
      4. In-batch dedup          → keep latest per full primary key
      5. JSON column expansion   → json_value text → N typed metric columns
      6. Enrich                  → event_id, event_timestamp, event_date
      7. Delta MERGE             → simple full replace per PK (no CASE WHEN)
 
    No blob decode needed — json_value is a TEXT column, not a blob.
    No EAV pivot needed — 1 CDC event = 1 complete record.
    """
    partition_keys  = cfg["partition_keys"]
    clustering_keys = cfg["clustering_keys"]
    json_col_names  = list(cfg["json_columns"].keys())
    all_key_names   = [pk["name"] for pk in partition_keys] + \
                      [ck["name"] for ck in clustering_keys]
 
    def _handle(batch_df: DataFrame, batch_id: int) -> None:
        count = batch_df.count()
 
        print("\n" + "═" * 90)
        print(f"  BATCH #{batch_id}  |  TABLE: {table_name}  |  TYPE: JSON_EMBEDDED  |  {count} records")
        print("═" * 90)
 
        if count == 0:
            print("  Empty batch — nothing to process.")
            return
 
        # ── STEP 1: PARSE DEBEZIUM ENVELOPE ───────────────────────────────────
        print("\n▶ [STEP 1] PARSE DEBEZIUM ENVELOPE")
        print("--- BEFORE (raw S3 text line sample) ---")
        batch_df.select("json_str").limit(1).show(truncate=100)
 
        select_exprs = [
            F.get_json_object("json_str", "$.ts_ms").cast(LongType()).alias("ts_ms"),
            F.get_json_object("json_str", "$.op").alias("op"),
        ]
        # Extract each partition key field from Debezium cell
        for pk in partition_keys:
            spark_type = SPARK_TYPE_MAP.get(pk["type"], StringType())
            select_exprs.append(
                F.get_json_object("json_str", f"$.after.{pk['name']}.value")
                .cast(spark_type).alias(pk["name"])
            )
        # Extract each clustering key field
        for ck in clustering_keys:
            spark_type = SPARK_TYPE_MAP.get(ck["type"], StringType())
            select_exprs.append(
                F.get_json_object("json_str", f"$.after.{ck['name']}.value")
                .cast(spark_type).alias(ck["name"])
            )
        # Extract the JSON text column(s) — json_value contains all metrics
        for jcol_name in json_col_names:
            select_exprs.append(
                F.get_json_object("json_str", f"$.after.{jcol_name}.value")
                .alias(jcol_name)
            )
        select_exprs.append(F.col("json_str"))
 
        parsed = batch_df.select(select_exprs) \
                         .filter(F.col(partition_keys[0]["name"]).isNotNull())
 
        # Pick a sample record to trace
        sample = parsed.filter(F.col(partition_keys[0]["name"]).isNotNull()).first()
        if sample is None:
            print("  No valid records after parse.")
            return
 
        trace_vals = {k: sample[k] for k in all_key_names if sample[k] is not None}
        trace_label = ", ".join(f"{k}={v}" for k, v in trace_vals.items())
        trace_filter = F.lit(True)
        for k, v in trace_vals.items():
            trace_filter = trace_filter & (F.col(k) == v)
 
        print(f"\n  [TRACE RECORD] → {trace_label}")
        print("--- AFTER (parsed fields for trace record) ---")
        parsed.filter(trace_filter) \
              .select(["ts_ms", "op"] + all_key_names + [json_col_names[0]]) \
              .show(truncate=80, vertical=True)
 
        # ── STEP 2: CORRUPT RECORD FILTER ─────────────────────────────────────
        corrupt = parsed.filter(F.col("ts_ms").isNull())
        corrupt_count = corrupt.count()
        if corrupt_count > 0:
            log.warning(f"[batch {batch_id}][{table_name}] {corrupt_count} corrupt record(s) → dead letter")
            write_dead_letter(corrupt, cfg, batch_id)
        parsed = parsed.filter(F.col("ts_ms").isNotNull())
 
        # ── STEP 3: TOMBSTONE CHECK ────────────────────────────────────────────
        # Range tombstone: last clustering key is null
        # e.g. DELETE WHERE country=X AND zipcode=Y AND epoch_month=Z
        # → minute_since_epoch is null in Debezium event
        if clustering_keys:
            last_ck = clustering_keys[-1]["name"]
            tombstones = parsed.filter(
                F.col(partition_keys[0]["name"]).isNotNull() &
                F.col(last_ck).isNull() &
                (F.col("op") == "d")
            )
            tombstone_count = tombstones.count()
            if tombstone_count > 0:
                print(f"\n▶ [STEP 3 — TOMBSTONE DETECTED] {tombstone_count} partition delete(s)")
                tombstones.select(["ts_ms", "op"] + [pk["name"] for pk in partition_keys]) \
                          .show(truncate=False)
                handle_json_embedded_entity_deletes(spark, tombstones, cfg, batch_id)
 
            parsed = parsed.filter(F.col(last_ck).isNotNull())
 
        if parsed.count() == 0:
            print("  No mutation events after tombstone filter.")
            return
 
        # ── STEP 4: IN-BATCH DEDUPLICATION ────────────────────────────────────
        before_count = parsed.filter(trace_filter).count()
        print(f"\n▶ [STEP 4] IN-BATCH DEDUPLICATION  ({before_count} rows for trace record)")
        print("--- BEFORE ---")
        parsed.filter(trace_filter) \
              .select(["ts_ms"] + all_key_names) \
              .show(truncate=False)
 
        w = Window.partitionBy(*all_key_names).orderBy(F.desc("ts_ms"))
        deduped = (
            parsed
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
 
        after_count = deduped.filter(trace_filter).count()
        print(f"--- AFTER ({after_count} row — latest ts_ms wins) ---")
 
        # ── STEP 5: JSON COLUMN EXPANSION ─────────────────────────────────────
        # json_value text → N individual typed columns (e.g. 20 float metrics)
        # No blob decode — json_value is TEXT, not a blob
        # No EAV pivot — 1 CDC event = 1 complete record
        print(f"\n▶ [STEP 5] JSON COLUMN EXPANSION")
        print(f"    No blob decode — {json_col_names[0]} is TEXT not binary blob")
        print(f"    No EAV pivot — 1 CDC event = 1 complete {table_name} record")
        print(f"--- BEFORE (raw {json_col_names[0]} for trace record) ---")
        deduped.filter(trace_filter).select(json_col_names[0]).show(truncate=False, vertical=True)
 
        expanded = expand_json_columns(deduped, cfg)
 
        print("--- AFTER (sample expanded metric columns for trace record) ---")
        first_5_metrics = [
            field_info["col"]
            for source_fields in cfg["json_columns"].values()
            for field_info in list(source_fields.values())[:5]
        ]
        expanded.filter(trace_filter).select(first_5_metrics).show(truncate=False)
 
        # ── STEP 6: ENRICH ─────────────────────────────────────────────────────
        print("\n▶ [STEP 6] METADATA ENRICHMENT")
        typed = (
            expanded
            .withColumn("event_id",        F.expr("uuid()"))
            .withColumn("event_timestamp",  F.col("ts_ms"))
            .withColumn(
                "event_date",
                F.to_date(F.from_unixtime(F.col("ts_ms") / 1000))
            )
            .drop("json_str", "ts_ms", "op", *json_col_names)
        )
 
        print("--- AFTER (enriched record for trace record) ---")
        typed.filter(trace_filter) \
             .select(all_key_names + ["event_id", "event_timestamp", "event_date"]) \
             .show(truncate=False, vertical=True)
 
        # ── STEP 7: DELTA MERGE ────────────────────────────────────────────────
        print("\n▶ [STEP 7] DELTA MERGE (full record replace — no CASE WHEN)")
        print("    1 CDC event = all metrics replaced (json_value is one column)")
        print("--- BEFORE ---")
        if DeltaTable.isDeltaTable(spark, cfg["silver_path"]):
            DeltaTable.forPath(spark, cfg["silver_path"]).history(1) \
                      .select("version", "timestamp", "operation").show(truncate=False)
        else:
            print("  Delta table does not exist yet — will be created.")
 
        record_count = typed.count()
        log.info(f"[batch {batch_id}][{table_name}] {record_count} record(s) → MERGE")
        merge_json_embedded(spark, typed, cfg, batch_id)
 
        print("--- AFTER ---")
        DeltaTable.forPath(spark, cfg["silver_path"]).history(1) \
                  .select("version", "timestamp", "operation", "operationMetrics") \
                  .show(truncate=80)
 
        print("\n" + "═" * 90)
        print(f"  BATCH #{batch_id} | {table_name} | JSON_EMBEDDED | COMPLETE ({record_count} records merged)")
        print("═" * 90 + "\n")
 
    return _handle
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# PLAIN TABLE HANDLER
# ═══════════════════════════════════════════════════════════════════════════════
 
def make_plain_handler(spark: SparkSession, table_name: str, cfg: dict):
    """
    foreachBatch handler for PLAIN tables.
 
    Steps:
      1. Parse Debezium envelope  → all column values extracted, no blob decode
      2. Corrupt record filter    → null ts_ms → dead letter
      3. Delete check             → op=d → row delete
      4. In-batch dedup           → keep latest per primary key
      5. Enrich                   → event_id, event_timestamp, event_date
      6. Delta MERGE              → simple full replace per PK
 
    No blob decode: Debezium emits native JSON types for standard Cassandra columns.
    No EAV pivot: 1 CDC event = 1 complete row.
    """
    partition_keys  = cfg["partition_keys"]
    clustering_keys = cfg.get("clustering_keys", [])
    all_key_names   = [pk["name"] for pk in partition_keys] + \
                      [ck["name"] for ck in clustering_keys]
    pk_col          = partition_keys[0]["name"]
 
    def _handle(batch_df: DataFrame, batch_id: int) -> None:
        count = batch_df.count()
 
        print("\n" + "═" * 90)
        print(f"  BATCH #{batch_id}  |  TABLE: {table_name}  |  TYPE: PLAIN  |  {count} records")
        print("═" * 90)
 
        if count == 0:
            print("  Empty batch — nothing to process.")
            return
 
        # ── STEP 1: PARSE DEBEZIUM ENVELOPE ───────────────────────────────────
        print("\n▶ [STEP 1] PARSE DEBEZIUM ENVELOPE")
        print("    No blob decode — Debezium emits native JSON types for standard columns")
        print(f"    int → JSON integer, boolean → JSON boolean, text → JSON string")
        print("--- BEFORE (raw S3 text line sample) ---")
        batch_df.select("json_str").limit(1).show(truncate=100)
 
        parsed = parse_plain_fields(batch_df, cfg)
 
        sample = parsed.filter(F.col(pk_col).isNotNull()).first()
        if sample is None:
            print("  No valid records after parse.")
            return
 
        sample_pk = sample[pk_col]
        trace_filter = F.col(pk_col) == sample_pk
        print(f"\n  [TRACE RECORD] → {pk_col}={sample_pk}")
        print("--- AFTER (all typed column values for trace record) ---")
        parsed.filter(trace_filter).drop("json_str").show(truncate=False, vertical=True)
 
        # ── STEP 2: CORRUPT RECORD FILTER ─────────────────────────────────────
        corrupt = parsed.filter(F.col("ts_ms").isNull())
        corrupt_count = corrupt.count()
        if corrupt_count > 0:
            log.warning(f"[batch {batch_id}][{table_name}] {corrupt_count} corrupt record(s) → dead letter")
            write_dead_letter(corrupt, cfg, batch_id)
        parsed = parsed.filter(F.col("ts_ms").isNotNull())
 
        # ── STEP 3: DELETE CHECK ───────────────────────────────────────────────
        # For PLAIN tables with simple PK (no clustering keys): op=d = full row delete
        # Action: null all non-key columns in Delta (soft delete, PK preserved)
        deletes = parsed.filter(
            F.col(pk_col).isNotNull() &
            (F.col("op") == "d")
        )
        delete_count = deletes.count()
        if delete_count > 0:
            print(f"\n▶ [STEP 3 — DELETE DETECTED] {delete_count} row delete(s)")
            deletes.select(["ts_ms", "op", pk_col]).show(truncate=False)
            handle_plain_entity_deletes(spark, deletes, cfg, batch_id)
 
        valid = parsed.filter(F.col("op") != "d")
        if valid.count() == 0:
            print("  No insert/update events after delete filter.")
            return
 
        # ── STEP 4: IN-BATCH DEDUPLICATION ────────────────────────────────────
        before_count = valid.filter(trace_filter).count()
        print(f"\n▶ [STEP 4] IN-BATCH DEDUPLICATION  ({before_count} row(s) for trace record)")
        print("--- BEFORE ---")
        valid.filter(trace_filter).select(["ts_ms"] + all_key_names).show(truncate=False)
 
        w = Window.partitionBy(*all_key_names).orderBy(F.desc("ts_ms"))
        deduped = (
            valid
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
 
        after_count = deduped.filter(trace_filter).count()
        print(f"--- AFTER ({after_count} row — latest ts_ms wins) ---")
 
        # ── STEP 5: ENRICH ─────────────────────────────────────────────────────
        print("\n▶ [STEP 5] METADATA ENRICHMENT")
        print("    No EAV pivot — 1 CDC event = 1 complete row replacement")
        print("    No CASE WHEN — PLAIN MERGE does full column replace")
 
        typed = (
            deduped
            .withColumn("event_id",        F.expr("uuid()"))
            .withColumn("event_timestamp",  F.col("ts_ms"))
            .withColumn(
                "event_date",
                F.to_date(F.from_unixtime(F.col("ts_ms") / 1000))
            )
            .drop("json_str", "ts_ms", "op")
        )
 
        print("--- AFTER (production row for trace record) ---")
        typed.filter(trace_filter).show(truncate=False, vertical=True)
 
        # ── STEP 6: DELTA MERGE ────────────────────────────────────────────────
        print("\n▶ [STEP 6] DELTA MERGE (full row replace)")
        print("--- BEFORE ---")
        if DeltaTable.isDeltaTable(spark, cfg["silver_path"]):
            DeltaTable.forPath(spark, cfg["silver_path"]).history(1) \
                      .select("version", "timestamp", "operation").show(truncate=False)
        else:
            print("  Delta table does not exist yet — will be created.")
 
        row_count = typed.count()
        log.info(f"[batch {batch_id}][{table_name}] {row_count} row(s) → MERGE")
        merge_plain(spark, typed, cfg, batch_id)
 
        print("--- AFTER ---")
        DeltaTable.forPath(spark, cfg["silver_path"]).history(1) \
                  .select("version", "timestamp", "operation", "operationMetrics") \
                  .show(truncate=80)
 
        print("\n" + "═" * 90)
        print(f"  BATCH #{batch_id} | {table_name} | PLAIN | COMPLETE ({row_count} rows merged)")
        print("═" * 90 + "\n")
 
    return _handle
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# ERROR ISOLATION WRAPPER
# ═══════════════════════════════════════════════════════════════════════════════
 
def make_safe_handler(handler_fn, table_name: str, cfg: dict):
    """
    Wraps any foreachBatch handler with per-table error isolation.
 
    On failure:
      1. Write failed batch to dead-letter path
      2. Send alert (logs + TODO: production webhook)
      3. DO NOT re-raise — stream continues for THIS table
      4. All other table streams are completely unaffected
 
    This is D5 from the architecture decisions:
      "if some table failed give me an alert but process the remaining ones"
    """
    def _safe(batch_df: DataFrame, batch_id: int) -> None:
        try:
            handler_fn(batch_df, batch_id)
        except Exception as e:
            log.error(
                f"[batch {batch_id}][{table_name}] HANDLER FAILED — "
                f"writing to dead letter and continuing",
                exc_info=True
            )
            write_dead_letter(batch_df, cfg, batch_id)
            alert_table_failure(table_name, batch_id, e)
            # DO NOT re-raise — this stream continues processing future batches
 
    return _safe
 
 
def make_handler(spark: SparkSession, table_name: str, cfg: dict):
    """Routes to the correct type handler and wraps with error isolation."""
    table_type = cfg["type"]
 
    if table_type == "EAV":
        handler = make_eav_handler(spark, table_name, cfg)
    elif table_type == "JSON_EMBEDDED":
        handler = make_json_embedded_handler(spark, table_name, cfg)
    elif table_type == "PLAIN":
        handler = make_plain_handler(spark, table_name, cfg)
    else:
        raise ValueError(f"Unknown table type '{table_type}' for table '{table_name}'")
 
    return make_safe_handler(handler, table_name, cfg)
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
 
 
def ensure_bronze_path_exists(spark: SparkSession, table_name: str, bronze_path: str) -> None:
    """
    Create the Bronze S3 path if it does not exist.
 
    Spark readStream throws PATH_NOT_FOUND immediately on startup if the
    Bronze path doesn't exist yet — this happens on a fresh stack before
    the Kafka S3 Sink Connector has written any files.
 
    Creating the directory here means Spark finds the path, starts the
    stream successfully, and simply sees zero files until the connector
    writes the first batch.
    """
    try:
        sc   = spark.sparkContext
        URI  = sc._jvm.java.net.URI
        Path = sc._jvm.org.apache.hadoop.fs.Path
        FS   = sc._jvm.org.apache.hadoop.fs.FileSystem
 
        fs = FS.get(URI(bronze_path + "/"), sc._jsc.hadoopConfiguration())
        p  = Path(bronze_path)
 
        if not fs.exists(p):
            fs.mkdirs(p)
            log.info(f"[{table_name}] Bronze path created: {bronze_path}")
        else:
            log.info(f"[{table_name}] Bronze path exists: {bronze_path}")
    except Exception as e:
        log.warning(f"[{table_name}] Could not create Bronze path (non-fatal): {e}")
 
 
def main() -> None:
    log.info("=" * 70)
    log.info("  Bidgely CDC Generic Pipeline — v2  STARTING")
    log.info("  Source : S3 Bronze (Kafka S3 Sink Connector JSON Lines)")
    log.info("  Config : %s", TABLE_CONFIG_PATH)
    log.info("  Format : readStream.format('text'), recursiveFileLookup=true")
    log.info("  Types  : EAV | JSON_EMBEDDED | PLAIN")
    log.info("=" * 70)
 
    # ── Step 1: Load and validate config ──────────────────────────────────────
    # Fails immediately with clear errors if any config entry is wrong
    config = load_config(TABLE_CONFIG_PATH)
 
    # ── Step 2: Build SparkSession ────────────────────────────────────────────
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
 
    # ── Step 3: Register EAV decoder UDFs ─────────────────────────────────────
    # Each EAV table gets its own UDF with its attr_type_map baked into the closure
    register_udfs(config)
 
    # ── Step 4: Check partition compatibility on existing Delta tables ─────────
    # Blocks startup if config tries to change partition_by on an existing table
    for table_name, cfg in config.items():
        if cfg.get("enabled", True):
            check_partition_compatibility(spark, table_name, cfg)
 
    # ── Step 5: Scan Bronze for unconfigured topics ────────────────────────────
    # Alerts on any Bronze S3 prefix that has no matching config entry
    check_unconfigured_bronze_topics(spark, config)
 
    # ── Step 6: Create one readStream per enabled table ────────────────────────
    #
    # WHY format("text") not format("json"):
    #   format("json") requires a schema defined upfront. All 3 table types
    #   have different "after" structures. format("text") reads each line as a
    #   string — identical to how Kafka messages were read in v1 as json_str.
    #
    # WHY recursiveFileLookup=true:
    #   Kafka S3 Sink Connector writes to date-partitioned subdirectories:
    #   bronze/topics/{topic}/year=.../month=.../day=.../hour=.../file.json
    #   Without this, Spark only looks at the top level and finds nothing.
    #
    # WHY one stream per table (not one stream for all):
    #   Per-table streams give independent checkpoints — you can replay
    #   or reset one table without affecting others (D4 decision).
    #
    queries = []
    for table_name, cfg in config.items():
        if not cfg.get("enabled", True):
            log.info(f"[{table_name}] DISABLED in config — skipping stream")
            continue
 
        log.info(
            f"[{table_name}] Starting stream "
            f"type={cfg['type']} "
            f"source={cfg['bronze_path']}"
        )
 
        # Create Bronze path if missing — prevents PATH_NOT_FOUND on fresh start
        ensure_bronze_path_exists(spark, table_name, cfg["bronze_path"])
 
        stream = (
            spark.readStream
            .format("text")
            .option("path",                cfg["bronze_path"])
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter",      "*.json")
            .load()
            .withColumnRenamed("value", "json_str")  # "value" is Spark's default text column
            .writeStream
            .foreachBatch(make_handler(spark, table_name, cfg))
            .option("checkpointLocation", cfg["checkpoint_path"])
            .trigger(processingTime="30 seconds")
            .start()
        )
 
        queries.append((table_name, stream))
        log.info(f"[{table_name}] Stream started ✓  checkpoint={cfg['checkpoint_path']}")
 
    if not queries:
        log.error("No enabled tables found in config. Exiting.")
        return
 
    active_tables = [t for t, _ in queries]
    log.info(f"\n{len(queries)} stream(s) running: {active_tables}")
    log.info("Pipeline is live. Waiting for termination...\n")
 
    spark.streams.awaitAnyTermination()
 
 
if __name__ == "__main__":
    main()