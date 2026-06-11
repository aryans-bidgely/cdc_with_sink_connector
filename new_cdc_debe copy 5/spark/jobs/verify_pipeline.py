"""
verify_pipeline.py
==================
Verification script for the Bidgely Generic CDC Pipeline — v2
 
Reads Bronze and Silver for ALL tables in table_config.yml and prints
stats, schema, sample rows, Delta history, and dead-letter status.
 
Changes from v1:
  - Config-driven: reads table_config.yml, loops over all 3 table types
  - Bronze now read with format("text") + recursiveFileLookup=true
    (Kafka S3 Sink Connector writes raw JSON lines, no kafka_ts/ingestion_date)
  - Bronze paths: s3a://bronze/topics/{kafka_topic}/ (not s3a://bronze/bidgely/)
  - pilot_info (PLAIN) added as third table
  - Dead-letter check per table
 
Run inside spark-etl container:
  docker exec spark-etl /opt/spark/bin/spark-submit \\
    --master local[1] /opt/spark/jobs/verify_pipeline.py
"""
 
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
 
from config_validator import load_config
 
# ─── Environment ──────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
TABLE_CONFIG_PATH = os.environ.get("TABLE_CONFIG_PATH", "/opt/spark/jobs/table_config.yml")
 
# ─── SparkSession ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("VerifyPipeline_v2")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
spark.sparkContext.setLogLevel("ERROR")
 
# ─── Load config ──────────────────────────────────────────────────────────────
config = load_config(TABLE_CONFIG_PATH)
 
print("\n" + "=" * 70)
print("  BIDGELY CDC PIPELINE v2 — VERIFICATION REPORT")
print(f"  Config : {TABLE_CONFIG_PATH}")
print(f"  Tables : {list(config.keys())}")
print("=" * 70)
 
# ─── Results collector ────────────────────────────────────────────────────────
results = {}
 
# ═══════════════════════════════════════════════════════════════════════════════
# LOOP: check each enabled table
# ═══════════════════════════════════════════════════════════════════════════════
 
for table_name, cfg in config.items():
    if not cfg.get("enabled", True):
        print(f"\n  [{table_name}] DISABLED — skipping")
        continue
 
    table_type = cfg["type"]
 
    print(f"\n{'─' * 70}")
    print(f"  TABLE : {table_name}  |  TYPE : {table_type}")
    print(f"{'─' * 70}")
 
    # ── BRONZE CHECK ──────────────────────────────────────────────────────────
    # v2 Bronze is written by Kafka S3 Sink Connector as JSON Lines.
    # Files are at: bronze/topics/{kafka_topic}/year=.../month=.../day=.../hour=.../
    # Read with format("text") + recursiveFileLookup=true — identical to pipeline.
    # Each row: one column "value" = raw Debezium JSON string.
    # No kafka_ts / kafka_offset / ingestion_date — those were v1 Spark-written fields.
    print(f"\n[ BRONZE — {cfg['bronze_path']} ]")
    try:
        bronze = (
            spark.read
            .format("text")
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", "*.json")
            .load(cfg["bronze_path"])
            .withColumnRenamed("value", "json_str")
        )
 
        b_count = bronze.count()
        print(f"  Raw JSON lines (CDC events) : {b_count}")
 
        if b_count > 0:
            # Parse ts_ms and op to show useful stats without loading all data
            parsed_sample = (
                bronze
                .withColumn("ts_ms", F.get_json_object("json_str", "$.ts_ms"))
                .withColumn("op",    F.get_json_object("json_str", "$.op"))
            )
 
            ops = {
                r.op: r.cnt
                for r in parsed_sample
                    .groupBy("op").agg(F.count("*").alias("cnt"))
                    .collect()
            }
            print(f"  Operation breakdown         : {ops}")
 
            # Show sample raw JSON lines
            print(f"\n  Sample (3 raw CDC events):")
            bronze.limit(3).show(truncate=100)
 
    except Exception as e:
        print(f"  ⚠  Bronze not yet created or empty: {e}")
        print(f"     → Kafka S3 Sink Connector may still be starting up")
        print(f"     → Expected path: {cfg['bronze_path']}")
 
    # ── SILVER CHECK ──────────────────────────────────────────────────────────
    print(f"\n[ SILVER DELTA — {cfg['silver_path']} ]")
 
    silver_exists = DeltaTable.isDeltaTable(spark, cfg["silver_path"])
 
    if not silver_exists:
        print(f"  ⚠  Delta table does not exist yet.")
        print(f"     → Wait 30-60s for the first Spark micro-batch to complete.")
        print(f"     → Then re-run this script.")
        results[table_name] = {"silver": False, "count": 0, "type": table_type}
    else:
        silver = spark.read.format("delta").load(cfg["silver_path"])
        total  = silver.count()
 
        print(f"  Total rows : {total}")
 
        # Show partition column distinct values (useful for verifying data routing)
        partition_by = cfg.get("partition_by")
        if partition_by:
            pcols = partition_by if isinstance(partition_by, list) else [partition_by]
            for pcol in pcols:
                try:
                    vals = sorted([
                        r[pcol] for r in silver.select(pcol).distinct().collect()
                        if r[pcol] is not None
                    ])
                    print(f"  Distinct {pcol:20} : {vals}")
                except Exception:
                    pass
 
        # Type-specific additional stats
        if table_type == "JSON_EMBEDDED":
            for pk in cfg["partition_keys"]:
                try:
                    vals = sorted([
                        r[pk["name"]] for r in
                        silver.select(pk["name"]).distinct().collect()
                        if r[pk["name"]] is not None
                    ])
                    print(f"  Distinct {pk['name']:20} : {vals}")
                except Exception:
                    pass
 
        # Schema
        print(f"\n  Schema:")
        silver.printSchema()
 
        # All rows
        print(f"\n  All rows:")
        silver.show(truncate=False)
 
        # Delta history
        print(f"\n  Delta history (last 5 operations):")
        DeltaTable.forPath(spark, cfg["silver_path"]).history(5).select(
            "version", "timestamp", "operation", "operationMetrics"
        ).show(truncate=60)
 
        results[table_name] = {"silver": True, "count": total, "type": table_type}
 
    # ── DEAD-LETTER CHECK ──────────────────────────────────────────────────────
    print(f"\n[ DEAD-LETTER — {cfg['dead_letter_path']} ]")
    try:
        dl = (
            spark.read
            .format("text")
            .option("recursiveFileLookup", "true")
            .load(cfg["dead_letter_path"])
        )
        dl_count = dl.count()
        if dl_count > 0:
            print(f"  ⚠  {dl_count} failed record(s) found in dead-letter path")
            print(f"     → Investigate: docker exec spark-etl ...")
            dl.limit(3).show(truncate=80)
        else:
            print(f"  ✓  No failed records (dead-letter path empty)")
    except Exception:
        print(f"  ✓  No failed records (dead-letter path does not exist)")
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════
 
print("\n" + "=" * 70)
print("  SUMMARY")
print("=" * 70)
 
all_ok = True
for table_name, res in results.items():
    exists  = res["silver"]
    count   = res["count"]
    ttype   = res["type"]
    status  = "✅ EXISTS" if exists else "❌ NOT CREATED"
    if not exists:
        all_ok = False
    print(f"  {table_name:<20} [{ttype:<13}] : {status}  ({count} rows)")
 
# Tables that were disabled
for table_name, cfg in config.items():
    if not cfg.get("enabled", True):
        print(f"  {table_name:<20} [DISABLED     ] : — (skipped)")
 
print("─" * 70)
if all_ok and results:
    print("  ✅ All tables verified successfully")
else:
    print("  ⚠  Some tables not yet created — pipeline may still be processing")
    print("     → Re-run after 30-60s for first batch to complete")
 
print("=" * 70 + "\n")
 
spark.stop()
 