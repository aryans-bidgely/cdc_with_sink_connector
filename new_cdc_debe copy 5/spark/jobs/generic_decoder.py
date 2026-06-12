"""
generic_decoder.py
==================
Config-driven decoder for all three Cassandra CDC table types.
 
Replaces v1's blob_decoder.py + weather_decoder.py.
 
─────────────────────────────────────────────────────────────────────────────
EAV TABLES (e.g. bidgely.users)
─────────────────────────────────────────────────────────────────────────────
make_eav_decoder(attr_type_map, table_name)
  Returns a Spark UDF. The UDF logic is identical to v1's decode_blob()
  EXCEPT the ATTRIBUTE_TYPE_MAP is now passed in from table_config.yml
  instead of being hardcoded.
 
  Why Python UDF instead of Spark get_json_object:
    The Cassandra EAV 'value' column produces this in Debezium:
      "value": {"value": "AAAnlA==", "deletion_ts": null, "set": true}
    The column name is "value" AND the inner key is also "value".
    Spark's get_json_object("$.after.value.value") returns null because
    of this naming collision. Python json.loads has no such limitation.
    (This is the same fix documented in v1's blob_decoder.py.)
 
  Closure pattern:
    make_eav_decoder(attr_type_map) returns an inner function that has
    attr_type_map baked into its closure. When Spark serializes the UDF
    to send to executors, the dict travels with it — no external file read.
 
pivot_eav(decoded_df, cfg, table_name)
  Generic equivalent of v1's pivot_to_wide().
  Groups by "entity_key", builds _wide_col + _has_wide_col pairs
  driven by cfg["attributes"] instead of hardcoded COLUMN2_TO_WIDE.
 
cast_eav(wide_df, cfg)
  Generic equivalent of v1's cast_and_enrich().
  Renames "entity_key" → cfg["merge_key"] (= "uuid" for users).
  Type-casts each wide column using SPARK_TYPE_MAP.
 
─────────────────────────────────────────────────────────────────────────────
JSON_EMBEDDED TABLES (e.g. bidgely.weather_data)
─────────────────────────────────────────────────────────────────────────────
expand_json_columns(deduped_df, cfg)
  Generic equivalent of v1's inline JSON expansion loop in the weather
  pipeline. Driven by cfg["json_columns"].
  Uses native Spark get_json_object — no UDF needed because the
  column names (country, zipcode, json_value, ...) don't collide with
  the inner Debezium "value" key.
 
─────────────────────────────────────────────────────────────────────────────
PLAIN TABLES (e.g. bidgely.pilot_info)
─────────────────────────────────────────────────────────────────────────────
parse_plain_fields(batch_df, cfg)
  New in v2 — no v1 equivalent (pilot_info is a new table type).
  Extracts all columns from $.after.{col_name}.value using get_json_object.
  Debezium emits native JSON types for typed Cassandra columns:
    int     → JSON integer  (no base64, no blob decode)
    boolean → JSON boolean
    text    → JSON string
    bigint  → JSON integer (long)
"""
 
import base64
import json as _json
import struct
import logging
from typing import Optional, Dict
 
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType,
    BooleanType, FloatType, DoubleType
)
 
log = logging.getLogger("generic_decoder")
 
# ─── Cassandra type string → Spark type ──────────────────────────────────────
# Used by cast_eav, expand_json_columns, parse_plain_fields
SPARK_TYPE_MAP = {
    "text":    StringType(),
    "int":     IntegerType(),
    "bigint":  LongType(),
    "boolean": BooleanType(),
    "float":   FloatType(),
    "double":  DoubleType(),
    "uuid":    StringType(),   # UUID stored as string in Delta
    "blob":    StringType(),   # raw blob passed through as string
}
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# EAV — BLOB DECODE UDF
# ═══════════════════════════════════════════════════════════════════════════════
 
def make_eav_decoder(attr_type_map: Dict[str, str], table_name: str):
    """
    Creates a Spark UDF that decodes one EAV attribute value.
 
    Args:
        attr_type_map : {cassandra_col2_name: cassandra_type_string}
                        Built from table_config.yml attributes section.
                        e.g. {"pilotId": "int", "userStatus": "text", ...}
        table_name    : used in warning messages for unknown attributes
 
    Returns:
        Spark UDF (column2: str, value_cell: str) → Optional[str]
 
    The returned UDF is registered once at pipeline startup per EAV table.
    The attr_type_map dict is captured in the closure — it travels to
    Spark executors when the UDF is serialized via pickle.
    """
    # Capture type_map in local name — travels to Spark executors via pickle
    _type_map = attr_type_map
 
    def _decode(column2: Optional[str], value_cell: Optional[str]) -> Optional[str]:
        """
        Decodes a Debezium Cassandra CDC value cell into a typed string.
 
        value_cell format (from Debezium Cassandra 4 connector):
          '{"value":"AAAnlA==","deletion_ts":null,"set":true}'
 
        Steps:
          1. Unwrap Debezium cell wrapper using Python json.loads
             (NOT Spark get_json_object — name collision with "value" key)
          2. base64-decode the blob bytes
          3. Convert to typed string using attr_type_map from config
        """
        if value_cell is None or column2 is None:
            return None
 
        # ── Step 1: Unwrap Debezium cell wrapper ──────────────────────────────
        # value_cell: '{"value":"AAAnlA==","deletion_ts":null,"set":true}'
        # After unwrap: actual = "AAAnlA==" (the base64 blob)
        #
        # Why Python here instead of Spark get_json_object:
        #   Cassandra column is named "value". Debezium wraps it as:
        #   {"value": "AAAnlA==", ...}
        #   So Spark would need: get_json_object("$.after.value.value")
        #   But Spark returns null for this because both the outer column name
        #   AND the inner key are "value" — a naming collision.
        #   Python json.loads has no such limitation.
        actual = value_cell
        try:
            parsed = _json.loads(value_cell)
            if isinstance(parsed, dict) and "value" in parsed:
                actual = parsed["value"]
                if actual is None:
                    # "value": null means the attribute was explicitly deleted
                    # (attribute-level tombstone, not entity delete)
                    return None
        except (_json.JSONDecodeError, TypeError, ValueError):
            # value_cell was not JSON — treat as plain value
            pass
 
        # ── Step 2: Look up Cassandra type from config ────────────────────────
        attr_type = _type_map.get(column2)
 
        if attr_type is None:
            # Unknown attribute — not in table_config.yml
            # Pass raw value through as string so data is not lost.
            # The pipeline logs a schema evolution alert separately.
            return str(actual) if actual is not None else None
 
        # ── Step 3: base64 decode + type conversion ───────────────────────────
        # All values in the EAV "value blob" column arrive base64-encoded.
        # Decode bytes then interpret per Cassandra type.
        try:
            raw_bytes = base64.b64decode(str(actual))
 
            if attr_type == "boolean":
                # Cassandra boolean: 0x00 = false, 0x01 = true
                return "true" if raw_bytes[0] != 0 else "false"
 
            elif attr_type == "int":
                # Cassandra int: 4-byte big-endian signed integer
                return str(struct.unpack(">i", raw_bytes)[0])
 
            elif attr_type == "bigint":
                # Cassandra bigint: 8-byte big-endian signed integer
                return str(struct.unpack(">q", raw_bytes)[0])
 
            elif attr_type == "text":
                # Cassandra text: raw UTF-8 bytes
                return raw_bytes.decode("utf-8")
 
            elif attr_type == "uuid":
                # Cassandra uuid: typically stored as string in blob
                return raw_bytes.decode("utf-8")
 
            else:
                # Any other type — pass raw decoded string
                return str(actual)
 
        except Exception:
            # Decode failed — return None rather than crashing the UDF
            # The pipeline's bad-record filter will catch null ts_ms rows
            return None
 
    return F.udf(_decode, StringType())
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# EAV — PIVOT
# ═══════════════════════════════════════════════════════════════════════════════
 
def pivot_eav(decoded_df: DataFrame, cfg: dict, table_name: str) -> DataFrame:
    """
    Pivots EAV rows into one wide row per entity.
    Generic equivalent of v1's pivot_to_wide().
 
    Input columns expected:
        entity_key    — Cassandra partition key value (e.g. user UUID)
        column2       — attribute name (e.g. "pilotId")
        decoded_value — typed string value after blob decode
        ts_ms         — Debezium event timestamp (milliseconds)
 
    Output columns per entity:
        entity_key           — entity identifier
        event_timestamp      — max ts_ms in batch for this entity
        deduptime            — same as event_timestamp (used for MERGE)
        _{wide_col}          — decoded value (None if not in batch)
        _has_{wide_col}      — 1 if attribute present in batch, else 0
 
    The has_ flag is critical for the MERGE CASE WHEN logic:
        has=1, val=X    → UPDATE column to X (even if X is null = explicit delete)
        has=0, val=None → KEEP existing Delta value (attribute not in this batch)
 
    Unknown attributes (column2 values not in config) trigger a schema
    evolution alert but do NOT stop the pipeline.
    """
    attributes = cfg["attributes"]
    known_attrs = set(attributes.keys())
 
    # ── Detect unknown attributes for schema evolution alerting ───────────────
    # collect() is an action — scans the DataFrame once to find unknowns.
    # If decoded_df is not cached by the caller, this causes a re-scan when
    # the groupBy runs. For large batches, cache decoded_df before calling this.
    from alerting import alert_unknown_attribute
    unknown_rows = (
        decoded_df
        .filter(
            F.col("column2").isNotNull() &
            (~F.col("column2").isin(list(known_attrs)))
        )
        .select("column2")
        .distinct()
        .collect()
    )
    for row in unknown_rows:
        alert_unknown_attribute(table_name, row[0])
 
    # ── Build aggregation expressions ─────────────────────────────────────────
    # Equivalent to v1's loop over COLUMN2_TO_WIDE, now driven by cfg
    agg_exprs = [
        F.max("ts_ms").alias("event_timestamp"),
        F.max("ts_ms").alias("deduptime"),
    ]
 
    for col2, attr_info in attributes.items():
        wide_col = attr_info["wide"]
 
        # Value: first non-null decoded_value for this attribute in the batch
        agg_exprs.append(
            F.first(
                F.when(F.col("column2") == col2, F.col("decoded_value")),
                ignorenulls=True
            ).alias(f"_{wide_col}")
        )
 
        # Presence flag: 1 if this attribute appeared in the batch, else 0
        # has=1 + val=None = explicit attribute delete (still an update)
        # has=0            = attribute not in this batch (keep existing)
        agg_exprs.append(
            F.max(
                F.when(F.col("column2") == col2, F.lit(1)).otherwise(F.lit(0))
            ).alias(f"_has_{wide_col}")
        )
 
    return decoded_df.groupBy("entity_key").agg(*agg_exprs)
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# EAV — TYPE CAST + ENRICH
# ═══════════════════════════════════════════════════════════════════════════════
 
def cast_eav(wide_df: DataFrame, cfg: dict) -> DataFrame:
    """
    Casts staging columns to production types and adds metadata.
    Generic equivalent of v1's cast_and_enrich().
 
    Steps:
      1. Rename "entity_key" → cfg["merge_key"]
         (= "uuid" for users, = other names for future EAV tables)
      2. For each attribute: cast _{wide_col} → wide_col (production type)
      3. Keep has_{wide_col} flags (needed by MERGE CASE WHEN in generic_merge)
      4. Drop staging _col and _has_col columns
      5. Add event_id (UUID) and event_date (DATE from event_timestamp)
 
    Output schema:
        {merge_key}          — entity identifier (e.g. "uuid")
        {wide_col}           — typed production column per attribute
        has_{wide_col}       — presence flag per attribute (kept for MERGE)
        event_id             — pipeline run UUID
        event_timestamp      — max ts_ms from batch
        event_date           — date derived from event_timestamp (UTC)
        deduptime            — same as event_timestamp
    """
    merge_key = cfg["merge_key"]   # "uuid" for users
 
    # Rename generic "entity_key" to the table-specific merge key column name
    df = wide_df.withColumnRenamed("entity_key", merge_key)
 
    for col2, attr_info in cfg["attributes"].items():
        wide_col   = attr_info["wide"]
        spark_type = SPARK_TYPE_MAP.get(attr_info["type"], StringType())
 
        # Cast staging string column to production type
        df = df.withColumn(wide_col, F.col(f"_{wide_col}").cast(spark_type))
 
        # Rename has_ flag (remove staging underscore prefix)
        df = df.withColumn(f"has_{wide_col}", F.col(f"_has_{wide_col}"))
 
        # Drop both staging columns
        df = df.drop(f"_{wide_col}", f"_has_{wide_col}")
 
    # Add metadata columns
    df = (
        df
        .withColumn("event_id",   F.expr("uuid()"))
        .withColumn(
            "event_date",
            F.to_date(F.from_unixtime(F.col("event_timestamp") / 1000))
        )
    )
 
    return df
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# JSON_EMBEDDED — METRIC EXPANSION
# ═══════════════════════════════════════════════════════════════════════════════
 
def expand_json_columns(deduped_df: DataFrame, cfg: dict) -> DataFrame:
    """
    Expands a JSON text column into individual typed metric columns.
    Generic equivalent of v1's inline JSON expansion loop in weather pipeline.
 
    No UDF needed — uses native Spark get_json_object because the
    column names (e.g. json_value, country) don't clash with the inner
    Debezium "value" key.
 
    Input:  DataFrame containing the json_value column (text with JSON)
    Output: Same DataFrame with 20 new float columns added per metric
 
    Driven by cfg["json_columns"]:
      json_value:
        temp:     {col: temp,      type: float}
        windGust: {col: wind_gust, type: float}
        ...
 
    For each entry:
      df.withColumn("wind_gust",
          get_json_object(col("json_value"), "$.windGust").cast(FloatType()))
    """
    df = deduped_df
 
    for source_col_name, field_mappings in cfg["json_columns"].items():
        # source_col_name = "json_value" (the Cassandra text column)
        for json_key, field_info in field_mappings.items():
            delta_col  = field_info["col"]
            spark_type = SPARK_TYPE_MAP.get(field_info["type"], StringType())
 
            df = df.withColumn(
                delta_col,
                F.get_json_object(F.col(source_col_name), f"$.{json_key}").cast(spark_type)
            )
 
    return df
 
 
# ═══════════════════════════════════════════════════════════════════════════════
# PLAIN — FIELD EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════════
 
def parse_plain_fields(batch_df: DataFrame, cfg: dict) -> DataFrame:
    """
    Extracts all configured columns from a PLAIN table Debezium CDC event.
 
    PLAIN tables have standard Cassandra types (int, text, boolean, bigint).
    Debezium emits native JSON types for these — NO base64 blob decoding needed:
        Cassandra int     → Debezium after field: {"value": 10116, ...}
        Cassandra boolean → Debezium after field: {"value": true,  ...}
        Cassandra text    → Debezium after field: {"value": "PGE", ...}
        Cassandra bigint  → Debezium after field: {"value": 1745230146, ...}
 
    get_json_object("$.after.{col_name}.value") works correctly here because
    the column names (pilot_id, pilot_name, ...) do NOT clash with the inner
    Debezium "value" key — unlike the EAV "value blob" column.
 
    get_json_object always returns StringType. We cast to the correct Spark
    type per cfg["columns"].
 
    Output columns:
        ts_ms         — event timestamp (LongType)
        op            — operation: c/u/d (StringType)
        {col_name}    — typed column value per cfg["columns"] entry
        json_str      — original raw JSON (kept for dead-letter writes)
    """
    select_exprs = [
        F.get_json_object("json_str", "$.ts_ms").cast(LongType()).alias("ts_ms"),
        F.get_json_object("json_str", "$.op").alias("op"),
    ]
 
    for col_def in cfg["columns"]:
        col_name   = col_def["name"]
        spark_type = SPARK_TYPE_MAP.get(col_def["type"], StringType())
 
        select_exprs.append(
            F.get_json_object("json_str", f"$.after.{col_name}.value")
            .cast(spark_type)
            .alias(col_name)
        )
 
    # Keep json_str for dead-letter writes on failure
    select_exprs.append(F.col("json_str"))
 
    return batch_df.select(select_exprs)