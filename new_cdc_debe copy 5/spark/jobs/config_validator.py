"""
config_validator.py
===================
Validates table_config.yml at pipeline startup.
 
Runs BEFORE any Spark stream starts. Fails immediately with a clear,
human-readable error message on any config mistake — no cryptic
KeyError or NoneType deep inside the pipeline.
 
Validation levels:
  1. File exists and is valid YAML
  2. Top-level 'tables' key present
  3. Each table has all common required fields
  4. Table type is one of: EAV, JSON_EMBEDDED, PLAIN
  5. Type-specific required fields present
  6. Attribute/column/field types are valid Cassandra type strings
  7. Collections are non-empty where required
  8. merge_key consistency (must reference an actual column/attribute)
"""
 
import os
import logging
import yaml
 
log = logging.getLogger("config_validator")
 
# ─── Valid type strings (Cassandra → Spark type mapping happens in decoder) ───
VALID_CASSANDRA_TYPES = {"text", "int", "bigint", "boolean", "float", "double", "uuid", "blob"}
 
# ─── Valid table types ────────────────────────────────────────────────────────
VALID_TABLE_TYPES = {"EAV", "JSON_EMBEDDED", "PLAIN"}
 
# ─── Required fields for every table regardless of type ──────────────────────
REQUIRED_COMMON = [
    "type",
    "enabled",
    "kafka_topic",
    "bronze_path",
    "silver_path",
    "checkpoint_path",
    "dead_letter_path",
]
 
# ─── Required fields per table type ──────────────────────────────────────────
REQUIRED_BY_TYPE = {
    "EAV": [
        "merge_key",
        "cassandra_key_field",
        "attributes",
        "partition_by",
    ],
    "JSON_EMBEDDED": [
        "partition_keys",
        "clustering_keys",
        "merge_key",
        "json_columns",
    ],
    "PLAIN": [
        "partition_keys",
        "clustering_keys",
        "merge_key",
        "columns",
    ],
}
 
 
def load_config(config_path: str) -> dict:
    """
    Load table_config.yml and validate all entries.
 
    Returns the 'tables' dict on success.
    Raises ValueError with a clear multi-line error report on failure.
    Raises FileNotFoundError if the config file is missing.
    """
    # ── 1. File existence ─────────────────────────────────────────────────────
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Config file not found: {config_path}\n"
            f"  Ensure table_config.yml is mounted at TABLE_CONFIG_PATH."
        )
 
    # ── 2. YAML parse ─────────────────────────────────────────────────────────
    with open(config_path, "r") as f:
        try:
            raw = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"table_config.yml is not valid YAML:\n  {e}")
 
    if not raw or "tables" not in raw:
        raise ValueError(
            "table_config.yml must have a top-level 'tables' key.\n"
            "  Example: tables:\n            users:\n              type: EAV\n              ..."
        )
 
    config = raw["tables"]
 
    if not config:
        raise ValueError("'tables' section is empty. Add at least one table entry.")
 
    # ── 3. Validate each table ────────────────────────────────────────────────
    errors = []
 
    for table_name, cfg in config.items():
        p = f"[{table_name}]"
 
        if not isinstance(cfg, dict):
            errors.append(f"{p} table entry must be a mapping, got {type(cfg).__name__}")
            continue
 
        # Common required fields
        for field in REQUIRED_COMMON:
            if field not in cfg:
                errors.append(f"{p} missing required field: '{field}'")
 
        # Table type
        table_type = cfg.get("type")
        if table_type not in VALID_TABLE_TYPES:
            errors.append(
                f"{p} invalid type '{table_type}'. "
                f"Must be one of: {sorted(VALID_TABLE_TYPES)}"
            )
            continue   # skip type-specific checks — type is unknown
 
        # Type-specific required fields
        for field in REQUIRED_BY_TYPE[table_type]:
            if field not in cfg:
                errors.append(f"{p} [{table_type}] missing required field: '{field}'")
 
        # ── EAV-specific validation ───────────────────────────────────────────
        if table_type == "EAV":
            errors.extend(_validate_eav(table_name, cfg))
 
        # ── JSON_EMBEDDED-specific validation ─────────────────────────────────
        elif table_type == "JSON_EMBEDDED":
            errors.extend(_validate_json_embedded(table_name, cfg))
 
        # ── PLAIN-specific validation ─────────────────────────────────────────
        elif table_type == "PLAIN":
            errors.extend(_validate_plain(table_name, cfg))
 
    # ── 4. Report errors ──────────────────────────────────────────────────────
    if errors:
        header = f"Config validation failed — {len(errors)} error(s) in {config_path}:"
        body   = "\n".join(f"  {e}" for e in errors)
        raise ValueError(f"{header}\n{body}")
 
    # ── 5. Success summary ────────────────────────────────────────────────────
    enabled_tables  = [n for n, c in config.items() if c.get("enabled", True)]
    disabled_tables = [n for n, c in config.items() if not c.get("enabled", True)]
 
    log.info("=" * 60)
    log.info("  Config validated: %s", config_path)
    log.info("  Tables total    : %d", len(config))
    log.info("  Enabled         : %d", len(enabled_tables))
    log.info("  Disabled        : %d", len(disabled_tables))
    log.info("-" * 60)
    for table_name, cfg in config.items():
        status = "ENABLED " if cfg.get("enabled", True) else "DISABLED"
        log.info(
            "  [%s] %-20s type=%-14s merge_key=%s",
            status, table_name, cfg.get("type", "?"),
            cfg.get("merge_key", "?")
        )
    log.info("=" * 60)
 
    return config
 
 
# ─── EAV Validator ────────────────────────────────────────────────────────────
 
def _validate_eav(table_name: str, cfg: dict) -> list:
    errors = []
    p = f"[{table_name}][EAV]"
 
    # merge_key must be a non-empty string
    merge_key = cfg.get("merge_key")
    if not isinstance(merge_key, str) or not merge_key.strip():
        errors.append(f"{p} 'merge_key' must be a non-empty string (e.g. 'uuid')")
 
    # cassandra_key_field must be a non-empty string
    key_field = cfg.get("cassandra_key_field")
    if not isinstance(key_field, str) or not key_field.strip():
        errors.append(
            f"{p} 'cassandra_key_field' must be a non-empty string "
            f"(the Cassandra column name, e.g. 'key')"
        )
 
    # attributes must be a non-empty dict
    attributes = cfg.get("attributes")
    if not isinstance(attributes, dict) or not attributes:
        errors.append(f"{p} 'attributes' must be a non-empty mapping")
        return errors
 
    # Each attribute must have 'wide' and valid 'type'
    wide_names = set()
    for attr_name, attr_def in attributes.items():
        ap = f"{p} attributes.{attr_name}"
 
        if not isinstance(attr_def, dict):
            errors.append(f"{ap}: must be a mapping with 'wide' and 'type'")
            continue
 
        wide = attr_def.get("wide")
        if not wide or not isinstance(wide, str):
            errors.append(f"{ap}: 'wide' must be a non-empty string (Delta column name)")
        else:
            wide_names.add(wide)
 
        attr_type = attr_def.get("type")
        if attr_type not in VALID_CASSANDRA_TYPES:
            errors.append(
                f"{ap}: invalid type '{attr_type}'. "
                f"Must be one of: {sorted(VALID_CASSANDRA_TYPES)}"
            )
 
    # partition_by (if not null) must reference a valid wide column name
    partition_by = cfg.get("partition_by")
    if partition_by is not None:
        parts = [partition_by] if isinstance(partition_by, str) else partition_by
        for col in parts:
            if col not in wide_names:
                errors.append(
                    f"{p} 'partition_by' value '{col}' is not a 'wide' name "
                    f"in 'attributes'. Available wide names: {sorted(wide_names)}"
                )
 
    return errors
 
 
# ─── JSON_EMBEDDED Validator ──────────────────────────────────────────────────
 
def _validate_json_embedded(table_name: str, cfg: dict) -> list:
    errors = []
    p = f"[{table_name}][JSON_EMBEDDED]"
 
    # partition_keys must be a non-empty list
    partition_keys = cfg.get("partition_keys")
    if not isinstance(partition_keys, list) or not partition_keys:
        errors.append(f"{p} 'partition_keys' must be a non-empty list")
    else:
        pk_names = set()
        for i, pk in enumerate(partition_keys):
            if not isinstance(pk, dict) or "name" not in pk or "type" not in pk:
                errors.append(f"{p} partition_keys[{i}]: must have 'name' and 'type'")
                continue
            if pk["type"] not in VALID_CASSANDRA_TYPES:
                errors.append(
                    f"{p} partition_keys[{i}] ('{pk['name']}'): "
                    f"invalid type '{pk['type']}'"
                )
            pk_names.add(pk["name"])
 
    # clustering_keys must be a list (can be empty for simple PK tables)
    clustering_keys = cfg.get("clustering_keys")
    if not isinstance(clustering_keys, list):
        errors.append(f"{p} 'clustering_keys' must be a list (use [] for simple PK)")
    else:
        ck_names = set()
        for i, ck in enumerate(clustering_keys):
            if not isinstance(ck, dict) or "name" not in ck or "type" not in ck:
                errors.append(f"{p} clustering_keys[{i}]: must have 'name' and 'type'")
                continue
            if ck["type"] not in VALID_CASSANDRA_TYPES:
                errors.append(
                    f"{p} clustering_keys[{i}] ('{ck['name']}'): "
                    f"invalid type '{ck['type']}'"
                )
            ck_names.add(ck["name"])
 
    # merge_key must be a non-empty list
    merge_key = cfg.get("merge_key")
    if not isinstance(merge_key, list) or not merge_key:
        errors.append(f"{p} 'merge_key' must be a non-empty list of column names")
 
    # json_columns must be a non-empty dict
    json_columns = cfg.get("json_columns")
    if not isinstance(json_columns, dict) or not json_columns:
        errors.append(f"{p} 'json_columns' must be a non-empty mapping")
        return errors
 
    for source_col, field_map in json_columns.items():
        if not isinstance(field_map, dict) or not field_map:
            errors.append(f"{p} json_columns.{source_col}: must be a non-empty mapping")
            continue
        for json_key, field_def in field_map.items():
            fp = f"{p} json_columns.{source_col}.{json_key}"
            if not isinstance(field_def, dict):
                errors.append(f"{fp}: must be a mapping with 'col' and 'type'")
                continue
            if "col" not in field_def or not field_def["col"]:
                errors.append(f"{fp}: missing or empty 'col' (Delta column name)")
            col_type = field_def.get("type")
            if col_type not in VALID_CASSANDRA_TYPES:
                errors.append(
                    f"{fp}: invalid type '{col_type}'. "
                    f"Must be one of: {sorted(VALID_CASSANDRA_TYPES)}"
                )
 
    return errors
 
 
# ─── PLAIN Validator ──────────────────────────────────────────────────────────
 
def _validate_plain(table_name: str, cfg: dict) -> list:
    errors = []
    p = f"[{table_name}][PLAIN]"
 
    # partition_keys must be a non-empty list
    partition_keys = cfg.get("partition_keys")
    if not isinstance(partition_keys, list) or not partition_keys:
        errors.append(f"{p} 'partition_keys' must be a non-empty list")
    else:
        for i, pk in enumerate(partition_keys):
            if not isinstance(pk, dict) or "name" not in pk or "type" not in pk:
                errors.append(f"{p} partition_keys[{i}]: must have 'name' and 'type'")
                continue
            if pk["type"] not in VALID_CASSANDRA_TYPES:
                errors.append(
                    f"{p} partition_keys[{i}] ('{pk['name']}'): "
                    f"invalid type '{pk['type']}'"
                )
 
    # clustering_keys must be a list (can be empty)
    clustering_keys = cfg.get("clustering_keys")
    if not isinstance(clustering_keys, list):
        errors.append(f"{p} 'clustering_keys' must be a list (use [] for simple PK)")
    else:
        for i, ck in enumerate(clustering_keys):
            if not isinstance(ck, dict) or "name" not in ck or "type" not in ck:
                errors.append(f"{p} clustering_keys[{i}]: must have 'name' and 'type'")
                continue
            if ck["type"] not in VALID_CASSANDRA_TYPES:
                errors.append(
                    f"{p} clustering_keys[{i}] ('{ck['name']}'): "
                    f"invalid type '{ck['type']}'"
                )
 
    # columns must be a non-empty list
    columns = cfg.get("columns")
    if not isinstance(columns, list) or not columns:
        errors.append(f"{p} 'columns' must be a non-empty list")
        return errors
 
    col_names = set()
    for i, col_def in enumerate(columns):
        if not isinstance(col_def, dict) or "name" not in col_def or "type" not in col_def:
            errors.append(f"{p} columns[{i}]: must have 'name' and 'type'")
            continue
        col_type = col_def.get("type")
        if col_type not in VALID_CASSANDRA_TYPES:
            errors.append(
                f"{p} columns[{i}] ('{col_def['name']}'): "
                f"invalid type '{col_type}'. "
                f"Must be one of: {sorted(VALID_CASSANDRA_TYPES)}"
            )
        col_names.add(col_def["name"])
 
    # merge_key must reference a column that exists in 'columns'
    merge_key = cfg.get("merge_key")
    if merge_key:
        keys_to_check = [merge_key] if isinstance(merge_key, str) else merge_key
        for key in keys_to_check:
            if key not in col_names:
                errors.append(
                    f"{p} 'merge_key' value '{key}' is not in 'columns'. "
                    f"Available column names: {sorted(col_names)}"
                )
 
    return errors
 