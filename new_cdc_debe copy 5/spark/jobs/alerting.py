"""
alerting.py
===========
Alerting utilities for the Bidgely CDC Generic Pipeline.
 
In Docker/testing:  logs to console at ERROR or WARNING level.
In production:      replace the TODO stubs with PagerDuty / Slack / SNS calls.
 
Alert types:
  1. alert_table_failure        — foreachBatch raised an exception
  2. alert_unknown_attribute    — EAV attribute arrived that is not in config
  3. alert_unknown_bronze_topic — Bronze S3 prefix found with no config entry
  4. alert_partition_change_blocked — config tried to change partition_by on
                                      existing Delta table (breaking change)
"""
 
import logging
 
log = logging.getLogger("alerting")
 
 
# ─── 1. Table Failure ─────────────────────────────────────────────────────────
 
def alert_table_failure(table_name: str, batch_id: int, error: Exception) -> None:
    """
    Called when a table's foreachBatch handler raises an unhandled exception.
 
    The failed batch has already been written to the dead-letter path before
    this function is called. The pipeline CONTINUES for all other tables —
    this is an alert, not a halt.
 
    In production: POST to PagerDuty / Slack / SNS here.
    """
    msg = (
        f"\n{'═' * 70}\n"
        f"  ⚠  TABLE FAILURE ALERT\n"
        f"  Table      : {table_name}\n"
        f"  Batch ID   : {batch_id}\n"
        f"  Error type : {type(error).__name__}\n"
        f"  Error msg  : {str(error)}\n"
        f"  Action     : Batch written to dead-letter path\n"
        f"               Pipeline continues for all other tables\n"
        f"  TODO       : Add PagerDuty / Slack / SNS call here for production\n"
        f"{'═' * 70}"
    )
    log.error(msg, exc_info=error)
 
 
# ─── 2. Unknown Attribute (Schema Evolution) ──────────────────────────────────
 
def alert_unknown_attribute(table_name: str, attribute_name: str) -> None:
    """
    Called when an EAV CDC event contains a column2 value (attribute name)
    that is NOT listed in table_config.yml under attributes.
 
    This is a schema evolution event — someone added a new attribute to the
    Cassandra EAV table. The pipeline passes the raw base64 value through as
    a string and continues, but the team must update table_config.yml.
 
    In production: POST to Slack / alerting system here.
    """
    msg = (
        f"\n{'─' * 70}\n"
        f"  ⚠  SCHEMA EVOLUTION ALERT\n"
        f"  Table     : {table_name}\n"
        f"  Attribute : '{attribute_name}' not in table_config.yml\n"
        f"  Impact    : Value passed through as raw string (not decoded)\n"
        f"              Attribute will NOT appear as a typed wide column\n"
        f"  Required  : Add '{attribute_name}' to table_config.yml\n"
        f"              under tables.{table_name}.attributes\n"
        f"              Then restart spark-etl\n"
        f"  TODO      : Add Slack / alerting call here for production\n"
        f"{'─' * 70}"
    )
    log.warning(msg)
 
 
# ─── 3. Unconfigured Bronze Topic ─────────────────────────────────────────────
 
def alert_unknown_bronze_topic(topic_path: str) -> None:
    """
    Called at pipeline startup when a Bronze S3 prefix is found that has
    no matching entry in table_config.yml.
 
    This means the Kafka S3 Sink Connector has been writing data for this
    topic but Spark is not processing it — data is accumulating silently.
 
    In production: POST to Slack / alerting system here.
    """
    msg = (
        f"\n{'─' * 70}\n"
        f"  ⚠  UNCONFIGURED BRONZE TOPIC ALERT\n"
        f"  Bronze path : {topic_path}\n"
        f"  Impact      : Data accumulating in Bronze — NOT being processed\n"
        f"  Required    : Add a table entry to table_config.yml for this topic\n"
        f"                Then restart spark-etl to start a new stream\n"
        f"  TODO        : Add Slack / alerting call here for production\n"
        f"{'─' * 70}"
    )
    log.warning(msg)
 
 
# ─── 4. Partition Change Blocked ──────────────────────────────────────────────
 
def alert_partition_change_blocked(
    table_name: str, existing: str, requested: str
) -> None:
    """
    Called when table_config.yml has a different partition_by value than
    what already exists on the Delta table.
 
    Changing partition columns on an existing Delta table requires a full
    table rewrite (not an ALTER). The pipeline blocks this at startup and
    raises ValueError so the operator must handle it manually.
 
    In production: POST to PagerDuty here — this requires manual intervention.
    """
    msg = (
        f"\n{'═' * 70}\n"
        f"  ✗  PARTITION CHANGE BLOCKED — REQUIRES MANUAL INTERVENTION\n"
        f"  Table     : {table_name}\n"
        f"  Existing  : partition_by = {existing}\n"
        f"  Requested : partition_by = {requested}\n"
        f"  Reason    : Delta Lake does not support ALTER PARTITION\n"
        f"  Required  : Manual table rewrite OR revert config change\n"
        f"              Option A: Delete Delta table + restart (loses history)\n"
        f"              Option B: Revert partition_by in table_config.yml\n"
        f"  TODO      : Add PagerDuty call here for production\n"
        f"{'═' * 70}"
    )
    log.error(msg)
 