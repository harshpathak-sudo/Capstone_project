# Databricks notebook source
# =============================================================================
# FILE:    gold_utils.py
# PURPOSE: Shared utility functions used by ALL Gold + KPI layer notebooks.
#          Import via: %run ./gold_utils
#
# WHY A SHARED UTILS FILE?
#   The same operations appear in every Gold notebook:
#     - Reading Silver Delta tables (full read, no watermark)
#     - Delta MERGE for Gold tables (accumulating history)
#     - Delta OVERWRITE for KPI tables (latest snapshot)
#     - Writing a JSON run log to ADLS
#     - OPTIMIZE + Z-ORDER after writes
#   One file = one fix = all notebooks updated.
#
# FUNCTIONS:
#   get_logger()          → configured Python logger for a Gold notebook
#   read_silver_table()   → full read of a Silver Delta table (no watermark)
#   delta_merge_gold()    → MERGE (upsert) into a Gold Delta table
#   delta_overwrite()     → OVERWRITE a KPI Delta table
#   write_run_log()       → persist a JSON summary to ADLS logs/gold/
#   get_gold_timestamp()  → fixed UTC timestamp as a Spark Column
#   optimize_delta()      → OPTIMIZE + Z-ORDER on a Delta table
# =============================================================================

import json
import logging

from typing import List, Dict, Any

from pyspark.sql            import DataFrame, SparkSession
from pyspark.sql            import functions as F
from pyspark.sql.types      import TimestampType
from delta.tables           import DeltaTable

# COMMAND ----------

# =============================================================================
# LOGGING SETUP
# =============================================================================

def get_logger(notebook_name: str) -> logging.Logger:
    """
    Return a configured Python logger for a Gold notebook.
    Call once at the top of each notebook:
        logger = get_logger("gold_daily_market_summary")
    """
    logger = logging.getLogger(f"gold.{notebook_name}")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ"
        ))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

# COMMAND ----------

# =============================================================================
# FUNCTION: read_silver_table()
#
# WHY FULL READ (no watermark)?
#   Gold's window functions (rolling averages, RANK, LAG) require the FULL
#   Silver history to compute correctly. A 7-day rolling average needs the
#   previous 6 days. RANK() needs all coins for that date.
#
#   Gold uses DELTA MERGE with primary keys — if a row already exists in
#   Gold, MERGE skips it. Re-reading the same Silver data never creates
#   duplicates. At this scale (50 coins × 365 days = ~18K rows), full
#   reads take seconds, not minutes.
# =============================================================================

def read_silver_table(
    spark  : SparkSession,
    path   : str,
    logger : logging.Logger,
) -> DataFrame:
    """
    Read a Silver Delta table in full. No watermark — Gold needs complete
    history for window functions.

    Args:
        spark  : Active SparkSession
        path   : Full abfss:// path to the Silver Delta table
        logger : Caller's logger

    Returns:
        DataFrame with all Silver rows
    """
    logger.info(f"  Reading Silver: {path}")

    df    = spark.read.format("delta").load(path)
    count = df.count()

    logger.info(f"  Silver row count: {count:,}")
    return df

# COMMAND ----------

# =============================================================================
# FUNCTION: delta_merge_gold()
#
# WHY MERGE INSTEAD OF OVERWRITE FOR GOLD?
#   Gold tables accumulate history day after day. Day 1 has 50 rows,
#   Day 30 has 1,500 rows, Day 90 has 4,500 rows. OVERWRITE would
#   destroy all that accumulated history every run.
#
#   MERGE with primary keys (coin_id + date) ensures:
#     - New rows (today's data) → INSERTED
#     - Existing rows (yesterday's data) → SKIPPED (no update)
#   This makes Gold idempotent: running it 10 times = same result as once.
# =============================================================================

def delta_merge_gold(
    spark       : SparkSession,
    new_df      : DataFrame,
    table_path  : str,
    merge_keys  : List[str],
    logger      : logging.Logger,
) -> Dict[str, Any]:
    """
    MERGE (upsert) new_df into a Gold Delta table.
    Insert-only: new rows are added, existing rows are left unchanged.
    Creates the table on first run.

    Args:
        spark      : Active SparkSession
        new_df     : Transformed Gold DataFrame to merge
        table_path : Full abfss:// path to the Gold Delta table
        merge_keys : List of column names forming the primary key
        logger     : Caller's logger

    Returns:
        Dict with "rows_before", "rows_after", "rows_inserted"
    """
    # Build MERGE condition: "existing.coin_id = new.coin_id AND ..."
    merge_condition = " AND ".join(
        [f"existing.{col} = new.{col}" for col in merge_keys]
    )
    logger.info(f"  MERGE into: {table_path}")
    logger.info(f"  Merge keys: {merge_keys}")

    table_exists = DeltaTable.isDeltaTable(spark, table_path)

    if not table_exists:
        # First run: no table yet → create it
        logger.info("  Table does not exist — creating on first write")
        (
            new_df
            .write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .save(table_path)
        )
        rows_after = new_df.count()
        logger.info(f"  ✓ Table created | rows: {rows_after:,}")
        return {"rows_before": 0, "rows_after": rows_after, "rows_inserted": rows_after}

    # Table exists → MERGE (insert-only, skip existing)
    delta_table = DeltaTable.forPath(spark, table_path)
    rows_before = delta_table.toDF().count()

    (
        delta_table.alias("existing")
        .merge(
            new_df.alias("new"),
            merge_condition
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    rows_after    = delta_table.toDF().count()
    rows_inserted = rows_after - rows_before

    logger.info(
        f"  ✓ MERGE complete | before: {rows_before:,} "
        f"| after: {rows_after:,} | inserted: {rows_inserted:,}"
    )
    return {
        "rows_before"  : rows_before,
        "rows_after"   : rows_after,
        "rows_inserted": rows_inserted,
    }

# COMMAND ----------

# =============================================================================
# FUNCTION: delta_overwrite()
#
# WHY OVERWRITE FOR KPI TABLES?
#   KPI tables are a thin convenience layer for Power BI. They contain
#   ONLY the latest date's data (or last 30/90 days for trend KPIs).
#   Every pipeline run replaces them entirely with fresh data.
#   Power BI always sees the most current snapshot — no stale rows.
# =============================================================================

def delta_overwrite(
    df         : DataFrame,
    table_path : str,
    logger     : logging.Logger,
) -> int:
    """
    Overwrite a KPI Delta table with the given DataFrame.

    Args:
        df         : KPI DataFrame to write
        table_path : Full abfss:// path to the KPI Delta table
        logger     : Caller's logger

    Returns:
        Row count written
    """
    logger.info(f"  OVERWRITE: {table_path}")

    (
        df
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(table_path)
    )

    count = df.count()
    logger.info(f"  ✓ Overwrite complete | rows: {count:,}")
    return count

# COMMAND ----------

# =============================================================================
# FUNCTION: write_run_log()
# Persists a JSON run summary to ADLS logs/gold/ directory.
# Same pattern as Silver — uses dbutils.fs.put.
# =============================================================================

def write_run_log(
    summary    : Dict[str, Any],
    log_path   : str,
    logger     : logging.Logger,
) -> None:
    """
    Write a JSON run summary to the ADLS logging directory.

    Args:
        summary  : Dict containing run metadata and results
        log_path : Full abfss:// path for the log file
        logger   : Caller's logger
    """
    try:
        log_content = json.dumps(summary, indent=2, default=str)
        dbutils.fs.put(log_path, log_content, overwrite=True)
        logger.info(f"  ✓ Run log written → {log_path}")
    except Exception as e:
        # Log writing failure should NEVER fail the pipeline.
        logger.warning(f"  ⚠ Failed to write run log (non-fatal): {e}")

# COMMAND ----------

# =============================================================================
# FUNCTION: get_gold_timestamp()
# Returns a fixed UTC timestamp as a PySpark Column.
# All rows in a Gold run share the same timestamp.
# =============================================================================

def get_gold_timestamp(run_ts) -> "Column":
    """
    Return a Spark Column with the fixed UTC run timestamp.
    Use as: df.withColumn("gold_processed_timestamp", get_gold_timestamp(RUN_TS))
    """
    return F.lit(run_ts.isoformat()).cast(TimestampType())

# COMMAND ----------

# =============================================================================
# FUNCTION: optimize_delta()
# OPTIMIZE + Z-ORDER on a Delta table for query performance.
# Same pattern as Bronze and Silver layers.
# =============================================================================

def optimize_delta(
    spark        : SparkSession,
    table_path   : str,
    zorder_cols  : str,
    label        : str,
    logger       : logging.Logger,
) -> None:
    """
    Run OPTIMIZE + Z-ORDER on a Gold/KPI Delta table.

    Args:
        spark       : Active SparkSession
        table_path  : Full abfss:// Delta table path
        zorder_cols : Comma-separated Z-ORDER columns (e.g., "coin_id, summary_date")
        label       : Short name for log messages
        logger      : Caller's logger
    """
    logger.info(f"  Optimizing {label} (Z-ORDER BY {zorder_cols}) ...")
    spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY ({zorder_cols})")
    logger.info(f"  ✓ {label} optimized")
