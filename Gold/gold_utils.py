# Databricks notebook source
import json
import logging

from typing import List, Dict, Any

from pyspark.sql            import DataFrame, SparkSession
from pyspark.sql            import functions as F
from pyspark.sql.types      import TimestampType
from delta.tables           import DeltaTable

# COMMAND ----------

# LOGGING SETUP


def get_logger(notebook_name: str) -> logging.Logger:
    """
    Return a configured Python logger for a Gold notebook.
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

def read_silver_table(
    spark  : SparkSession,
    path   : str,
    logger : logging.Logger,
) -> DataFrame:
    """
    Read a Silver Delta table in full. No watermark — Gold needs complete
    """
    logger.info(f"  Reading Silver: {path}")

    df    = spark.read.format("delta").load(path)
    count = df.count()

    logger.info(f"  Silver row count: {count:,}")
    return df

# COMMAND ----------

# FUNCTION: delta_merge_gold()
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
    merge_condition = " AND ".join(
        [f"existing.{col} = new.{col}" for col in merge_keys]
    )
    logger.info(f"  MERGE into: {table_path}")
    logger.info(f"  Merge keys: {merge_keys}")

    table_exists = DeltaTable.isDeltaTable(spark, table_path)

    if not table_exists:
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

# FUNCTION: delta_overwrite()

def delta_overwrite(
    df         : DataFrame,
    table_path : str,
    logger     : logging.Logger,
) -> int:
    """
    Overwrite a KPI Delta table with the given DataFrame.
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

# FUNCTION: write_run_log()

def write_run_log(
    summary    : Dict[str, Any],
    log_path   : str,
    logger     : logging.Logger,
) -> None:
    """
    Write a JSON run summary to the ADLS logging directory.
    try:
        log_content = json.dumps(summary, indent=2, default=str)
        dbutils.fs.put(log_path, log_content, overwrite=True)
        logger.info(f"  ✓ Run log written → {log_path}")
    except Exception as e:
        # Log writing failure should NEVER fail the pipeline.
        logger.warning(f"  ⚠ Failed to write run log (non-fatal): {e}")

# COMMAND ----------

# FUNCTION: get_gold_timestamp()

def get_gold_timestamp(run_ts) -> "Column":
    """
    Return a Spark Column with the fixed UTC run timestamp.
    """
    return F.lit(run_ts.isoformat()).cast(TimestampType())

# COMMAND ----------

# FUNCTION: optimize_delta()

def optimize_delta(
    spark        : SparkSession,
    table_path   : str,
    zorder_cols  : str,
    label        : str,
    logger       : logging.Logger,
) -> None:
    """
    Run OPTIMIZE + Z-ORDER on a Gold/KPI Delta table.
    """
    logger.info(f"  Optimizing {label} (Z-ORDER BY {zorder_cols}) ...")
    spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY ({zorder_cols})")
    logger.info(f"  ✓ {label} optimized")
