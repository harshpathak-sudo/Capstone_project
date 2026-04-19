# Databricks notebook source
# MAGIC %run ../connection

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./gold_utils

# COMMAND ----------


from pyspark.sql import functions as F

adls_name = "adlsnewhp1"
init_gold_config(adls_name)

logger = get_logger("kpi_trending_widget")

logger.info("=" * 70)
logger.info("KPI: trending_widget — START")
logger.info(f"  Run ID     : {GoldConfig.RUN_ID}")
logger.info(f"  Gold in    : {GoldPaths.TRENDING_ENRICHED}")
logger.info(f"  KPI out    : {GoldPaths.KPI_TRENDING_WIDGET}")
logger.info("=" * 70)

# COMMAND ----------



logger.info("CELL 2: Reading gold/trending_enriched (latest date)")

gold_df = spark.read.format("delta").load(GoldPaths.TRENDING_ENRICHED)

max_date = gold_df.agg(F.max("trend_run_date")).collect()[0][0]
logger.info(f"  Latest trend date: {max_date}")

latest_df = gold_df.filter(F.col("trend_run_date") == max_date)

row_count = latest_df.count()
logger.info(f"  Rows for latest date: {row_count:,} (expected ~7)")

# COMMAND ----------

# RENAME TO POWER BI-FRIENDLY COLUMNS


logger.info("CELL 3: Renaming columns to Power BI labels")

kpi_df = (
    latest_df
    .select(
        F.col("trend_position").alias("trending_position"),
        F.col("coin_name"),
        F.col("coin_symbol").alias("symbol"),
        F.col("market_cap_rank"),
        F.col("coin_price"),
        F.col("price_change_24h_percent"),
        F.col("is_also_top50").alias("is_in_top50"),
        F.col("trend_run_date").alias("snapshot_date"),
    )
)

# COMMAND ----------

kpi_df.display()

# COMMAND ----------

# OVERWRITE KPI TABLE


logger.info("CELL 4: OVERWRITE gold/kpi_trending_widget")

written_count = delta_overwrite(kpi_df, GoldPaths.KPI_TRENDING_WIDGET, logger)

# COMMAND ----------

# RUN LOG + COMPLETION


summary = {
    "notebook"           : "08_kpi_trending_widget",
    "pipeline_run_id"    : GoldConfig.RUN_ID,
    "run_timestamp_utc"  : GoldConfig.RUN_TS.isoformat(),
    "gold_source"        : GoldPaths.TRENDING_ENRICHED,
    "kpi_target"         : GoldPaths.KPI_TRENDING_WIDGET,
    "snapshot_date"      : str(max_date),
    "rows_written"       : written_count,
    "status"             : "SUCCESS",
}

write_run_log(summary, GoldLogPaths.KPI_TRENDING_WIDGET, logger)

logger.info("=" * 70)
logger.info("KPI: trending_widget — COMPLETE")
for k, v in summary.items():
    logger.info(f"  {k:<30}: {v}")
logger.info("=" * 70)
