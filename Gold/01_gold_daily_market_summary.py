# Databricks notebook source
# =============================================================================
# NOTEBOOK: 01_gold_daily_market_summary.py
# LAYER:    Gold
# PURPOSE:  Create the core market analytics table from Silver market_snapshot.
#           One row per coin per day with enriched business columns:
#           price range, top 5 gainer/loser flags, market cap share,
#           distance from ATH, and supply utilisation.
#
# SOURCE:   silver/market_snapshot   (full read, no watermark)
# OUTPUT:   gold/daily_market_summary (DELTA MERGE — accumulates daily)
#
# WHY FULL SILVER READ (no watermark)?
#   Window functions RANK() and SUM() OVER (PARTITION BY date) need ALL
#   coins for each date. Reading only "new" rows would produce wrong ranks.
#   MERGE handles dedup — existing (coin_id, summary_date) rows are skipped.
#
# MERGE KEY: (coin_id, summary_date)
# Z-ORDER:   (coin_id, summary_date)
# =============================================================================

# COMMAND ----------

# MAGIC %run ../connection

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./gold_utils

# COMMAND ----------

# MAGIC %run ./transformations

# COMMAND ----------

# =============================================================================
# CELL 1 — SETUP
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType, BooleanType

adls_name = "adlsnewhp1"
init_gold_config(adls_name)

logger = get_logger("gold_daily_market_summary")

logger.info("=" * 70)
logger.info("Gold: daily_market_summary — START")
logger.info(f"  Run ID     : {GoldConfig.RUN_ID}")
logger.info(f"  Run TS     : {GoldConfig.RUN_TS.isoformat()}")
logger.info(f"  Silver in  : {SilverInputPaths.MARKET_SNAPSHOT}")
logger.info(f"  Gold out   : {GoldPaths.DAILY_MARKET_SUMMARY}")
logger.info("=" * 70)

# COMMAND ----------

# =============================================================================
# CELL 2 — READ SILVER (full read, no watermark)
# =============================================================================

silver_df = read_silver_table(spark, SilverInputPaths.MARKET_SNAPSHOT, logger)

# COMMAND ----------

silver_df.display()

# COMMAND ----------

# =============================================================================
# CELL 3 — TRANSFORMATIONS
#
# Business logic is in transformations.py (imported via %run ./transformations).
# This makes the transformation functions independently unit-testable.
#
# apply_daily_market_transforms() applies:
#   1. Price range (high - low)
#   2. Top 5 gainer/loser flags (ROW_NUMBER window function)
#   3. Market cap share % (coin / total × 100)
#   4. Distance from ATH %
#   5. Supply utilisation %
#
# See transformations.py for full documentation of each formula.
# =============================================================================

logger.info("CELL 3: Applying Gold transformations (via transformations.py)")

gold_df = (
    apply_daily_market_transforms(silver_df)
    .withColumn("gold_processed_timestamp", get_gold_timestamp(GoldConfig.RUN_TS))
)

raw_count = gold_df.count()
logger.info(f"  Rows after transformation: {raw_count:,}")

# COMMAND ----------

gold_df.display()

# COMMAND ----------

# =============================================================================
# CELL 4 — FINAL COLUMN REORDER
# =============================================================================

logger.info("CELL 4: Reordering to final Gold schema")
final_df = gold_df.select(*GoldColumns.DAILY_MARKET_SUMMARY)

# COMMAND ----------

# =============================================================================
# CELL 5 — DELTA MERGE INTO GOLD
# =============================================================================

logger.info("CELL 5: MERGE into gold/daily_market_summary")

merge_stats = delta_merge_gold(
    spark      = spark,
    new_df     = final_df,
    table_path = GoldPaths.DAILY_MARKET_SUMMARY,
    merge_keys = GoldMergeKeys.DAILY_MARKET_SUMMARY,
    logger     = logger,
)

# COMMAND ----------

# =============================================================================
# CELL 6 — OPTIMIZE + Z-ORDER
# Z-ORDER BY coin_id, summary_date:
#   KPI queries filter by date (latest). Trend queries filter by coin_id.
#   Both axes benefit from data skipping via Z-ORDER.
# =============================================================================

logger.info("CELL 6: OPTIMIZE gold/daily_market_summary")
optimize_delta(spark, GoldPaths.DAILY_MARKET_SUMMARY, "coin_id, summary_date",
               "daily_market_summary", logger)

# COMMAND ----------

# =============================================================================
# CELL 7 — RUN LOG + COMPLETION
# =============================================================================

summary = {
    "notebook"            : "01_gold_daily_market_summary",
    "pipeline_run_id"     : GoldConfig.RUN_ID,
    "run_timestamp_utc"   : GoldConfig.RUN_TS.isoformat(),
    "silver_source"       : SilverInputPaths.MARKET_SNAPSHOT,
    "gold_target"         : GoldPaths.DAILY_MARKET_SUMMARY,
    "rows_transformed"    : raw_count,
    "merge_rows_before"   : merge_stats["rows_before"],
    "merge_rows_after"    : merge_stats["rows_after"],
    "merge_rows_inserted" : merge_stats["rows_inserted"],
    "status"              : "SUCCESS",
}

write_run_log(summary, GoldLogPaths.DAILY_MARKET_SUMMARY, logger)

logger.info("=" * 70)
logger.info("Gold: daily_market_summary — COMPLETE")
for k, v in summary.items():
    logger.info(f"  {k:<30}: {v}")
logger.info("=" * 70)
