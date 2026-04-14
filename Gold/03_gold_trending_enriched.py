# Databricks notebook source
# =============================================================================
# NOTEBOOK: 03_gold_trending_enriched.py
# LAYER:    Gold
# PURPOSE:  Enrich the trending coins list (7 coins/day) with live market
#           data from Silver market_snapshot via a LEFT JOIN.
#
# SOURCE:   silver/trending_coins  LEFT JOIN  silver/market_snapshot
# OUTPUT:   gold/trending_enriched  (DELTA MERGE — accumulates daily)
#
# WHY LEFT JOIN (not INNER)?
#   Trending coins may include coins OUTSIDE our top-50 tracked list.
#   LEFT JOIN keeps all 7 trending coins — market data fields become null
#   for coins not in our top 50. The is_also_top50 flag tells Power BI
#   whether we have market data for this coin or not.
#
# MERGE KEY: (trend_run_date, coin_id)
# Z-ORDER:   (trend_run_date)
# =============================================================================

# COMMAND ----------

# MAGIC %run ../connection

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./gold_utils

# COMMAND ----------

# =============================================================================
# CELL 1 — SETUP
# =============================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType

adls_name = "adlsnewhp"
init_gold_config(adls_name)

logger = get_logger("gold_trending_enriched")

logger.info("=" * 70)
logger.info("Gold: trending_enriched — START")
logger.info(f"  Run ID     : {GoldConfig.RUN_ID}")
logger.info(f"  Silver in  : {SilverInputPaths.TRENDING_COINS}")
logger.info(f"  Silver join: {SilverInputPaths.MARKET_SNAPSHOT}")
logger.info(f"  Gold out   : {GoldPaths.TRENDING_ENRICHED}")
logger.info("=" * 70)

# COMMAND ----------

# =============================================================================
# CELL 2 — READ SILVER TABLES (full reads)
# =============================================================================

trending_df = read_silver_table(spark, SilverInputPaths.TRENDING_COINS, logger)
market_df   = read_silver_table(spark, SilverInputPaths.MARKET_SNAPSHOT, logger)

# COMMAND ----------

# =============================================================================
# CELL 3 — LEFT JOIN TRENDING WITH MARKET DATA
# =============================================================================

logger.info("CELL 3: LEFT JOIN trending_coins with market_snapshot")

t = trending_df.alias("t")
m = market_df.alias("m")

joined_df = (
    t.join(
        m,
        on=(
            (F.col("t.coin_id") == F.col("m.coin_id")) &
            (F.col("t.trend_run_date") == F.col("m.snapshot_date"))
        ),
        how="left"
    )
    .select(
        F.col("t.trend_run_date"),
        F.col("t.trend_position"),
        F.col("t.coin_id"),
        F.col("t.coin_name"),
        F.col("t.coin_symbol"),
        F.col("t.market_cap_rank"),
        F.col("m.current_price_usd").cast(DoubleType()),
        F.col("m.price_change_pct_24h").cast(DoubleType()),
        F.col("m.total_volume_24h_usd"),
        F.when(
            F.col("m.coin_id").isNotNull(), True
        ).otherwise(False)
        .cast(BooleanType())
        .alias("is_also_top50"),
    )
    .withColumn("gold_processed_timestamp", get_gold_timestamp(GoldConfig.RUN_TS))
)

raw_count = joined_df.count()
logger.info(f"  Rows after join: {raw_count:,}")

# COMMAND ----------

joined_df.display()

# COMMAND ----------

# =============================================================================
# CELL 4 — FINAL COLUMN REORDER
# =============================================================================

logger.info("CELL 4: Reordering to final Gold schema")
final_df = joined_df.select(*GoldColumns.TRENDING_ENRICHED)

# COMMAND ----------

# =============================================================================
# CELL 5 — DELTA MERGE INTO GOLD
# =============================================================================

logger.info("CELL 5: MERGE into gold/trending_enriched")

merge_stats = delta_merge_gold(
    spark      = spark,
    new_df     = final_df,
    table_path = GoldPaths.TRENDING_ENRICHED,
    merge_keys = GoldMergeKeys.TRENDING_ENRICHED,
    logger     = logger,
)

# COMMAND ----------

# =============================================================================
# CELL 6 — OPTIMIZE + Z-ORDER
# =============================================================================

logger.info("CELL 6: OPTIMIZE gold/trending_enriched")
optimize_delta(spark, GoldPaths.TRENDING_ENRICHED, "trend_run_date",
               "trending_enriched", logger)

# COMMAND ----------

# =============================================================================
# CELL 7 — RUN LOG + COMPLETION
# =============================================================================

summary = {
    "notebook"            : "03_gold_trending_enriched",
    "pipeline_run_id"     : GoldConfig.RUN_ID,
    "run_timestamp_utc"   : GoldConfig.RUN_TS.isoformat(),
    "silver_sources"      : [SilverInputPaths.TRENDING_COINS, SilverInputPaths.MARKET_SNAPSHOT],
    "gold_target"         : GoldPaths.TRENDING_ENRICHED,
    "rows_transformed"    : raw_count,
    "merge_rows_before"   : merge_stats["rows_before"],
    "merge_rows_after"    : merge_stats["rows_after"],
    "merge_rows_inserted" : merge_stats["rows_inserted"],
    "status"              : "SUCCESS",
}

write_run_log(summary, GoldLogPaths.TRENDING_ENRICHED, logger)

logger.info("=" * 70)
logger.info("Gold: trending_enriched — COMPLETE")
for k, v in summary.items():
    logger.info(f"  {k:<30}: {v}")
logger.info("=" * 70)
