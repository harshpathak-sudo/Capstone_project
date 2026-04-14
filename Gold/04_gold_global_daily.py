# Databricks notebook source
# =============================================================================
# NOTEBOOK: 04_gold_global_daily.py
# LAYER:    Gold
# PURPOSE:  Enrich Silver global_stats with derived dominance metrics
#           and market sentiment classification.
#
# SOURCE:   silver/global_stats   (full read, no watermark)
# OUTPUT:   gold/global_daily     (DELTA MERGE — accumulates daily)
#
# MERGE KEY: (stats_date)
# Z-ORDER:   (stats_date)
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
from pyspark.sql.types import DoubleType, StringType

adls_name = "adlsnewhp"
init_gold_config(adls_name)

logger = get_logger("gold_global_daily")

logger.info("=" * 70)
logger.info("Gold: global_daily — START")
logger.info(f"  Run ID     : {GoldConfig.RUN_ID}")
logger.info(f"  Silver in  : {SilverInputPaths.GLOBAL_STATS}")
logger.info(f"  Gold out   : {GoldPaths.GLOBAL_DAILY}")
logger.info("=" * 70)

# COMMAND ----------

# =============================================================================
# CELL 2 — READ SILVER (full read)
# =============================================================================

silver_df = read_silver_table(spark, SilverInputPaths.GLOBAL_STATS, logger)

# COMMAND ----------

silver_df.display()

# COMMAND ----------

# =============================================================================
# CELL 3 — TRANSFORMATIONS
# =============================================================================

logger.info("CELL 3: Applying global daily enrichment")

gold_df = (
    silver_df
    .select(
        F.col("stats_date"),
        F.col("total_market_cap_usd"),
        F.col("total_volume_24h_usd"),
        F.col("btc_dominance_pct"),
        F.col("eth_dominance_pct"),

        # altcoin dominance = everything that is NOT BTC or ETH
        (100 - F.col("btc_dominance_pct") - F.col("eth_dominance_pct"))
            .cast(DoubleType())
            .alias("altcoin_dominance_pct"),

        # Combined BTC + ETH dominance
        (F.col("btc_dominance_pct") + F.col("eth_dominance_pct"))
            .cast(DoubleType())
            .alias("btc_eth_combined_pct"),

        F.col("active_cryptos"),
        F.col("market_cap_change_pct_24h"),

        # Market sentiment: Bullish if market cap grew in 24h
        F.when(
            F.col("market_cap_change_pct_24h") > 0, "Bullish"
        ).otherwise("Bearish")
        .cast(StringType())
        .alias("market_sentiment"),
    )
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
final_df = gold_df.select(*GoldColumns.GLOBAL_DAILY)

# COMMAND ----------

# =============================================================================
# CELL 5 — DELTA MERGE INTO GOLD
# =============================================================================

logger.info("CELL 5: MERGE into gold/global_daily")

merge_stats = delta_merge_gold(
    spark      = spark,
    new_df     = final_df,
    table_path = GoldPaths.GLOBAL_DAILY,
    merge_keys = GoldMergeKeys.GLOBAL_DAILY,
    logger     = logger,
)

# COMMAND ----------

# =============================================================================
# CELL 6 — OPTIMIZE + Z-ORDER
# =============================================================================

logger.info("CELL 6: OPTIMIZE gold/global_daily")
optimize_delta(spark, GoldPaths.GLOBAL_DAILY, "stats_date",
               "global_daily", logger)

# COMMAND ----------

# =============================================================================
# CELL 7 — RUN LOG + COMPLETION
# =============================================================================

summary = {
    "notebook"            : "04_gold_global_daily",
    "pipeline_run_id"     : GoldConfig.RUN_ID,
    "run_timestamp_utc"   : GoldConfig.RUN_TS.isoformat(),
    "silver_source"       : SilverInputPaths.GLOBAL_STATS,
    "gold_target"         : GoldPaths.GLOBAL_DAILY,
    "rows_transformed"    : raw_count,
    "merge_rows_before"   : merge_stats["rows_before"],
    "merge_rows_after"    : merge_stats["rows_after"],
    "merge_rows_inserted" : merge_stats["rows_inserted"],
    "status"              : "SUCCESS",
}

write_run_log(summary, GoldLogPaths.GLOBAL_DAILY, logger)

logger.info("=" * 70)
logger.info("Gold: global_daily — COMPLETE")
for k, v in summary.items():
    logger.info(f"  {k:<30}: {v}")
logger.info("=" * 70)
