# Databricks notebook source
# =============================================================================
# NOTEBOOK: 06_kpi_gainers_losers.py
# LAYER:    Platinum (KPI)
# PURPOSE:  Create a Power BI-ready table for the "Gainers & Losers" dashboard.
#           Filters gold/daily_market_summary to the LAST 30 DAYS,
#           derives gainer rank, category, and badge columns.
#
# SOURCE:   gold/daily_market_summary (last 30 days)
# OUTPUT:   gold/kpi_gainers_losers   (DELTA OVERWRITE)
#
# POWER BI USAGE:
#   This table feeds Page 2 — "Gainers & Losers":
#     - Horizontal bar chart sorted by 24h change % (green=gain, red=loss)
#     - KPI Cards: Top Gainer, Top Loser, Avg Market Change
#     - Leaderboard with rank badges (#1, #2...)
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
from pyspark.sql import Window
from pyspark.sql.types import StringType

adls_name = "adlsnewhp1"
init_gold_config(adls_name)

logger = get_logger("kpi_gainers_losers")

logger.info("=" * 70)
logger.info("KPI: gainers_losers — START")
logger.info(f"  Run ID     : {GoldConfig.RUN_ID}")
logger.info(f"  Gold in    : {GoldPaths.DAILY_MARKET_SUMMARY}")
logger.info(f"  KPI out    : {GoldPaths.KPI_GAINERS_LOSERS}")
logger.info("=" * 70)

# COMMAND ----------

# =============================================================================
# CELL 2 — READ GOLD AND FILTER TO LAST 30 DAYS
# =============================================================================

logger.info("CELL 2: Reading gold/daily_market_summary (last 30 days)")

gold_df = spark.read.format("delta").load(GoldPaths.DAILY_MARKET_SUMMARY)

max_date   = gold_df.agg(F.max("summary_date")).collect()[0][0]
cutoff_30d = F.date_sub(F.lit(max_date), 30)

last_30d_df = gold_df.filter(F.col("summary_date") >= cutoff_30d)

row_count = last_30d_df.count()
logger.info(f"  Latest date: {max_date}")
logger.info(f"  Rows (last 30 days): {row_count:,}")

# COMMAND ----------

# =============================================================================
# CELL 3 — DERIVE KPI COLUMNS
#
# gainer_rank_daily: RANK per day, ordered by price change % descending.
#   Rank 1 = biggest gainer of the day.
#
# category: 'Gainer' if positive change, 'Loser' if negative, 'Flat' if 0.
#   Used as a slicer/filter in Power BI.
#
# gainer_loser_badge: Top 5 flag for conditional formatting.
#   'Top 5 Gainer', 'Top 5 Loser', or null.
# =============================================================================

logger.info("CELL 3: Deriving gainer/loser KPI columns")

w_daily = Window.partitionBy("summary_date").orderBy(F.col("price_change_pct_24h").desc())

kpi_df = (
    last_30d_df
    .withColumn("gainer_rank_daily", F.rank().over(w_daily))
    .withColumn(
        "category",
        F.when(F.col("price_change_pct_24h") > 0, "Gainer")
         .when(F.col("price_change_pct_24h") < 0, "Loser")
         .otherwise("Flat")
         .cast(StringType())
    )
    .withColumn(
        "gainer_loser_badge",
        F.when(F.col("is_top5_gainer") == True, "Top 5 Gainer")
         .when(F.col("is_top5_loser") == True, "Top 5 Loser")
         .otherwise(None)
         .cast(StringType())
    )
    .select(
        F.col("summary_date").alias("date"),
        F.col("name").alias("coin_name"),
        F.col("symbol"),
        F.col("price_change_pct_24h"),
        F.col("current_price_usd").alias("price_usd"),
        F.col("gainer_rank_daily"),
        F.col("category"),
        F.col("gainer_loser_badge"),
    )
)

kpi_count = kpi_df.count()
logger.info(f"  KPI rows: {kpi_count:,}")

# COMMAND ----------

kpi_df.display()

# COMMAND ----------

# =============================================================================
# CELL 4 — OVERWRITE KPI TABLE
# =============================================================================

logger.info("CELL 4: OVERWRITE gold/kpi_gainers_losers")

written_count = delta_overwrite(kpi_df, GoldPaths.KPI_GAINERS_LOSERS, logger)

# COMMAND ----------

# =============================================================================
# CELL 5 — RUN LOG + COMPLETION
# =============================================================================

summary = {
    "notebook"           : "06_kpi_gainers_losers",
    "pipeline_run_id"    : GoldConfig.RUN_ID,
    "run_timestamp_utc"  : GoldConfig.RUN_TS.isoformat(),
    "gold_source"        : GoldPaths.DAILY_MARKET_SUMMARY,
    "kpi_target"         : GoldPaths.KPI_GAINERS_LOSERS,
    "date_range"         : f"last 30 days up to {max_date}",
    "rows_written"       : written_count,
    "status"             : "SUCCESS",
}

write_run_log(summary, GoldLogPaths.KPI_GAINERS_LOSERS, logger)

logger.info("=" * 70)
logger.info("KPI: gainers_losers — COMPLETE")
for k, v in summary.items():
    logger.info(f"  {k:<30}: {v}")
logger.info("=" * 70)
