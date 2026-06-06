# Databricks notebook source
# =============================================================================
# NOTEBOOK: 05_kpi_market_overview.py
# LAYER:    Platinum (KPI)
# PURPOSE:  Create a Power BI-ready snapshot of the market overview.
#           Filters gold/daily_market_summary to the LATEST DATE only,
#           renames columns to human-readable Power BI labels, and
#           OVERWRITES the KPI table.
#
# SOURCE:   gold/daily_market_summary (latest date only)
# OUTPUT:   gold/kpi_market_overview  (DELTA OVERWRITE — fresh each run)
#
# POWER BI USAGE:
#   This table feeds Page 1 — "Market Overview":
#     - KPI Cards: Total Market Cap, Total Volume, # Gainers, # Losers
#     - Bar chart: Top 10 by Market Cap
#     - Donut chart: Market Cap Share
#     - Gainer/Loser leaderboards
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

adls_name = "adlsnewhp3"
init_gold_config(adls_name)

logger = get_logger("kpi_market_overview")

logger.info("=" * 70)
logger.info("KPI: market_overview — START")
logger.info(f"  Run ID     : {GoldConfig.RUN_ID}")
logger.info(f"  Gold in    : {GoldPaths.DAILY_MARKET_SUMMARY}")
logger.info(f"  KPI out    : {GoldPaths.KPI_MARKET_OVERVIEW}")
logger.info("=" * 70)

# COMMAND ----------

# =============================================================================
# CELL 2 — READ GOLD TABLE AND FILTER TO LATEST DATE
# =============================================================================

logger.info("CELL 2: Reading gold/daily_market_summary (latest date)")

gold_df = spark.read.format("delta").load(GoldPaths.DAILY_MARKET_SUMMARY)

max_date = gold_df.agg(F.max("summary_date")).collect()[0][0]
logger.info(f"  Latest date in Gold: {max_date}")

latest_df = gold_df.filter(F.col("summary_date") == max_date)

row_count = latest_df.count()
logger.info(f"  Rows for latest date: {row_count:,}")

# COMMAND ----------


# =============================================================================
# CELL 3 — JOIN GLOBAL MARKET DATA + RENAME TO POWER BI-FRIENDLY COLUMNS
#
# WHY JOIN global_daily HERE?
#   kpi_market_overview originally had SUM(market_cap_usd) across our top 50
#   coins as the "total market cap". But the real crypto market has 10,000+
#   coins — our top-50 SUM underestimates the true global market cap.
#
#   global_daily (from CoinGecko's /global endpoint) has the ACTUAL global
#   total_market_cap_usd and total_volume_24h_usd across ALL coins.
#   We attach these as columns so the dashboard shows the real global numbers
#   alongside each coin's per-coin metrics.
# =============================================================================
logger.info("CELL 3: Reading global_daily for real global market numbers")
global_df = spark.read.format("delta").load(GoldPaths.GLOBAL_DAILY)
global_max_date = global_df.agg(F.max("stats_date")).collect()[0][0]
logger.info(f"  Latest global_daily date: {global_max_date}")
# Single row with global metrics for the latest date
global_latest = (
    global_df
    .filter(F.col("stats_date") == global_max_date)
    .select(
        F.col("total_market_cap_usd").alias("global_market_cap_usd"),
        F.col("total_volume_24h_usd").alias("global_volume_24h_usd"),
        F.col("market_sentiment"),
    )
)
logger.info("CELL 3: Renaming columns and joining global data")
kpi_df = (
    latest_df
    .select(
        F.col("coin_id"),
        F.col("name").alias("coin_name"),
        F.col("symbol"),
        F.col("current_price_usd").alias("price_usd"),
        F.col("market_cap_usd"),
        F.col("market_cap_rank"),
        F.col("total_volume_24h_usd").alias("volume_24h_usd"),
        F.col("price_change_pct_24h"),
        F.col("price_range_24h_usd"),
        F.col("mkt_cap_share_pct"),
        F.col("is_top5_gainer"),
        F.col("is_top5_loser"),
        F.col("price_to_ath_pct"),
        F.col("summary_date").alias("snapshot_date"),
    )
    .crossJoin(global_latest)  # 1 global row × 50 coin rows = 50 rows
)

# COMMAND ----------

kpi_df.display()

# COMMAND ----------

# =============================================================================
# CELL 4 — OVERWRITE KPI TABLE
# =============================================================================

logger.info("CELL 4: OVERWRITE gold/kpi_market_overview")

written_count = delta_overwrite(kpi_df, GoldPaths.KPI_MARKET_OVERVIEW, logger)

# COMMAND ----------

# =============================================================================
# CELL 5 — RUN LOG + COMPLETION
# =============================================================================

summary = {
    "notebook"            : "05_kpi_market_overview",
    "pipeline_run_id"     : GoldConfig.RUN_ID,
    "run_timestamp_utc"   : GoldConfig.RUN_TS.isoformat(),
    "gold_source"         : GoldPaths.DAILY_MARKET_SUMMARY,
    "kpi_target"          : GoldPaths.KPI_MARKET_OVERVIEW,
    "snapshot_date"       : str(max_date),
    "rows_written"        : written_count,
    "status"              : "SUCCESS",
}

write_run_log(summary, GoldLogPaths.KPI_MARKET_OVERVIEW, logger)

logger.info("=" * 70)
logger.info("KPI: market_overview — COMPLETE")
for k, v in summary.items():
    logger.info(f"  {k:<30}: {v}")
logger.info("=" * 70)
