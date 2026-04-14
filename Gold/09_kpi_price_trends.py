# Databricks notebook source
# =============================================================================
# NOTEBOOK: 09_kpi_price_trends.py
# LAYER:    Platinum (KPI)
# PURPOSE:  Create a Power BI-ready time-series table for the "Price Trends"
#           dashboard page. Combines hourly price/volume data with global
#           market context and coin names.
#
# SOURCES:
#   silver/hourly_timeseries  — hourly price + volume per coin (last 30 days)
#   gold/global_daily         — daily global market cap, BTC dominance, sentiment
#   silver/market_snapshot    — coin names (for chart labels)
#
# OUTPUT:   gold/kpi_price_trends  (DELTA OVERWRITE)
#
# VOLUME SPIKE DETECTION:
#   is_volume_spike = true when a coin's hourly volume exceeds 2× its
#   7-day average volume. This flags unusual trading activity.
#
# POWER BI USAGE:
#   This table feeds Page 3 — "Price Trends":
#     - Multi-line chart: hourly price with volume bars below
#     - BTC dominance secondary axis
#     - Volume spike markers on chart
#     - Market sentiment background shade
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
from pyspark.sql.types import BooleanType, DoubleType

adls_name = "adlsnewhp"
init_gold_config(adls_name)

logger = get_logger("kpi_price_trends")

logger.info("=" * 70)
logger.info("KPI: price_trends — START")
logger.info(f"  Run ID     : {GoldConfig.RUN_ID}")
logger.info(f"  Hourly in  : {SilverInputPaths.HOURLY_TIMESERIES}")
logger.info(f"  Global in  : {GoldPaths.GLOBAL_DAILY}")
logger.info(f"  Names in   : {SilverInputPaths.MARKET_SNAPSHOT}")
logger.info(f"  KPI out    : {GoldPaths.KPI_PRICE_TRENDS}")
logger.info("=" * 70)

# COMMAND ----------

# =============================================================================
# CELL 2 — READ SOURCE TABLES
# =============================================================================

logger.info("CELL 2: Reading source tables")

hourly_df = spark.read.format("delta").load(SilverInputPaths.HOURLY_TIMESERIES)
logger.info(f"  Hourly rows: {hourly_df.count():,}")

global_df = spark.read.format("delta").load(GoldPaths.GLOBAL_DAILY)
logger.info(f"  Global rows: {global_df.count():,}")

market_df = spark.read.format("delta").load(SilverInputPaths.MARKET_SNAPSHOT)

# COMMAND ----------

# =============================================================================
# CELL 3 — PREPARE COIN NAME LOOKUP
# =============================================================================

logger.info("CELL 3: Building coin name lookup")

market_max_date = market_df.agg(F.max("snapshot_date")).collect()[0][0]
name_lookup = (
    market_df
    .filter(F.col("snapshot_date") == market_max_date)
    .select(
        F.col("coin_id").alias("n_coin_id"),
        F.col("name").alias("coin_name"),
    )
    .dropDuplicates(["n_coin_id"])
)

# COMMAND ----------

# =============================================================================
# CELL 4 — COMPUTE VOLUME SPIKE FLAG
#
# VOLUME SPIKE = hourly volume > 2× the 7-day rolling average volume.
# 7 days × 24 hours = 168 rows for the rolling window.
# =============================================================================

logger.info("CELL 4: Computing volume spike detection")

w_7d_vol = (
    Window.partitionBy("coin_id")
    .orderBy("hour_timestamp")
    .rowsBetween(-168, 0)
)

hourly_with_spike = (
    hourly_df
    .withColumn(
        "avg_volume_7d",
        F.avg("volume_usd").over(w_7d_vol)
    )
    .withColumn(
        "is_volume_spike",
        F.when(
            F.col("avg_volume_7d").isNotNull() & (F.col("avg_volume_7d") > 0),
            F.col("volume_usd") > (F.col("avg_volume_7d") * 2)
        ).otherwise(False)
        .cast(BooleanType())
    )
    .drop("avg_volume_7d")
)

# COMMAND ----------

# =============================================================================
# CELL 5 — PREPARE GLOBAL DAILY FOR JOIN
# =============================================================================

logger.info("CELL 5: Preparing global daily for join")

global_join = (
    global_df
    .select(
        F.col("stats_date").alias("g_date"),
        F.col("total_market_cap_usd").alias("global_market_cap_usd"),
        F.col("btc_dominance_pct"),
        F.col("market_sentiment"),
    )
)

# COMMAND ----------

# =============================================================================
# CELL 6 — FINAL JOIN: hourly + names + global context
# =============================================================================

logger.info("CELL 6: Joining hourly + names + global context")

kpi_df = (
    hourly_with_spike
    .join(name_lookup, hourly_with_spike["coin_id"] == name_lookup["n_coin_id"], "left")
    .withColumn("hour_date", F.to_date("hour_timestamp"))
    .join(global_join, F.col("hour_date") == global_join["g_date"], "left")
    .select(
        F.col("coin_id"),
        F.coalesce(F.col("coin_name"), F.col("coin_id")).alias("coin_name"),
        F.col("hour_timestamp"),
        F.col("price_usd"),
        F.col("volume_usd"),
        F.col("market_cap_usd"),
        F.col("is_volume_spike"),
        F.col("global_market_cap_usd"),
        F.col("btc_dominance_pct"),
        F.col("market_sentiment"),
    )
)

kpi_count = kpi_df.count()
logger.info(f"  KPI rows: {kpi_count:,}")

# COMMAND ----------

kpi_df.display()

# COMMAND ----------

# =============================================================================
# CELL 7 — OVERWRITE KPI TABLE
# =============================================================================

logger.info("CELL 7: OVERWRITE gold/kpi_price_trends")

written_count = delta_overwrite(kpi_df, GoldPaths.KPI_PRICE_TRENDS, logger)

# COMMAND ----------

# =============================================================================
# CELL 8 — RUN LOG + COMPLETION
# =============================================================================

summary = {
    "notebook"           : "09_kpi_price_trends",
    "pipeline_run_id"    : GoldConfig.RUN_ID,
    "run_timestamp_utc"  : GoldConfig.RUN_TS.isoformat(),
    "sources"            : [
        SilverInputPaths.HOURLY_TIMESERIES,
        GoldPaths.GLOBAL_DAILY,
        SilverInputPaths.MARKET_SNAPSHOT,
    ],
    "kpi_target"         : GoldPaths.KPI_PRICE_TRENDS,
    "rows_written"       : written_count,
    "status"             : "SUCCESS",
}

write_run_log(summary, GoldLogPaths.KPI_PRICE_TRENDS, logger)

logger.info("=" * 70)
logger.info("KPI: price_trends — COMPLETE")
for k, v in summary.items():
    logger.info(f"  {k:<30}: {v}")
logger.info("=" * 70)
