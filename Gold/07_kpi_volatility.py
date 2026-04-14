# Databricks notebook source
# =============================================================================
# NOTEBOOK: 07_kpi_volatility.py
# LAYER:    Platinum (KPI)
# PURPOSE:  Create a Power BI-ready volatility/OHLC analysis table.
#           Filters gold/ohlc_enriched to the LAST 90 DAYS, joins coin names
#           from Silver, and derives 30-day annualised volatility.
#
# SOURCE:   gold/ohlc_enriched (last 90 days) + silver/market_snapshot (names)
# OUTPUT:   gold/kpi_volatility  (DELTA OVERWRITE)
#
# ANNUALISED VOLATILITY FORMULA:
#   annualised_vol = STDDEV(daily_return_pct) * SQRT(365)
#   Higher value = riskier coin. Used for volatility ranking bar chart.
#
# POWER BI USAGE:
#   This table feeds Page 4 — "Volatility & OHLC":
#     - Candlestick chart per coin
#     - Volatility ranking bar chart
#     - KPI Cards: Most/Least Volatile Coin
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
from pyspark.sql.types import DoubleType
import math

adls_name = "adlsnewhp"
init_gold_config(adls_name)

logger = get_logger("kpi_volatility")

logger.info("=" * 70)
logger.info("KPI: volatility — START")
logger.info(f"  Run ID     : {GoldConfig.RUN_ID}")
logger.info(f"  Gold in    : {GoldPaths.OHLC_ENRICHED}")
logger.info(f"  Silver join: {SilverInputPaths.MARKET_SNAPSHOT}")
logger.info(f"  KPI out    : {GoldPaths.KPI_VOLATILITY}")
logger.info("=" * 70)

# COMMAND ----------

# =============================================================================
# CELL 2 — READ GOLD OHLC AND FILTER TO LAST 90 DAYS
# =============================================================================

logger.info("CELL 2: Reading gold/ohlc_enriched (last 90 days)")

ohlc_df = spark.read.format("delta").load(GoldPaths.OHLC_ENRICHED)

max_date   = ohlc_df.agg(F.max("ohlc_date")).collect()[0][0]
cutoff_90d = F.date_sub(F.lit(max_date), 90)

last_90d_df = ohlc_df.filter(F.col("ohlc_date") >= cutoff_90d)

ohlc_count = last_90d_df.count()
logger.info(f"  Latest OHLC date: {max_date}")
logger.info(f"  Rows (last 90 days): {ohlc_count:,}")

# COMMAND ----------

# =============================================================================
# CELL 3 — JOIN COIN NAMES FROM SILVER MARKET SNAPSHOT
# =============================================================================

logger.info("CELL 3: Joining coin names from Silver market_snapshot")

market_df = spark.read.format("delta").load(SilverInputPaths.MARKET_SNAPSHOT)

market_max_date = market_df.agg(F.max("snapshot_date")).collect()[0][0]
name_lookup = (
    market_df
    .filter(F.col("snapshot_date") == market_max_date)
    .select(
        F.col("coin_id").alias("m_coin_id"),
        F.col("name").alias("coin_name"),
    )
    .dropDuplicates(["m_coin_id"])
)

# COMMAND ----------

# =============================================================================
# CELL 4 — DERIVE ANNUALISED VOLATILITY
#
# 30-Day Annualised Volatility = STDDEV(daily_return_pct) * SQRT(365)
#   SQRT(365) annualises the daily STDDEV to an annual figure.
# =============================================================================

logger.info("CELL 4: Computing annualised volatility")

w_vol = (
    Window.partitionBy("coin_id")
    .orderBy("ohlc_date")
    .rowsBetween(-29, 0)
)

volatility_df = (
    last_90d_df
    .withColumn(
        "annualised_volatility_30d",
        (F.stddev("daily_return_pct").over(w_vol) * math.sqrt(365))
        .cast(DoubleType())
    )
)

# Join coin names
kpi_df = (
    volatility_df
    .join(name_lookup, volatility_df["coin_id"] == name_lookup["m_coin_id"], "left")
    .select(
        F.col("coin_id"),
        F.coalesce(F.col("coin_name"), F.col("coin_id")).alias("coin_name"),
        F.col("ohlc_date"),
        F.col("close_price"),
        F.col("rolling_7day_avg_close"),
        F.col("rolling_30day_avg_close"),
        F.col("candle_range"),
        F.col("is_bullish"),
        F.col("candle_body"),
        F.col("daily_return_pct"),
        F.col("annualised_volatility_30d"),
    )
)

kpi_count = kpi_df.count()
logger.info(f"  KPI rows: {kpi_count:,}")

# COMMAND ----------

kpi_df.display()

# COMMAND ----------

# =============================================================================
# CELL 5 — OVERWRITE KPI TABLE
# =============================================================================

logger.info("CELL 5: OVERWRITE gold/kpi_volatility")

written_count = delta_overwrite(kpi_df, GoldPaths.KPI_VOLATILITY, logger)

# COMMAND ----------

# =============================================================================
# CELL 6 — RUN LOG + COMPLETION
# =============================================================================

summary = {
    "notebook"           : "07_kpi_volatility",
    "pipeline_run_id"    : GoldConfig.RUN_ID,
    "run_timestamp_utc"  : GoldConfig.RUN_TS.isoformat(),
    "gold_source"        : GoldPaths.OHLC_ENRICHED,
    "kpi_target"         : GoldPaths.KPI_VOLATILITY,
    "date_range"         : f"last 90 days up to {max_date}",
    "rows_written"       : written_count,
    "status"             : "SUCCESS",
}

write_run_log(summary, GoldLogPaths.KPI_VOLATILITY, logger)

logger.info("=" * 70)
logger.info("KPI: volatility — COMPLETE")
for k, v in summary.items():
    logger.info(f"  {k:<30}: {v}")
logger.info("=" * 70)
