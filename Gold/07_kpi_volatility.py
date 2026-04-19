# Databricks notebook source
# MAGIC %run ../connection

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./gold_utils

# COMMAND ----------

# MAGIC %run ./transformations

# COMMAND ----------



from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType
import math

adls_name = "adlsnewhp1"
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



logger.info("CELL 2: Reading gold/ohlc_enriched (last 90 days)")

ohlc_df = spark.read.format("delta").load(GoldPaths.OHLC_ENRICHED)

max_date   = ohlc_df.agg(F.max("ohlc_date")).collect()[0][0]
cutoff_90d = F.date_sub(F.lit(max_date), 90)

last_90d_df = ohlc_df.filter(F.col("ohlc_date") >= cutoff_90d)

ohlc_count = last_90d_df.count()
logger.info(f"  Latest OHLC date: {max_date}")
logger.info(f"  Rows (last 90 days): {ohlc_count:,}")

# COMMAND ----------

# JOIN COIN NAMES FROM SILVER MARKET SNAPSHOT


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

# DERIVE ANNUALISED VOLATILITY
#
# Business logic is in transformations.py (imported via %run ./transformations).
# compute_annualised_volatility() calculates: STDDEV(daily_return_pct) × √365


logger.info("CELL 4: Computing annualised volatility (via transformations.py)")

volatility_df = compute_annualised_volatility(last_90d_df, window_days=30)


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


logger.info("CELL 5: OVERWRITE gold/kpi_volatility")

written_count = delta_overwrite(kpi_df, GoldPaths.KPI_VOLATILITY, logger)


optimize_delta(spark, GoldPaths.KPI_VOLATILITY, "coin_id, ohlc_date",
               "kpi_volatility", logger)

# COMMAND ----------


# RUN LOG + COMPLETION


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
