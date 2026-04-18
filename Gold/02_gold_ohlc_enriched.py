# Databricks notebook source
# =============================================================================
# NOTEBOOK: 02_gold_ohlc_enriched.py
# LAYER:    Gold
# PURPOSE:  Enrich Silver OHLC candle data with technical analysis features:
#           candle body/range, bullish/bearish flag, rolling averages,
#           and daily return percentage.
#
# SOURCE:   silver/ohlc_history   (full read, no watermark)
# OUTPUT:   gold/ohlc_enriched    (DELTA MERGE — accumulates candle history)
#
# MERGE KEY: (coin_id, ohlc_timestamp)
# Z-ORDER:   (coin_id, ohlc_date)
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
from pyspark.sql.types import DoubleType, BooleanType

adls_name = "adlsnewhp1"
init_gold_config(adls_name)

logger = get_logger("gold_ohlc_enriched")

logger.info("=" * 70)
logger.info("Gold: ohlc_enriched — START")
logger.info(f"  Run ID     : {GoldConfig.RUN_ID}")
logger.info(f"  Silver in  : {SilverInputPaths.OHLC_HISTORY}")
logger.info(f"  Gold out   : {GoldPaths.OHLC_ENRICHED}")
logger.info("=" * 70)

# COMMAND ----------

# =============================================================================
# CELL 2 — READ SILVER (full read)
# =============================================================================

silver_df = read_silver_table(spark, SilverInputPaths.OHLC_HISTORY, logger)

# COMMAND ----------

silver_df.display()

# COMMAND ----------

# =============================================================================
# CELL 3 — TRANSFORMATIONS
#
# TECHNICAL ANALYSIS COLUMNS:
#
#   candle_body = ABS(close - open)
#     The "body" of a candlestick chart. Large body = strong price movement.
#
#   candle_range = high - low
#     Total price swing within this period. High range = volatile period.
#
#   is_bullish = close > open
#     Green candle (price went UP). False = red candle (price went DOWN).
#
#   pct_change_in_period = (close - open) / open * 100
#     How much % the price moved within this single candle.
#
#   rolling_7day_avg_close = AVG(close) OVER (... ROWS 6 PRECEDING)
#     Short-term trend line. Used to detect momentum.
#
#   rolling_30day_avg_close = AVG(close) OVER (... ROWS 29 PRECEDING)
#     Long-term trend line. When 7-day crosses above 30-day = bullish signal.
#
#   daily_return_pct = (close - prev_close) / prev_close * 100
#     Day-over-day percentage change in close price.
# =============================================================================

logger.info("CELL 3: Applying OHLC enrichment transformations")

# Window: per coin, ordered by timestamp
w_coin = Window.partitionBy("coin_id").orderBy("ohlc_timestamp")

# ── Rolling windows for moving averages (TIME-BASED, not row-based) ──────
#
# WHY rangeBetween INSTEAD OF rowsBetween?
#   rowsBetween(-6, 0) counts 7 physical rows, NOT 7 days of time.
#   CoinGecko's OHLC API with days=30 returns 4-HOUR candles (6/day).
#   With rowsBetween:  7 rows × 4 hours = 28 hours  (NOT 7 days!)
#                     30 rows × 4 hours = 120 hours (NOT 30 days!)
#   This silently corrupts Golden Cross / Death Cross signals.
#
#   rangeBetween operates on the NUMERIC VALUE of the order column.
#   By ordering on ohlc_timestamp cast to Unix epoch (seconds),
#   we specify the window in seconds → correct regardless of candle
#   granularity (4-hour, daily) or missing data gaps.
#
#   7 days  = 7 × 86400  =  604,800 seconds
#   30 days = 30 × 86400 = 2,592,000 seconds
# ─────────────────────────────────────────────────────────────────────────
SECONDS_7_DAYS  = 7  * 24 * 60 * 60   # 604,800
SECONDS_30_DAYS = 30 * 24 * 60 * 60   # 2,592,000

w_coin_time = (
    Window.partitionBy("coin_id")
          .orderBy(F.col("ohlc_timestamp").cast("long"))
)
w_7day  = w_coin_time.rangeBetween(-SECONDS_7_DAYS, 0)
w_30day = w_coin_time.rangeBetween(-SECONDS_30_DAYS, 0)

gold_df = (
    silver_df
    .select(
        # ── Carry from Silver ─────────────────────────────────────────────────
        F.col("coin_id"),
        F.col("ohlc_timestamp"),
        F.col("ohlc_date"),
        F.col("open_price"),
        F.col("high_price"),
        F.col("low_price"),
        F.col("close_price"),

        # ── Derived: candle metrics ───────────────────────────────────────────
        F.abs(F.col("close_price") - F.col("open_price"))
            .cast(DoubleType())
            .alias("candle_body"),

        (F.col("high_price") - F.col("low_price"))
            .cast(DoubleType())
            .alias("candle_range"),

        (F.col("close_price") > F.col("open_price"))
            .cast(BooleanType())
            .alias("is_bullish"),

        F.when(
            F.col("open_price").isNotNull() & (F.col("open_price") > 0),
            ((F.col("close_price") - F.col("open_price")) / F.col("open_price") * 100)
        ).cast(DoubleType()).alias("pct_change_in_period"),
    )
    .withColumn(
        "rolling_7day_avg_close",
        F.avg("close_price").over(w_7day).cast(DoubleType())
    )
    .withColumn(
        "rolling_30day_avg_close",
        F.avg("close_price").over(w_30day).cast(DoubleType())
    )
    .withColumn(
        "daily_return_pct",
        F.when(
            F.lag("close_price", 1).over(w_coin).isNotNull() &
            (F.lag("close_price", 1).over(w_coin) > 0),
            (
                (F.col("close_price") - F.lag("close_price", 1).over(w_coin))
                / F.lag("close_price", 1).over(w_coin) * 100
            )
        ).cast(DoubleType())
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
final_df = gold_df.select(*GoldColumns.OHLC_ENRICHED)

# COMMAND ----------

# =============================================================================
# CELL 5 — DELTA MERGE INTO GOLD
# =============================================================================

logger.info("CELL 5: MERGE into gold/ohlc_enriched")

merge_stats = delta_merge_gold(
    spark      = spark,
    new_df     = final_df,
    table_path = GoldPaths.OHLC_ENRICHED,
    merge_keys = GoldMergeKeys.OHLC_ENRICHED,
    logger     = logger,
)

# COMMAND ----------

# =============================================================================
# CELL 6 — OPTIMIZE + Z-ORDER
# =============================================================================

logger.info("CELL 6: OPTIMIZE gold/ohlc_enriched")
optimize_delta(spark, GoldPaths.OHLC_ENRICHED, "coin_id, ohlc_date",
               "ohlc_enriched", logger)

# COMMAND ----------

# =============================================================================
# CELL 7 — RUN LOG + COMPLETION
# =============================================================================

summary = {
    "notebook"            : "02_gold_ohlc_enriched",
    "pipeline_run_id"     : GoldConfig.RUN_ID,
    "run_timestamp_utc"   : GoldConfig.RUN_TS.isoformat(),
    "silver_source"       : SilverInputPaths.OHLC_HISTORY,
    "gold_target"         : GoldPaths.OHLC_ENRICHED,
    "rows_transformed"    : raw_count,
    "merge_rows_before"   : merge_stats["rows_before"],
    "merge_rows_after"    : merge_stats["rows_after"],
    "merge_rows_inserted" : merge_stats["rows_inserted"],
    "status"              : "SUCCESS",
}

write_run_log(summary, GoldLogPaths.OHLC_ENRICHED, logger)

logger.info("=" * 70)
logger.info("Gold: ohlc_enriched — COMPLETE")
for k, v in summary.items():
    logger.info(f"  {k:<30}: {v}")
logger.info("=" * 70)
