# Databricks notebook source
# MAGIC %run ../connection

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %run ./gold_utils

# COMMAND ----------


# CELL 1 — SETUP

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


# CELL 2 — READ SILVER (full read)

silver_df = read_silver_table(spark, SilverInputPaths.OHLC_HISTORY, logger)

# COMMAND ----------

silver_df.display()

# COMMAND ----------

# CELL 3 — TRANSFORMATIONS

logger.info("CELL 3: Applying OHLC enrichment transformations")

w_coin = Window.partitionBy("coin_id").orderBy("ohlc_timestamp")

SECONDS_7_DAYS  = 7  * 24 * 60 * 60   
SECONDS_30_DAYS = 30 * 24 * 60 * 60   

w_coin_time = (
    Window.partitionBy("coin_id")
          .orderBy(F.col("ohlc_timestamp").cast("long"))
)
w_7day  = w_coin_time.rangeBetween(-SECONDS_7_DAYS, 0)
w_30day = w_coin_time.rangeBetween(-SECONDS_30_DAYS, 0)

gold_df = (
    silver_df
    .select(
        F.col("coin_id"),
        F.col("ohlc_timestamp"),
        F.col("ohlc_date"),
        F.col("open_price"),
        F.col("high_price"),
        F.col("low_price"),
        F.col("close_price"),

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


# CELL 4 — FINAL COLUMN REORDER

logger.info("CELL 4: Reordering to final Gold schema")
final_df = gold_df.select(*GoldColumns.OHLC_ENRICHED)

# COMMAND ----------


# CELL 5 — DELTA MERGE INTO GOLD

logger.info("CELL 5: MERGE into gold/ohlc_enriched")

merge_stats = delta_merge_gold(
    spark      = spark,
    new_df     = final_df,
    table_path = GoldPaths.OHLC_ENRICHED,
    merge_keys = GoldMergeKeys.OHLC_ENRICHED,
    logger     = logger,
)

# COMMAND ----------

# CELL 6 — OPTIMIZE + Z-ORDER

logger.info("CELL 6: OPTIMIZE gold/ohlc_enriched")
optimize_delta(spark, GoldPaths.OHLC_ENRICHED, "coin_id, ohlc_date",
               "ohlc_enriched", logger)

# COMMAND ----------


# CELL 7 — RUN LOG + COMPLETION

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
