# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType, BooleanType
import math

# COMMAND ----------

# FUNCTION: apply_daily_market_transforms()

def apply_daily_market_transforms(silver_df: DataFrame) -> DataFrame:
    """
    Apply all Gold-layer business transformations to Silver market_snapshot.
    """
    w_date = Window.partitionBy("snapshot_date")

    w_gainer = Window.partitionBy("snapshot_date").orderBy(
        F.col("price_change_pct_24h").desc()
    )

    w_loser = Window.partitionBy("snapshot_date").orderBy(
        F.col("price_change_pct_24h").asc()
    )

    return silver_df.select(
        # ── Carry from Silver ────────────────────────────────────────────────
        F.col("snapshot_date").alias("summary_date"),
        F.col("coin_id"),
        F.col("name"),
        F.col("symbol"),
        F.col("current_price_usd"),
        F.col("market_cap_usd"),
        F.col("market_cap_rank"),
        F.col("total_volume_24h_usd"),

        # ── Derived: price range ─────────────────────────────────────────────
        (F.col("high_24h_usd") - F.col("low_24h_usd"))
            .cast(DoubleType())
            .alias("price_range_24h_usd"),

        F.col("price_change_pct_24h"),

        # ── Derived: top 5 gainer/loser flags ────────────────────────────────
        (F.row_number().over(w_gainer) <= 5)
            .cast(BooleanType())
            .alias("is_top5_gainer"),

        (F.row_number().over(w_loser) <= 5)
            .cast(BooleanType())
            .alias("is_top5_loser"),

        # ── Derived: market cap share ────────────────────────────────────────
        (
            F.col("market_cap_usd")
            / F.sum("market_cap_usd").over(w_date)
            * 100
        ).cast(DoubleType()).alias("mkt_cap_share_pct"),

        # ── Derived: distance from ATH ───────────────────────────────────────
        F.when(
            F.col("ath_usd").isNotNull() & (F.col("ath_usd") > 0),
            ((F.col("current_price_usd") - F.col("ath_usd")) / F.col("ath_usd") * 100)
        ).cast(DoubleType()).alias("price_to_ath_pct"),

        # ── Derived: supply utilisation ──────────────────────────────────────
        F.when(
            F.col("total_supply").isNotNull() & (F.col("total_supply") > 0),
            (F.col("circulating_supply") / F.col("total_supply") * 100)
        ).cast(DoubleType()).alias("supply_utilisation_pct"),
    )

# COMMAND ----------


# FUNCTION: compute_volume_spikes()

def compute_volume_spikes(
    hourly_df     : DataFrame,
    window_hours  : int = 168,
    threshold_multiplier : int = 2,
) -> DataFrame:
    """
    Detect volume spikes in hourly trading data.
    """
    w_vol = (
        Window.partitionBy("coin_id")
        .orderBy("hour_timestamp")
        .rowsBetween(-window_hours, 0)
    )

    return (
        hourly_df
        .withColumn("avg_volume_7d", F.avg("volume_usd").over(w_vol))
        .withColumn(
            "is_volume_spike",
            F.when(
                F.col("avg_volume_7d").isNotNull() & (F.col("avg_volume_7d") > 0),
                F.col("volume_usd") > (F.col("avg_volume_7d") * threshold_multiplier)
            ).otherwise(False)
            .cast(BooleanType())
        )
        .drop("avg_volume_7d")
    )

# COMMAND ----------

# FUNCTION: compute_annualised_volatility()

def compute_annualised_volatility(
    ohlc_df     : DataFrame,
    window_days : int = 30,
) -> DataFrame:
    """
    Compute rolling annualised volatility from daily returns.
    """
    w_vol = (
        Window.partitionBy("coin_id")
        .orderBy("ohlc_date")
        .rowsBetween(-(window_days - 1), 0)
    )

    return ohlc_df.withColumn(
        "annualised_volatility_30d",
        (F.stddev("daily_return_pct").over(w_vol) * math.sqrt(365))
        .cast(DoubleType())
    )

# COMMAND ----------

# FUNCTION: build_merge_condition()

def build_merge_condition(merge_keys: list) -> str:
    """
    Build a Delta MERGE condition string from primary key columns.

    Example:
        build_merge_condition(["coin_id", "summary_date"])
        → "existing.coin_id = new.coin_id AND existing.summary_date = new.summary_date"

    Args:
        merge_keys: List of column names forming the composite primary key

    Returns:
        SQL condition string for Delta MERGE ON clause
    """
    return " AND ".join(
        [f"existing.{col} = new.{col}" for col in merge_keys]
    )
