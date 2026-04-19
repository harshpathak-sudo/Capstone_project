# Databricks notebook source
# =============================================================================
# FILE:    transformations.py
# LAYER:   Gold
# PURPOSE: Pure transformation functions extracted from Gold notebooks.
#          Contains business logic that can be independently unit tested.
#
#          These functions are called by their respective Gold notebooks
#          via %run ./transformations. By keeping business logic here
#          (not inline in notebooks), we can:
#            1. Test each formula in isolation with known inputs
#            2. Change a formula in one place — all notebooks update
#            3. Review/audit business logic without reading through
#               boilerplate setup, merge, and logging code
#
# EXTRACTED FROM:
#   01_gold_daily_market_summary.py → apply_daily_market_transforms()
#   09_kpi_price_trends.py          → compute_volume_spikes()
#   07_kpi_volatility.py            → compute_annualised_volatility()
#   gold_utils.py                   → build_merge_condition()
#
# USAGE:
#   # In any Gold notebook, after %run ./gold_utils:
#   %run ./transformations
#   gold_df = apply_daily_market_transforms(silver_df)
# =============================================================================

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType, BooleanType
import math

# COMMAND ----------

# =============================================================================
# FUNCTION: apply_daily_market_transforms()
#
# EXTRACTED FROM: 01_gold_daily_market_summary.py (Cell 3)
#
# BUSINESS LOGIC:
#   1. Price range = high_24h - low_24h
#      → How much the price swung in 24 hours
#
#   2. Top 5 gainer flag = ROW_NUMBER() OVER (PARTITION BY date ORDER BY change DESC) <= 5
#      → For each day, rank all coins by % change. Top 5 get flagged.
#
#   3. Top 5 loser flag = ROW_NUMBER() OVER (PARTITION BY date ORDER BY change ASC) <= 5
#      → Same as above but ascending — worst 5 performers.
#
#   4. Market cap share = (coin_market_cap / SUM(all_market_caps_that_day)) × 100
#      → What percentage of the total top-50 market cap does this coin represent?
#
#   5. Distance from ATH = ((current_price - ath) / ath) × 100
#      → Negative means below ATH (e.g., -30% = 30% below peak)
#
#   6. Supply utilisation = (circulating_supply / total_supply) × 100
#      → What % of the coin's total supply is in circulation?
#      → null if total_supply is null (unlimited supply coins)
#
# NOTE: Does NOT add gold_processed_timestamp — the calling notebook adds
#       that column after this function returns.
# =============================================================================

def apply_daily_market_transforms(silver_df: DataFrame) -> DataFrame:
    """
    Apply all Gold-layer business transformations to Silver market_snapshot.

    Args:
        silver_df: Silver market_snapshot DataFrame with columns:
                   snapshot_date, coin_id, name, symbol, current_price_usd,
                   market_cap_usd, market_cap_rank, total_volume_24h_usd,
                   high_24h_usd, low_24h_usd, price_change_pct_24h,
                   ath_usd, circulating_supply, total_supply

    Returns:
        Transformed DataFrame with derived columns (without gold_processed_timestamp)
    """
    # Window: all coins for the same date
    w_date = Window.partitionBy("snapshot_date")

    # Window: rank by price change — best performers first (for gainer flag)
    w_gainer = Window.partitionBy("snapshot_date").orderBy(
        F.col("price_change_pct_24h").desc()
    )

    # Window: rank by price change — worst performers first (for loser flag)
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

# =============================================================================
# FUNCTION: compute_volume_spikes()
#
# EXTRACTED FROM: 09_kpi_price_trends.py (Cell 4)
#
# BUSINESS LOGIC:
#   A "volume spike" occurs when a coin's hourly trading volume exceeds
#   threshold_multiplier × its rolling average volume over window_hours.
#
#   Default: volume > 2× the 7-day (168-hour) rolling average.
#
#   This flags unusual trading activity — a potential signal of:
#     - Breaking news about that coin
#     - Whale activity (large holders buying/selling)
#     - Exchange listing/delisting announcements
# =============================================================================

def compute_volume_spikes(
    hourly_df     : DataFrame,
    window_hours  : int = 168,
    threshold_multiplier : int = 2,
) -> DataFrame:
    """
    Detect volume spikes in hourly trading data.

    Args:
        hourly_df: DataFrame with [coin_id, hour_timestamp, volume_usd]
        window_hours: Rolling window in hours (default 168 = 7 days × 24h)
        threshold_multiplier: Spike if volume > avg × this (default 2)

    Returns:
        DataFrame with added column: is_volume_spike (Boolean)
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

# =============================================================================
# FUNCTION: compute_annualised_volatility()
#
# EXTRACTED FROM: 07_kpi_volatility.py (Cell 4)
#
# BUSINESS LOGIC:
#   Annualised Volatility = STDDEV(daily_return_pct) × √365
#
#   - STDDEV measures how much daily returns vary from the mean
#   - √365 scales the daily figure to an annual one
#   - Higher value = riskier/more volatile coin
#   - This is the standard financial measure used by portfolio managers
# =============================================================================

def compute_annualised_volatility(
    ohlc_df     : DataFrame,
    window_days : int = 30,
) -> DataFrame:
    """
    Compute rolling annualised volatility from daily returns.

    Args:
        ohlc_df: DataFrame with [coin_id, ohlc_date, daily_return_pct]
        window_days: Rolling window in days (default 30)

    Returns:
        DataFrame with added column: annualised_volatility_30d (Double)
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

# =============================================================================
# FUNCTION: build_merge_condition()
#
# EXTRACTED FROM: gold_utils.py (delta_merge_gold function, lines 139-141)
#
# BUSINESS LOGIC:
#   Builds the SQL string for Delta MERGE's ON clause from a list of
#   primary key column names. This determines which rows are "matching"
#   (already exist) vs "not matching" (new → INSERT).
# =============================================================================

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
