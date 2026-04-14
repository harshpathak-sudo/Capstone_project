# Databricks notebook source
# =============================================================================
# FILE:    config.py
# LAYER:   Gold + Platinum (KPI)
# PURPOSE: Single source of truth for ALL Gold/Platinum configuration.
#          Every Gold notebook imports from here via %run ./config.
#          Nothing is hardcoded inside individual notebooks.
#
# DESIGN PRINCIPLE:
#   If a value might change (path, threshold, merge key, column name),
#   it belongs here — not inside a notebook. This means:
#     - Changing the ADLS account name → edit one line here, all notebooks update
#     - Changing a merge key → edit one entry, no risk of inconsistency
#     - Onboarding a new team member → they read this file to understand the layer
#
# HOW TO USE IN A NOTEBOOK:
#   # At the top of every Gold notebook:
#   %run ./config
#   # Then:
#   init_gold_config("adlsnewhp")
#   df = spark.read.format("delta").load(SilverInputPaths.MARKET_SNAPSHOT)
#
# SECTIONS:
#   1. GoldConfig           — Account roots, run metadata
#   2. SilverInputPaths     — Silver tables (read-only inputs)
#   3. GoldPaths            — Gold output table paths (MERGE + OVERWRITE)
#   4. GoldLogPaths         — Log file paths
#   5. GoldMergeKeys        — Primary key combinations for MERGE upserts
#   6. GoldColumns          — Final column lists for Gold tables
#   7. KPIColumns           — Final column lists for KPI/Platinum tables
#   8. init_gold_config()   — One-call initialiser
# =============================================================================

import uuid
from datetime import datetime, timezone


# =============================================================================
# SECTION 1 — GOLD CONFIG (Account & Container Roots)
# =============================================================================

class GoldConfig:
    """
    Runtime-initialised config. Call GoldConfig.init(adls_name) once
    per notebook after reading secrets, then access all paths as class attrs.
    """

    ADLS_ACCOUNT_NAME : str = ""

    # Container roots (populated by init)
    SILVER_ROOT  : str = ""
    GOLD_ROOT    : str = ""
    LOGGING_ROOT : str = ""

    # Run-level metadata
    RUN_ID    : str = ""
    RUN_TS    : datetime = None
    DATE_PATH : str = ""

    @classmethod
    def init(cls, adls_account_name: str) -> None:
        """
        Initialise all path constants from the ADLS account name.

        Args:
            adls_account_name: Storage account name (e.g., "adlsnewhp")
        """
        cls.ADLS_ACCOUNT_NAME = adls_account_name

        base = f"abfss://capstone@{adls_account_name}.dfs.core.windows.net"
        cls.SILVER_ROOT  = f"{base}/silver"
        cls.GOLD_ROOT    = f"{base}/gold"
        cls.LOGGING_ROOT = f"{base}/logs"

        cls.RUN_ID    = str(uuid.uuid4())
        cls.RUN_TS    = datetime.now(timezone.utc)
        cls.DATE_PATH = cls.RUN_TS.strftime("%Y/%m/%d")

# COMMAND ----------

# =============================================================================
# SECTION 2 — SILVER INPUT PATHS (read-only)
# Gold notebooks READ from these Silver Delta tables — never write to them.
# =============================================================================

class SilverInputPaths:
    """
    Full ADLS paths for the five Silver Delta tables.
    Populated after GoldConfig.init() is called.
    """
    MARKET_SNAPSHOT    : str = ""
    OHLC_HISTORY       : str = ""
    HOURLY_TIMESERIES  : str = ""
    TRENDING_COINS     : str = ""
    GLOBAL_STATS       : str = ""

    @classmethod
    def init(cls, silver_root: str) -> None:
        cls.MARKET_SNAPSHOT   = f"{silver_root}/market_snapshot"
        cls.OHLC_HISTORY      = f"{silver_root}/ohlc_history"
        cls.HOURLY_TIMESERIES = f"{silver_root}/hourly_timeseries"
        cls.TRENDING_COINS    = f"{silver_root}/trending_coins"
        cls.GLOBAL_STATS      = f"{silver_root}/global_stats"

# COMMAND ----------

# =============================================================================
# SECTION 3 — GOLD OUTPUT PATHS
# Gold tables (MERGE — accumulate history) + KPI tables (OVERWRITE — latest snapshot)
# =============================================================================

class GoldPaths:
    """
    ADLS paths for the 4 Gold tables (MERGE) and 5 KPI tables (OVERWRITE).

    Gold tables accumulate day-over-day data for historical trend analysis.
    KPI tables are thin convenience layers — filtered to latest date for Power BI.
    """

    # ── Gold tables (DELTA MERGE — accumulate history) ────────────────────────
    DAILY_MARKET_SUMMARY : str = ""
    OHLC_ENRICHED        : str = ""
    TRENDING_ENRICHED    : str = ""
    GLOBAL_DAILY         : str = ""

    # ── KPI tables (OVERWRITE — latest snapshot for Power BI) ─────────────────
    KPI_MARKET_OVERVIEW  : str = ""
    KPI_GAINERS_LOSERS   : str = ""
    KPI_VOLATILITY       : str = ""
    KPI_TRENDING_WIDGET  : str = ""
    KPI_PRICE_TRENDS     : str = ""

    @classmethod
    def init(cls, gold_root: str) -> None:
        # Gold tables — accumulating
        cls.DAILY_MARKET_SUMMARY = f"{gold_root}/daily_market_summary"
        cls.OHLC_ENRICHED        = f"{gold_root}/ohlc_enriched"
        cls.TRENDING_ENRICHED    = f"{gold_root}/trending_enriched"
        cls.GLOBAL_DAILY         = f"{gold_root}/global_daily"

        # KPI tables — latest snapshot only
        cls.KPI_MARKET_OVERVIEW  = f"{gold_root}/kpi_market_overview"
        cls.KPI_GAINERS_LOSERS   = f"{gold_root}/kpi_gainers_losers"
        cls.KPI_VOLATILITY       = f"{gold_root}/kpi_volatility"
        cls.KPI_TRENDING_WIDGET  = f"{gold_root}/kpi_trending_widget"
        cls.KPI_PRICE_TRENDS     = f"{gold_root}/kpi_price_trends"

# COMMAND ----------

# =============================================================================
# SECTION 4 — LOG PATHS
# One JSON log file per notebook per run, date-partitioned.
# Follows the same format as Silver: logs/<layer>/<notebook>/YYYY/MM/DD/run_<uuid>.json
# =============================================================================

class GoldLogPaths:
    """
    ADLS log file paths for Gold layer notebooks.
    Written to the central logs/ directory in ADLS (same as Silver logs).
    """
    DAILY_MARKET_SUMMARY : str = ""
    OHLC_ENRICHED        : str = ""
    TRENDING_ENRICHED    : str = ""
    GLOBAL_DAILY         : str = ""
    KPI_MARKET_OVERVIEW  : str = ""
    KPI_GAINERS_LOSERS   : str = ""
    KPI_VOLATILITY       : str = ""
    KPI_TRENDING_WIDGET  : str = ""
    KPI_PRICE_TRENDS     : str = ""

    @classmethod
    def init(cls, logging_root: str, date_path: str, run_id: str) -> None:
        base = f"{logging_root}/gold"
        cls.DAILY_MARKET_SUMMARY = f"{base}/daily_market_summary/{date_path}/run_{run_id}.json"
        cls.OHLC_ENRICHED        = f"{base}/ohlc_enriched/{date_path}/run_{run_id}.json"
        cls.TRENDING_ENRICHED    = f"{base}/trending_enriched/{date_path}/run_{run_id}.json"
        cls.GLOBAL_DAILY         = f"{base}/global_daily/{date_path}/run_{run_id}.json"
        cls.KPI_MARKET_OVERVIEW  = f"{base}/kpi_market_overview/{date_path}/run_{run_id}.json"
        cls.KPI_GAINERS_LOSERS   = f"{base}/kpi_gainers_losers/{date_path}/run_{run_id}.json"
        cls.KPI_VOLATILITY       = f"{base}/kpi_volatility/{date_path}/run_{run_id}.json"
        cls.KPI_TRENDING_WIDGET  = f"{base}/kpi_trending_widget/{date_path}/run_{run_id}.json"
        cls.KPI_PRICE_TRENDS     = f"{base}/kpi_price_trends/{date_path}/run_{run_id}.json"

# COMMAND ----------

# =============================================================================
# SECTION 5 — GOLD MERGE KEYS
# Primary key column combinations used in DELTA MERGE for Gold tables.
# Gold tables accumulate history: MERGE ensures no duplicates across daily runs.
# KPI tables use OVERWRITE (not MERGE) — no keys needed.
# =============================================================================

class GoldMergeKeys:
    """
    Primary key combinations for Gold Delta MERGE upserts.
    """
    # daily_market_summary: one row per coin per day
    DAILY_MARKET_SUMMARY = ["coin_id", "summary_date"]

    # ohlc_enriched: one row per candle per coin per timestamp
    OHLC_ENRICHED        = ["coin_id", "ohlc_timestamp"]

    # trending_enriched: one row per trending coin per day
    TRENDING_ENRICHED    = ["trend_run_date", "coin_id"]

    # global_daily: one row per day (no coin_id — global stats)
    GLOBAL_DAILY         = ["stats_date"]

# COMMAND ----------

# =============================================================================
# SECTION 6 — GOLD COLUMN LISTS
# Exact final column order for each Gold table.
# Used in a final .select(*cols) before MERGE write.
# =============================================================================

class GoldColumns:
    """
    Final column lists for each Gold table.
    """

    DAILY_MARKET_SUMMARY = [
        "summary_date",
        "coin_id",
        "name",
        "symbol",
        "current_price_usd",
        "market_cap_usd",
        "market_cap_rank",
        "total_volume_24h_usd",
        "price_range_24h_usd",
        "price_change_pct_24h",
        "is_top5_gainer",
        "is_top5_loser",
        "mkt_cap_share_pct",
        "price_to_ath_pct",
        "supply_utilisation_pct",
        "gold_processed_timestamp",
    ]

    OHLC_ENRICHED = [
        "coin_id",
        "ohlc_timestamp",
        "ohlc_date",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "candle_body",
        "candle_range",
        "is_bullish",
        "pct_change_in_period",
        "rolling_7day_avg_close",
        "rolling_30day_avg_close",
        "daily_return_pct",
        "gold_processed_timestamp",
    ]

    TRENDING_ENRICHED = [
        "trend_run_date",
        "trend_position",
        "coin_id",
        "coin_name",
        "coin_symbol",
        "market_cap_rank",
        "current_price_usd",
        "price_change_pct_24h",
        "total_volume_24h_usd",
        "is_also_top50",
        "gold_processed_timestamp",
    ]

    GLOBAL_DAILY = [
        "stats_date",
        "total_market_cap_usd",
        "total_volume_24h_usd",
        "btc_dominance_pct",
        "eth_dominance_pct",
        "altcoin_dominance_pct",
        "btc_eth_combined_pct",
        "active_cryptos",
        "market_cap_change_pct_24h",
        "market_sentiment",
        "gold_processed_timestamp",
    ]

# COMMAND ----------

# =============================================================================
# SECTION 7 — KPI COLUMN LISTS
# Exact final column order for each KPI/Platinum table.
# These are the columns Power BI developers will see.
# =============================================================================

class KPIColumns:
    """
    Final column lists for each KPI table — Power BI ready.
    """

    MARKET_OVERVIEW = [
        "coin_id",
        "coin_name",
        "symbol",
        "price_usd",
        "market_cap_usd",
        "market_cap_rank",
        "volume_24h_usd",
        "price_change_pct_24h",
        "price_range_24h_usd",
        "mkt_cap_share_pct",
        "is_top5_gainer",
        "is_top5_loser",
        "price_to_ath_pct",
        "snapshot_date",
    ]

    GAINERS_LOSERS = [
        "date",
        "coin_name",
        "symbol",
        "price_change_pct_24h",
        "price_usd",
        "gainer_rank_daily",
        "category",
        "gainer_loser_badge",
    ]

    VOLATILITY = [
        "coin_id",
        "coin_name",
        "ohlc_date",
        "close_price",
        "rolling_7day_avg_close",
        "rolling_30day_avg_close",
        "candle_range",
        "is_bullish",
        "candle_body",
        "daily_return_pct",
        "annualised_volatility_30d",
    ]

    TRENDING_WIDGET = [
        "trending_position",
        "coin_name",
        "symbol",
        "market_cap_rank",
        "current_price_usd",
        "price_change_pct_24h",
        "is_in_top50",
        "snapshot_date",
    ]

    PRICE_TRENDS = [
        "coin_id",
        "coin_name",
        "hour_timestamp",
        "price_usd",
        "volume_usd",
        "market_cap_usd",
        "is_volume_spike",
        "global_market_cap_usd",
        "btc_dominance_pct",
        "market_sentiment",
    ]

# COMMAND ----------

# =============================================================================
# SECTION 8 — CONVENIENCE INIT FUNCTION
# Call this single function at the top of every Gold notebook.
# =============================================================================

def init_gold_config(adls_account_name: str) -> None:
    """
    One-call initialiser for all Gold config classes.

    Example (in any Gold notebook):
        init_gold_config("adlsnewhp")
        df = spark.read.format("delta").load(SilverInputPaths.MARKET_SNAPSHOT)
    """
    GoldConfig.init(adls_account_name)
    SilverInputPaths.init(GoldConfig.SILVER_ROOT)
    GoldPaths.init(GoldConfig.GOLD_ROOT)
    GoldLogPaths.init(
        GoldConfig.LOGGING_ROOT,
        GoldConfig.DATE_PATH,
        GoldConfig.RUN_ID,
    )
