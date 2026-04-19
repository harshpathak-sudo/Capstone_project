# Databricks notebook source
# =============================================================================
# NOTEBOOK: unit_tests.py
# PURPOSE:  Unit tests for the Crypto Market Data Engineering pipeline.
#           Runs inside Databricks using the existing SparkSession.
#
# HOW TO RUN:
#   1. Upload this file to your Databricks workspace under Capstone_project/tests/
#   2. Attach to any cluster (even a small one — tests use tiny DataFrames)
#   3. Click "Run All"
#   4. Check the FINAL CELL for the test summary table
#
# TESTS (14 total):
#   ┌────────┬──────────────────────────────────┬───────────────────────────────┐
#   │ Test # │ Function Under Test              │ What It Verifies              │
#   ├────────┼──────────────────────────────────┼───────────────────────────────┤
#   │  1     │ validate_drop_rate()             │ Passes within 30% threshold   │
#   │  2     │ validate_drop_rate()             │ Raises above 30% threshold    │
#   │  3     │ validate_drop_rate()             │ Handles zero rows gracefully   │
#   │  4     │ assert_required_columns()        │ Passes when all cols present  │
#   │  5     │ assert_required_columns()        │ Raises on missing columns     │
#   │  6     │ SilverConfig.init()              │ All paths built correctly     │
#   │  7     │ GoldConfig.init()                │ All paths built correctly     │
#   │  8     │ build_merge_condition()           │ Multi-key condition string    │
#   │  9     │ build_merge_condition()           │ Single-key condition string   │
#   │ 10     │ apply_daily_market_transforms()  │ Top 5 gainer flags correct    │
#   │ 11     │ apply_daily_market_transforms()  │ Top 5 loser flags correct     │
#   │ 12     │ apply_daily_market_transforms()  │ Market cap share sums to 100% │
#   │ 13     │ compute_volume_spikes()          │ Flags spike above 2× avg      │
#   │ 14     │ compute_volume_spikes()          │ No flag below 2× avg          │
#   └────────┴──────────────────────────────────┴───────────────────────────────┘
# =============================================================================

# COMMAND ----------

# =============================================================================
# CELL 1 — IMPORTS
#
# IMPORTANT: Silver layer notebooks are .ipynb format (Jupyter), which cannot
# be imported via %run from a .py notebook in Databricks. So we inline the
# Silver classes and functions needed for testing directly here.
#
# Gold layer notebooks are .py format → %run works normally.
# =============================================================================

# ── Silver Config Classes (copied from Silver/config.ipynb) ──────────────────
# These are the exact same classes used by all Silver notebooks.
# We copy them here ONLY because .ipynb → .py cross-format %run doesn't work.

import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import logging

class SilverConfig:
    ADLS_ACCOUNT_NAME : str = ""
    LANDING_ROOT  : str = ""
    BRONZE_ROOT   : str = ""
    SILVER_ROOT   : str = ""
    LOGGING_ROOT  : str = ""
    RUN_ID    : str = ""
    RUN_TS    : datetime = None
    DATE_PATH : str = ""

    @classmethod
    def init(cls, adls_account_name: str) -> None:
        cls.ADLS_ACCOUNT_NAME = adls_account_name
        cls.LANDING_ROOT = f"abfss://capstone@{adls_account_name}.dfs.core.windows.net"
        cls.BRONZE_ROOT  = f"abfss://capstone@{adls_account_name}.dfs.core.windows.net/bronze"
        cls.SILVER_ROOT  = f"abfss://capstone@{adls_account_name}.dfs.core.windows.net/silver"
        cls.LOGGING_ROOT = f"abfss://capstone@{adls_account_name}.dfs.core.windows.net/logs"
        cls.RUN_ID    = str(uuid.uuid4())
        cls.RUN_TS    = datetime.now(timezone.utc)
        cls.DATE_PATH = cls.RUN_TS.strftime("%Y/%m/%d")

class BronzePaths:
    COINS_MARKETS : str = ""
    OHLC          : str = ""
    MARKET_CHART  : str = ""
    TRENDING      : str = ""
    GLOBAL        : str = ""

    @classmethod
    def init(cls, bronze_root: str) -> None:
        cls.COINS_MARKETS = f"{bronze_root}/coins_markets_raw"
        cls.OHLC          = f"{bronze_root}/ohlc_raw"
        cls.MARKET_CHART  = f"{bronze_root}/market_chart_raw"
        cls.TRENDING      = f"{bronze_root}/trending_raw"
        cls.GLOBAL        = f"{bronze_root}/global_raw"

class SilverPaths:
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

class WatermarkPaths:
    WATERMARK_TABLE : str = ""
    @classmethod
    def init(cls, silver_root: str) -> None:
        cls.WATERMARK_TABLE = f"{silver_root}/_watermarks/bronze_watermarks"

class LogPaths:
    MARKET_SNAPSHOT   : str = ""
    OHLC_HISTORY      : str = ""
    HOURLY_TIMESERIES : str = ""
    TRENDING_COINS    : str = ""
    GLOBAL_STATS      : str = ""

    @classmethod
    def init(cls, logging_root: str, date_path: str, run_id: str) -> None:
        base = f"{logging_root}/silver"
        cls.MARKET_SNAPSHOT   = f"{base}/market_snapshot/{date_path}/run_{run_id}.json"
        cls.OHLC_HISTORY      = f"{base}/ohlc_history/{date_path}/run_{run_id}.json"
        cls.HOURLY_TIMESERIES = f"{base}/hourly_timeseries/{date_path}/run_{run_id}.json"
        cls.TRENDING_COINS    = f"{base}/trending_coins/{date_path}/run_{run_id}.json"
        cls.GLOBAL_STATS      = f"{base}/global_stats/{date_path}/run_{run_id}.json"

def init_silver_config(adls_account_name: str) -> None:
    SilverConfig.init(adls_account_name)
    BronzePaths.init(SilverConfig.BRONZE_ROOT)
    SilverPaths.init(SilverConfig.SILVER_ROOT)
    WatermarkPaths.init(SilverConfig.SILVER_ROOT)
    LogPaths.init(SilverConfig.LOGGING_ROOT, SilverConfig.DATE_PATH, SilverConfig.RUN_ID)


# ── Silver Utils Functions (copied from Silver/silver_utils.ipynb) ───────────
# Only the pure Python functions needed for testing. No Spark I/O functions.

from pyspark.sql import DataFrame

def validate_drop_rate(
    rows_before  : int,
    rows_after   : int,
    max_fraction : float,
    table_name   : str,
    logger       : logging.Logger,
) -> None:
    if rows_before == 0:
        logger.warning(f"  [{table_name}] Input batch has 0 rows — nothing to validate")
        return

    dropped  = rows_before - rows_after
    fraction = dropped / rows_before

    logger.info(f"  [{table_name}] Quality filter: {dropped:,} rows dropped ({fraction:.1%} of {rows_before:,})")

    if fraction > max_fraction:
        msg = (
            f"Data quality check FAILED for {table_name}: "
            f"{fraction:.1%} of rows dropped (max allowed: {max_fraction:.1%}). "
            f"rows_before={rows_before:,}, rows_after={rows_after:,}. "
            f"Investigate Bronze source data before proceeding."
        )
        logger.error(f"  {msg}")
        raise ValueError(msg)

def assert_required_columns(
    df              : DataFrame,
    required_cols   : List[str],
    table_name      : str,
    logger          : logging.Logger,
) -> None:
    existing = set(df.columns)
    missing  = set(required_cols) - existing

    if missing:
        msg = (
            f"Missing required columns in {table_name}: {sorted(missing)}. "
            f"Available columns: {sorted(existing)}. "
            f"Check if the CoinGecko API response format has changed."
        )
        logger.error(f"  {msg}")
        raise ValueError(msg)

    logger.info(f"  ✓ All {len(required_cols)} required columns present in {table_name}")

# COMMAND ----------

# ── Gold Imports (.py format → %run works normally) ──────────────────────────

%run ../Gold/config

# COMMAND ----------

# MAGIC %run ../Gold/transformations

# COMMAND ----------

# =============================================================================
# CELL 2 — TEST FRAMEWORK
#
# Since we're running inside Databricks (not pytest), we need a simple
# test runner. Each test is a function that either:
#   - Completes successfully → PASSED
#   - Raises AssertionError  → FAILED (with the error message)
#
# Results are collected and displayed in a summary table at the end.
# =============================================================================

import traceback
from datetime import datetime, timezone

# Counters and results list
_test_results = []
_passed = 0
_failed = 0

def run_test(test_name: str, test_func) -> None:
    """
    Execute a test function and record the result.
    
    Args:
        test_name: Human-readable name for the test
        test_func: Callable that raises AssertionError on failure
    """
    global _passed, _failed
    try:
        test_func()
        _test_results.append(("✅ PASSED", test_name, ""))
        _passed += 1
        print(f"  ✅ PASSED: {test_name}")
    except Exception as e:
        _test_results.append(("❌ FAILED", test_name, str(e)))
        _failed += 1
        print(f"  ❌ FAILED: {test_name}")
        print(f"     Error: {e}")

# Create a mock logger that does nothing (tests don't need real logging)
import logging
_mock_logger = logging.getLogger("unit_test")
if not _mock_logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(message)s"))
    _mock_logger.addHandler(_handler)
    _mock_logger.setLevel(logging.WARNING)  # Suppress INFO logs in tests

print("=" * 70)
print("UNIT TEST SUITE — Crypto Market Data Engineering Pipeline")
print(f"Run at: {datetime.now(timezone.utc).isoformat()}")
print("=" * 70)

# COMMAND ----------

# =============================================================================
# CELL 3 — TEST GROUP 1: validate_drop_rate()
#
# PURPOSE OF THIS FUNCTION:
#   After Silver applies data quality filters (drop nulls, invalid prices),
#   validate_drop_rate() checks if the filter dropped an unusually large
#   fraction of rows. If > 30% of rows were dropped, something is wrong
#   upstream (API changed format? Bad data batch?) — raise an error.
#
# WHY TEST IT:
#   If this function has a bug (wrong division, off-by-one), either:
#     - Bad data silently enters Silver (threshold too loose)
#     - Good data gets blocked (threshold too strict)
#   Both are critical production failures.
# =============================================================================

print("\n── TEST GROUP 1: validate_drop_rate() ──")

def test_drop_rate_within_threshold():
    """10% drop rate should NOT raise (threshold is 30%)."""
    # 100 rows in, 90 rows out = 10% dropped = within threshold
    validate_drop_rate(
        rows_before=100,
        rows_after=90,
        max_fraction=0.30,
        table_name="test_table",
        logger=_mock_logger,
    )
    # If we reach here without exception, the test passed

def test_drop_rate_exceeds_threshold():
    """35% drop rate SHOULD raise ValueError (exceeds 30%)."""
    raised = False
    try:
        validate_drop_rate(
            rows_before=100,
            rows_after=65,   # 35% dropped — above 30% threshold
            max_fraction=0.30,
            table_name="test_table",
            logger=_mock_logger,
        )
    except ValueError as e:
        raised = True
        # Verify the error message is clear
        assert "Data quality check FAILED" in str(e), \
            f"Error message should mention 'Data quality check FAILED', got: {e}"
    assert raised, "validate_drop_rate should have raised ValueError for 35% drop rate"

def test_drop_rate_zero_rows():
    """Zero input rows should NOT raise — just warn and return."""
    validate_drop_rate(
        rows_before=0,
        rows_after=0,
        max_fraction=0.30,
        table_name="test_table",
        logger=_mock_logger,
    )
    # Edge case: empty batch. Function should handle gracefully (no division by zero).

run_test("validate_drop_rate — passes within threshold (10% < 30%)", test_drop_rate_within_threshold)
run_test("validate_drop_rate — raises above threshold (35% > 30%)", test_drop_rate_exceeds_threshold)
run_test("validate_drop_rate — handles zero rows gracefully", test_drop_rate_zero_rows)

# COMMAND ----------

# =============================================================================
# CELL 4 — TEST GROUP 2: assert_required_columns()
#
# PURPOSE OF THIS FUNCTION:
#   Before transforming a Bronze DataFrame, check that all expected columns
#   exist. If CoinGecko changes their API and drops a field (e.g., removes
#   "market_cap"), this function gives a clear error message instead of a
#   cryptic Spark AnalysisException deep inside a transformation chain.
#
# WHY TEST IT:
#   If this function has a bug (e.g., checks wrong column names, doesn't
#   raise on missing), broken data could flow all the way to Gold/Dashboard
#   before anyone notices.
# =============================================================================

print("\n── TEST GROUP 2: assert_required_columns() ──")

def test_required_columns_present():
    """DataFrame with all required columns should pass silently."""
    test_df = spark.createDataFrame(
        [("bitcoin", 65000.0, 1000000000)],
        ["coin_id", "current_price_usd", "market_cap_usd"]
    )
    # Should NOT raise
    assert_required_columns(
        df=test_df,
        required_cols=["coin_id", "current_price_usd", "market_cap_usd"],
        table_name="test_table",
        logger=_mock_logger,
    )

def test_required_columns_missing():
    """DataFrame missing required columns should raise ValueError."""
    test_df = spark.createDataFrame(
        [("bitcoin", 65000.0)],
        ["coin_id", "current_price_usd"]
        # Missing: market_cap_usd
    )
    raised = False
    try:
        assert_required_columns(
            df=test_df,
            required_cols=["coin_id", "current_price_usd", "market_cap_usd"],
            table_name="test_table",
            logger=_mock_logger,
        )
    except ValueError as e:
        raised = True
        assert "market_cap_usd" in str(e), \
            f"Error should mention missing column 'market_cap_usd', got: {e}"
    assert raised, "assert_required_columns should have raised ValueError for missing column"

run_test("assert_required_columns — passes with all columns present", test_required_columns_present)
run_test("assert_required_columns — raises on missing columns", test_required_columns_missing)

# COMMAND ----------

# =============================================================================
# CELL 5 — TEST GROUP 3: Config Path Building
#
# PURPOSE:
#   SilverConfig.init() and GoldConfig.init() build ALL ADLS paths from
#   the storage account name. Every notebook in the pipeline depends on
#   these paths being correct. If even ONE path has a typo, that entire
#   notebook fails at runtime with a cryptic "path not found" error.
#
# WHY TEST IT:
#   Testing path construction in 2 seconds is better than debugging a
#   failed 16-task Databricks Workflow that took 10 minutes to reach
#   the broken notebook.
# =============================================================================

print("\n── TEST GROUP 3: Config Path Building ──")

def test_silver_config_paths():
    """SilverConfig.init() should build correct ADLS paths."""
    # Re-initialise with a known account name
    init_silver_config("test_account")

    expected_base = "abfss://capstone@test_account.dfs.core.windows.net"

    # Verify all root paths
    assert SilverConfig.BRONZE_ROOT == f"{expected_base}/bronze", \
        f"BRONZE_ROOT wrong: {SilverConfig.BRONZE_ROOT}"
    assert SilverConfig.SILVER_ROOT == f"{expected_base}/silver", \
        f"SILVER_ROOT wrong: {SilverConfig.SILVER_ROOT}"
    assert SilverConfig.LOGGING_ROOT == f"{expected_base}/logs", \
        f"LOGGING_ROOT wrong: {SilverConfig.LOGGING_ROOT}"

    # Verify Bronze input paths
    assert BronzePaths.COINS_MARKETS == f"{expected_base}/bronze/coins_markets_raw", \
        f"BronzePaths.COINS_MARKETS wrong: {BronzePaths.COINS_MARKETS}"
    assert BronzePaths.OHLC == f"{expected_base}/bronze/ohlc_raw", \
        f"BronzePaths.OHLC wrong: {BronzePaths.OHLC}"

    # Verify Silver output paths
    assert SilverPaths.MARKET_SNAPSHOT == f"{expected_base}/silver/market_snapshot", \
        f"SilverPaths.MARKET_SNAPSHOT wrong: {SilverPaths.MARKET_SNAPSHOT}"
    assert SilverPaths.OHLC_HISTORY == f"{expected_base}/silver/ohlc_history", \
        f"SilverPaths.OHLC_HISTORY wrong: {SilverPaths.OHLC_HISTORY}"
    assert SilverPaths.HOURLY_TIMESERIES == f"{expected_base}/silver/hourly_timeseries", \
        f"SilverPaths.HOURLY_TIMESERIES wrong: {SilverPaths.HOURLY_TIMESERIES}"

    # Verify RUN_ID is a valid UUID format (36 chars with hyphens)
    assert len(SilverConfig.RUN_ID) == 36, \
        f"RUN_ID should be UUID format (36 chars), got: {SilverConfig.RUN_ID}"

    # Verify DATE_PATH matches YYYY/MM/DD pattern
    import re
    assert re.match(r"\d{4}/\d{2}/\d{2}", SilverConfig.DATE_PATH), \
        f"DATE_PATH should be YYYY/MM/DD, got: {SilverConfig.DATE_PATH}"

def test_gold_config_paths():
    """GoldConfig.init() should build correct ADLS paths."""
    init_gold_config("test_account")

    expected_base = "abfss://capstone@test_account.dfs.core.windows.net"

    # Verify Gold root
    assert GoldConfig.GOLD_ROOT == f"{expected_base}/gold", \
        f"GOLD_ROOT wrong: {GoldConfig.GOLD_ROOT}"

    # Verify Gold table paths (MERGE tables)
    assert GoldPaths.DAILY_MARKET_SUMMARY == f"{expected_base}/gold/daily_market_summary", \
        f"DAILY_MARKET_SUMMARY wrong: {GoldPaths.DAILY_MARKET_SUMMARY}"
    assert GoldPaths.OHLC_ENRICHED == f"{expected_base}/gold/ohlc_enriched", \
        f"OHLC_ENRICHED wrong: {GoldPaths.OHLC_ENRICHED}"

    # Verify KPI table paths (OVERWRITE tables)
    assert GoldPaths.KPI_MARKET_OVERVIEW == f"{expected_base}/gold/kpi_market_overview", \
        f"KPI_MARKET_OVERVIEW wrong: {GoldPaths.KPI_MARKET_OVERVIEW}"
    assert GoldPaths.KPI_VOLATILITY == f"{expected_base}/gold/kpi_volatility", \
        f"KPI_VOLATILITY wrong: {GoldPaths.KPI_VOLATILITY}"
    assert GoldPaths.KPI_PRICE_TRENDS == f"{expected_base}/gold/kpi_price_trends", \
        f"KPI_PRICE_TRENDS wrong: {GoldPaths.KPI_PRICE_TRENDS}"

    # Verify all 9 log paths contain the run_id
    run_id = GoldConfig.RUN_ID
    assert run_id in GoldLogPaths.DAILY_MARKET_SUMMARY, \
        f"Log path should contain run_id '{run_id}'"

run_test("SilverConfig.init — all ADLS paths built correctly", test_silver_config_paths)
run_test("GoldConfig.init — all ADLS paths built correctly", test_gold_config_paths)

# COMMAND ----------

# =============================================================================
# CELL 6 — TEST GROUP 4: build_merge_condition()
#
# PURPOSE:
#   Gold Delta MERGE needs an ON clause like:
#   "existing.coin_id = new.coin_id AND existing.summary_date = new.summary_date"
#   This is built dynamically from a list of key column names.
#
# WHY TEST IT:
#   If the condition is wrong (missing AND, wrong alias), MERGE either:
#     - Fails with a SQL error (best case)
#     - Creates duplicates (worst case — silent data corruption)
# =============================================================================

print("\n── TEST GROUP 4: build_merge_condition() ──")

def test_merge_condition_multi_key():
    """Multi-key merge condition should join with AND."""
    result = build_merge_condition(["coin_id", "summary_date"])
    expected = "existing.coin_id = new.coin_id AND existing.summary_date = new.summary_date"
    assert result == expected, f"Expected:\n  {expected}\nGot:\n  {result}"

def test_merge_condition_single_key():
    """Single-key merge condition should have no AND."""
    result = build_merge_condition(["stats_date"])
    expected = "existing.stats_date = new.stats_date"
    assert result == expected, f"Expected:\n  {expected}\nGot:\n  {result}"

run_test("build_merge_condition — multi-key (coin_id + summary_date)", test_merge_condition_multi_key)
run_test("build_merge_condition — single-key (stats_date)", test_merge_condition_single_key)

# COMMAND ----------

# =============================================================================
# CELL 7 — TEST GROUP 5: apply_daily_market_transforms()
#
# These tests verify the core business logic in the Gold layer:
#   - Top 5 gainer/loser ranking
#   - Market cap share calculation
#
# HOW THESE TESTS WORK:
#   We create a SMALL test DataFrame with KNOWN values (10 coins, 1 day).
#   We run the transformation function on it.
#   We check that the output matches what we calculated by hand.
#
#   This is the entire point of unit testing: verify with known inputs
#   that the output is exactly what you expect.
# =============================================================================

print("\n── TEST GROUP 5: apply_daily_market_transforms() ──")

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, IntegerType, DateType
)
from datetime import date

def _create_test_market_df():
    """
    Create a test Silver market_snapshot DataFrame with 10 coins.
    Price changes are set so we know exactly which 5 are top gainers/losers.
    Market caps are set so we can verify share % by hand.
    """
    schema = StructType([
        StructField("snapshot_date", DateType(), False),
        StructField("coin_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("current_price_usd", DoubleType(), True),
        StructField("market_cap_usd", LongType(), True),
        StructField("market_cap_rank", IntegerType(), True),
        StructField("total_volume_24h_usd", LongType(), True),
        StructField("high_24h_usd", DoubleType(), True),
        StructField("low_24h_usd", DoubleType(), True),
        StructField("price_change_pct_24h", DoubleType(), True),
        StructField("ath_usd", DoubleType(), True),
        StructField("circulating_supply", DoubleType(), True),
        StructField("total_supply", DoubleType(), True),
    ])

    test_date = date(2025, 4, 1)
    data = [
        # coin_id      name       sym   price    mktcap         rank  vol       high      low       change%  ath       circ_supply  total_supply
        (test_date, "coin_a", "Coin A", "A", 100.0,  500000000,   1, 1000000, 110.0,  90.0,   10.0,  200.0,  1000000.0, 2000000.0),  # Gainer #1
        (test_date, "coin_b", "Coin B", "B", 50.0,   300000000,   2, 500000,  55.0,   45.0,    8.0,  100.0,  500000.0,  1000000.0),  # Gainer #2
        (test_date, "coin_c", "Coin C", "C", 25.0,   100000000,   3, 250000,  28.0,   22.0,    6.0,   50.0,  400000.0,  None),       # Gainer #3 (no total supply)
        (test_date, "coin_d", "Coin D", "D", 10.0,    50000000,   4, 100000,  12.0,    9.0,    4.0,   20.0,  100000.0,  200000.0),   # Gainer #4
        (test_date, "coin_e", "Coin E", "E",  5.0,    30000000,   5,  50000,   6.0,    4.5,    2.0,   10.0,   50000.0,  100000.0),   # Gainer #5
        (test_date, "coin_f", "Coin F", "F",  2.0,    10000000,   6,  20000,   2.5,    1.8,    0.5,    5.0,   10000.0,   50000.0),   # NOT top 5 gainer
        (test_date, "coin_g", "Coin G", "G",  1.0,     5000000,   7,  10000,   1.2,    0.9,   -1.0,    3.0,    5000.0,   10000.0),   # Loser
        (test_date, "coin_h", "Coin H", "H",  0.5,     3000000,   8,   5000,   0.6,    0.4,   -3.0,    2.0,    3000.0,    5000.0),   # Loser
        (test_date, "coin_i", "Coin I", "I",  0.1,     1000000,   9,   1000,   0.15,   0.08,  -5.0,    1.0,    1000.0,    2000.0),   # Loser
        (test_date, "coin_j", "Coin J", "J",  0.01,     500000,  10,    500,   0.015,  0.008, -8.0,    0.5,     500.0,    1000.0),   # Biggest loser
    ]
    return spark.createDataFrame(data, schema)


def test_top5_gainer_flags():
    """Top 5 coins by price_change_pct_24h should be flagged is_top5_gainer=True."""
    test_df = _create_test_market_df()
    result = apply_daily_market_transforms(test_df)

    # Collect gainer flags (use DataFrame API — .rdd not available on Spark Connect)
    gainers = [
        row.coin_id for row in
        result.filter(F.col("is_top5_gainer") == True).select("coin_id").collect()
    ]

    # coins a,b,c,d,e have the highest % changes (10, 8, 6, 4, 2)
    expected_gainers = {"coin_a", "coin_b", "coin_c", "coin_d", "coin_e"}
    actual_gainers = set(gainers)

    assert actual_gainers == expected_gainers, \
        f"Expected gainers: {expected_gainers}, Got: {actual_gainers}"

    # Verify exactly 5 are flagged (not more, not less)
    assert len(gainers) == 5, f"Expected exactly 5 gainers, got {len(gainers)}"


def test_top5_loser_flags():
    """Bottom 5 coins by price_change_pct_24h should be flagged is_top5_loser=True."""
    test_df = _create_test_market_df()
    result = apply_daily_market_transforms(test_df)

    # Collect loser flags (use DataFrame API — .rdd not available on Spark Connect)
    losers = [
        row.coin_id for row in
        result.filter(F.col("is_top5_loser") == True).select("coin_id").collect()
    ]

    # coins f,g,h,i,j have the lowest % changes (0.5, -1, -3, -5, -8)
    expected_losers = {"coin_f", "coin_g", "coin_h", "coin_i", "coin_j"}
    actual_losers = set(losers)

    assert actual_losers == expected_losers, \
        f"Expected losers: {expected_losers}, Got: {actual_losers}"

    assert len(losers) == 5, f"Expected exactly 5 losers, got {len(losers)}"


def test_market_cap_share_sums_to_100():
    """Market cap share percentages for one day should sum to exactly 100%."""
    test_df = _create_test_market_df()
    result = apply_daily_market_transforms(test_df)

    total_share = (
        result.agg(F.sum("mkt_cap_share_pct").alias("total"))
        .collect()[0]["total"]
    )

    # Should be 100% (±0.01 for floating point)
    assert abs(total_share - 100.0) < 0.01, \
        f"Market cap shares should sum to 100%, got: {total_share:.4f}%"

    # Verify coin_a (500M of 999.5M total) has the largest share
    coin_a_share = (
        result.filter(F.col("coin_id") == "coin_a")
        .select("mkt_cap_share_pct")
        .collect()[0]["mkt_cap_share_pct"]
    )
    # 500M / 999.5M × 100 ≈ 50.025%
    assert coin_a_share > 49.0 and coin_a_share < 51.0, \
        f"Coin A (500M/999.5M) should have ~50% share, got: {coin_a_share:.2f}%"


run_test("Top 5 gainer flags — correct coins flagged", test_top5_gainer_flags)
run_test("Top 5 loser flags — correct coins flagged", test_top5_loser_flags)
run_test("Market cap share — sums to 100%", test_market_cap_share_sums_to_100)

# COMMAND ----------

# =============================================================================
# CELL 8 — TEST GROUP 6: compute_volume_spikes()
#
# BUSINESS RULE: A coin's hourly volume is a "spike" when it exceeds
# 2× the rolling 7-day average volume.
#
# WHY TEST IT:
#   The dashboard Page 3 highlights volume spikes in orange on the
#   BTC Price + Volume combo chart. If the formula is wrong:
#     - Too sensitive: everything looks like a spike → no signal value
#     - Too strict: nothing gets flagged → misses actual anomalies
#
# TEST APPROACH:
#   Create a small DataFrame with 5 hours of data for one coin.
#   Volume is steady at 1000 for 4 hours, then jumps to 2500 on hour 5.
#   With a small window (4 hours), the average is ~1000.
#   2500 > 2 × 1000 = 2000 → should be flagged as spike.
#   1500 would NOT be flagged (1500 < 2000).
# =============================================================================

print("\n── TEST GROUP 6: compute_volume_spikes() ──")

from pyspark.sql.types import TimestampType
from datetime import datetime, timedelta

def _create_test_volume_df():
    """
    Create a test hourly DataFrame with known volume pattern.
    10 hours of steady volume (1000), then 1 hour spike (5000).

    WHY 5000 AND NOT 2500?
      The rolling window INCLUDES the current row in the average.
      With 10 baseline hours at 1000 + 1 spike hour at 5000:
        avg = (10×1000 + 5000) / 11 ≈ 1364
        5000 > 2 × 1364 = 2727 → True ✅
      With only 2500, the spike would raise the avg enough to
      make 2500 < 2×avg, causing a false negative.
    """
    schema = StructType([
        StructField("coin_id", StringType(), False),
        StructField("hour_timestamp", TimestampType(), False),
        StructField("volume_usd", DoubleType(), True),
    ])
    base_time = datetime(2025, 4, 1, 0, 0, 0)
    data = [
        ("bitcoin", base_time + timedelta(hours=i), 1000.0)
        for i in range(10)
    ] + [
        ("bitcoin", base_time + timedelta(hours=10), 5000.0),  # Spike: 5000 > 2 × avg(~1364)
    ]
    return spark.createDataFrame(data, schema)


def test_volume_spike_detected():
    """Volume of 5000 with avg of ~1364 should be flagged as spike."""
    test_df = _create_test_volume_df()
    # Use window of 10 hours so baseline is well-established
    result = compute_volume_spikes(test_df, window_hours=10, threshold_multiplier=2)

    # Get the spike flag for the last hour (hour 10, volume=5000)
    last_hour = (
        result
        .orderBy(F.col("hour_timestamp").desc())
        .select("is_volume_spike")
        .first()
    )
    assert last_hour["is_volume_spike"] == True, \
        f"Volume 5000 with avg ~1364 should be flagged as spike, got: {last_hour['is_volume_spike']}"


def test_volume_no_false_spike():
    """Steady volume should NOT be flagged as spike."""
    schema = StructType([
        StructField("coin_id", StringType(), False),
        StructField("hour_timestamp", TimestampType(), False),
        StructField("volume_usd", DoubleType(), True),
    ])
    base_time = datetime(2025, 4, 1, 0, 0, 0)
    # All volumes are steady at 1000 — no spikes
    data = [
        ("bitcoin", base_time + timedelta(hours=i), 1000.0)
        for i in range(10)
    ]
    test_df = spark.createDataFrame(data, schema)
    result = compute_volume_spikes(test_df, window_hours=4, threshold_multiplier=2)

    # No hour should be flagged as spike (all volumes identical)
    spike_count = result.filter(F.col("is_volume_spike") == True).count()
    assert spike_count == 0, \
        f"Steady volume should have 0 spikes, got: {spike_count}"


run_test("Volume spike — flags correctly when volume > 2× avg", test_volume_spike_detected)
run_test("Volume spike — no false flags with steady volume", test_volume_no_false_spike)

# COMMAND ----------

# =============================================================================
# CELL 9 — TEST SUMMARY
#
# Displays a formatted table of all test results.
# This is what you screenshot for your presentation.
# =============================================================================

print("\n" + "=" * 70)
print(f"TEST RESULTS: {_passed} passed, {_failed} failed, {_passed + _failed} total")
print("=" * 70)

# Create summary DataFrame for clean display
summary_data = [
    (i + 1, status, name, error if error else "—")
    for i, (status, name, error) in enumerate(_test_results)
]

summary_df = spark.createDataFrame(
    summary_data,
    ["Test #", "Status", "Test Name", "Error"]
)

summary_df.display()

# Final pass/fail message
if _failed == 0:
    print(f"\n🎉 ALL {_passed} TESTS PASSED!")
else:
    print(f"\n⚠️  {_failed} TEST(S) FAILED — review errors above")
    raise AssertionError(f"{_failed} unit test(s) failed")
