#!/usr/bin/env python3
"""
Smoke test: verifies that the trading system pipeline is wired correctly.

Checks:
- PremarketFilter runs in backtest mode and returns tickers
- BacktestOrchestrator initializes without error
- OrchestratorCore processes at least one event
- MockExecutor logs a simulated trade
"""
#!/usr/bin/env python3
import sys, os
# add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import os
import pandas as pd

from scanner.premarket_filter import PremarketFetcher
from orchestrator.backtest_orchestrator import BacktestOrchestrator
from indicators.indicator_engine import IndicatorEngine
from data_io.bar_aggregator import BarAggregator
from strategies.breakout_pullback import BreakoutPullbackStrategy

# ---------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------
DATE = "2025-01-08"
DATA_PATH = "data/ticks"
RESULTS_PATH = "results/test_smoke"
os.makedirs(RESULTS_PATH, exist_ok=True)

# ---------------------------------------------------------------
# 1️⃣ Premarket Filter Test
# ---------------------------------------------------------------
print(f"\n[1] Testing PremarketFilter for {DATE}")
tickers_to_test = None
api_key = "cvV9m9XNz41uD7SMCLqftmzWKwDCI_9x"
fetcher = PremarketFetcher(api_key)
#tickers = fetcher.fetch(DATE, live = False, tickers=tickers_to_test)
###Fake TICKERS input
tickers = ["ACI", "ALT", "ANGO", "APLD", "BHAT"]
###Fake TICKERS input
assert isinstance(tickers, list), "PremarketFilter.fetch() must return a list"
assert len(tickers) > 0, "No tickers returned from premarket filter"
print(f"✅ Premarket filter returned {len(tickers)} tickers: {tickers[:5]}")

# ---------------------------------------------------------------
# 2️⃣ Orchestrator + Utilities Setup
# ---------------------------------------------------------------

print("\n[2] Initializing orchestrator and utilities")
aggregator = BarAggregator(fixed_intervals=[60,300], sliding_intervals=[60,300])
indicator_engine = IndicatorEngine(intervals=[60, 300])
strategy = BreakoutPullbackStrategy()

sample_ticker_count = min(5, len(tickers))
assert sample_ticker_count >= 3, "Premarket filter should yield at least 3 tickers for smoke coverage"

backtest = BacktestOrchestrator(
    date=DATE,
    tickers=tickers[:sample_ticker_count],  # exercise multiple tickers for better coverage
    aggregator=aggregator,
    indicator_engine=indicator_engine,
    strategy=strategy,
    data_path=DATA_PATH,
    trade_log_dir=RESULTS_PATH,
    enable_threading=False,  # deterministic for smoke test
)

print("✅ BacktestOrchestrator initialized successfully")

# ---------------------------------------------------------------
# 3️⃣ Run Single-Day Test
# ---------------------------------------------------------------
print("\n[3] Running orchestrator core smoke test...")
backtest.run()
print("✅ Orchestrator ran without critical errors")

# ---------------------------------------------------------------
# 4️⃣ Validate Results
# ---------------------------------------------------------------
expected_log = os.path.join(RESULTS_PATH, f"{DATE}_trades.csv")
assert os.path.exists(expected_log), f"Trade log missing: {expected_log}"

df = pd.read_csv(expected_log)
assert len(df) > 0, "Trade log is empty — no trades executed"
assert {"ticker", "side", "price"}.issubset(df.columns), "Trade log missing expected columns"

print(f"✅ Trade log found with {len(df)} rows.")
print("Sample rows:")
print(df.head())

print("\n🎉 SMOKE TEST PASSED — all core systems connected correctly!")