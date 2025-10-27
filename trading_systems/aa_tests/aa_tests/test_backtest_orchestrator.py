#!/usr/bin/env python3
import os
import sys
from pathlib import Path
import pandas as pd

# ensure project imports work
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))
from data_io.bar_aggregator import BarAggregator
from indicators.indicator_engine import IndicatorEngine
from orchestrator.backtest_orchestrator import run_backtests
from strategies.breakout_pullback import BreakoutPullbackStrategy

def collect_trade_logs(results_dir: Path) -> pd.DataFrame:
    csvs = sorted(results_dir.glob("*_trades.csv"))
    frames = []
    for fp in csvs:
        df = pd.read_csv(fp)
        df["source_file"] = fp.name
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def run_debug() -> None:
    dates = ["2025-01-03", "2025-01-08"]
    print(f"[DEBUG] Running backtests for {dates}")
    aggregator = BarAggregator(fixed_intervals=[60, 300], sliding_intervals=[60, 300])
    indicator_engine = IndicatorEngine(intervals=[60, 300])
    strategy = BreakoutPullbackStrategy()

    results_dir = PROJECT_ROOT / "results" / "debug"
    os.makedirs(results_dir, exist_ok=True)

    run_backtests(
        dates,
        tickers=["APLD"],
        aggregator=aggregator,
        indicator_engine=indicator_engine,
        strategy=strategy,
        data_path=str(PROJECT_ROOT / "data" / "ticks"),
        trade_log_dir=str(results_dir),
        enable_threading=False,
    )
    trades = collect_trade_logs(results_dir)
    if trades.empty:
        print("[WARN] No trades were recorded.")
        return
    print("\nCollected trades:")
    print(trades.head())


if __name__ == "__main__":
    run_debug()