import os
from orchestrator.backtest_orchestrator import BacktestOrchestrator
from indicators.indicator_engine import IndicatorEngine
from data_io.bar_aggregator import BarAggregator
from strategies.breakout_pullback import BreakoutPullbackStrategy
from scanner.premarket_filter import PremarketFetcher


def run_backtest_for_dates(
    dates,
    data_path="data/ticks",
    results_path="results/backtests",
    enable_threading=False,
):
    """
    Run full-day backtests across multiple trading dates using premarket-filtered tickers.
    """
    os.makedirs(results_path, exist_ok=True)

    for date in dates:
        print(f"\n=== Starting backtest for {date} ===")

        # 1️⃣ Premarket scan
        premarket = PremarketFetcher(data_path=data_path)
        tickers = premarket.fetch(date, backtest_mode=True)

        if not tickers:
            print(f"No tickers passed filters for {date}")
            continue

        print(f"Running backtest for {len(tickers)} tickers: {tickers[:10]}...")

        # 2️⃣ Initialize core components
        aggregator = BarAggregator(intervals=["30s", "1m", "5m"])
        indicator_engine = IndicatorEngine()
        strategy = BreakoutPullbackStrategy()

        # 3️⃣ Initialize orchestrator
        orchestrator = BacktestOrchestrator(
            date=date,
            tickers=tickers,
            aggregator=aggregator,
            indicator_engine=indicator_engine,
            strategy=strategy,
            data_path=data_path,
            trade_log_dir=results_path,
            enable_threading=enable_threading,
        )

        # 4️⃣ Run the full trading session
        orchestrator.run()

        print(f"✅ Finished backtest for {date}")


if __name__ == "__main__":
    # Example: iterate through several backtest days
    backtest_dates = [
        "2025-01-08",
        "2025-01-09",
        "2025-01-10",
    ]
    run_backtest_for_dates(backtest_dates)