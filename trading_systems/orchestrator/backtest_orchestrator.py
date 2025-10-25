# indicators/indicator_engine.py
import sys, os
# add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
#!/usr/bin/env python3
import os
import asyncio
import pandas as pd
import pytz
from datetime import datetime, time, timedelta

import os
from datetime import datetime
from orchestrator.core_orchestrator import OrchestratorCore
from executor.mock_executor import MockExecutor
from data_io.parquet_loader import ParquetLoader
from time_utils.clock import Clock
from executor.trade_logger import TradeLogger
from data_io.warmup_loader import ParquetWarmupLoader


class BacktestOrchestrator(OrchestratorCore):
    """
    Orchestrator for backtesting. Replays historical ticks or minute bars
    through the same pipeline used in live trading, but uses a mock broker
    for order execution and historical parquet files for data.
    """

    def __init__(
        self,
        date: str,
        tickers: list,
        aggregator,
        indicator_engine,
        strategy,
        data_path: str = "data/ticks",
        trade_log_dir: str = "results/backtests",
        enable_threading: bool = False,
        max_workers: int = 4,
    ):
        # 1️⃣ Initialize clock for that trading day
        UTC = pytz.UTC
        start = UTC.localize(datetime(2025, 1, 8, 12, 0, 0))
        clock = Clock(mode='replay', start_time=start)
        # 2️⃣ Use the MockExecutor (no real trades)
        executor = MockExecutor(starting_balance=100000)

        # 3️⃣ Trade logger (save trade history)
        os.makedirs(trade_log_dir, exist_ok=True)
        trade_logger = TradeLogger(os.path.join(trade_log_dir, f"{date}_trades.csv"))

        # 4️⃣ Initialize the Parquet data loader
        self.loader = ParquetLoader(data_path, date)

        # 5️⃣ Initialize the parent orchestrator
        super().__init__(
            tickers=tickers,
            aggregator=aggregator,
            indicator_engine=indicator_engine,
            strategy=strategy,
            executor=executor,
            clock=clock,
            trade_logger=trade_logger,
            timezone="US/Eastern",
            enable_threading=enable_threading,
            max_workers=max_workers,
        )
    
    # ------------------------------------------------------------------
    # Run the backtest
    # ------------------------------------------------------------------
    def run(self):
        """
        Replays tick or bar data for all tickers chronologically.
        Optionally feeds warmup bars into the indicator engine before the day starts.
        """
        self.log.info(f"Running backtest for {len(self.tickers)} tickers on {self.clock.start_time}")

        # 🧠 STEP 1 — Warmup indicators with recent bars
        try:
            warmup_loader = ParquetWarmupLoader(base_path="data/bars/1m")
            utc = pytz.UTC

            # For each ticker, feed last 30 minutes before session start
            warmup_minutes = 30
            for ticker in self.tickers:
                self.log.info(f"Warming up indicators for {ticker}...")
                warmup_bars = warmup_loader.load_minute_bars(
                    ticker,
                    self.clock.start_time,        # e.g. 2025-01-08 09:30 ET (13:30 UTC)
                    lookback_minutes=warmup_minutes
                )
                for bar in warmup_bars:
                    # Ensure bar dict includes interval for IndicatorEngine
                    bar["interval"] = 60
                    self.indicator_engine.on_sliding_bar(bar)
        except Exception as e:
            self.log.warning(f"Warmup failed: {e}")

        # 🕒 STEP 2 — Start the main replay (ticks → bars → indicators → strategy)
        data_stream = self.loader.stream_ticks(self.tickers)
        super().run(data_stream)