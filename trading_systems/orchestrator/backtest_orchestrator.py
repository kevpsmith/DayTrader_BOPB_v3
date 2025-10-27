"""Backtest orchestrator built on top of :mod:`core_orchestrator`."""

from __future__ import annotations

import os
from typing import Iterable, List, Optional

import pytz

from .core_orchestrator import OrchestratorCore
from ..data_io.parquet_loader import ParquetLoader
from ..data_io.warmup_loader import ParquetWarmupLoader
from ..executor.mock_executor import MockExecutor
from ..executor.trade_logger import TradeLogger
from ..time_utils.clock import Clock


class BacktestOrchestrator(OrchestratorCore):
    """Replay historical ticks through the live trading pipeline."""
    def __init__(
        self,
        *,
        date: str,
        tickers: Iterable[str],
        aggregator,
        indicator_engine,
        strategy,
        data_path: str = "data/ticks",
        trade_log_dir: str = "results/backtests",
        enable_threading: bool = False,
        max_workers: int = 4,
        default_trade_size: int = 100,
        starting_balance: float = 100_000.0,
        warmup_loader: Optional[ParquetWarmupLoader] = None,
        ) -> None:
        eastern = pytz.timezone("US/Eastern")
        session_start_local = eastern.localize(datetime.fromisoformat(f"{date}T09:30:00"))
        session_start_utc = session_start_local.astimezone(pytz.UTC)

        clock = Clock(mode="replay", start_time=session_start_utc)
        executor = MockExecutor(starting_balance=starting_balance)

        os.makedirs(trade_log_dir, exist_ok=True)
        trade_log_path = os.path.join(trade_log_dir, f"{date}_trades.csv")
        trade_logger = TradeLogger(log_dir=trade_log_dir, log_file=trade_log_path)

        super().__init__(
            tickers=tickers,
            aggregator=aggregator,
            indicator_engine=indicator_engine,
            strategy=strategy,
            executor=executor,
            clock=clock,
            trade_logger=trade_logger,
            enable_threading=enable_threading,
            max_workers=max_workers,
        )

        self.date = date
        self.loader = ParquetLoader(data_path, date)
        self.warmup_loader = warmup_loader or ParquetWarmupLoader(
            base_path=os.path.join("data", "bars", "1m")
        )
    
    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run(self) -> None:
        self.log.info("Running backtest for %s (%d tickers)", self.date, len(self.tickers))
        self._run_warmup()
        data_stream = self.loader.stream_ticks(list(self.tickers))
        super().run(data_stream)

    # ------------------------------------------------------------------
    # Warmup helpers
    # ------------------------------------------------------------------
    def _run_warmup(self) -> None:
        lookback_minutes = 30
        for ticker in self.tickers:
            try:
                warmup_bars = self.warmup_loader.load_minute_bars(
                    ticker,
                    self.clock.start_time,
                    lookback_minutes=lookback_minutes,
                )
            except FileNotFoundError:
                self.log.warning("Warmup data missing for %s", ticker)
                continue
            except Exception as exc:  # pragma: no cover - defensive
                self.log.warning("Warmup failed for %s: %s", ticker, exc)
                continue

            if not warmup_bars:
                continue

            self.log.info(
                "Seeding %d warmup bars for %s", len(warmup_bars), ticker
            )
            self.seed_warmup_bars(ticker, warmup_bars)


def run_backtests(
    dates: Iterable[str],
    *,
    tickers: Iterable[str],
    aggregator,
    indicator_engine,
    strategy,
    data_path: str = "data/ticks",
    trade_log_dir: str = "results/backtests",
    enable_threading: bool = False,
) -> List[BacktestOrchestrator]:
    """Utility helper used by smoke tests to iterate over multiple sessions."""

    orchestrators: List[BacktestOrchestrator] = []
    for date in dates:
        orchestrator = BacktestOrchestrator(
            date=date,
            tickers=tickers,
            aggregator=aggregator,
            indicator_engine=indicator_engine,
            strategy=strategy,
            data_path=data_path,
            trade_log_dir=trade_log_dir,
            enable_threading=enable_threading,
        )
        orchestrator.run()
        orchestrators.append(orchestrator)

    return orchestrators