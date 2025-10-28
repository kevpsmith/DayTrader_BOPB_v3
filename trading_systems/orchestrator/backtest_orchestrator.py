"""Backtest orchestrator built on top of :mod:`core_orchestrator`."""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Iterable, List, Optional

import pytz
import pandas as pd

from .core_orchestrator import OrchestratorCore
from data_io.parquet_loader import ParquetLoader
from data_io.warmup_loader import ParquetWarmupLoader
from executor.mock_executor import MockExecutor
from executor.trade_logger import TradeLogger
from time_utils.clock import Clock


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
        session_end_local = eastern.localize(datetime.fromisoformat(f"{date}T16:00:00"))
        session_start_utc = session_start_local.astimezone(pytz.UTC)
        session_end_utc = session_end_local.astimezone(pytz.UTC)

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
            default_trade_size=default_trade_size
        )

        self.date = date
        self.loader = ParquetLoader(data_path, date)
        self.warmup_loader = warmup_loader or ParquetWarmupLoader(
            base_path=os.path.join("data", "bars", "1m")
        )
        self.session_start_utc = session_start_utc
        self.session_end_utc = session_end_utc
        self.session_timezone = eastern

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def run(self) -> None:
        self.log.info("Running backtest for %s (%d tickers)", self.date, len(self.tickers))
        start_local = self.session_start_utc.astimezone(self.session_timezone)
        end_local = self.session_end_utc.astimezone(self.session_timezone)
        self.log.info(
            "Streaming ticks between %s and %s (%s)",
            start_local.strftime("%Y-%m-%d %H:%M"),
            end_local.strftime("%Y-%m-%d %H:%M"),
            self.session_timezone.zone,
        )        
        self._run_warmup()
        data_stream = self.loader.stream_ticks(
            list(self.tickers),
            start_time=self.session_start_utc,
            end_time=self.session_end_utc,
        )
        super().run(data_stream)

    # ------------------------------------------------------------------
    # Warmup helpers
    # ------------------------------------------------------------------
    def _run_warmup(self) -> None:
        lookback_minutes = 30
        for ticker in self.tickers:
            warmup_bars = self._load_or_download_warmup_bars(
                ticker, lookback_minutes
            )

            if not warmup_bars:
                continue

            self.log.info(
                "Seeding %d warmup bars for %s", len(warmup_bars), ticker
            )
            self.seed_warmup_bars(ticker, warmup_bars)

    def _load_or_download_warmup_bars(
        self, ticker: str, lookback_minutes: int
    ) -> List[dict]:
        try:
            warmup_bars = self.warmup_loader.load_minute_bars(
                ticker,
                self.clock.start_time,
                lookback_minutes=lookback_minutes,
            )
        except FileNotFoundError:
            warmup_bars = []
        except Exception as exc:  # pragma: no cover - defensive
            self.log.warning("Warmup failed for %s: %s", ticker, exc)
            warmup_bars = []

        if warmup_bars:
            return warmup_bars

        self.log.info(
            "Warmup data missing for %s, backfilling from tick parquet", ticker
        )
        backfilled = self._backfill_warmup_from_ticks(ticker, lookback_minutes)
        if not backfilled:
            self.log.warning("Unable to backfill warmup data for %s", ticker)
        return backfilled

    def _backfill_warmup_from_ticks(
        self, ticker: str, lookback_minutes: int
    ) -> List[dict]:
        try:
            df = self.loader.load_ticker_data(ticker)
        except FileNotFoundError:
            self.log.warning("Tick data missing for %s", ticker)
            return []
        except Exception as exc:  # pragma: no cover - defensive
            self.log.warning("Tick data load failed for %s: %s", ticker, exc)
            return []

        df = df.sort_values("timestamp")
        start_time = self.session_start_utc - timedelta(minutes=lookback_minutes)
        mask = (df["timestamp"] >= start_time) & (
            df["timestamp"] < self.session_start_utc
        )
        df = df.loc[mask]
        if df.empty:
            return []

        df = df.set_index("timestamp")
        resampled = (
            df.resample("1min", label="left", closed="left")
            .agg(
                open=("price", "first"),
                high=("price", "max"),
                low=("price", "min"),
                close=("price", "last"),
                volume=("size", "sum"),
            )
            .dropna(subset=["open", "high", "low", "close"])
        )

        if resampled.empty:
            return []

        resampled = resampled.tail(lookback_minutes)
        bars: List[dict] = []
        for ts, row in resampled.iterrows():
            volume_value = row["volume"]
            if pd.isna(volume_value):
                volume_value = 0
            bars.append(
                {
                    "start_time": ts,
                    "end_time": ts + timedelta(minutes=1),
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": int(volume_value),
                }
            )

        self.log.info(
            "Backfilled %d warmup bars for %s from tick data",
            len(bars),
            ticker,
        )
        return bars


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