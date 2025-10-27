"""Core orchestration logic shared by backtest and live runners."""

from __future__ import annotations
import logging
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from queue import Empty, Queue
from threading import Event, Thread
from typing import DefaultDict, Dict, Iterable, List, Optional
import pandas as pd

def _ensure_utc(ts: datetime) -> datetime:
    """Normalise incoming timestamps to timezone-aware UTC datetimes."""
    if isinstance(ts, pd.Timestamp):
        return ts.tz_localize("UTC") if ts.tz is None else ts.tz_convert("UTC")
    if isinstance(ts, datetime):
        return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts.astimezone(
            timezone.utc
        )
    raise TypeError(f"Unsupported timestamp type: {type(ts)!r}")


class OrchestratorCore:
    """Route market data through aggregation, indicators, strategy and execution.

    The core is intentionally opinionated: it manages per-ticker bar histories,
    keeps a very small position book, and calls into the injected components in
    the right order.  Both the backtest and live orchestrators simply provide a
    data stream to :meth:`run`.
    """

    def __init__(
        self,
        *,
        tickers: Iterable[str],
        aggregator,
        indicator_engine,
        strategy,
        executor,
        clock,
        trade_logger=None,
        timezone: str = "US/Eastern",
        max_workers: int = 8,
        enable_threading: bool = True,
        default_trade_size: int = 100,
        ) -> None:
        self.tickers = list(tickers)
        self.aggregator = aggregator
        self.indicator_engine = indicator_engine
        self.strategy = strategy
        self.executor = executor
        self.clock = clock
        self.trade_logger = trade_logger
        self.timezone = timezone
        self.enable_threading = enable_threading
        self.max_workers = max_workers
        self.default_trade_size = default_trade_size

        self.event_queue: "Queue[Dict]" = Queue(maxsize=10_000)
        self.stop_event = Event()
        self.bar_history: DefaultDict[str, Dict[str, Dict[int, pd.DataFrame]]]
        self.bar_history = defaultdict(lambda: {"sliding": {}, "fixed": {}})
        self.positions: Dict[str, Dict] = {}

        intervals = sorted(getattr(indicator_engine, "intervals", []) or [60])
        self.primary_interval = intervals[0]
        larger = [iv for iv in intervals[1:] if iv > self.primary_interval]
        self.trend_interval = larger[0] if larger else None
        smaller = [iv for iv in intervals if iv < self.primary_interval]
        self.fast_interval = smaller[-1] if smaller else None
        self._setup_logger()

    # ------------------------------------------------------------------
    # Logging helpers
    # ------------------------------------------------------------------
    def _setup_logger(self) -> None:
        self.log = logging.getLogger(self.__class__.__name__)
        if not self.log.handlers:
            handler = logging.StreamHandler()
            fmt = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
            handler.setFormatter(fmt)
            self.log.addHandler(handler)
            self.log.setLevel(logging.INFO)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------
    def run(self, data_stream: Iterable[Dict]) -> None:
        """Drive the orchestrator with an iterable of tick dictionaries."""
        self.log.info(
            "▶ Starting %s | %d tickers | Threading=%s",
            self.__class__.__name__,
            len(self.tickers),
            self.enable_threading,
        )

        if self.enable_threading:
            with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                consumer = Thread(target=self._consume_events, args=(pool,), daemon=True)
                consumer.start()

                for event in data_stream:
                    if self.stop_event.is_set():
                        break
                    self.event_queue.put(event)

                self.event_queue.join()
                self.stop_event.set()
                consumer.join(timeout=5)
        else:
            for event in data_stream:
                if self.stop_event.is_set():
                    break
                self._safe_process_event(event)

        self._finalize()
        self.log.info("✅ Run complete.")

    # ------------------------------------------------------------------
    # Event processing helpers
    # ------------------------------------------------------------------
    def _consume_events(self, pool: ThreadPoolExecutor) -> None:
        futures = set()
        while not self.stop_event.is_set():
            try:
                event = self.event_queue.get(timeout=0.25)
            except Empty:
                continue

            futures.add(pool.submit(self._safe_process_event, event))

            done = {f for f in futures if f.done()}
            for f in done:
                futures.remove(f)
                self.event_queue.task_done()

        for f in as_completed(futures):
            self.event_queue.task_done()

    def _safe_process_event(self, event: Dict) -> None:
        try:
            self._process_event(event)
        except Exception:
            self.log.error("Event processing error:")
            self.log.debug(traceback.format_exc())

    def _process_event(self, event: Dict) -> None:
        ticker = event.get("ticker")
        ts = event.get("timestamp")
        price = event.get("price")
        size = event.get("size", 1)
        if not (ticker and ts and price is not None):
            return

        ts_utc = _ensure_utc(ts)

        if getattr(self.clock, "mode", None) == "replay" and hasattr(self.clock, "set"):
            self.clock.set(ts_utc)

        tick = {
            "ticker": ticker,
            "timestamp": ts_utc,
            "price": float(price),
            "size": int(size),
        }

        bars = self.aggregator.add_tick(tick)
        if not bars:
            return

        signals_by_ticker: Dict[str, List[Dict]] = {}

        for bar in bars:
            ticker = bar.get("ticker")
            if ticker is None:
                continue
            signals = self._ingest_bar(bar)
            if signals:
                signals_by_ticker.setdefault(ticker, []).extend(signals)

        if signals_by_ticker:
            self._dispatch_signals(signals_by_ticker)

    # ------------------------------------------------------------------
    # Bar ingestion & storage
    # ------------------------------------------------------------------
    def _ingest_bar(self, bar: Dict) -> List[Dict]:
        ticker = bar["ticker"]
        interval = bar["interval"]
        bar_type = bar.get("type", "sliding")

        indicators = None
        indicator_intervals = getattr(self.indicator_engine, "intervals", [])
        if bar_type == "sliding" and (
            not indicator_intervals or interval in indicator_intervals
        ):
            indicators = self.indicator_engine.on_sliding_bar(
                {
                    "interval": interval,
                    "start_time": bar["start_time"],
                    "end_time": bar["end_time"],
                    "open": bar["open"],
                    "high": bar["high"],
                    "low": bar["low"],
                    "close": bar["close"],
                    "volume": bar["volume"],
                }
            )

        record = self._build_record(bar, indicators)
        self._append_bar_to_history(ticker, interval, record, bar_type)

        if bar_type == "sliding" and interval == self.primary_interval:
            return self._generate_signals(ticker)
        return []

    def _build_record(self, bar: Dict, indicators: Optional[Dict]) -> Dict:
        ts = _ensure_utc(bar["end_time"])
        record = {
            "interval": bar["interval"],
            "start_time": _ensure_utc(bar["start_time"]),
            "end_time": ts,
            "timestamp_dt": ts,
            "open": bar["open"],
            "high": bar["high"],
            "low": bar["low"],
            "close": bar["close"],
            "volume": bar["volume"],
        }

        if indicators:
            record.update(indicators)
            record.setdefault("VWAP_D", indicators.get("vwap"))
            record.setdefault("MACDh_12_26_9", indicators.get("macd_hist"))
        return record

    def _append_bar_to_history(
        self,
        ticker: str,
        interval: int,
        record: Dict,
        bar_type: str,
        keep_last: int = 1_000,
    ) -> None:
        container = self.bar_history[ticker][bar_type]
        df = container.get(interval)
        row = pd.DataFrame([record]).set_index("timestamp_dt", drop=False)
        if df is None:
            container[interval] = row
        else:
            container[interval] = pd.concat([df, row]).sort_index().tail(keep_last)

    # ------------------------------------------------------------------
    # Signal generation & execution
    # ------------------------------------------------------------------
    def _generate_signals(self, ticker: str) -> List[Dict]:
        sliding = self.bar_history[ticker]["sliding"]
        df_primary = sliding.get(self.primary_interval)
        if df_primary is None or len(df_primary) < 2:
            return []

        df_trend = None
        if self.trend_interval is not None:
            df_trend = sliding.get(self.trend_interval) or self.bar_history[ticker][
                "fixed"
            ].get(self.trend_interval)
            if df_trend is None or len(df_trend) < 2:
                return []

        df_fast = sliding.get(self.fast_interval) if self.fast_interval else None

        last_row = df_primary.iloc[-1]
        signals: List[Dict] = []

        position = self.positions.get(ticker)
        if position:
            self.positions[ticker]["peak_price"] = max(
                position.get("peak_price", last_row["close"]), last_row["close"]
            )
            if self.strategy.check_exit(
                last_row,
                self.positions[ticker]["peak_price"],
                position["entry_price"],
                position["entry_time"],
                last_row["timestamp_dt"],
            ):
                signals.append(
                    {
                        "ticker": ticker,
                        "side": "sell",
                        "size": position["size"],
                        "timestamp": last_row["timestamp_dt"],
                    }
                )
        else:
            if df_trend is None or self.strategy.check_preconditions(df_trend):
                if self.strategy.check_entry(
                    df_primary,
                    df_fast,
                    len(df_primary) - 1,
                ):
                    signals.append(
                        {
                            "ticker": ticker,
                            "side": "buy",
                            "size": self.default_trade_size,
                            "timestamp": last_row["timestamp_dt"],
                        }
                    )

        return signals

    def _dispatch_signals(self, signals_by_ticker: Dict[str, List[Dict]]) -> None:
        for ticker, signals in signals_by_ticker.items():
            if not signals:
                continue
            df_primary = self.bar_history[ticker]["sliding"].get(self.primary_interval)
            if df_primary is None or df_primary.empty:
                continue

            orders = self.executor.execute(signals, df_primary)
            if not orders:
                continue

            for order in orders:
                self._update_positions_from_order(ticker, order, df_primary)
                self._log_trade(order, df_primary)

    def _update_positions_from_order(self, ticker: str, order: Dict, bars: pd.DataFrame) -> None:
        side = order.get("side")
        last_row = bars.iloc[-1]
        if side == "buy":
            self.positions[ticker] = {
                "size": order.get("size", self.default_trade_size),
                "entry_price": order.get("price", last_row["close"]),
                "entry_time": last_row["timestamp_dt"],
                "peak_price": last_row["close"],
            }
        elif side == "sell":
            self.positions.pop(ticker, None)

    def _log_trade(self, order: Dict, bars: pd.DataFrame) -> None:
        if not self.trade_logger:
            return

        timestamp = order.get("timestamp")
        if timestamp is None:
            timestamp = bars.iloc[-1]["timestamp_dt"]

        if hasattr(self.trade_logger, "log_trade"):
            self.trade_logger.log_trade(order)
            return

        ticker = order.get("ticker")
        price = order.get("price", bars.iloc[-1]["close"])
        qty = order.get("size", 0)
        if order.get("side") == "buy" and hasattr(self.trade_logger, "log_entry"):
            self.trade_logger.log_entry(ticker, price, timestamp, quantity=qty)
        elif order.get("side") == "sell" and hasattr(self.trade_logger, "log_exit"):
            self.trade_logger.log_exit(ticker, price, timestamp, quantity=qty)

    # ------------------------------------------------------------------
    # Warmup helpers
    # ------------------------------------------------------------------
    def seed_warmup_bars(
        self, ticker: str, bars: Iterable[Dict], interval: Optional[int] = None
    ) -> None:
        interval = interval or self.primary_interval
        for bar in bars:
            payload = {
                "ticker": ticker,
                "interval": interval,
                "type": "sliding",
                "start_time": bar["start_time"],
                "end_time": bar["end_time"],
                "open": bar["open"],
                "high": bar["high"],
                "low": bar["low"],
                "close": bar["close"],
                "volume": bar["volume"],
            }
            indicators = self.indicator_engine.on_sliding_bar(
                {
                    "interval": interval,
                    "start_time": bar["start_time"],
                    "end_time": bar["end_time"],
                    "open": bar["open"],
                    "high": bar["high"],
                    "low": bar["low"],
                    "close": bar["close"],
                    "volume": bar["volume"],
                }
            )
            record = self._build_record(payload, indicators)
            self._append_bar_to_history(ticker, interval, record, "sliding")

    # ------------------------------------------------------------------
    # Finalization
    # ------------------------------------------------------------------
    def _finalize(self) -> None:
        if hasattr(self.executor, "finalize"):
            try:
                self.executor.finalize()
            except Exception as exc:  # pragma: no cover - defensive
                self.log.warning("Executor finalize() failed: %s", exc)

        if hasattr(self.trade_logger, "close"):
            try:
                self.trade_logger.close()
            except Exception:  # pragma: no cover - defensive
                pass
