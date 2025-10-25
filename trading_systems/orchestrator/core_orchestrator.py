import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
from threading import Event, Thread
from datetime import datetime, timezone


# --------------------------------------------------------------------------------
# Helper: ensure UTC awareness for timestamps
# --------------------------------------------------------------------------------

def _ensure_utc(ts):
    # pandas.Timestamp path (no hard dep if pandas isn’t installed)
    try:
        import pandas as pd
        if isinstance(ts, pd.Timestamp):
            return ts.tz_localize("UTC") if ts.tz is None else ts.tz_convert("UTC")
    except Exception:
        pass
    # python datetime path
    if isinstance(ts, datetime):
        return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts.astimezone(timezone.utc)
    return ts  # fallback (already fine)

class OrchestratorCore:
    """
    Central event router for both live and backtest modes.
    It does NOT aggregate bars or compute indicators itself—
    it simply moves market events through the pipeline:

        tick  →  BarAggregator.add_tick()
              →  IndicatorManager.update()
              →  Strategy.evaluate()
              →  Executor.execute()
              →  TradeLogger.log_trade()
    """

    def __init__(
        self,
        *,
        tickers,
        aggregator,
        indicator_engine,
        strategy,
        executor,
        clock,
        trade_logger=None,
        timezone="US/Eastern",
        max_workers=8,
        enable_threading=True,
    ):
        self.tickers = tickers
        self.aggregator = aggregator
        self.indicator_engine = indicator_engine
        self.strategy = strategy
        self.executor = executor
        self.clock = clock
        self.trade_logger = trade_logger
        self.timezone = timezone
        self.enable_threading = enable_threading
        self.max_workers = max_workers

        self.event_queue = Queue(maxsize=10000)
        self.stop_event = Event()
        self._setup_logger()

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------
    def _setup_logger(self):
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
    def run(self, data_stream):
        """Main orchestrator loop: feed tick events into the pipeline."""
        self.log.info(
            f"▶ Starting {self.__class__.__name__} | {len(self.tickers)} tickers | Threading={self.enable_threading}"
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
                self._safe_process_event(event)

        self._finalize()
        self.log.info("✅ Run complete.")

    # ------------------------------------------------------------------
    # Queue Consumer
    # ------------------------------------------------------------------
    def _consume_events(self, pool):
        futures = set()
        while not self.stop_event.is_set():
            try:
                event = self.event_queue.get(timeout=0.25)
            except Empty:
                continue
            futures.add(pool.submit(self._safe_process_event, event))

            # cleanup finished
            done = {f for f in futures if f.done()}
            for f in done:
                futures.remove(f)
                self.event_queue.task_done()

        for f in as_completed(futures):
            self.event_queue.task_done()

    # ------------------------------------------------------------------
    # Safe wrapper
    # ------------------------------------------------------------------
    def _safe_process_event(self, event):
        try:
            self._process_event(event)
        except Exception as e:
            self.log.error(f"Event processing error: {e}")
            self.log.debug(traceback.format_exc())

    # ------------------------------------------------------------------
    # Core event handler (delegates to utilities)
    # ------------------------------------------------------------------
    def _process_event(self, event):
        """
        Expected event: {'ticker': str, 'timestamp': datetime, 'price': float, 'size': int}
        """
        ticker = event.get("ticker")
        ts = event.get("timestamp")
        price = event.get("price")
        size = event.get("size", 1)
        if not (ticker and ts and price is not None):
            return

        # 1️⃣ Advance replay clock if applicable
        if getattr(self.clock, "mode", None) == "replay" and hasattr(self.clock, "set"):
            self.clock.set(_ensure_utc(ts))

        # 2️⃣ Build tick dict (exactly like aggregator tests)
        tick = {"timestamp": ts, "price": float(price), "size": int(size)}

        # 3️⃣ Delegate tick→bars to aggregator
        bars = self.aggregator.add_tick(tick)
        if not bars:
            return

        self.bar_history = getattr(self, "bar_history", {})  # keep between ticks

        for bar in bars:
            try:
                ticker = bar.get("ticker")
                if ticker not in self.bar_history:
                    # create a new empty DataFrame for this ticker
                    self.bar_history[ticker] = pd.DataFrame(
                        columns=["open", "high", "low", "close", "volume", "end_time"]
                    ).set_index("end_time")

                # --- 1️⃣ Append this new bar to history ---
                df_new = pd.DataFrame([{
                    "open": bar["open"],
                    "high": bar["high"],
                    "low": bar["low"],
                    "close": bar["close"],
                    "volume": bar["volume"],
                    "end_time": bar["end_time"],
                }]).set_index("end_time")

                self.bar_history[ticker] = pd.concat(
                    [self.bar_history[ticker], df_new]
                ).sort_index().tail(500)  # keep last 500 bars (adjust as needed)

                # --- 2️⃣ Compute indicators on the whole DF ---
                df_with_indicators = self.indicator_engine.update_dataframe(
                    self.bar_history[ticker]
                )

                # --- 3️⃣ Pass the entire DataFrame (not just one bar) to strategy ---
                signals = self.strategy.evaluate(df_with_indicators)

                # --- 4️⃣ Execute & log ---
                if signals:
                    orders = self.executor.execute(signals, df_with_indicators)
                    if self.trade_logger and orders:
                        for order in orders:
                            self.trade_logger.log_trade(order)

            except Exception as e:
                self.log.error(f"Processing error for {ticker}: {e}")
                self.log.debug(traceback.format_exc())

    # ------------------------------------------------------------------
    # Finalization
    # ------------------------------------------------------------------
    def _finalize(self):
        if hasattr(self.executor, "finalize"):
            try:
                self.executor.finalize()
            except Exception as e:
                self.log.warning(f"Executor finalize() failed: {e}")
        if self.trade_logger:
            try:
                self.trade_logger.close()
            except Exception:
                pass
