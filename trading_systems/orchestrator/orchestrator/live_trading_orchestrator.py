import logging
import sys
import time

import pandas as pd

from executor.trade_executor import TradeExecutor
from executor.trade_logger import TradeLogger
from strategies.breakout_pullback import BreakoutPullbackStrategy
from tickstream.bar_aggregator import BarAggregator
from tickstream.indicator_manager import IndicatorManager
from tickstream.tick_buffer import TickBuffer
from tickstream.warmup_loader import WarmupBarLoader
from tickstream.websocket_client import PolygonWebSocketClient
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

class LiveTradingOrchestrator:
    def __init__(self, api_key, tickers, account_id, dry_run=False, reference_time=None):
        self.api_key = api_key
        self.tickers = tickers
        self.tick_buffer = TickBuffer()
        self.aggregator = BarAggregator(self.tick_buffer, intervals=[30, 60, 300])
        self.warmup_loader = WarmupBarLoader(api_key)
        self.indicator_manager = IndicatorManager()
        self.strategy = BreakoutPullbackStrategy()
        self.executor = TradeExecutor(account_id, api_key, dry_run=dry_run)
        self.logger = TradeLogger()
        self.bars = {ticker: {"1m": None, "5m": None, "30s": None} for ticker in tickers}
        self.positions = {}  # Track active positions
        self.cash_reserve = 27900  # Amount to keep untouched
        self.reference_time = reference_time

    def handle_tick(self, tick):
        print(f"[TICK RECEIVED] {tick}")
        self.tick_buffer.add_tick(tick)

    def warmup_bars(self, reference_time=None):
        logger = logging.getLogger(__name__)
        if reference_time is None:
            reference_time = datetime.now(pytz.UTC)
        for ticker in self.tickers:
            end_time = reference_time or self.reference_time
            df_1m = self.warmup_loader.load_1m_bars(ticker, end_time)
            df_1m = self.indicator_manager.update_indicators(df_1m)
            self.bars[ticker]["1m"] = df_1m
            logger.info("Warmed up %d×1-minute bars for %s", len(self.bars[ticker]["1m"]), ticker)
            # Optional: resample or load 5m bars
            df_5m = df_1m.resample("5min", label="right", closed="right").agg({
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum"
            }).dropna()
            df_5m = self.indicator_manager.update_indicators(df_5m)
            self.bars[ticker]["5m"] = df_5m

            # Optional: resample or load 30s bars (resample if not directly available)
            df_30s = self.warmup_loader.load_30s_bars(ticker, reference_time)  # <-- only if implemented
            if df_30s is not None:
                df_30s = self.indicator_manager.update_indicators(df_30s)
                self.bars[ticker]["30s"] = df_30s

    def get_available_cash(self):
        # You may already have a method in TradeExecutor for this. Adjust accordingly.
        return self.executor.get_cash_balance()

    def start(self):
        print("[RUN] Starting orchestrator...")
        self.warmup_bars()

        ws = PolygonWebSocketClient(
            api_key=self.api_key,
            tickers=self.tickers,
            on_tick_callback=self.handle_tick
        )
        ws.run()

        try:
            while True:
                for ticker in self.tickers:
                    new_bars = self.aggregator.build_bars(ticker)
                    for interval_key in ["60s", "300s", "30s"]:
                        if not new_bars[interval_key].empty:
                            df = new_bars[interval_key]
                            df = self.indicator_manager.update_indicators(df)
                            label = interval_key.replace("60s", "1m").replace("300s", "5m")
                            self.bars[ticker][label] = df

                    df_1m = self.bars[ticker]["1m"]
                    df_5m = self.bars[ticker]["5m"]
                    df_30s = self.bars[ticker]["30s"]

                    if df_1m is not None and df_5m is not None and df_30s is not None:
                        i_1m = len(df_1m) - 1

                        # If already in position, monitor for exit
                        if ticker in self.positions:
                            pos = self.positions[ticker]
                            row = df_1m.iloc[-1]
                            current_time = pd.Timestamp.utcnow()
                            if self.strategy.check_exit(
                                row=row,
                                peak_price=pos['peak_price'],
                                entry_price=pos['entry_price'],
                                entry_time=pos['entry_time'],
                                current_time=current_time
                            ):
                                print(f"[EXIT] Signal to exit {ticker} @ {row['close']}")
                                result = self.executor.sell_all(
                                    symbol=ticker,
                                    price=row['close'],
                                    atr=0.5,
                                    fallback="market"
                                )
                                if result:
                                    self.logger.log_exit(ticker, row['close'], current_time, result['qty'])
                                    del self.positions[ticker]
                            else:
                                # Update peak price
                                self.positions[ticker]['peak_price'] = max(
                                    self.positions[ticker]['peak_price'], row['close']
                                )

                        # Otherwise, look for new entry
                        elif self.strategy.check_preconditions(df_5m):
                            if self.strategy.check_entry(df_1m, df_30s, i_1m):
                                last_price = df_1m.iloc[-1]['close']
                                estimated_cost = last_price * 100  # Assume 100 shares per trade

                                available_cash = self.get_available_cash()
                                if available_cash - estimated_cost >= self.cash_reserve:
                                    print(f"[SIGNAL] Entry for {ticker} @ {last_price}")
                                    result = self.executor.place_marketable_limit_order_with_failover(
                                        symbol=ticker,
                                        side="buy",
                                        price=last_price,
                                        ask=last_price + 0.01,
                                        atr=0.5,
                                        fallback="market"
                                    )
                                    if result:
                                        entry_time = pd.Timestamp.utcnow()
                                        self.positions[ticker] = {
                                            "entry_price": last_price,
                                            "entry_time": entry_time,
                                            "peak_price": last_price
                                        }
                                        self.logger.log_entry(ticker, last_price, entry_time, result['qty'])
                                else:
                                    print(f"[SKIP] Not enough cash for {ticker}. Available: {available_cash:.2f}, Needed: {estimated_cost + self.cash_reserve:.2f}")

                time.sleep(5)
        except KeyboardInterrupt:
            print("[SHUTDOWN] Exiting and canceling orders...")
            self.executor.cancel_all_orders()
            ws.close()

    def run_logic_for_ticker(self, ticker):
        df_1m = self.bars[ticker]["1m"]
        df_5m = self.bars[ticker]["5m"]
        df_30s = self.bars[ticker]["30s"]

        if df_1m is not None and df_5m is not None and df_30s is not None:
            i_1m = len(df_1m) - 1
            row = df_1m.iloc[-1]
            current_time = row.name  # Use historical timestamp

            # EXIT
            if ticker in self.positions:
                pos = self.positions[ticker]
                if self.strategy.check_exit(row, pos["peak_price"], pos["entry_price"], pos["entry_time"], current_time):
                    print(f"[EXIT] {ticker} @ {row['close']}")
                    self.logger.log_exit(ticker, row['close'], current_time, qty=100)
                    del self.positions[ticker]
                else:
                    self.positions[ticker]['peak_price'] = max(pos['peak_price'], row['close'])

            # ENTRY
            elif self.strategy.check_preconditions(df_5m):
                if self.strategy.check_entry(df_1m, df_30s, i_1m):
                    last_price = df_1m.iloc[-1]['close']
                    print(f"[ENTRY] {ticker} @ {last_price}")
                    self.positions[ticker] = {
                        "entry_price": last_price,
                        "entry_time": current_time,
                        "peak_price": last_price
                    }
                    self.logger.log_entry(ticker, last_price, current_time, qty=100)