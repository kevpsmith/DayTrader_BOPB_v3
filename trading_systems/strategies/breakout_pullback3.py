"""Breakout Pullback strategy variant that enforces no same-bar exits.

This module implements a third iteration of the breakout pullback strategy
aligned with the entry / exit specification discussed in the design notes:

Entry setup (5-minute bars):
* Require at least 10 completed 5-minute candles plus buffer for indicator
  warm-up.
* Last close must exceed the previous 10-bar high by at least 2%.
* Breakout level is anchored to that prior high and reused during the
  1-minute evaluation.

Entry trigger (1-minute bars):
* Evaluate between 9:45am and 11:00am New York time.
* Price must sit within 1% of either the breakout level or VWAP and reclaim
  the level (closing above the reference price).
* The latest candle must close higher than the previous, RSI must rise,
  the MACD histogram must be positive, and volume must spike to ≥1.5× the
  average of the prior 10 bars.

Exit trigger (1-minute bars):
* Price drops below VWAP.
* MACD histogram turns negative.
* RSI falls below 50.
* Drawdown from the running peak exceeds 3%.
* Position age exceeds 20 minutes.

Unlike the earlier implementation, this variant keeps a lightweight ledger of
symbol timestamps so that exit checks are skipped while the strategy is still
processing the entry candle.  That ensures the same bar cannot simultaneously
produce both an entry and exit signal.
"""

from __future__ import annotations

from datetime import time
from typing import Any, Dict, Hashable, Optional

import pandas as pd
from zoneinfo import ZoneInfo


class BreakoutPullbackStrategy3:
    """Sliding-bar breakout pullback strategy with same-bar exit guard."""

    def __init__(self, local_tz: str = "America/New_York") -> None:
        self.tz = ZoneInfo(local_tz)
        self.entry_window_start = time(9, 45, tzinfo=self.tz)
        self.entry_window_end = time(11, 0, tzinfo=self.tz)

        # Track breakout levels and the most recent entry timestamp per symbol.
        self._breakout_levels: Dict[Hashable, float] = {}
        self._last_entry_ts: Dict[Hashable, pd.Timestamp] = {}
        self.min_primary_bars = 15
        self.min_trend_bars = 16

    # ------------------------------------------------------------------
    # Helper utilities
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_symbol(row: pd.Series) -> Hashable:
        for key in ("symbol", "ticker", "asset"):
            value = row.get(key)
            if value is not None:
                return value
        return "__default__"

    @staticmethod
    def _normalise_timestamp(value: Any) -> Optional[pd.Timestamp]:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return ts

    # ------------------------------------------------------------------
    # 5-minute setup validation
    # ------------------------------------------------------------------
    def check_preconditions(self, df_5m: pd.DataFrame) -> bool:
        if df_5m is None:
            return False

        required_bars = max(10, int(getattr(self, "min_trend_bars", 10)))
        if len(df_5m) < required_bars:
            return False

        df_window = df_5m.tail(required_bars)

        recent_highs = df_window["high"].rolling(window=10).max()
        if recent_highs.isna().iloc[-2]:
            return False

        last_close = df_window.iloc[-1]["close"]
        prev_high = recent_highs.iloc[-2]

        if last_close <= prev_high * 1.02:
            return False

        symbol = self._extract_symbol(df_5m.iloc[-1])
        self._breakout_levels[symbol] = prev_high
        return True

    # ------------------------------------------------------------------
    # 1-minute entry evaluation
    # ------------------------------------------------------------------
    def check_entry(
        self,
        df_1m: pd.DataFrame,
        df_30s: Optional[pd.DataFrame] = None,
        i_1m: Optional[int] = None,
    ) -> bool:
        if df_1m is None or df_1m.empty:
            return False

        if i_1m is None:
            i_1m = len(df_1m) - 1

        if i_1m < 1 or i_1m >= len(df_1m):
            return False

        if (i_1m + 1) < self.min_primary_bars or i_1m < 10:
            return False

        curr = df_1m.iloc[i_1m]
        symbol = self._extract_symbol(curr)
        breakout_level = self._breakout_levels.get(symbol)
        if breakout_level is None:
            return False

        prev = df_1m.iloc[i_1m - 1]
        curr_ts = self._normalise_timestamp(curr.get("timestamp_dt") or df_1m.index[i_1m])
        if curr_ts is None:
            return False

        curr_local_time = curr_ts.astimezone(self.tz).timetz()
        if not (self.entry_window_start <= curr_local_time <= self.entry_window_end):
            return False

        price = curr.get("close")
        prev_close = prev.get("close")
        vwap = curr.get("VWAP_D")
        if price is None or prev_close is None or vwap is None:
            return False

        # Proximity check against breakout or VWAP.
        near_breakout = (
            breakout_level > 0
            and abs(price - breakout_level) / breakout_level <= 0.01
            and price >= breakout_level * 0.995
        )
        near_vwap = (
            vwap > 0
            and abs(price - vwap) / vwap <= 0.01
            and price >= vwap
        )
        if not (near_breakout or near_vwap):
            return False

        curr_rsi = curr.get("rsi")
        prev_rsi = prev.get("rsi")
        macd_hist = curr.get("MACDh_12_26_9")
        if curr_rsi is None or prev_rsi is None or macd_hist is None:
            return False

        reclaim = (
            price > prev_close
            and curr_rsi > prev_rsi
            and macd_hist > 0
        )
        if not reclaim:
            return False

        window = df_1m.iloc[max(0, i_1m - 10) : i_1m]
        avg_volume = window["volume"].mean() if not window.empty else None
        curr_volume = curr.get("volume")
        if avg_volume is None or avg_volume <= 0 or curr_volume is None:
            return False

        if curr_volume < 1.5 * avg_volume:
            return False

        # Remember entry timestamp for same-bar exit guard.
        self._last_entry_ts[symbol] = curr_ts
        return True

    # ------------------------------------------------------------------
    # Exit evaluation
    # ------------------------------------------------------------------
    def check_exit(
        self,
        row: pd.Series,
        peak_price: float,
        entry_price: float,
        entry_time: Any,
        current_time: Any,
    ) -> bool:
        if row is None:
            return False

        symbol = self._extract_symbol(row)
        current_ts = self._normalise_timestamp(row.get("timestamp_dt") or current_time)
        entry_ts = self._normalise_timestamp(entry_time)
        if current_ts is None or entry_ts is None:
            return False

        # Enforce no same-bar exits: wait for a strictly newer candle.
        if current_ts <= entry_ts:
            return False

        last_recorded_entry = self._last_entry_ts.get(symbol)
        if last_recorded_entry is not None and current_ts <= last_recorded_entry:
            return False

        vwap = row.get("VWAP_D")
        macd_hist = row.get("MACDh_12_26_9")
        rsi = row.get("rsi")
        price = row.get("close")
        if vwap is None or macd_hist is None or rsi is None or price is None:
            return False

        if peak_price is None or peak_price <= 0:
            return False

        drawdown = (peak_price - price) / peak_price
        minutes_held = (current_ts - entry_ts).total_seconds() / 60

        exit_signal = (
            price < vwap
            or macd_hist < 0
            or rsi < 50
            or drawdown > 0.03
            or minutes_held > 20
        )

        if exit_signal:
            # Move the timestamp guard forward so repeated evaluations of the
            # same bar do not re-trigger immediately.
            self._last_entry_ts.pop(symbol, None)
        return exit_signal