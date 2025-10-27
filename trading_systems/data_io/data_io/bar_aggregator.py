from collections import deque
from datetime import datetime, timedelta
from typing import Any, Deque, Dict, List, Optional

class BarAggregator:
    """Aggregate raw ticks into fixed and sliding bars.

    The aggregator now supports multi-ticker streams.  Each ticker maintains its
    own fixed-bar state and sliding-window buffers, so the caller can pass events
    for different symbols in chronological order without manual bookkeeping.

    For backwards compatibility an ``intervals`` argument is accepted which will
    seed both the fixed and sliding collections with the same values (the old
    helper scripts inside ``aa_tests`` were written against that API).
    """

    def __init__(
        self,
        fixed_intervals: Optional[List[int]] = None,
        sliding_intervals: Optional[List[int]] = None,
        *,
        intervals: Optional[List[int]] = None,
    ) -> None:
        if intervals is not None:
            fixed_intervals = list(intervals)
            sliding_intervals = list(intervals)

        self.fixed_intervals = sorted(fixed_intervals or [])
        self.sliding_intervals = sorted(sliding_intervals or [])

        # state keyed by ticker → interval
        self._fixed_bars: Dict[str, Dict[int, Dict[str, Any]]] = {}
        self._sliding_windows: Dict[str, Dict[int, Deque]] = {}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _ticker_key(self, ticker: Optional[str]) -> str:
        return ticker or "__default__"

    def _ensure_state(self, ticker: Optional[str]) -> None:
        key = self._ticker_key(ticker)
        if key not in self._fixed_bars:
            self._fixed_bars[key] = {}
        if key not in self._sliding_windows:
            self._sliding_windows[key] = {
                iv: deque() for iv in self.sliding_intervals
            }

    def _floor_time(self, ts: datetime, interval: int) -> datetime:
        epoch = int(ts.timestamp())
        start = epoch - (epoch % interval)
        return datetime.fromtimestamp(start, tz=ts.tzinfo)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_tick(self, tick: Dict) -> List[Dict]:
        ticker = tick.get("ticker")
        ts = tick["timestamp"]
        price = tick["price"]
        size = tick["size"]

        self._ensure_state(ticker)
        key = self._ticker_key(ticker)

        out_bars: List[Dict] = []

        # --- 1) fixed bars ---
        for iv in self.fixed_intervals:
            bucket_start = self._floor_time(ts, iv)
            bar = self._fixed_bars[key].get(iv)

            if bar is None or bucket_start > bar["start_time"]:
                if bar is not None:
                    out_bars.append({**bar, "type": "fixed", "ticker": ticker})
                bar = {
                    "interval":  iv,
                    "start_time": bucket_start,
                    "end_time":   bucket_start + timedelta(seconds=iv),
                    "open":      price,
                    "high":      price,
                    "low":       price,
                    "close":     price,
                    "volume":    size,
                }
                self._fixed_bars[key][iv] = bar
            else:
                # update existing
                bar["high"] = max(bar["high"], price)
                bar["low"] = min(bar["low"], price)
                bar["close"] = price
                bar["volume"] += size

        # --- 2) sliding bars ---
        for iv, window in self._sliding_windows[key].items():
            window.append((ts, price, size))
            cutoff = ts - timedelta(seconds=iv)
            while window and window[0][0] < cutoff:
                window.popleft()
            prices = [p for (_, p, _) in window]
            if not prices:
                continue

            volume = sum(s for (_, _, s) in window)
            out_bars.append(
                {
                    "type": "sliding",
                    "ticker": ticker,
                    "interval": iv,
                    "start_time": cutoff,
                    "end_time": ts,
                    "open": prices[0],
                    "high": max(prices),
                    "low": min(prices),
                    "close": prices[-1],
                    "volume": volume,
                }
            )

        return out_bars