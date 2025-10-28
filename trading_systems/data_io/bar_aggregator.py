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
        sliding_window_bars: int = 15,
    ) -> None:
        if intervals is not None:
            fixed_intervals = list(intervals)
            sliding_intervals = list(intervals)

        self.fixed_intervals = sorted(fixed_intervals or [])
        self.sliding_intervals = sorted(sliding_intervals or [])
        self.sliding_window_bars = max(1, int(sliding_window_bars))

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
            retention = ts - timedelta(seconds=iv * self.sliding_window_bars)
            while window and window[0][0] < retention:
                window.popleft()

            ticks = list(window)
            if not ticks:
                continue

            interval_delta = timedelta(seconds=iv)
            bars_for_interval: List[Dict[str, Any]] = []
            end_time = ts

            for seq in range(self.sliding_window_bars):
                start_time = end_time - interval_delta
                segment = [
                    entry for entry in ticks if start_time < entry[0] <= end_time
                ]
                if not segment:
                    if seq == 0:
                        bars_for_interval = []
                    break

                open_price = segment[0][1]
                high_price = max(val[1] for val in segment)
                low_price = min(val[1] for val in segment)
                close_price = segment[-1][1]
                volume = sum(val[2] for val in segment)

                bars_for_interval.append(
                    {
                        "interval": iv,
                        "start_time": start_time,
                        "end_time": end_time,
                        "open": open_price,
                        "high": high_price,
                        "low": low_price,
                        "close": close_price,
                        "volume": volume,
                    }
                )

                end_time = start_time

            if not bars_for_interval:
                continue

            latest_bar = bars_for_interval[0]
            out_bars.append({**latest_bar, "type": "sliding", "ticker": ticker})

            snapshot = {
                "type": "sliding_snapshot",
                "ticker": ticker,
                "interval": iv,
                "as_of": ts,
                "bars": [
                    {**bar, "ticker": ticker}
                    for bar in reversed(bars_for_interval)
                ],
            }
            out_bars.append(snapshot)

        return out_bars