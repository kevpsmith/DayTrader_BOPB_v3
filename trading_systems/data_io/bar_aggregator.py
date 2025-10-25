from collections import deque
from datetime import datetime, timedelta
from typing import List, Dict, Deque, Any

class BarAggregator:
    """
    Aggregates a stream of ticks into:
      - fixed bars (aligned to interval boundaries)
      - sliding bars (rolling window ending at each tick)

    Usage:
        agg = BarAggregator(
            fixed_intervals=[60, 300],    # 1-m and 5-m fixed
            sliding_intervals=[60, 300],  # 1-m and 5-m rolling
        )

        for tick in tick_stream:
            bars = agg.add_tick(tick)
            for bar in bars:
                # bar = {
                #   "type": "fixed" | "sliding",
                #   "interval": 60,
                #   "start_time": datetime,
                #   "end_time": datetime,
                #   "open": float, "high": float, "low": float, "close": float,
                #   "volume": int
                # }
                print(bar)
    """

    def __init__(
        self,
        fixed_intervals: List[int]    = None,
        sliding_intervals: List[int]  = None,
    ):
        self.fixed_intervals   = sorted(fixed_intervals or [])
        self.sliding_intervals = sorted(sliding_intervals or [])

        # state for fixed bars: interval → current bar object
        self._fixed_bars: Dict[int, Any] = {}

        # state for sliding bars: interval → deque[(ts, price, size)]
        self._sliding_windows: Dict[int, Deque] = {
            iv: deque() for iv in self.sliding_intervals
        }

    def _floor_time(self, ts: datetime, interval: int) -> datetime:
        epoch = int(ts.timestamp())
        start = epoch - (epoch % interval)
        return datetime.fromtimestamp(start, tz=ts.tzinfo)

    def add_tick(self, tick: Dict) -> List[Dict]:
        ts    = tick["timestamp"]
        price = tick["price"]
        size  = tick["size"]

        out_bars: List[Dict] = []

        # --- 1) fixed bars ---
        for iv in self.fixed_intervals:
            bucket_start = self._floor_time(ts, iv)
            bar = self._fixed_bars.get(iv)

            if bar is None or bucket_start > bar["start_time"]:
                # close previous
                if bar is not None:
                    out_bars.append({**bar, "type": "fixed"})
                # start new
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
                self._fixed_bars[iv] = bar
            else:
                # update existing
                bar["high"]   = max(bar["high"], price)
                bar["low"]    = min(bar["low"], price)
                bar["close"]  = price
                bar["volume"] += size

        # --- 2) sliding bars ---
        for iv, window in self._sliding_windows.items():
            window.append((ts, price, size))
            cutoff = ts - timedelta(seconds=iv)
            # evict old ticks
            while window and window[0][0] < cutoff:
                window.popleft()

            # build bar from window
            prices = [p for (_, p, _) in window]
            volume = sum(s for (_, _, s) in window)
            if not prices:
                continue

            out_bars.append({
                "type":      "sliding",
                "interval":  iv,
                "start_time": cutoff,
                "end_time":   ts,
                "open":      prices[0],
                "high":      max(prices),
                "low":       min(prices),
                "close":     prices[-1],
                "volume":    volume,
            })

        return out_bars