# indicators/indicator_engine.py
import sys, os
# add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from collections import deque
from typing import List, Dict, Any


class EMA:
    """
    Exponential Moving Average.
    """
    def __init__(self, period: int):
        self.period = period
        self.multiplier = 2 / (period + 1)
        self.value = None

    def update(self, price: float) -> float:
        """
        Feed in a new price; returns the updated EMA once enough data has arrived.
        """
        if self.value is None:
            # bootstrap with first price
            self.value = price
        else:
            self.value = (price - self.value) * self.multiplier + self.value
        return self.value


class RSI:
    """
    Relative Strength Index.
    """
    def __init__(self, period: int = 14):
        self.period = period
        self.gains = deque(maxlen=period)
        self.losses = deque(maxlen=period)
        self.prev_close = None

    def update(self, close: float) -> float:
        """
        Feed in a new close price; returns RSI once you have `period` closes.
        """
        if self.prev_close is not None:
            change = close - self.prev_close
            self.gains.append(max(change, 0))
            self.losses.append(max(-change, 0))

        self.prev_close = close

        if len(self.gains) < self.period:
            return None  # not enough data yet

        avg_gain = sum(self.gains) / self.period
        avg_loss = sum(self.losses) / self.period
        if avg_loss == 0:
            return 100.0  # no down moves
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))


class MACD:
    """
    MACD line and signal line.
    """
    def __init__(self, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9):
        self.ema_fast = EMA(fast_period)
        self.ema_slow = EMA(slow_period)
        self.signal_ema = EMA(signal_period)
        self.macd = None
        self.signal = None

    def update(self, price: float) -> Dict[str, float]:
        """
        Feed in a new price; returns dict with 'macd' and 'signal' once available.
        """
        fast = self.ema_fast.update(price)
        slow = self.ema_slow.update(price)
        self.macd = fast - slow

        # signal line only after macd has stabilized
        self.signal = self.signal_ema.update(self.macd)
        return {"macd": self.macd, "macd_signal": self.signal}


class VWAP:
    """
    Cumulative VWAP since start of session (or since last reset).
    """
    def __init__(self):
        self._cum_vp = 0.0
        self._cum_vol = 0

    def update(self, typical_price: float, volume: int) -> float:
        self._cum_vp  += typical_price * volume
        self._cum_vol += volume
        return self._cum_vp / self._cum_vol if self._cum_vol else None

class ATR:
    """
    Average True Range over a fixed period.
    """
    def __init__(self, period: int = 14):
        self.period     = period
        self.tr_window  = deque(maxlen=period)
        self.prev_close = None

    def update(self, high: float, low: float, close: float) -> float:
        if self.prev_close is None:
            tr = high - low
        else:
            tr = max(
                high - low,
                abs(high - self.prev_close),
                abs(self.prev_close - low),
            )
        self.tr_window.append(tr)
        self.prev_close = close
        if len(self.tr_window) < self.period:
            return None
        return sum(self.tr_window) / self.period

class RollingHigh:
    """
    Simple rolling maximum over the last N closes.
    """
    def __init__(self, period: int):
        self.period = period
        self.window = deque(maxlen=period)

    def update(self, price: float) -> float:
        self.window.append(price)
        return max(self.window)

class RollingLow:
    """
    Simple rolling minimum over the last N closes.
    """
    def __init__(self, period: int):
        self.period = period
        self.window = deque(maxlen=period)

    def update(self, price: float) -> float:
        self.window.append(price)
        return min(self.window)


class IndicatorEngine:
    """
    Maintains all of the above per sliding‐bar interval.
    """

    def __init__(
        self,
        intervals: List[int],
        rsi_period:     int = 14,
        macd_fast:      int = 12,
        macd_slow:      int = 26,
        macd_signal:    int = 9,
        atr_period:     int = 14,
        roll_period:    int = 20,   # for your breakout lookback
    ):
        self.intervals = intervals

        # momentum/trend
        self._rsi_map   = {iv: RSI(rsi_period) for iv in intervals}
        self._macd_map  = {iv: MACD(macd_fast, macd_slow, macd_signal) for iv in intervals}

        # volatility
        self._atr_map   = {iv: ATR(atr_period) for iv in intervals}

        # vwap (session VWAP, so only one per day per instrument)
        self._vwap_map  = {iv: VWAP() for iv in intervals}

        # breakout reference levels
        self._high_map  = {iv: RollingHigh(roll_period) for iv in intervals}
        self._low_map   = {iv: RollingLow(roll_period)  for iv in intervals}


    def on_sliding_bar(self, bar: Dict[str, Any]) -> Dict[str, Any]:
        """
        Feed in each sliding‐bar.  Returns a flat dict of all indicators for that interval.
        """
        iv    = bar["interval"]
        close = bar["close"]
        high  = bar["high"]
        low   = bar["low"]
        vol   = bar["volume"]
        tp    = (high + low + close) / 3.0

        # momentum
        rsi   = self._rsi_map[iv].update(close)
        macd  = self._macd_map[iv].update(close)

        # volatility & breakout levels
        atr   = self._atr_map[iv].update(high, low, close)
        hh    = self._high_map[iv].update(close)
        ll    = self._low_map[iv].update(close)

        # volume‐weighted price
        vwap  = self._vwap_map[iv].update(tp, vol)

        hist = macd["macd"] - macd["macd_signal"]
        return {
            "interval":   iv,
            "rsi":        rsi,
            "macd":       macd["macd"],
            "macd_signal":macd["macd_signal"],
            "macd_hist":  hist,
            "atr":        atr,
            "vwap":       vwap,
            "roll_high":  hh,
            "roll_low":   ll,
        }