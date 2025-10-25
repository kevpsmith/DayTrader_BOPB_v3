from datetime import time
from zoneinfo import ZoneInfo   # Python ≥3.9

class BreakoutPullbackStrategy:
    """
    Breakout-pullback strategy that stores all data in UTC but
    evaluates the time window in America/New_York, automatically
    handling DST changes.
    """

    def __init__(self, local_tz: str = "America/New_York"):
        self.tz = ZoneInfo(local_tz)          # handles EST / EDT for you
        self.breakout_level = None

        # Entry window expressed in local NY time
        self.entry_window_start = time(9, 45, tzinfo=self.tz)
        self.entry_window_end   = time(11, 0, tzinfo=self.tz)

    # ------------------------------------------------------------
    # 1.  Pre-conditions on 5-minute chart
    # ------------------------------------------------------------
    def check_preconditions(self, df_5m):
        if len(df_5m) < 10:
            return False

        recent_highs = df_5m["high"].rolling(window=10).max()
        last_close   = df_5m.iloc[-1]["close"]
        prev_high    = recent_highs.iloc[-2]

        if last_close > prev_high * 1.02:
            self.breakout_level = prev_high
            return True
        return False

    # ------------------------------------------------------------
    # 2.  Entry signal on 1-minute (optionally 30-sec) chart
    # ------------------------------------------------------------
    def check_entry(self, df_1m, df_30s=None, i: int = None):
        if i is None:
            i = len(df_1m) - 1        # fallback if index not supplied

        if i < 5 or self.breakout_level is None:
            return False

        curr = df_1m.iloc[i]
        prev = df_1m.iloc[i - 1]
        curr_ts_utc = curr["timestamp_dt"]          # aware UTC Timestamp

        # Convert to NY time for window comparison
        curr_ts_ny = curr_ts_utc.astimezone(self.tz).timetz()

        if not (self.entry_window_start <= curr_ts_ny <= self.entry_window_end):
            return False

        # -------- Pullback-and-reclaim logic --------
        near_support = (
            abs(curr["close"] - self.breakout_level) / self.breakout_level < 0.01
            or abs(curr["close"] - curr.get("VWAP_D", self.breakout_level))
               / curr.get("VWAP_D", self.breakout_level) < 0.01
        )

        reclaim = (
            curr["close"] > prev["close"]
            and curr.get("MACDh_12_26_9", 0) > 0
            and curr.get("rsi", 0) > 50
        )

        return near_support and reclaim

    # ------------------------------------------------------------
    # 3.  Exit signal
    # ------------------------------------------------------------
    def check_exit(self, row, peak_price, entry_price, entry_time, current_time):
        drawdown     = (peak_price - row["close"]) / peak_price
        minutes_held = (current_time - entry_time).total_seconds() / 60

        return (
            row["close"] < row.get("VWAP_D", row["close"])
            or row.get("MACDh_12_26_9", 0) < 0
            or row.get("rsi", 0) < 50
            or drawdown > 0.03
            or minutes_held > 15
        )