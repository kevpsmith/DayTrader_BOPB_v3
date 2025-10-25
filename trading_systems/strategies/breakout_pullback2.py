from datetime import time

class BreakoutPullbackStrategy2:
    def __init__(self):
        self.breakout_level = None
        self.entry_window_start = time(9, 45)
        self.entry_window_end = time(11, 0)

    def check_preconditions(self, df_5m):
        if len(df_5m) < 15:
            return False

        recent_highs = df_5m['high'].rolling(window=10).max()
        last_close = df_5m.iloc[-1]['close']
        prev_high = recent_highs.iloc[-2]

        if last_close > prev_high * 1.02:
            self.breakout_level = prev_high
            return True
        return False

    def check_entry(self, df_1m, df_30s, i_1m):
        if i_1m < 15 or self.breakout_level is None:
            return False

        curr_1m = df_1m.iloc[i_1m]
        prev_1m = df_1m.iloc[i_1m - 1]
        curr_time = curr_1m['timestamp_dt']

        if not (self.entry_window_start <= curr_time.time() <= self.entry_window_end):
            return False

        # Near breakout level or VWAP
        near_support = (
            abs(curr_1m['close'] - self.breakout_level) / self.breakout_level < 0.01 or
            abs(curr_1m['close'] - curr_1m.get('VWAP_D', self.breakout_level)) / curr_1m.get('VWAP_D', self.breakout_level) < 0.01
        )

        # Reclaim signs in 1-min
        reclaiming = (
            curr_1m['close'] > prev_1m['close'] and
            curr_1m.get('rsi', 0) > prev_1m.get('rsi', 0) and
            curr_1m.get('MACDh_12_26_9', 0) > 0
        )

        # Confirmation in 30s bars
        recent_30s = df_30s[df_30s['timestamp_dt'] <= curr_time].tail(3)
        if len(recent_30s) < 3:
            return False

        confirm_30s = (
            recent_30s.iloc[-1]['close'] > recent_30s.iloc[-2]['close'] and
            recent_30s.iloc[-1]['volume'] > 1.5 * recent_30s.iloc[:-1]['volume'].mean() and
            recent_30s.iloc[-1].get('rsi', 0) > recent_30s.iloc[-2].get('rsi', 0) and
            recent_30s.iloc[-1].get('MACDh_12_26_9', 0) > 0
        )

        return near_support and reclaiming and confirm_30s

    def check_exit(self, row, peak_price, entry_price, entry_time, current_time):
        drawdown = (peak_price - row['close']) / peak_price
        minutes_held = (current_time - entry_time).total_seconds() / 60

        return (
            row['close'] < row.get('VWAP_D', row['close']) or
            row.get('MACDh_12_26_9', 0) < 0 or
            row.get('rsi', 0) < 50 or
            drawdown > 0.03 or
            minutes_held > 20
        )