#!/usr/bin/env python3
import sys, os

from pyarrow import transcoding_input_stream
# add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from datetime import datetime, timedelta
import pytz
import pandas as pd
from pathlib import Path

# make sure we can import your modules
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir))
sys.path.insert(0, PROJECT_ROOT)

from data_io.warmup_loader import ParquetWarmupLoader
from indicators.indicator_engine import IndicatorEngine

def main(symbol: str, date: str):
    # 1) Compute end‐of‐warmup in UTC (9:30 ET → 13:30 UTC)
    eastern  = pytz.timezone("US/Eastern")
    end_east = eastern.localize(datetime.fromisoformat(f"{date}T09:30:00"))
    end_utc  = end_east.astimezone(pytz.UTC)

    # 2) Load & seed warmup bars
    loader = ParquetWarmupLoader(base_path="/teamspace/studios/this_studio/data/bars/1m")
    warmup_bars = loader.load_minute_bars(symbol, end_utc, lookback_minutes=14)
    print(f"\nSeeded {len(warmup_bars)} warmup bars (up to {end_utc.time()} UTC)")

    engine = IndicatorEngine(intervals=[60, 300])

    # Feed warmup bars into the engine
    for bar in warmup_bars:
        bar_dict = {
            "interval":   60,
            "start_time": bar["start_time"],
            "end_time":   bar["end_time"],
            "open":       bar["open"],
            "high":       bar["high"],
            "low":        bar["low"],
            "close":      bar["close"],
            "volume":     bar["volume"],
        }
        engine.on_sliding_bar(bar_dict)

    # 3) Now iterate through the rest of the minute bars as sliding bars
    trading_root = Path(__file__).resolve().parents[2]
    #studio_root = trading_root.parent
    parquet_path = trading_root / "data" / "bars" / "1m" / date / f"{symbol}.parquet"
    df = pd.read_parquet(parquet_path)
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")

    print("\n▶︎ First 20 indicator outputs after warmup:\n")
    print(" Time    │   RSI   │  MACD    │ SIG LINE │ HISTOGRAM │   ATR    │   VWAP   ")
    print("─"*79)

    count = 0
    for ts, row in df.iterrows():
        # only process bars *after* the warmup end
        if ts <= end_utc:
            continue

        bar = {
            "interval":   60,
            "start_time": ts,
            "end_time":   ts + timedelta(minutes=1),
            "open":       row["open"],
            "high":       row["high"],
            "low":        row["low"],
            "close":      row["close"],
            "volume":     int(row["volume"]),
        }
        inds = engine.on_sliding_bar(bar)
        # print the row
        print(f" {ts.strftime('%H:%M')}   │"
              f" {inds['rsi'] or 0:7.2f} │"
              f" {inds['macd'] or 0:8.4f} │"
              f" {inds['macd_signal'] or 0:8.4f} │"
              f" {inds['macd_hist'] or 0:8.4f} │"
              f" {inds['atr'] or 0:7.4f} │"
              f" {inds['vwap'] or 0:7.2f}"
        )
        count += 1
        if count >= 20:
            break

if __name__ == "__main__":
    import sys

    # strip off the script name
    args = sys.argv[1:]

    if len(args) == 2:
        symbol, date = args
    else:
        # ← DEFAULTS for quick debug
        symbol, date = "APLD", "2025-01-03"
        print(f"[WARN] No args provided; defaulting to {symbol} {date}")

    main(symbol, date)