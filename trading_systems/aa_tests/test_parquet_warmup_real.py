#!/usr/bin/env python3
import sys, os
# add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import sys, os
from datetime import datetime, timedelta
import pytz
import pandas as pd

# ensure we can import your modules
sys.path.insert(0, os.getcwd())

from data_io.warmup_loader import ParquetWarmupLoader

def main():
    symbol = "APLD"
    date   = "2025-01-03"
    # end_time = 9:30 Eastern converted to UTC
    eastern   = pytz.timezone("US/Eastern")
    end_east  = eastern.localize(datetime.fromisoformat(f"{date}T09:30:00"))
    end_utc   = end_east.astimezone(pytz.UTC)

    loader = ParquetWarmupLoader()
    bars = loader.load_minute_bars(symbol, end_utc, lookback_minutes=14)

    print(f"\nLoaded {len(bars)} minute bars for {symbol} ending at {end_utc}")
    for b in bars[:5]:
        print(f" • {b['start_time']} → O:{b['open']} H:{b['high']} L:{b['low']} C:{b['close']} V:{b['volume']}")

    # spot-check last bar is exactly the 09:29→09:30 bar
    last = bars[-1]
    print("\nLast bar:", last['start_time'], "→", last['end_time'])

if __name__ == "__main__":
    main()