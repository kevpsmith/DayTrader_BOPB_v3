#!/usr/bin/env python3
import sys, os
# add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import os
import sys
from datetime import datetime
import pytz
import pandas as pd

# ensure project root is on PYTHONPATH
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir))
sys.path.insert(0, PROJECT_ROOT)

from polygon.rest import RESTClient
from data_io.warmup_loader import ParquetWarmupLoader

API_KEY   = "cvV9m9XNz41uD7SMCLqftmzWKwDCI_9x"  # your key
BASE_PATH = os.path.join(PROJECT_ROOT, "..", "data", "bars", "1m")
BASE_PATH = os.path.abspath(BASE_PATH)  # normalize the '..' to a full absolute path
TICKERS = ["APLD", "DATS", "RR"]
DATES   = ["2025-01-03", "2025-01-07", "2025-01-10"]
WARMUP_LOOKBACK = 14  # minutes to verify

def save_minute_bars(symbol: str, date: str):
    client = RESTClient(API_KEY)
    print(f"\nFetching 1m bars for {symbol} on {date}…")
    aggs = client.get_aggs(
        symbol,
        1, "minute",
        date, date,
        limit=5000
    )
    if not aggs:
        print(f"  → No bars returned for {symbol} on {date}")
        return False

    df = pd.DataFrame([a.__dict__ for a in aggs])
    df['timestamp_dt'] = (
        pd.to_datetime(df['timestamp'], unit='ms')
          .dt.tz_localize('UTC')
    )
    df = df.set_index('timestamp_dt').sort_index()
    df = df[['open','high','low','close','volume']]

    out_dir  = os.path.join(BASE_PATH, date)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{symbol}.parquet")
    df.to_parquet(out_path)
    print(f"  → Saved {len(df)} bars to {out_path}")
    return True

def verify_parquet(symbol: str, date: str):
    loader   = ParquetWarmupLoader(base_path=BASE_PATH)
    # end_time = 9:30 Eastern converted to UTC
    eastern  = pytz.timezone("US/Eastern")
    end_east = eastern.localize(datetime.fromisoformat(f"{date}T09:30:00"))
    end_utc  = end_east.astimezone(pytz.UTC)

    bars = loader.load_minute_bars(symbol, end_utc, lookback_minutes=WARMUP_LOOKBACK)
    print(f"  → Loaded {len(bars)} warmup bars for {symbol} ending at {end_utc.time()} UTC")
    if bars:
        print(f"    first: {bars[0]['start_time'].time()}  last: {bars[-1]['start_time'].time()}")

def main():
    for date in DATES:
        for symbol in TICKERS:
            ok = save_minute_bars(symbol, date)
            if ok:
                verify_parquet(symbol, date)

if __name__ == "__main__":
    main()
