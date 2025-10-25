# tests/test_aggregator_with_parquet.py
import sys, os
# add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pandas as pd
import pytz
import pytest
from data_io.bar_aggregator import BarAggregator

TICKER    = "APLD"
DATE      = "2025-01-03"
PARQUET   = f"data/ticks/{DATE}/{TICKER}_ticks.parquet"

@pytest.fixture
def tick_df():
    # load your saved tick parquet, indexed by timestamp
    df = pd.read_parquet(PARQUET)
    # make sure the index is tz-aware UTC
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    return df

def test_aggregator_parquet(tick_df):
    # build aggregator for 1m and 5m fixed + sliding
    agg = BarAggregator(fixed_intervals=[60,300], sliding_intervals=[60,300])

    fixed_bars = []
    sliding_bars = []
    for ts, row in tick_df.iterrows():
        tick = {
            "timestamp": ts,
            "price":     float(row["price"]),   # adjust column name if needed
            "size":      int(row.get("size", 1))
        }
        bars = agg.add_tick(tick)
        for b in bars:
            if b["type"] == "fixed":
                fixed_bars.append(b)
            else:
                sliding_bars.append(b)

    # basic smoke assertions:
    #  - we should have exactly 390 one-minute fixed bars in a full session
    num_1m_fixed = sum(1 for b in fixed_bars if b["interval"] == 60)
    assert num_1m_fixed > 0, "No 1-minute bars produced"
    #  - sliding bars should be one per tick for each sliding interval
    assert len(sliding_bars) == len(tick_df) * 2

    # (Optionally) inspect the first few bars
    print("First 3 fixed 1m bars:", fixed_bars[:3])
    print("First 3 sliding 5m bars:", [b for b in sliding_bars if b["interval"]==300][:3])

if __name__ == "__main__":
    # Demo mode: load the parquet and print the first few bars
    df = pd.read_parquet(PARQUET)
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")

    agg = BarAggregator(fixed_intervals=[60,300], sliding_intervals=[60,300])
    print(f"\n▶︎ DEMO: Running BarAggregator on first 10 ticks from {PARQUET}\n")

    for i, (ts, row) in enumerate(df.iterrows()):
        tick = {
            "timestamp": ts,
            "price":     float(row["price"]),
            "size":      int(row.get("size", 1))
        }
        bars = agg.add_tick(tick)
        if bars:
            print(f"Tick {i} @ {ts.time()}:")
            for b in bars:
                print(" ", b)
            print()
        if i >= 9:
            break
    print("▶︎ DEMO complete.\n")