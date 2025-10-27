#!/usr/bin/env python3
import sys, os
# add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import os
from scanner.premarket_filter import PremarketFetcher  # adjust import to your folder structure
import datetime

# ensure we can import your modules
sys.path.insert(0, os.getcwd())

# CONFIG
api_key = "cvV9m9XNz41uD7SMCLqftmzWKwDCI_9x"  # Replace with your credential management
test_date = "2025-10-20"  # Use a known date you have parquet or expect to fetch for
tickers_to_test = None  # or provide a small list like ["DATS", "APLD"]

# STEP 1: Run Fetcher
fetcher = PremarketFetcher(api_key)
print(f"Running fetch for {test_date}...")

tickers_ready = fetcher.fetch(test_date, live = True, tickers=tickers_to_test)

print("\n✅ Final tickers ready for trading:")
for t in tickers_ready:
    tick_path = fetcher.get_tick_path(t, test_date)
    bar_path = fetcher.get_bar_path(t, test_date)
    print(f"  {t}:")
    print(f"    Ticks: {os.path.exists(tick_path)} - {tick_path}")
    print(f"    1min:  {os.path.exists(bar_path)} - {bar_path}")
