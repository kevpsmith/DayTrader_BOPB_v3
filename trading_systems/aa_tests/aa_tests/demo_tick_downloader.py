# demo_tick_download.py
import sys, os
# add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import asyncio
from datetime import datetime
import pytz
from time_utils.clock import Clock
from data_io.tick_downloader import create_tick_source

async def demo_replay():
    start = pytz.UTC.localize(datetime(2025,1,1,9,30))
    clk   = Clock(mode='replay', start_time=start)
    src   = create_tick_source(
        mode='replay',
        api_key="cvV9m9XNz41uD7SMCLqftmzWKwDCI_9x",
        symbol="AAPL",
        date="2025-01-03",
        clock=clk,
    )
    await src.connect()
    for _ in range(3):
        tick = await src.get_next_tick()
        print("[replay tick]", tick)
        print("   → clock is now:", clk.now_utc())
    await src.close()

async def demo_live():
    clk   = Clock(mode='live')
    src   = create_tick_source(
        mode='live',
        api_key="cvV9m9XNz41uD7SMCLqftmzWKwDCI_9x",
        symbols=["AAPL"],
        clock=clk,
    )
    await src.connect()
    for _ in range(3):
        tick = await src.get_next_tick()
        print("[ live tick ]", tick)
    await src.close()

if __name__ == "__main__":
    asyncio.run(demo_replay())
    # asyncio.run(demo_live())   ← uncomment to try live WS
