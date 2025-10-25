import sys, os
# add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from datetime import datetime, timedelta
import pytz
from time_utils.clock import Clock

def demo_replay():
    start = pytz.UTC.localize(datetime(2025, 7, 13, 12, 0, 0))
    clk = Clock(mode='replay', start_time=start)
    print("▶︎ Replay start:", clk.now_utc())
    clk.sleep(timedelta(seconds=5))
    print("▶︎ Replay +5s →", clk.now_utc())

def demo_live():
    clk = Clock(mode='live')
    t0 = clk.now_utc()
    print("▶︎ Live  start:", t0)
    clk.sleep(timedelta(seconds=2))
    t1 = clk.now_utc()
    print("▶︎ Live +2s  →", t1)
    print("   (elapsed:", (t1 - t0).total_seconds(), "s)")

if __name__ == "__main__":
    print("=== REPLAY MODE ===")
    demo_replay()
    print("\n=== LIVE MODE ===")
    demo_live()