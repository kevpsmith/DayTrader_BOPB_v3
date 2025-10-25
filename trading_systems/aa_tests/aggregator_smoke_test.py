import sys, os
# add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from data_io.bar_aggregator import BarAggregator
from datetime import datetime, timedelta
import pytz

agg = BarAggregator(fixed_intervals=[60,300], sliding_intervals=[60,300])
now = pytz.UTC.localize(datetime(2025,1,1,9,30,0))
for sec in [0, 30, 60, 90, 300]:
    tick = {"timestamp": now + timedelta(seconds=sec), "price": 100+sec, "size": 1}
    bars = agg.add_tick(tick)
    print(sec, bars)