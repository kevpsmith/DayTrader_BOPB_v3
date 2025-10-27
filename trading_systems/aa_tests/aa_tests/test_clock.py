# tests/test_clock.py
import sys, os
# add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pytest
from datetime import datetime, timedelta
import pytz
import time as real_time
from time_utils.clock import Clock


UTC = pytz.UTC
NY = pytz.timezone('America/New_York')


def test_replay_initial_now():
    start = UTC.localize(datetime(2025, 7, 13, 12, 0, 0))
    clk = Clock(mode='replay', start_time=start)
    # now_utc should equal exactly the start time
    assert clk.now_utc() == start
    # now_ny should be the same instant in NY tz
    expected_ny = start.astimezone(NY)
    assert clk.now_ny() == expected_ny


def test_replay_sleep_advances_time():
    start = UTC.localize(datetime(2025, 7, 13, 12, 0, 0))
    clk = Clock(mode='replay', start_time=start)
    clk.sleep(timedelta(minutes=5, seconds=30))
    # internal clock should have advanced by exactly 5m30s
    assert clk.now_utc() == start + timedelta(minutes=5, seconds=30)


def test_replay_sleep_negative_raises():
    clk = Clock(mode='replay', start_time=UTC.localize(datetime(2025, 7, 13, 12, 0, 0)))
    with pytest.raises(ValueError):
        clk.sleep(timedelta(seconds=-1))


def test_live_sleep_calls_time_sleep(monkeypatch):
    clk = Clock(mode='live')
    # capture arguments passed to real time.sleep
    called = []
    monkeypatch.setattr(real_time, 'sleep', lambda seconds: called.append(seconds))

    dt = timedelta(seconds=1.23)
    clk.sleep(dt)
    assert called == [1.23]


def test_live_now_utc(monkeypatch):
    clk = Clock(mode='live')
    fake = UTC.localize(datetime(2025, 7, 13, 14, 0, 0))
    # patch datetime.now inside our clock module
    import time.clock as clk_mod
    monkeypatch.setattr(clk_mod.datetime, 'now', lambda tz=None: fake)

    assert clk.now_utc() == fake


def test_live_now_ny(monkeypatch):
    clk = Clock(mode='live')
    # choose a fake UTC time, and compute corresponding NY
    fake_utc = UTC.localize(datetime(2025, 7, 13, 16, 0, 0))
    expected_ny = fake_utc.astimezone(NY)

    import time.clock as clk_mod
    monkeypatch.setattr(clk_mod.datetime, 'now', lambda tz=None: fake_utc)

    assert clk.now_ny() == expected_ny