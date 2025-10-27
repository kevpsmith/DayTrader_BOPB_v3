import sys, os
# add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import asyncio
import pytest
import json
from datetime import datetime, timedelta
import pytz
import websockets

from time_utils.clock import Clock
from data_io.tick_downloader import WebSocketTickDownloader, HistoricalTickDownloader

# Dummy WebSocket for testing
class DummyWebSocket:
    def __init__(self, messages):
        self._messages = messages[:]
        self.sent = []

    async def send(self, msg):
        self.sent.append(msg)

    async def recv(self):
        return self._messages.pop(0)

    async def close(self):
        pass

@pytest.fixture
def fake_clock():
    # Start at 2025-01-01 09:30 UTC in replay mode
    start = pytz.UTC.localize(datetime(2025, 1, 1, 9, 30))
    return Clock(mode='replay', start_time=start)

@pytest.mark.asyncio
async def test_websocket_tick_downloader(monkeypatch, fake_clock):
    # Prepare a single tick message
    msg = [{
        "sym": "AAPL",
        "p": 150.0,
        "sz": 10,
        # timestamp in ms
        "t": int(datetime(2025,1,1,9,40,0, tzinfo=pytz.UTC).timestamp() * 1000)
    }]
    raw = json.dumps(msg)
    dummy_ws = DummyWebSocket([raw])

    # Monkeypatch websockets.connect to return our dummy
    async def fake_connect(url):
        return dummy_ws
    monkeypatch.setattr(websockets, 'connect', fake_connect)

    downloader = WebSocketTickDownloader(
        api_key="TESTKEY",
        symbols=["AAPL"],
        clock=fake_clock
    )
    await downloader.connect()

    # Verify auth & subscribe messages were sent
    assert dummy_ws.sent[0] == json.dumps({"action": "auth", "params": "TESTKEY"})
    assert dummy_ws.sent[1] == json.dumps({"action": "subscribe", "params": "AAPL"})

    tick = await downloader.get_next_tick()
    assert tick["symbol"] == "AAPL"
    assert tick["price"] == 150.0
    assert tick["size"] == 10
    expected_ts = pytz.UTC.localize(datetime(2025,1,1,9,40,0))
    assert tick["timestamp"] == expected_ts

@pytest.mark.asyncio
async def test_historical_tick_downloader(monkeypatch, fake_clock):
    # Dummy trade and response
    class DummyTrade:
        def __init__(self, price, size, iso):
            self.price = price
            self.size = size
            self.network_timestamp = iso

    class DummyResp:
        def __init__(self):
            self.results = [
                DummyTrade(100.0, 5, "2025-01-01T09:31:00Z"),
                DummyTrade(101.0, 6, "2025-01-01T09:32:30Z")
            ]
            self.next_url = None

    # Monkeypatch the client.get_trades method
    monkeypatch.setattr(
        HistoricalTickDownloader,
        "__init__",
        lambda self, api_key, symbol, date, clock, limit=50000: (
            setattr(self, "client", type("C", (), {"get_trades": lambda *args, **kwargs: DummyResp()})),
            setattr(self, "symbol", symbol),
            setattr(self, "date", date),
            setattr(self, "clock", clock),
            setattr(self, "limit", limit),
            setattr(self, "_results", []),
            setattr(self, "_cursor", None),
            setattr(self, "_idx", 0),
            setattr(self, "_prev_ts", None),
        )
    )

    downloader = HistoricalTickDownloader(
        api_key="TESTKEY",
        symbol="AAPL",
        date="2025-01-01",
        clock=fake_clock
    )

    # First tick
    tick1 = await downloader.get_next_tick()
    assert tick1["symbol"] == "AAPL"
    assert tick1["price"] == 100.0
    assert tick1["size"] == 5
    assert tick1["timestamp"] == pytz.UTC.localize(datetime(2025,1,1,9,31,0))

    # Second tick
    tick2 = await downloader.get_next_tick()
    assert tick2["symbol"] == "AAPL"
    assert tick2["price"] == 101.0
    assert tick2["size"] == 6
    assert tick2["timestamp"] == pytz.UTC.localize(datetime(2025,1,1,9,32,30))

    # Clock should have advanced to the last timestamp
    assert fake_clock.now_utc() == pytz.UTC.localize(datetime(2025,1,1,9,32,30))

    # No more data: should raise StopAsyncIteration
    with pytest.raises(StopAsyncIteration):
        await downloader.get_next_tick()
