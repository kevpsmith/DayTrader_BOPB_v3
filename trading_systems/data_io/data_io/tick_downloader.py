import sys, os
# add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import json
from typing import Any, Dict, List, Optional
import pytz
from datetime import datetime, timedelta
import websockets
from polygon import StocksClient
from time_utils.clock import Clock


class TickSource:
    """
    Abstract base for tick sources (live WS, HTTP poller, parquet replay).
    """
    async def connect(self) -> None:
        raise NotImplementedError

    async def get_next_tick(self) -> Dict[str, Any]:
        """
        Return a dict with:
          - 'symbol': str
          - 'price': float
          - 'size': int
          - 'timestamp': datetime (tz-aware UTC)
        """
        raise NotImplementedError

    async def close(self) -> None:
        raise NotImplementedError


class WebSocketTickDownloader(TickSource):
    """
    Live tick downloader using Polygon.io WebSocket API.
    """
    def __init__(
        self,
        api_key: str,
        symbols: List[str],
        clock: Clock,
        url: str = "wss://socket.polygon.io/stocks",
    ):
        self.api_key = api_key
        self.symbols = symbols
        self.clock = clock
        self.url = url
        self.ws: Optional[websockets.WebSocketClientProtocol] = None

    def set(self, new_time: datetime):
        if self.mode != 'replay':
            raise RuntimeError("Can only set time in replay mode")
        if new_time.tzinfo is None:
            raise ValueError("new_time must be timezone-aware")
        # never go backwards
        if new_time < self._current_time:
            return
        self._current_time = new_time

    async def connect(self) -> None:
        self.ws = await websockets.connect(self.url)
        # Authenticate
        await self.ws.send(json.dumps({"action": "auth", "params": self.api_key}))
        # Subscribe to symbols
        params = ",".join(self.symbols)
        await self.ws.send(json.dumps({"action": "subscribe", "params": params}))

    async def get_next_tick(self) -> Dict[str, Any]:
        raw = await self.ws.recv()
        messages = json.loads(raw)
        msg = messages[0]
        ts = datetime.fromtimestamp((msg.get('sip_ts', msg.get('t')) or 0) / 1000, tz=pytz.UTC)
        tick = {
            'symbol': msg['sym'],
            'price': msg['p'],
            'size': msg.get('sz', 0),
            'timestamp': ts,
        }
        return tick

    async def close(self) -> None:
        if self.ws:
            await self.ws.close()


class HistoricalTickDownloader:
    def __init__(self, api_key: str, symbol: str, date: str, clock, limit: int = 50000):
        """
        api_key: your Polygon key
        symbol: ticker, e.g. "AAPL"
        date:   "YYYY-MM-DD"
        clock:  your Clock instance (mode='replay')
        """
        # async vs sync doesn’t matter here since we're blocking on REST
        self.client = StocksClient(api_key, use_async=False)
        self.symbol = symbol
        self.date   = date
        self.clock  = clock
        self.limit  = limit

        # will be populated in connect()
        self._results = []
        self._idx     = 0

    async def connect(self):
        # Parse ISO strings directly into aware datetimes, then normalize to UTC
        start_dt = datetime.fromisoformat(f"{self.date}T09:30:00+00:00")
        start_dt = start_dt.astimezone(pytz.UTC)

        end_dt = datetime.fromisoformat(f"{self.date}T16:00:00+00:00")
        end_dt = end_dt.astimezone(pytz.UTC)

        # Now call the v3 endpoint with those UTC datetimes
        resp = self.client.get_trades_v3(
            self.symbol,
            timestamp_gte=start_dt,
            timestamp_lt  =end_dt,
            all_pages     = True,
            limit         = self.limit,
            order          = "ASC"
        )

        # Handle both dict (v3 returns with "results") and plain list:
        if isinstance(resp, dict):
            self._results = resp.get("results", [])
        elif isinstance(resp, list):
            self._results = resp
        else:
            raise RuntimeError(f"Unexpected response type {type(resp)} from get_trades_v3")

        self._idx = 0

    async def get_next_tick(self):
        if self._idx >= len(self._results):
            raise StopAsyncIteration

        raw = self._results[self._idx]
        self._idx += 1

        # parse timestamp (ns → UTC datetime)
        ns = (
            raw.get("sip_timestamp")
            or raw.get("participant_timestamp")
            or raw.get("trf_timestamp")
        )
        timestamp = datetime.fromtimestamp(ns / 1e9, tz=pytz.UTC)

        # in replay mode, force the clock up to this tick’s time
        if self.clock.mode == 'replay':
            self.clock.set(timestamp)

        return {
            "symbol":    self.symbol,
            "price":     raw["price"],
            "size":      raw["size"],
            "timestamp": timestamp,
        }
    
    async def close(self):
        """
        No resources to clean up in replay mode, but we define this
        so the interface matches WebSocketTickDownloader.
        """
        return

def create_tick_source(mode: str, **kwargs) -> TickSource:
    """
    Factory for live vs. replay TickSource.
    :param mode: 'live' or 'replay'
    :param kwargs:
      - api_key: str
      - clock: Clock
      - symbols: List[str]          # for live
      - symbol: str, date: str      # for replay
    """
    mode = mode.lower()
    if mode == 'live':
        return WebSocketTickDownloader(
            api_key=kwargs['api_key'],
            symbols=kwargs['symbols'],
            clock=kwargs['clock'],
            url=kwargs.get('url'),
        )
    elif mode == 'replay':
        return HistoricalTickDownloader(
            api_key=kwargs['api_key'],
            symbol=kwargs['symbol'],
            date=kwargs['date'],
            clock=kwargs['clock'],
            limit=kwargs.get('limit', 50000),
        )
    else:
        raise ValueError(f"Unknown mode: {mode}")


def save_tick_parquet(
    api_key: str,
    symbol: str,
    date: str,
    out_dir: str = "data/ticks",
    limit: int   = 50000
):
    """
    Downloads all ticks for `symbol` on `date` via Polygon REST V3 and
    writes them to {out_dir}/{date}/{symbol}_ticks.parquet.
    Skips if the file already exists.
    """
    # 1) build output path and skip if already there
    date_dir = os.path.join(out_dir, date)
    os.makedirs(date_dir, exist_ok=True)
    out_path = os.path.join(date_dir, f"{symbol}_ticks.parquet")
    if os.path.exists(out_path):
        print(f"[skip] ticks already saved: {out_path}")
        return

    # 2) set up client and time bounds
    client = StocksClient(api_key, use_async=False)
    # parse ISO into UTC-aware
    start_dt = datetime.fromisoformat(f"{date}T09:30:00+00:00").astimezone(pytz.UTC)
    end_dt   = datetime.fromisoformat(f"{date}T16:00:00+00:00").astimezone(pytz.UTC)

    # 3) fetch trades (all pages, ascending time)
    resp = client.get_trades_v3(
        symbol,
        timestamp_gte=start_dt,
        timestamp_lt  =end_dt,
        all_pages     = True,
        limit         = limit,
        order         = "ASC",
    )
    # resp may be dict with "results" or a bare list
    raw = resp.get("results", resp) if isinstance(resp, dict) else resp

    if not raw:
        print(f"[warn] no ticks for {symbol} on {date}")
        return

    # 4) normalize into DataFrame
    df = pd.DataFrame(raw)

    # pick the best nanosecond timestamp column
    for col in ("sip_timestamp","participant_timestamp","trf_timestamp"):
        if col in df.columns and df[col].notna().any():
            ts_col = col
            break
    else:
        raise RuntimeError(f"No timestamp field in tick response for {symbol} on {date}")

    # convert ns → datetime index
    df["timestamp"] = pd.to_datetime(df[ts_col], unit="ns", errors="coerce")
    df = df.dropna(subset=["timestamp"]).set_index("timestamp").sort_index()

    # 5) write to parquet
    df.to_parquet(out_path)
    print(f"[saved] {len(df)} ticks → {out_path}")
