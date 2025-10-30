from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Iterator, List

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import pytest

from trading_system2.app.main import BarManager, run_trading_app


class DummyFetcher:
    min_volume = 0
    min_atr = 0

    def get_active_common_stock_tickers(self, date: str) -> List[str]:
        return ["AAPL", "MSFT"]

    def compute_loose_candidates(self, date: str, tickers: Iterable[str]) -> Iterable[str]:
        return list(tickers)

    def scan_with_filters(self, tickers: Iterable[str], date: str) -> Iterable[str]:
        return list(tickers)[:1]


class DummyProvider:
    def __init__(self, *, start: datetime, bars: int = 2) -> None:
        self.start = start
        self.bars = bars

    def fetch_aggregates(self, *, symbol: str, multiplier: int, timespan: str, start: datetime, end: datetime):
        assert multiplier == 1
        assert timespan == "minute"
        base = self.start
        payload = []
        for idx in range(self.bars):
            ts = base + timedelta(minutes=idx)
            payload.append(
                {
                    "timestamp": ts.isoformat(),
                    "open": 100.0 + idx,
                    "high": 101.0 + idx,
                    "low": 99.5 + idx,
                    "close": 100.5 + idx,
                    "volume": 1_000 + idx,
                }
            )
        return payload


def make_tick_stream() -> Iterator[dict]:
    base = datetime(2024, 3, 14, 9, 30, tzinfo=timezone.utc)
    ticks = [
        {"symbol": "AAPL", "timestamp": base, "price": 101.0, "size": 100},
        {
            "symbol": "AAPL",
            "timestamp": base + timedelta(seconds=45),
            "price": 101.5,
            "size": 50,
            "entry_signal": True,
        },
        {"symbol": "AAPL", "timestamp": base + timedelta(minutes=1), "price": 102.0, "size": 75},
        {
            "symbol": "AAPL",
            "timestamp": base + timedelta(minutes=1, seconds=30),
            "price": 102.5,
            "size": 50,
            "exit_signal": True,
        },
    ]
    for tick in ticks:
        yield tick


@pytest.fixture
def config(tmp_path: Path) -> dict:
    return {
        "session": {"date": "2024-03-14"},
        "data": {
            "warmup_minutes": 15,
            "warmup_end": datetime(2024, 3, 14, 9, 5, tzinfo=timezone.utc).isoformat(),
        },
        "bars": {"timeframe_seconds": 60},
        "execution": {"quantity": 100, "output_csv": tmp_path / "trades.csv"},
    }


@pytest.fixture
def dependencies() -> dict:
    start = datetime(2024, 3, 14, 9, 0, tzinfo=timezone.utc)
    return {
        "fetcher": DummyFetcher(),
        "provider": DummyProvider(start=start),
        "tick_stream": make_tick_stream,
    }


def test_trading_flow_records_trades_and_generates_csv(config: dict, dependencies: dict) -> None:
    result = run_trading_app(config, dependencies=dependencies)

    assert result["symbols"] == ["AAPL"]
    assert len(result["trades"]) == 2
    assert result["trades"][0]["side"] == "BUY"
    assert result["trades"][1]["side"] == "SELL"

    csv_path = result["csv_path"]
    assert csv_path is not None
    assert csv_path.exists()

    with csv_path.open(newline="") as handle:
        rows = list(csv.reader(handle))

    assert rows[0][:3] == ["timestamp", "symbol", "side"]
    assert any(row[0] == "metric" for row in rows if row)


@pytest.mark.parametrize("timeframe", [30, 60])
def test_bar_manager_emits_warmup(timeframe: int) -> None:
    manager = BarManager(timeframe_seconds=timeframe)
    ts = pd.date_range(
        datetime(2024, 3, 14, 9, 0, tzinfo=timezone.utc),
        periods=2,
        freq="1min",
    )
    frame = pd.DataFrame(
        {
            "open": [10.0, 11.0],
            "high": [10.5, 11.5],
            "low": [9.5, 10.5],
            "close": [10.25, 11.25],
            "volume": [100, 150],
        },
        index=pd.MultiIndex.from_product((['1m'], ts), names=["timeframe", "timestamp"]),
    )

    events: List[dict] = []

    manager.bootstrap({"AAPL": frame})
    manager.register("AAPL", lambda symbol, bar: events.append({"symbol": symbol, **bar}))
    manager.replay_warmup()

    assert len(events) == 2
    assert all(event["symbol"] == "AAPL" for event in events)
    assert all(event.get("warmup") for event in events)