from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from data.pipeline import fetch_candidates, load_warmup


class DummyFetcher:
    def __init__(self, universe, loose, final):
        self._universe = universe
        self._loose = loose
        self._final = final

    def get_active_common_stock_tickers(self, date: str):
        return self._universe

    def compute_loose_candidates(self, date: str, tickers):
        assert list(tickers) == list(self._universe)
        return self._loose

    def scan_with_filters(self, tickers, date: str):
        assert list(tickers) == list(self._loose)
        return self._final


class DummyProvider:
    def __init__(self, frames):
        self._frames = frames

    def fetch_aggregates(self, symbol, multiplier, timespan, start, end):
        # Only 1 minute data is provided for tests.
        assert multiplier == 1
        frame = self._frames[symbol]
        window = frame.loc[(frame.index >= start) & (frame.index <= end)]
        payload = window.reset_index().rename(columns={"index": "timestamp"})
        return payload


def test_fetch_candidates_returns_ranked_dataframe():
    fetcher = DummyFetcher(
        universe=["AAPL", "TSLA", "AMD"],
        loose=["AAPL", "TSLA"],
        final=["TSLA"],
    )

    result = fetch_candidates("2024-01-02", fetcher=fetcher)

    assert list(result.columns) == ["symbol"]
    assert result.iloc[0]["symbol"] == "TSLA"
    assert result.index.name == "rank"


def test_load_warmup_produces_expected_coverage():
    index = pd.date_range("2024-01-01 09:30", periods=6, freq="1min", tz="UTC")
    frame = pd.DataFrame(
        {
            "open": pd.Series(range(6), index=index) + 100,
            "high": pd.Series(range(6), index=index) + 101,
            "low": pd.Series(range(6), index=index) + 99,
            "close": pd.Series(range(6), index=index) + 100.5,
            "volume": pd.Series(range(6), index=index) + 1,
        }
    )

    provider = DummyProvider({"AAPL": frame})
    end = pd.Timestamp("2024-01-01 09:35", tz="UTC")

    warmup = load_warmup(["AAPL"], warmup_minutes=5, provider=provider, end=end)
    assert set(warmup.keys()) == {"AAPL"}

    aapl = warmup["AAPL"]
    assert set(aapl.index.get_level_values("timeframe")) == {"1m", "5m"}

    one_minute = aapl.xs("1m", level="timeframe")
    assert one_minute.index.min() >= pd.Timestamp("2024-01-01 09:30", tz="UTC")
    assert one_minute.index.max() <= end
    assert len(one_minute) >= 5
    assert one_minute.index.max() - one_minute.index.min() == pd.Timedelta(minutes=5)

    five_minute = aapl.xs("5m", level="timeframe")
    assert end in five_minute.index
    assert list(five_minute.columns) == ["open", "high", "low", "close", "volume"]