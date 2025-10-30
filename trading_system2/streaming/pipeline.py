"""Data loading and screening pipeline helpers.

This module provides helper functions that coordinate with the
``PremarketFetcher`` logic from :mod:`trading_systems.scanner.premarket_filter`.
The goal is to make the premarket screening process and warmup data loading
easy to reuse from notebooks, scripts, or tests without depending on the
full trading application wiring.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Protocol, Sequence

import pandas as pd


class _ScreeningFetcher(Protocol):
    """Protocol describing the subset of ``PremarketFetcher`` we rely on."""

    min_volume: int
    min_atr: float

    def get_active_common_stock_tickers(self, date: str) -> Sequence[str]:
        ...

    def compute_loose_candidates(self, date: str, tickers: Sequence[str]) -> Sequence[str]:
        ...

    def scan_with_filters(self, tickers: Sequence[str], date: str) -> Sequence[str]:
        ...


@dataclass
class _ProviderAdapter:
    """Normalises warmup data returned by different provider clients."""

    provider: object

    def fetch(self, symbol: str, multiplier: int, *, start: datetime, end: datetime) -> pd.DataFrame:
        """Fetch aggregated bars and normalise to a canonical OHLCV frame."""

        if not hasattr(self.provider, "fetch_aggregates"):
            raise TypeError("provider must define a fetch_aggregates() method")

        raw = self.provider.fetch_aggregates(
            symbol=symbol,
            multiplier=multiplier,
            timespan="minute",
            start=start,
            end=end,
        )

        frame = pd.DataFrame(raw)
        if frame.empty:
            return frame

        if "timestamp" in frame.columns:
            frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
            frame = frame.set_index("timestamp")
        elif frame.index.name != "timestamp":
            frame.index = pd.to_datetime(frame.index, utc=True)
            frame.index.name = "timestamp"

        rename_map = {"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"}
        for old, new in rename_map.items():
            if old in frame.columns and new not in frame.columns:
                frame = frame.rename(columns={old: new})

        required = {"open", "high", "low", "close", "volume"}
        missing = required.difference(frame.columns)
        if missing:
            raise ValueError(f"provider data missing columns: {sorted(missing)}")

        frame = frame.sort_index()
        frame.index = frame.index.tz_convert("UTC")
        ordered = ["open", "high", "low", "close", "volume"]
        return frame[ordered]


def fetch_candidates(
    date: str,
    *,
    fetcher: _ScreeningFetcher,
    tickers: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Run the premarket screening stack and return the surviving symbols.

    Parameters
    ----------
    date:
        Trading session date in ``YYYY-mm-dd`` format.
    fetcher:
        Object implementing the minimal interface of
        :class:`PremarketFetcher`. The production implementation can be
        passed directly, while tests may provide a light-weight stub.
    tickers:
        Optional subset of tickers to evaluate. When omitted the fetcher is
        asked to supply the active universe via
        :meth:`get_active_common_stock_tickers`.

    Returns
    -------
    pandas.DataFrame
        DataFrame with a ``symbol`` column listing candidates ranked by the
        order returned from the filters.
    """

    if fetcher is None:
        raise TypeError("fetcher is required")

    if tickers is None:
        tickers = list(fetcher.get_active_common_stock_tickers(date))
    else:
        tickers = list(tickers)

    loose = fetcher.compute_loose_candidates(date, tickers)
    screened = fetcher.scan_with_filters(loose, date)

    result = pd.DataFrame({"symbol": list(screened)})
    result.index.name = "rank"
    return result


def load_warmup(
    symbols: Iterable[str],
    *,
    warmup_minutes: int,
    provider: object,
    end: datetime | None = None,
) -> dict[str, pd.DataFrame]:
    """Load warmup OHLCV data at 1-minute and 5-minute resolutions.

    The provider is expected to expose a :func:`fetch_aggregates` method that
    mirrors the Polygon aggregates REST API. A light-weight adapter normalises
    the returned payload to pandas DataFrames so that the rest of the project
    can work with a consistent shape regardless of the underlying data source.

    Parameters
    ----------
    symbols:
        Iterable of ticker symbols to fetch.
    warmup_minutes:
        Lookback window in minutes.
    provider:
        Client instance that provides a ``fetch_aggregates`` method.
    end:
        Optional end timestamp, defaults to ``datetime.utcnow()``.

    Returns
    -------
    dict[str, pandas.DataFrame]
        Mapping of symbol -> DataFrame with a MultiIndex (``timeframe``,
        ``timestamp``) index containing OHLCV bars at both 1-minute and
        5-minute frequencies.
    """

    if warmup_minutes <= 0:
        raise ValueError("warmup_minutes must be positive")

    if end is None:
        end_ts = pd.Timestamp.utcnow().tz_localize("UTC")
    else:
        end_ts = pd.Timestamp(end)
        if end_ts.tzinfo is None:
            end_ts = end_ts.tz_localize("UTC")
        else:
            end_ts = end_ts.tz_convert("UTC")

    end = end_ts
    start = end - timedelta(minutes=warmup_minutes)

    adapter = _ProviderAdapter(provider)
    results: dict[str, pd.DataFrame] = {}

    for symbol in symbols:
        base_1m = adapter.fetch(symbol, 1, start=start, end=end)
        base_1m = base_1m.loc[(base_1m.index >= start) & (base_1m.index <= end)]

        if base_1m.empty:
            empty = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
            empty.index = pd.MultiIndex.from_arrays(
                [[], []], names=["timeframe", "timestamp"]
            )
            results[symbol] = empty
            continue

        resampled_5m = _resample_to_ohlc(base_1m, "5min")

        frames = {
            "1m": base_1m,
            "5m": resampled_5m,
        }
        combined = pd.concat(frames, names=["timeframe"])
        combined.index = combined.index.set_names(["timeframe", "timestamp"])
        results[symbol] = combined

    return results


def _resample_to_ohlc(frame: pd.DataFrame, freq: str) -> pd.DataFrame:
    """Resample an OHLCV frame to the requested frequency."""

    if frame.empty:
        return frame.copy()

    ohlc = frame.resample(freq, label="right", closed="right").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    )
    ordered = ["open", "high", "low", "close", "volume"]
    return ohlc[ordered].dropna(how="any")