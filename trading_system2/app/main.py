"""Entry point helpers that wire together the refactored trading system."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, MutableMapping, Optional, Sequence

import pandas as pd

from trading_system2.data.pipeline import fetch_candidates, load_warmup
from trading_system2.execution.blotter import Blotter
from trading_system2.strategy.engine import StrategyState, TradeInstruction, evaluate


LOGGER = logging.getLogger(__name__)


def load_runtime_config(path: str | Path) -> Mapping[str, Any]:
    """Load runtime configuration from a JSON or YAML file."""

    location = Path(path)
    if not location.exists():
        raise FileNotFoundError(location)

    payload = location.read_text(encoding="utf-8")

    if location.suffix.lower() == ".json":
        return json.loads(payload)

    if location.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore
        except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "PyYAML is required to load YAML configuration files"
            ) from exc

        data = yaml.safe_load(payload)
        return data or {}

    raise ValueError(f"Unsupported configuration format: {location.suffix}")


@dataclass
class _BarState:
    symbol: str
    start: datetime
    end: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    entry_signal: bool = False
    exit_signal: bool = False
    metadata: Any = None


class BarManager:
    """Aggregate ticks into fixed bars and broadcast them to listeners."""

    def __init__(self, *, timeframe_seconds: int = 60) -> None:
        if timeframe_seconds <= 0:
            raise ValueError("timeframe_seconds must be positive")

        self._delta = timedelta(seconds=int(timeframe_seconds))
        self._callbacks: Dict[str, List[Callable[[str, Mapping[str, Any]], None]]] = defaultdict(list)
        self._current: Dict[str, _BarState] = {}
        self._last_closed_end: Dict[str, datetime | None] = defaultdict(lambda: None)
        self._warmup: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    # ------------------------------------------------------------------
    # Subscription management
    # ------------------------------------------------------------------
    def register(self, symbol: str, callback: Callable[[str, Mapping[str, Any]], None]) -> None:
        self._callbacks[symbol].append(callback)

    # ------------------------------------------------------------------
    # Warmup support
    # ------------------------------------------------------------------
    def bootstrap(self, warmup_bars: Mapping[str, Any] | None) -> None:
        if not warmup_bars:
            return

        for symbol, frame in warmup_bars.items():
            normalised = self._normalise_warmup(symbol, frame)
            if not normalised:
                continue
            self._warmup[symbol].extend(normalised)
            self._last_closed_end[symbol] = normalised[-1]["timestamp"]

    def replay_warmup(self) -> None:
        for symbol, events in self._warmup.items():
            for event in events:
                payload = {**event, "warmup": True}
                self._dispatch(symbol, payload)

    # ------------------------------------------------------------------
    # Tick processing
    # ------------------------------------------------------------------
    def process_tick(self, tick: Mapping[str, Any]) -> None:
        if tick is None:
            return

        symbol = tick.get("symbol") or tick.get("ticker")
        if not symbol:
            raise KeyError("tick missing 'symbol'")

        timestamp = tick.get("timestamp")
        if not isinstance(timestamp, datetime):
            raise TypeError("tick 'timestamp' must be a datetime instance")

        price = float(tick["price"])
        size = int(tick.get("size", 0))

        state = self._current.get(symbol)

        bucket_start = self._floor_time(timestamp)
        bucket_end = bucket_start + self._delta

        if state is None or timestamp >= state.end:
            if state is not None:
                self._finalise(symbol, state)

            state = _BarState(
                symbol=symbol,
                start=bucket_start,
                end=bucket_end,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=size,
                entry_signal=bool(tick.get("entry_signal", False)),
                exit_signal=bool(tick.get("exit_signal", False)),
                metadata=tick.get("metadata"),
            )
            self._current[symbol] = state
        else:
            state.high = max(state.high, price)
            state.low = min(state.low, price)
            state.close = price
            state.volume += size

            if "entry_signal" in tick:
                state.entry_signal = bool(tick["entry_signal"])
            if "exit_signal" in tick:
                state.exit_signal = bool(tick["exit_signal"])
            if tick.get("metadata") is not None:
                state.metadata = tick["metadata"]

    def close(self) -> None:
        for symbol, state in list(self._current.items()):
            if state is not None:
                self._finalise(symbol, state)
        self._current.clear()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _finalise(self, symbol: str, state: _BarState) -> None:
        payload = {
            "timestamp": state.end,
            "open": state.open,
            "high": state.high,
            "low": state.low,
            "close": state.close,
            "volume": state.volume,
            "price": state.close,
            "entry_signal": state.entry_signal,
            "exit_signal": state.exit_signal,
        }
        if state.metadata is not None:
            payload["metadata"] = state.metadata

        self._last_closed_end[symbol] = state.end
        self._dispatch(symbol, payload)

    def _dispatch(self, symbol: str, payload: Mapping[str, Any]) -> None:
        for callback in self._callbacks.get(symbol, []):
            callback(symbol, payload)

    def _floor_time(self, ts: datetime) -> datetime:
        epoch = int(ts.timestamp())
        bucket = epoch - (epoch % int(self._delta.total_seconds()))
        return datetime.fromtimestamp(bucket, tz=ts.tzinfo)

    @staticmethod
    def _normalise_warmup(symbol: str, frame: Any) -> List[Dict[str, Any]]:
        if frame is None:
            return []

        if isinstance(frame, pd.DataFrame):
            working = frame
            if isinstance(frame.index, pd.MultiIndex) and "timeframe" in frame.index.names:
                try:
                    working = frame.xs("1m", level="timeframe")
                except KeyError:
                    working = frame
            if isinstance(working.index, pd.MultiIndex):
                working = working.droplevel("timeframe")

            working = working.sort_index()
            rows = []
            for ts, row in working.iterrows():
                timestamp = pd.Timestamp(ts)
                if timestamp.tzinfo is None:
                    timestamp = timestamp.tz_localize("UTC")
                else:
                    timestamp = timestamp.tz_convert("UTC")
                rows.append(
                    {
                        "symbol": symbol,
                        "timestamp": timestamp.to_pydatetime(),
                        "open": float(row["open"]),
                        "high": float(row["high"]),
                        "low": float(row["low"]),
                        "close": float(row["close"]),
                        "volume": int(row["volume"]),
                        "price": float(row["close"]),
                        "entry_signal": False,
                        "exit_signal": False,
                    }
                )
            return rows

        if isinstance(frame, Iterable):
            rows = []
            for raw in frame:
                if not isinstance(raw, Mapping):
                    continue
                timestamp = raw.get("timestamp")
                if isinstance(timestamp, str):
                    timestamp = pd.Timestamp(timestamp)
                if isinstance(timestamp, pd.Timestamp):
                    timestamp = timestamp.tz_convert("UTC") if timestamp.tzinfo else timestamp.tz_localize("UTC")
                if isinstance(timestamp, datetime):
                    ts_value = timestamp
                else:
                    continue
                rows.append(
                    {
                        "symbol": symbol,
                        "timestamp": ts_value,
                        "open": float(raw.get("open", 0.0)),
                        "high": float(raw.get("high", 0.0)),
                        "low": float(raw.get("low", 0.0)),
                        "close": float(raw.get("close", 0.0)),
                        "volume": int(raw.get("volume", 0)),
                        "price": float(raw.get("close", 0.0)),
                        "entry_signal": bool(raw.get("entry_signal", False)),
                        "exit_signal": bool(raw.get("exit_signal", False)),
                    }
                )
            return sorted(rows, key=lambda item: item["timestamp"])

        return []


def _resolve_symbols(
    *,
    config: Mapping[str, Any],
    fetcher: Any,
) -> List[str]:
    session_cfg = config.get("session", {})
    explicit = session_cfg.get("tickers")
    if explicit:
        seen = []
        for ticker in explicit:
            if ticker not in seen:
                seen.append(ticker)
        return seen

    date = session_cfg.get("date")
    if not date:
        raise ValueError("session.date is required when tickers are not provided")

    candidates = fetch_candidates(str(date), fetcher=fetcher)
    symbols = candidates["symbol"].tolist()

    limit = session_cfg.get("limit")
    if limit is not None:
        symbols = symbols[: int(limit)]

    return symbols


def _iter_ticks(source: Any) -> Iterator[Mapping[str, Any]]:
    if callable(source):
        iterator = source()
    else:
        iterator = source
    return iter(iterator)


def run_trading_app(
    config: Mapping[str, Any],
    *,
    dependencies: Mapping[str, Any],
) -> Dict[str, Any]:
    """Execute the trading flow using the supplied configuration and dependencies."""

    if not dependencies:
        raise ValueError("dependencies are required")

    fetcher = dependencies.get("fetcher")
    provider = dependencies.get("provider")
    tick_stream = dependencies.get("tick_stream")

    if fetcher is None:
        raise ValueError("'fetcher' dependency is required")
    if provider is None:
        raise ValueError("'provider' dependency is required")
    if tick_stream is None:
        raise ValueError("'tick_stream' dependency is required")

    logger = dependencies.get("logger", LOGGER)
    blotter: Blotter = dependencies.get("blotter") or Blotter()

    symbols = _resolve_symbols(config=config, fetcher=fetcher)
    if not symbols:
        logger.warning("No symbols to trade - exiting early")
        return {"symbols": [], "warmup": {}, "trades": [], "csv_path": None, "blotter": blotter}

    data_cfg = config.get("data", {})
    warmup_minutes = int(data_cfg.get("warmup_minutes", 30))
    warmup_end = data_cfg.get("warmup_end")

    warmup = load_warmup(
        symbols,
        warmup_minutes=warmup_minutes,
        provider=provider,
        end=warmup_end,
    )

    bars_cfg = config.get("bars", {})
    bar_manager: BarManager = dependencies.get("bar_manager") or BarManager(
        timeframe_seconds=int(bars_cfg.get("timeframe_seconds", 60))
    )
    bar_manager.bootstrap(warmup)

    quantity = int(config.get("execution", {}).get("quantity", 1))
    csv_path_cfg = config.get("execution", {}).get("output_csv")

    strategy_states: Dict[str, StrategyState] = {symbol: StrategyState() for symbol in symbols}
    trades: List[Dict[str, Any]] = []

    def on_bar(symbol: str, bar: Mapping[str, Any]) -> None:
        state = strategy_states[symbol]
        instruction: Optional[TradeInstruction] = evaluate(symbol, bar, state)
        if not instruction:
            return

        side = "BUY" if instruction.action.lower() == "buy" else "SELL"
        trade_event = {
            "timestamp": instruction.timestamp,
            "symbol": instruction.symbol,
            "side": side,
            "quantity": quantity,
            "price": instruction.price,
        }
        record = blotter.record_trade(trade_event)
        trades.append(record)
        logger.info("Executed %s trade for %s at %.2f", side, symbol, instruction.price)

    for symbol in symbols:
        bar_manager.register(symbol, on_bar)

    bar_manager.replay_warmup()

    for tick in _iter_ticks(tick_stream):
        bar_manager.process_tick(tick)

    bar_manager.close()

    csv_path: Optional[Path] = None
    if csv_path_cfg:
        csv_path = blotter.flush_to_csv(csv_path_cfg)
        logger.info("Persisted %d trades to %s", len(trades), csv_path)

    return {
        "symbols": symbols,
        "warmup": warmup,
        "trades": trades,
        "csv_path": csv_path,
        "blotter": blotter,
    }


__all__ = ["BarManager", "load_runtime_config", "run_trading_app"]