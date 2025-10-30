"""Light-weight strategy evaluation engine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Optional


@dataclass
class StrategyState:
    """Holds the minimal state required by the strategy engine."""

    last_action_timestamp: Optional[datetime] = None
    position: Optional[str] = None


@dataclass
class TradeInstruction:
    """Represents a single trade instruction produced by the engine."""

    symbol: str
    action: str
    timestamp: datetime
    price: float
    metadata: Optional[Mapping[str, Any]] = None


def _read(bundle: Any, key: str, default: Any = None) -> Any:
    """Read ``key`` from a mapping or object falling back to ``default``."""

    if isinstance(bundle, Mapping):
        return bundle.get(key, default)

    if hasattr(bundle, key):
        return getattr(bundle, key, default)

    if default is not None:
        return default

    raise KeyError(key)


def evaluate(symbol: str, bar_bundle: Any, state: StrategyState) -> Optional[TradeInstruction]:
    """Evaluate incoming bar data to produce a trade instruction.

    The ``bar_bundle`` argument is expected to expose ``timestamp``, ``price``,
    ``entry_signal`` and ``exit_signal`` either as mapping keys or attributes.
    Only one of the entry/exit signals can be actioned for a single bar and an
    exit is ignored when it lands on the same timestamp as the most recent
    action, preventing immediate exits right after entries.
    """

    timestamp = _read(bar_bundle, "timestamp")
    price = _read(bar_bundle, "price")
    entry_signal = bool(_read(bar_bundle, "entry_signal", False))
    exit_signal = bool(_read(bar_bundle, "exit_signal", False))

    if state.position is None:
        exit_signal = False
    else:
        entry_signal = False

    if exit_signal and state.last_action_timestamp and timestamp == state.last_action_timestamp:
        exit_signal = False

    if entry_signal:
        state.position = "long"
        state.last_action_timestamp = timestamp
        metadata = _read(bar_bundle, "metadata", None)
        return TradeInstruction(symbol, "buy", timestamp, price, metadata)

    if exit_signal:
        state.position = None
        state.last_action_timestamp = timestamp
        metadata = _read(bar_bundle, "metadata", None)
        return TradeInstruction(symbol, "sell", timestamp, price, metadata)

    return None