from __future__ import annotations

import csv
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Deque, Dict, List, Optional


@dataclass
class _Lot:
    quantity: int
    price: float
    timestamp: datetime


class Blotter:
    """Collects filled trades and computes running execution metrics."""

    def __init__(self) -> None:
        self.trades: List[Dict[str, object]] = []
        self._open_lots: Dict[str, Dict[str, Deque[_Lot]]] = {}
        self.total_volume: int = 0
        self.total_turnover: float = 0.0
        self.realized_pnl: float = 0.0
        self.trade_count: int = 0
        self._holding_time_seconds: float = 0.0
        self._closed_quantity: int = 0
        self._realized_trade_results: List[float] = []

    def record_trade(self, event: Dict[str, object]) -> Dict[str, object]:
        """Record a filled trade and update running metrics.

        Parameters
        ----------
        event: Dict[str, object]
            Trade data with keys: ``timestamp`` (datetime), ``symbol`` (str),
            ``side`` ("BUY" or "SELL"), ``quantity`` (int), and ``price`` (float).

        Returns
        -------
        Dict[str, object]
            The normalized trade record stored internally.
        """

        timestamp = self._require_type(event, "timestamp", datetime)
        symbol = self._require_type(event, "symbol", str)
        side = self._require_type(event, "side", str).upper()
        quantity = int(self._require_type(event, "quantity", (int, float)))
        price = float(self._require_type(event, "price", (int, float)))

        if quantity <= 0:
            raise ValueError("quantity must be positive")
        if side not in {"BUY", "SELL"}:
            raise ValueError("side must be either 'BUY' or 'SELL'")

        lots = self._open_lots.setdefault(symbol, {"long": deque(), "short": deque()})

        realized_pnl, holding_seconds, closed_qty = self._close_positions(
            side=side,
            price=price,
            quantity=quantity,
            timestamp=timestamp,
            lots=lots,
        )

        remaining_qty = quantity - closed_qty
        if remaining_qty:
            self._open_new_position(
                side=side,
                price=price,
                quantity=remaining_qty,
                timestamp=timestamp,
                lots=lots,
            )

        self.total_volume += quantity
        self.total_turnover += price * quantity
        self.realized_pnl += realized_pnl
        self.trade_count += 1

        if closed_qty:
            self._holding_time_seconds += holding_seconds
            self._closed_quantity += closed_qty
            self._realized_trade_results.append(realized_pnl)

        net_position = self._net_position(lots)
        holding_time_sec: Optional[float]
        if closed_qty:
            holding_time_sec = holding_seconds / closed_qty
        else:
            holding_time_sec = None

        record = {
            "timestamp": timestamp,
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": price,
            "notional": price * quantity,
            "net_position": net_position,
            "realized_pnl": realized_pnl,
            "cumulative_pnl": self.realized_pnl,
            "holding_time_sec": holding_time_sec,
        }

        self.trades.append(record)
        return record

    def flush_to_csv(self, path: str | Path) -> Path:
        """Persist trade history and summary metrics to ``path``."""

        destination = Path(path)
        destination.parent.mkdir(parents=True, exist_ok=True)

        fieldnames = [
            "timestamp",
            "symbol",
            "side",
            "quantity",
            "price",
            "notional",
            "net_position",
            "realized_pnl",
            "cumulative_pnl",
            "holding_time_sec",
        ]

        with destination.open("w", newline="") as handle:
            dict_writer = csv.DictWriter(handle, fieldnames=fieldnames)
            dict_writer.writeheader()
            for trade in self.trades:
                row = trade.copy()
                row["timestamp"] = row["timestamp"].isoformat()
                dict_writer.writerow(row)

            writer = csv.writer(handle)
            writer.writerow([])
            writer.writerow(["metric", "value"])
            for metric, value in self._summary_metrics().items():
                writer.writerow([metric, value])

        return destination

    def _summary_metrics(self) -> Dict[str, object]:
        avg_holding_time = (
            self._holding_time_seconds / self._closed_quantity
            if self._closed_quantity
            else 0.0
        )
        return {
            "total_trades": self.trade_count,
            "total_volume": self.total_volume,
            "total_turnover": round(self.total_turnover, 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "return_on_turnover": round(
                self.realized_pnl / self.total_turnover, 6
            )
            if self.total_turnover
            else 0.0,
            "average_holding_time_sec": round(avg_holding_time, 2),
            "win_rate": round(
                sum(1 for pnl in self._realized_trade_results if pnl > 0)
                / len(self._realized_trade_results),
                4,
            )
            if self._realized_trade_results
            else 0.0,
        }

    @staticmethod
    def _require_type(event: Dict[str, object], key: str, expected_type):
        if key not in event:
            raise KeyError(f"'{key}' missing from trade event")
        value = event[key]
        if not isinstance(value, expected_type):
            raise TypeError(f"'{key}' must be of type {expected_type}")
        return value

    @staticmethod
    def _open_new_position(
        side: str,
        price: float,
        quantity: int,
        timestamp: datetime,
        lots: Dict[str, Deque[_Lot]],
    ) -> None:
        bucket = "long" if side == "BUY" else "short"
        lots[bucket].append(_Lot(quantity=quantity, price=price, timestamp=timestamp))

    @staticmethod
    def _net_position(lots: Dict[str, Deque[_Lot]]) -> int:
        long_qty = sum(lot.quantity for lot in lots["long"])
        short_qty = sum(lot.quantity for lot in lots["short"])
        return long_qty - short_qty

    @staticmethod
    def _close_positions(
        side: str,
        price: float,
        quantity: int,
        timestamp: datetime,
        lots: Dict[str, Deque[_Lot]],
    ) -> tuple[float, float, int]:
        if side == "BUY":
            opposing = lots["short"]
            pnl_fn = lambda entry_price: entry_price - price
        else:
            opposing = lots["long"]
            pnl_fn = lambda entry_price: price - entry_price

        realized_pnl = 0.0
        holding_seconds = 0.0
        closed_qty = 0
        qty_remaining = quantity

        while opposing and qty_remaining:
            lot = opposing[0]
            match_qty = min(qty_remaining, lot.quantity)
            realized_pnl += pnl_fn(lot.price) * match_qty
            holding_seconds += (timestamp - lot.timestamp).total_seconds() * match_qty
            lot.quantity -= match_qty
            qty_remaining -= match_qty
            closed_qty += match_qty

            if lot.quantity == 0:
                opposing.popleft()

        return realized_pnl, holding_seconds, closed_qty