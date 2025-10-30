from __future__ import annotations

import csv
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from src.execution.blotter import Blotter


def _trade_event(ts: datetime, symbol: str, side: str, qty: int, price: float):
    return {
        "timestamp": ts,
        "symbol": symbol,
        "side": side,
        "quantity": qty,
        "price": price,
    }


def test_record_trade_tracks_positions_and_pnl():
    blotter = Blotter()
    start = datetime(2024, 1, 1, 9, 30)

    blotter.record_trade(_trade_event(start, "AAPL", "BUY", 10, 100.0))
    blotter.record_trade(_trade_event(start + timedelta(minutes=1), "AAPL", "BUY", 5, 105.0))
    trade = blotter.record_trade(
        _trade_event(start + timedelta(minutes=5), "AAPL", "SELL", 8, 110.0)
    )

    assert trade["realized_pnl"] == pytest.approx(80.0)
    assert blotter.realized_pnl == pytest.approx(80.0)
    assert trade["net_position"] == 7
    assert trade["holding_time_sec"] == pytest.approx(300.0)
    assert blotter.trades[-1]["cumulative_pnl"] == pytest.approx(80.0)


def test_flush_to_csv_writes_trades_and_summary(tmp_path: Path):
    blotter = Blotter()
    start = datetime(2024, 1, 1, 9, 30)

    blotter.record_trade(_trade_event(start, "AAPL", "BUY", 10, 100.0))
    blotter.record_trade(_trade_event(start + timedelta(minutes=5), "AAPL", "SELL", 10, 110.0))

    output = blotter.flush_to_csv(tmp_path / "blotter.csv")
    assert output.exists()

    with output.open() as handle:
        rows = list(csv.reader(handle))

    header = rows[0]
    assert header == [
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

    trade_rows = rows[1:3]
    assert all(len(row) == len(header) for row in trade_rows)
    assert trade_rows[0][0].endswith("09:30:00")
    assert trade_rows[1][0].endswith("09:35:00")

    assert rows[3] == []
    assert rows[4] == ["metric", "value"]

    summary = dict(rows[5:])
    assert summary["total_trades"] == "2"
    assert summary["total_volume"] == "20"
    assert float(summary["realized_pnl"]) == pytest.approx(100.0)
    assert float(summary["return_on_turnover"]) == pytest.approx(100.0 / 2100.0, abs=1e-6)
    assert float(summary["average_holding_time_sec"]) == pytest.approx(300.0)