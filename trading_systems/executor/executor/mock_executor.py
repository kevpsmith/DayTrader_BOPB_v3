import csv
import os
from datetime import datetime

class MockExecutor:
    def __init__(self, starting_balance=100000):
        self.balance = starting_balance
        self.positions = {}

    def execute(self, signals, bars):
        executed_orders = []
        for signal in signals:
            ticker = signal["ticker"]
            side = signal["side"]
            size = signal["size"]
            price = bars.iloc[-1]["close"] if hasattr(bars, "iloc") else bars["close"]
            self._apply_trade(ticker, side, size, price)
            executed_orders.append({
                "ticker": ticker,
                "side": side,
                "size": size,
                "price": price,
                "timestamp": datetime.now()
            })
        return executed_orders

    def _apply_trade(self, ticker, side, size, price):
        if side == "buy":
            self.positions[ticker] = self.positions.get(ticker, 0) + size
            self.balance -= size * price
        elif side == "sell":
            self.positions[ticker] = self.positions.get(ticker, 0) - size
            self.balance += size * price

    def finalize(self):
        # Optional: mark to market or print summary
        print(f"Final balance: {self.balance:.2f}, open positions: {self.positions}")
