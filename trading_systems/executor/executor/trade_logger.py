import csv
import os
from datetime import datetime
from typing import Optional

class TradeLogger:
    def __init__(self, log_dir="data/logs", log_file: Optional[str] = None):
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)

        self.log_file = (
            log_file
            if log_file is not None
            else os.path.join(log_dir, f"trades_{datetime.now().date()}.csv")
        )

        if not os.path.exists(self.log_file):
            with open(self.log_file, mode="w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "timestamp", "ticker", "action", "price", "quantity", "note"
                ])

    def log_entry(self, ticker, price, timestamp, quantity=0, note=""):
        self._log("BUY", ticker, price, timestamp, quantity, note)

    def log_exit(self, ticker, price, timestamp, quantity=0, note=""):
        self._log("SELL", ticker, price, timestamp, quantity, note)

    def _log(self, action, ticker, price, timestamp, quantity, note):
        try:
            with open(self.log_file, mode="a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    ticker,
                    action,
                    round(price, 2),
                    int(quantity),
                    note
                ])
            print(f"[LOG] {action} {ticker} @ {price} ({quantity})")
        except Exception as e:
            print(f"[ERROR] Failed to log {action} for {ticker}: {e}")
