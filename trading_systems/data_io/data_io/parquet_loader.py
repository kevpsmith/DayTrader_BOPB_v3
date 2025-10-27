import os
import pandas as pd

class ParquetLoader:
    def __init__(self, base_path: str, date: str):
        self.base_path = base_path
        self.date = date

    def load_ticker_data(self, ticker: str):
        fp = os.path.join(self.base_path, self.date, f"{ticker}_ticks.parquet")
        df = pd.read_parquet(fp)

        # ✅ Always ensure timestamp is a normal column
        if "timestamp" in df.index.names:
            df = df.reset_index()

        # ✅ Normalize timestamps (Polygon format)
        if "timestamp" not in df.columns:
            for col in ["sip_timestamp", "participant_timestamp", "trf_timestamp"]:
                if col in df.columns:
                    df["timestamp"] = df[col]
                    break

        # ✅ Force tz-aware UTC no matter the dtype (ints ns, strings, pandas Timestamps)
        if pd.api.types.is_integer_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ns", utc=True)
        else:
            # to_datetime with utc=True handles naive/aware correctly
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

        df = df.dropna(subset=["timestamp"])
        return df

    def stream_ticks(self, tickers: list):
        for ticker in tickers:
            df = self.load_ticker_data(ticker)
            # ✅ No ambiguity now — timestamp is a normal column
            df = df.sort_values("timestamp")
            for _, row in df.iterrows():
                yield {
                    "ticker": ticker,
                    "timestamp": row["timestamp"],
                    "price": row["price"],
                    "size": row["size"],
                }
