# data_io/warmup_loader.py
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import List, Dict, Any
from pathlib import Path

# 1) Interface
class WarmupLoader(ABC):
    @abstractmethod
    def load_minute_bars(
        self,
        symbol: str,
        end_time: datetime,
        lookback_minutes: int
    ) -> List[Dict[str, Any]]:
        """
        Return minute bars ending at `end_time`, going back `lookback_minutes`.
        Each bar:
          {
            "start_time": datetime,
            "end_time":   datetime,
            "open":  float, "high": float,
            "low":   float, "close": float,
            "volume": int
          }
        """
        ...

# 2) REST (live) implementation
from polygon.rest import RESTClient
import pytz

class RESTWarmupLoader(WarmupLoader):
    def __init__(self, api_key: str):
        self.client = RESTClient(api_key)

    def load_minute_bars(self, symbol, end_time, lookback_minutes):
        end_dt   = end_time.replace(second=0, microsecond=0, tzinfo=pytz.UTC)
        start_dt = end_dt - timedelta(minutes=lookback_minutes)
        aggs = self.client.get_aggs(
            symbol,
            1, "minute",
            start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            limit=lookback_minutes + 1
        )
        bars = []
        for agg in aggs:
            st = datetime.fromtimestamp(agg.timestamp/1000, tz=pytz.UTC)
            bars.append({
                "start_time": st,
                "end_time":   st + timedelta(minutes=1),
                "open":  agg.open,
                "high":  agg.high,
                "low":   agg.low,
                "close": agg.close,
                "volume": agg.volume,
            })
        return bars

# 3) Parquet (backtest) implementation
import pandas as pd
import os

class ParquetWarmupLoader(WarmupLoader):
    def __init__(self, base_path="/teamspace/studios/this_studio/data/bars/1m"):
        self.base_path = base_path
        print(f"[WarmupLoader] Base path forced to: {self.base_path}")

    def load_minute_bars(self, symbol, end_time, lookback_minutes):
        date = end_time.strftime("%Y-%m-%d")
        path = os.path.join(self.base_path, date, f"{symbol}.parquet")
        df = pd.read_parquet(path)
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        st = end_time - timedelta(minutes=lookback_minutes)
        sel = df.loc[st:end_time]
        bars = []
        for ts, row in sel.iterrows():
            bars.append({
                "start_time": ts,
                "end_time":   ts + timedelta(minutes=1),
                "open":  row["open"],
                "high":  row["high"],
                "low":   row["low"],
                "close": row["close"],
                "volume": row["volume"],
            })
        return bars