import os
import pandas as pd
import requests
from datetime import datetime
from time_utils.clock import Clock
from polygon import StocksClient
from polygon.rest import RESTClient
import pytz

class PremarketFetcher:
    def __init__(self, api_key, tick_dir="data/ticks", bar_dir="data/bars/1min"):
        self.api_key = api_key
        self.tick_dir = tick_dir
        self.bar_dir = bar_dir
        self.stocks_client = StocksClient(api_key, connect_timeout=60, read_timeout=60)
        self.client = RESTClient(api_key) # for aggregates, daily bars, etc.
        self.eastern = pytz.timezone("America/New_York")
        self.min_volume = 100_000
        self.min_atr = 0.5
        self.atr_window = 5

    def fetch(self, date: str, live: bool = False, tickers: list[str] | None = None) -> list[str]:
        mode = "LIVE" if live else "BACKTEST"
        print(f"[PremarketFilter] Fetch running in {mode} mode for {date}")
        ticker_pool = tickers or self.get_active_common_stock_tickers(date)
        universe = self.compute_loose_candidates(date, ticker_pool)
        filtered = self.scan_with_filters(universe, date)
        if live:
            return filtered
        else:
            ready = []
            for ticker in filtered:
                tick_path = self.get_tick_path(ticker, date)
                bar_path = self.get_bar_path(ticker, date)

                if not os.path.exists(tick_path):
                    self.download_ticks(ticker, date)


                if os.path.exists(tick_path):
                    if not os.path.exists(bar_path):
                        self.build_1min_bars(ticker, date)
                    ready.append(ticker)
        return filtered

    def get_active_common_stock_tickers(self, date):
        url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
        params = {"apiKey": self.api_key}
        try:
            resp = requests.get(url, params=params)
            if resp.status_code != 200:
                print(f"[ERROR] Snapshot request failed: {resp.status_code}")
                return []
            data = resp.json().get("tickers", [])
            tickers = []
            for item in data:
                tkr = item.get("ticker", "")
                if tkr.startswith("C:") or tkr.startswith("X:"):
                    continue # skip crypto or unknown
                tickers.append(tkr)
            return tickers
        except Exception as e:
            print(f"[ERROR] Failed to fetch active tickers: {e}")
            return []

    def get_tick_path(self, ticker, date):
        return os.path.join(self.tick_dir, date, f"{ticker}_ticks.parquet")

    def get_bar_path(self, ticker, date):
        return os.path.join(self.bar_dir, date, f"{ticker}.parquet")

    def compute_loose_candidates(self, date, tickers):
        qualified = []
        for ticker in tickers:
            try:
                premarket_vol, gap_pct, price = self.get_premarket_stats(ticker, date)
                if ticker in {"DATS", "ARTW", "IMRX"}:
                    print(f"[DEBUG] {ticker} → volume={premarket_vol}, gap={gap_pct:.2f}%, price={price}")
                if premarket_vol > 25000 and gap_pct > 1.5 and price > 1:
                    qualified.append(ticker)
            except:
                continue
        return qualified

    def get_premarket_stats(self, ticker, date):
        # Pull 1-min bars from 04:00 to 09:30
        start_ts = Clock.get_unix_timestamp_et(date, "04:30:00")
        end_ts   = Clock.get_unix_timestamp_et(date, "09:30:00")
        print(f"[DEBUG] Fetching 1-min bars for {ticker} from {start_ts} to {end_ts}")
        bars = self.client.get_aggs(ticker, 1, "minute", start_ts, end_ts, limit=5000)
        df = pd.DataFrame([b.__dict__ for b in bars])
        volume = df["volume"].sum()

        first_price = df.iloc[0]["open"]
        prev_close = self.get_previous_close(ticker, date)
        gap_pct = ((first_price / prev_close) - 1) * 100

        return volume, gap_pct, first_price

    def get_previous_close(self, ticker, date):
        """
        Get the most recent closing price prior to `date`, skipping weekends and market holidays.
        """
        from datetime import datetime, timedelta

        date_obj = datetime.strptime(date, "%Y-%m-%d")
        max_lookback = 10  # safeguard in case of extended closure

        for _ in range(max_lookback):
            prev_day = date_obj - timedelta(days=1)
            prev_day_str = prev_day.strftime("%Y-%m-%d")

            try:
                bars = self.client.get_aggs(ticker, 1, "day", prev_day_str, prev_day_str, limit=1)
                df = pd.DataFrame([bar.__dict__ for bar in bars])

                if not df.empty:
                    return df.iloc[0]["close"]

                print(f"[SKIP] No market data for {ticker} on {prev_day_str}, trying earlier...")
            except Exception as e:
                print(f"[ERROR] get_previous_close failed for {ticker} on {prev_day_str}: {e}")

            date_obj = prev_day  # continue stepping back

        print(f"[FAIL] Could not find previous close for {ticker} within {max_lookback} days.")
        return None

    def scan_with_filters(self, tickers, date):
        results = []
        for ticker in tickers:
            try:
                pre_vol = self.get_premarket_volume(ticker, date)
                if pre_vol < self.min_volume:
                    continue
                atr = self.get_atr(ticker, date)
                if not atr or atr < self.min_atr:
                    continue
                results.append(ticker)
            except:
                continue
        return results

    def get_premarket_volume(self, ticker, date):
        start_ts = Clock.get_unix_timestamp_et(date, "04:30:00")
        end_ts   = Clock.get_unix_timestamp_et(date, "09:30:00")
        try:
            bars = self.client.get_aggs(ticker, 1, "minute", start_ts, end_ts, limit=5000)
            df = pd.DataFrame([bar.__dict__ for bar in bars])
            return df['volume'].sum()
        except:
            return 0

    def get_atr(self, ticker, date):
        end_date = datetime.strptime(date, "%Y-%m-%d")
        start_date = end_date - pd.Timedelta(days=30)
        try:
            bars = self.client.get_aggs(
                ticker, 1, "day",
                start_date.strftime("%Y-%m-%d"),
                date, limit=self.atr_window + 5
            )
            df = pd.DataFrame([bar.__dict__ for bar in bars])
            if len(df) < self.atr_window:
                return None
            tr = pd.concat([
                df['high'] - df['low'],
                (df['high'] - df['close'].shift()).abs(),
                (df['low'] - df['close'].shift()).abs()
            ], axis=1).max(axis=1)
            return tr.rolling(window=self.atr_window).mean().iloc[-1]
        except:
            return None

    def download_ticks(self, ticker, date):
        from datetime import timedelta

        all_data = []
        url = f"https://api.polygon.io/v3/trades/{ticker}?timestamp.gte={date}T04:00:00Z&timestamp.lt={date}T20:00:00Z&limit=50000&apiKey={self.api_key}"

        while url:
            response = requests.get(url)
            if response.status_code == 403:
                return
            elif response.status_code != 200:
                return

            data = response.json()
            results = data.get("results", [])
            all_data.extend(results)

            url = data.get("next_url", None)
            if url:
                url += f"&apiKey={self.api_key}"

        if not all_data:
            return

        df = pd.DataFrame(all_data)
        for col in ["sip_timestamp", "participant_timestamp", "trf_timestamp", "t"]:
            if col in df.columns and df[col].notna().sum() > 0:
                df["timestamp"] = pd.to_datetime(df[col], unit="ns", errors="coerce")
                break

        df = df[df["timestamp"].notna()]
        df = df.set_index("timestamp").sort_index()

        save_path = self.get_tick_path(ticker, date)
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        df.to_parquet(save_path)

    def build_1min_bars(self, ticker, date):
        import pandas_ta as ta

        tick_path = self.get_tick_path(ticker, date)
        if not os.path.exists(tick_path):
            return

        df = pd.read_parquet(tick_path)
        df = df.between_time("09:30", "16:00")

        df_resampled = df["price"].resample("1min", label="right", closed = "right").ohlc()
        df_resampled["v"] = df["size"].resample("1min", label="right", closed="right").sum()
        df_resampled = df_resampled.dropna()

        save_path = self.get_bar_path(ticker, date)
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        df_resampled.to_parquet(save_path)
