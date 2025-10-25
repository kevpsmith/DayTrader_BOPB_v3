import requests
import json
import base64
import time
import os
from polygon.rest import RESTClient  
import polygon
from datetime import datetime, timedelta, timedelta
import pytz

def get_polygon_stock_prices(self, tickers):
    """Fetches real-time stock prices using Polygon.io."""
    if not self.polygon_api_key:
        print("[ERROR] Polygon API key is missing in config.json.")
        return {}

    # ✅ Create a Polygon client
    client = RESTClient(self.polygon_api_key)
    all_prices = {}

    for ticker in tickers:
        try:
            response = client.get_last_trade(f"C:{ticker}")
            price = response['last']['price']
            all_prices[ticker] = price
            print(f"[INFO] {ticker}: ${price}")
        except Exception as e:
            print(f"[ERROR] Failed to fetch price for {ticker}: {str(e)}")

    print(f"[INFO] Successfully fetched prices for {len(all_prices)} symbols.")
    return all_prices


class BrokerAPI:
    def __init__(self, config_path="config.json"):
        with open(config_path, "r") as file:
            self.config = json.load(file)
        self.config_path = config_path
        self.token_url = self.config["token_url"]
        self.base_url = self.config["base_url"]
        self.polygon_api_key = self.config["polygon_api_key"]

    def refresh_tokens(self):
        """Refresh 'access_token' if expired"""
        with open(self.config_path, "r") as file:
            config = json.load(file)

        #if time.time() < config.get("expires_at",0):
        #    return config #token is still valid
        
        print("[INFO] refreshing access tokens...")
        credentials = f"{config['client_id']}:{config['client_secret']}"
        base64_credentials = base64.b64encode(credentials.encode()).decode()
        headers = {
            "Authorization": f"Basic {base64_credentials}",
            "Content-Type": "application/x-www-form-urlencoded"
            }
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": config["refresh_token"],
        }
        response = requests.post(self.token_url, headers=headers, data=payload)
        tokens = response.json()

        if "access_token" in tokens:
            print("[INFO] token refreshed successfully")
            config.update({
                "access_token": tokens["access_token"],
                "refresh_token": tokens["refresh_token"],
                "expires_at": time.time() + tokens.get("expires_in", 1800)
            })
            with open(self.config_path, "w") as file:
                json.dump(config, file, indent=4)
            return config
        else:
            print("[ERROR] Failed to refresh token:", response.text)
            return None
    
    def get_access_token(self):
        """get valid 'access_token'."""
        config = self.refresh_tokens()
        return config.get("access_token") if config else None
    
    def _send_request(self, endpoint, method="GET", payload=None):
        """
        Internal helper method to send API requests.
        """
        access_token =  self.get_access_token()
        if not access_token:
            print("[ERROR] No valid access token available.")
            return None
        
        url = f"{self.base_url}{endpoint}"
        headers_post = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        headers_get = {
            "Authorization": f"Bearer {access_token}"
        }

        try:
            if method == "GET":
                response = requests.get(url, headers=headers_get)
            elif method == "POST":
                response = requests.post(url, headers=headers_post, json=payload)
            else:
                raise ValueError("Unsupported HTTP method")
            print(f"[DEBUG] Full Response Text: {response.text}")
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] API request failed: {e}")
            return None

    def get_account_details(self):
        """Fetches the account details, including settled cash."""
        if "account_id" not in self.config:
            account_data = self._send_request("/accounts/accountNumbers")
            if account_data:
                self.config["account_id"] = account_data[0]["hashValue"]
                with open(self.config_path, "w") as file:
                    json.dump(self.config, file, indent=4)
            else:
                print("[ERROR] Unable to fetch account ID.")
                return None
        return self._send_request(f"/accounts/{self.config['account_id']}")

    def get_settled_cash(self):
        """
        Fetches the available settled cash for trading.

        Returns:
            float: Amount of settled cash available.
        """
        access_token = self.get_access_token()
        if not access_token:
            print("[ERROR] Failed to refresh tokens. Exiting...")
            return 0.0
        account_details = self.get_account_details()
        if not account_details:
            print("[ERROR] Failed to fetch account details.")
            return 0.0  # Return 0.0 if API call fails
        reserve = 0 #25000
        settled_cash = account_details['securitiesAccount']['projectedBalances']['availableFunds']
        portfolio_value = account_details['securitiesAccount']['currentBalances']['liquidationValue']

        if settled_cash > reserve:
            settled_cash_less_reserve = settled_cash - reserve
            portfolio_value_less_reserve = portfolio_value - reserve
        else:
            return 0.0

        one_tenth = portfolio_value_less_reserve / 10
        available_cash = min(settled_cash_less_reserve, one_tenth)

        print(f"[INFO] Settled cash available: ${available_cash:.2f}")
        return available_cash

    def get_schwab_stock_prices(self, tickers):
        """
        Fetches real-time stock prices for the given tickers.

        Args:
            tickers (list): List of stock ticker symbols.

        Returns:
            dict: Dictionary mapping tickers to current prices.
        """
        access_token = self.get_access_token()
        if not access_token:
            print("[ERROR] Failed to refresh tokens. Exiting...")
            return {}
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        all_prices = {}
        batch_size=1
        total_batches = (len(tickers) + batch_size - 1) // batch_size
        for i in range(total_batches):
            batch = tickers[i * batch_size:(i + 1) * batch_size]
            tickers_str = ",".join(batch)
            endpoint = f"/marketdata/quotes?symbols={tickers_str}"
            response = self._send_request(endpoint)
            print(f"[DEBUG] URL: {url}")
            print(f"[DEBUG] Response Status Code: {response.status_code}")
            print(f"[DEBUG] Response Text: {response.text[:200]}... (truncated)")

            if response.status_code == 200:
                batch_prices = response.json().get('quotes', [])
                for quote in batch_prices:
                    symbol = quote.get('symbol')
                    price = quote.get('lastPrice')
                    if symbol and price:
                        all_prices[symbol] = float(price)
            else:
                print(f"[ERROR] Failed to fetch prices for batch {i + 1}: {response.text}")

        print(f"[INFO] Successfully fetched prices for {len(all_prices)} symbols.")
        return all_prices

    def get_polygon_stock_prices(self, tickers):
        if not self.polygon_api_key:
            print("[ERROR] Polygon API key is missing in config.json.")
            return {}
        stocks_client = polygon.StocksClient(self.polygon_api_key, connect_timeout = 240, read_timeout = 240)
        all_prices = {}
        eastern_tz = pytz.timezone("America/New_York")
        today_eastern = datetime.now(eastern_tz).strftime("%Y-%m-%d")
        for ticker in tickers:
            try:
                response = stocks_client.get_aggregate_bars(
                    ticker, 
                    "2025-03-07", #today_eastern, 
                    "2025-03-07", #today_eastern, 
                    timespan='day', 
                    full_range=False, 
                    run_parallel=False
                )
                price = response['results'][0]['c']
                all_prices[ticker] = price
                print(f"[INFO] {ticker}: ${price}")
            except Exception as e:
                print(f"[ERROR] Failed to fetch price for {ticker}: {str(e)}")

        print(f"[INFO] Successfully fetched prices for {len(all_prices)} symbols.")
        return all_prices

    def check_if_open(self):
        if not self.polygon_api_key:
            print("[ERROR] Polygon API key is missing in config.json.")
            return False

        reference_client = polygon.ReferenceClient(self.polygon_api_key, connect_timeout = 240, read_timeout = 240)
        try:
            response = reference_client.get_market_status()
            if response['market'] == 'closed' and response['afterHours']:
                print("[INFO] Market was open earlier today.")
                return True
            else:
                print("[INFO] Market was not open today.")
                return False
        except Exception as e:
            print(f"[ERROR] Failed to check market status: {str(e)}")
            return False

    def get_previous_closes(self, tickers):
        """
        Fetches previous day's closing prices for the given tickers.

        Args:
            tickers (list): List of stock ticker symbols.

        Returns:
            dict: Dictionary mapping tickers to their previous close prices.
        """
        access_token = self.get_access_token()
        if not access_token:
            print("[ERROR] Failed to refresh tokens. Exiting...")
            return {}
        tickers_str = ",".join(tickers)
        endpoint = f"/v1/marketdata/quotes?symbols={tickers_str}"
        response = self._send_request(endpoint)

        if response and "quotes" in response:
            return {quote["symbol"]: float(quote["previousClose"]) for quote in response["quotes"]}

        print("[ERROR] Failed to fetch previous close prices.")
        return {}

    def place_1st_triggers_sequential_order(self, ticker, quantity, limit_price, cancel_price, trigger_price, trailing_percent):
        """
        Place a 1st Triggers Sequential Order with Charles Schwab's API.

        Args:
            ticker (str): Stock ticker symbol.
            quantity (int): Number of shares to buy.
            limit_price (float): Limit price for the buy order.
            cancel_price (float): Price above which the buy order will be canceled.
            trigger_price (float): Price to trigger the trailing stop order.
            trailing_percent (float): Percentage for the trailing stop.

        Returns:
            dict: API response.
        """
        access_token = self.get_access_token()
        if not access_token:
            print("[ERROR] Failed to refresh tokens. Exiting...")
            return None
        # Define the 1st order (limit buy order)
        if quantity > 1:
            first_order = {
                "session": "SEAMLESS",
                "duration": "END_OF_WEEK",
                "orderType": "LIMIT",
                "price": limit_price,
                "orderStrategyType": "TRIGGER",
                "orderLegCollection": [
                    {
                        "instruction": "BUY",
                        "quantity": quantity,
                        "instrument": {
                            "symbol": ticker,
                            "assetType": "EQUITY"
                        }
                    }
                ],
                "childOrderStrategies":[
                    {
                        "orderType": "STOP",
                        "session": "NORMAL",
                        "stopPrice": trigger_price,
                        "duration": "GOOD_TILL_CANCEL",
                        "orderStrategyType": "TRIGGER",
                        "orderLegCollection": [
                            {
                                "instruction": "SELL",
                                "quantity": 1,
                                "instrument": {
                                    "symbol": ticker,
                                    "assetType": "EQUITY"
                                }
                            }
                        ],
                        "childOrderStrategies": [
                            {
                                "orderType": "TRAILING_STOP",
                                "session": "NORMAL",
                                "stopPriceLinkBasis": "LAST",
                                "stopPriceLinkType": "PERCENT",
                                "stopPriceOffset": "0.50",
                                "duration": "GOOD_TILL_CANCEL",
                                "orderStrategyType": "SINGLE",
                                "orderLegCollection": [{
                                    "instruction": "SELL",
                                    "quantity": quantity-1,
                                    "instrument": {
                                        "symbol": ticker,
                                        "assetType": "EQUITY"
                                        }
                                    }
                                ]
                            }
                        ]
                    }  
                ]                    
            }
        else:
            first_order = {
                "session": "SEAMLESS",
                "duration": "DAY",
                "orderType": "LIMIT",
                "price": limit_price,
                "orderStrategyType": "TRIGGER",
                "orderLegCollection": [
                    {
                        "instruction": "BUY",
                        "quantity": quantity,
                        "instrument": {
                            "symbol": ticker,
                            "assetType": "EQUITY"
                        }
                    }
                ],
                "childOrderStrategies":[
                    {
                        "orderType": "STOP",
                        "session": "NORMAL",
                        "stopPrice": trigger_price,
                        "duration": "GOOD_TILL_CANCEL",
                        "orderStrategyType": "TRIGGER",
                        "orderLegCollection": [
                            {
                                "instruction": "SELL",
                                "quantity": quantity,
                                "instrument": {
                                    "symbol": ticker,
                                    "assetType": "EQUITY"
                                }
                            }
                        ]
                    }
                ]
            }
                         
        order_payload = json.dumps(first_order)
        # Send the request to Schwab's API
        url = f"{self.base_url}/accounts/{self.config['account_id']}/orders"
        access_token = self.get_access_token()
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        response = requests.post(url, headers=headers, data=order_payload)

        if response.status_code == 201:
            return response.json()
        else:
            response.raise_for_status()

    def get_open_orders(self, from_entered_time=None, to_entered_time=None):
        # Fetch open orders with optional fromEnteredTime and toEnteredTime parameters
        utc = pytz.UTC
        now = datetime.now(utc)
        
        if not from_entered_time:
            # Format: yyyy-MM-dd'T'HH:mm:ss.SSSZ (with milliseconds and Z for UTC)
            from_entered_time = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        if not to_entered_time:
            # Format: yyyy-MM-dd'T'HH:mm:ss.SSSZ (with milliseconds and Z for UTC)
            to_entered_time = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        
        endpoint = f"/orders?fromEnteredTime={from_entered_time}&toEnteredTime={to_entered_time}"
        return self._send_request(endpoint)

    def cancel_order(self, order_id):
        # Cancel a specific order
        return self._send_request(f"/orders/{order_id}", method="DELETE")

    def place_market_sell_order(self, ticker, quantity):
        # Place a market sell order
        order = {
            "symbol": ticker,
            "quantity": quantity,
            "orderType": "MARKET",
            "instruction": "SELL"
        }
        return self._send_request("/orders", method="POST", payload=order)

    def place_market_buy_order(self, symbol, quantity):
        order = {
            "orderType": "MARKET",
            "session": "NORMAL",
            "duration": "DAY",
            "orderStrategyType": "SINGLE",
            "orderLegCollection": [
                {
                    "instruction": "BUY",
                    "quantity": quantity,
                    "instrument": {
                        "symbol": symbol,
                        "assetType": "EQUITY"
                    }
                }
            ]
        }
        return self._send_request(
            f"/accounts/{self.account_id}/orders",
            method="POST",
            data=order
        )

    def place_limit_order(self, symbol, quantity, side, limit_price):
        order = {
            "orderType": "LIMIT",
            "price": round(limit_price, 2),
            "session": "NORMAL",
            "duration": "DAY",
            "orderStrategyType": "SINGLE",
            "orderLegCollection": [
                {
                    "instruction": side.upper(),  # "BUY" or "SELL"
                    "quantity": quantity,
                    "instrument": {
                        "symbol": symbol,
                        "assetType": "EQUITY"
                    }
                }
            ]
        }
        return self._send_request(
            f"/accounts/{self.account_id}/orders",
            method="POST",
            data=order
        )

    def get_order_status(self, order_id):
        return self._send_request(
            f"/accounts/{self.account_id}/orders/{order_id}"
        )

    def get_position(self, symbol):
        account = self.get_account_details()
        positions = account.get("securitiesAccount", {}).get("positions", [])
        for pos in positions:
            if pos["instrument"]["symbol"].upper() == symbol.upper():
                return {
                    "qty": pos["longQuantity"],
                    "avgPrice": pos.get("averagePrice", None)
                }
        return {"qty": 0}