import time
from broker_api.broker_api import BrokerAPI  # Adjust this path if needed

class TradeExecutor:
    def __init__(self, account_id, api_key=None, capital_per_trade=1000, dry_run=False):
        self.dry_run = dry_run
        self.account_id = account_id
        self.capital_per_trade = capital_per_trade

        if not dry_run:
            from broker_api.broker_api import BrokerAPI
            self.api = BrokerAPI(account_id)
        else:
            self.api = None  # no live connection

    def place_marketable_limit_order_with_failover(
        self, symbol, side, price, ask=None, bid=None, atr=None, fallback="market"
    ):
        qty = int(self.capital_per_trade // price)
        if qty <= 0:
            print(f"[ERROR] {side.upper()} skipped: {symbol} price too high")
            return None

        if self.dry_run:
            print(f"[DRY RUN] Simulated {side.upper()} {qty} shares of {symbol} @ {price}")
            return {"ticker": symbol, "qty": qty, "side": side, "order_id": "SIMULATED"}

        base_buffer = min(0.01, atr / 100 if atr else 0.01)
        retry_buffer = base_buffer * 3

        limit_price = (ask + base_buffer) if side == "buy" else (bid - base_buffer)
        try:
            order_id = self.api.place_limit_order(
                symbol=symbol,
                quantity=qty,
                side=side,
                limit_price=round(limit_price, 2)
            )
            print(f"[{side.upper()}] Limit @ {round(limit_price, 2)} placed (qty={qty})")

            time.sleep(3)

            status = self.api.get_order_status(order_id)
            if status.lower() == "filled":
                print(f"[{symbol}] Order filled at limit")
                return {"ticker": symbol, "qty": qty, "side": side, "order_id": order_id}

            # Cancel and retry
            self.api.cancel_order(order_id)
            print(f"[{symbol}] Limit not filled — canceled")

            if fallback == "market":
                order_id = self.api.place_market_order(
                    symbol=symbol,
                    quantity=qty,
                    side=side
                )
                print(f"[{side.upper()}] Fallback: Market order sent")
                return {"ticker": symbol, "qty": qty, "side": side, "order_id": order_id}

            elif fallback == "widen":
                wider_price = (ask + retry_buffer) if side == "buy" else (bid - retry_buffer)
                order_id = self.api.place_limit_order(
                    symbol=symbol,
                    quantity=qty,
                    side=side,
                    limit_price=round(wider_price, 2)
                )
                print(f"[{side.upper()}] Fallback: Widened limit @ {round(wider_price, 2)}")
                return {"ticker": symbol, "qty": qty, "side": side, "order_id": order_id}

        except Exception as e:
            print(f"[ERROR] {side.upper()} order failed for {symbol}: {e}")
            return None

    def sell_all(self, symbol, price, fallback="market", atr=None):
        if self.dry_run:
            print(f"[DRY RUN] Simulated SELL ALL for {symbol} @ {price}")
            return {"ticker": symbol, "qty": 100, "side": "sell", "order_id": "SIMULATED"}

        try:
            pos = self.api.get_position(symbol)
            qty = int(pos.get("qty", 0))
            if qty <= 0:
                print(f"[SELL] No position to sell for {symbol}")
                return None

            bid = price
            return self.place_marketable_limit_order_with_failover(
                symbol=symbol,
                side="sell",
                price=price,
                bid=bid,
                atr=atr,
                fallback=fallback
            )

        except Exception as e:
            print(f"[ERROR] sell_all failed for {symbol}: {e}")
            return None

    def cancel_all_orders(self):
        if self.dry_run:
            print("[DRY RUN] Simulated cancel_all_orders()")
            return

        try:
            self.api.cancel_all_orders()
            print("[CANCEL] All open orders canceled")
        except Exception as e:
            print(f"[ERROR] Cancel all orders failed: {e}")