import os
import alpaca_trade_api as tradeapi

API_KEY = os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
BASE_URL = "https://paper-api.alpaca.markets"

api = tradeapi.REST(API_KEY, SECRET_KEY, BASE_URL)

account = api.get_account()
print(account)

barset = api.get_bars('AAPL', tradeapi.rest.TimeFrame.Minute, limit=5).df
print(barset)