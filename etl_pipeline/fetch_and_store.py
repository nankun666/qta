import alpaca_trade_api as tradeapi
import pandas as pd
import os
import sqlite3
from datetime import datetime, timedelta
import time

# ====== API 配置 ======
API_KEY = os.getenv("ALPACA_API_KEY")
SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")
BASE_URL = "https://paper-api.alpaca.markets"
api = tradeapi.REST(API_KEY, SECRET_KEY, BASE_URL)

# ====== 股票池 ======
symbols = ["SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA",
           "GOOGL", "META", "JPM", "BAC", "XOM", "CVX", "AMZN", "WMT", "JNJ"]

# ====== 路径与数据库 ======
data_folder = './data'
os.makedirs(data_folder, exist_ok=True)
db_path = os.path.join(data_folder, 'quant_market_data.db')

# 避免拉最新数据（延迟免费兼容）
end_date = datetime.now() - timedelta(days=2)
start_date = end_date - timedelta(days=90)

# ====== 拉取分钟线，分段拉取避免限流 ======
def fetch_minute_data(symbol, start_date, end_date):
    print(f"\nFetching minute data for {symbol}")
    all_dfs = []
    current_start = start_date

    while current_start < end_date:
        current_end = min(current_start + timedelta(days=5), end_date)
        try:
            df = api.get_bars(
                symbol,
                tradeapi.rest.TimeFrame.Minute,
                start=current_start.strftime('%Y-%m-%d'),
                end=current_end.strftime('%Y-%m-%d'),
                adjustment='raw'
            ).df

            df['symbol'] = symbol

            if not df.empty:
                df.reset_index(inplace=True)
                df.rename(columns={'timestamp': 'Datetime'}, inplace=True)
                all_dfs.append(df)

            time.sleep(0.3)

        except Exception as e:
            print(f"Error fetching minute data for {symbol} ({current_start.date()} - {current_end.date()}): {e}")
            time.sleep(1)

        current_start = current_end

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    else:
        return pd.DataFrame()

# ====== 拉取日线 ======
def fetch_daily_data(symbol, start_date, end_date):
    print(f"\nFetching daily data for {symbol}")
    try:
        df = api.get_bars(
            symbol,
            tradeapi.rest.TimeFrame.Day,
            start=start_date.strftime('%Y-%m-%d'),
            end=end_date.strftime('%Y-%m-%d'),
            adjustment='raw'
        ).df

        df['symbol'] = symbol
        if not df.empty:
            df.reset_index(inplace=True)
            df.rename(columns={'timestamp': 'Date'}, inplace=True)
        return df

    except Exception as e:
        print(f"Error fetching daily data for {symbol}: {e}")
        return pd.DataFrame()

# ====== 保存函数 ======
def save_to_csv(df, path):
    df.to_csv(path, index=False)
    print(f"✅ Saved CSV: {path}")

def save_to_sqlite(df, conn, table_name):
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"✅ Saved SQLite table: {table_name}")

# ====== 主程序 ======
def main():
    conn = sqlite3.connect(db_path)

    for symbol in symbols:
        # 拉取分钟线
        minute_df = fetch_minute_data(symbol, start_date, end_date)
        if not minute_df.empty:
            csv_path = os.path.join(data_folder, f'{symbol}_minute.csv')
            save_to_csv(minute_df, csv_path)
            save_to_sqlite(minute_df, conn, f'{symbol}_minute')
        else:
            print(f"⚠️ No minute data for {symbol}")

        # 拉取过去一年日线
        daily_df = fetch_daily_data(symbol, start_date - timedelta(days=365), end_date)
        if not daily_df.empty:
            csv_path = os.path.join(data_folder, f'{symbol}_daily.csv')
            save_to_csv(daily_df, csv_path)
            save_to_sqlite(daily_df, conn, f'{symbol}_daily')
        else:
            print(f"⚠️ No daily data for {symbol}")

        time.sleep(0.5)

    conn.close()
    print("\n🎯 ETL Pipeline Completed: All data fetched and saved.")

if __name__ == "__main__":
    main()
