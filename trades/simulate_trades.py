# simulate_trades.py

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

symbols = ["SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "GOOGL", "META", "JPM", "BAC", "XOM", "CVX", "AMZN", "WMT", "JNJ"]
data_folder = '../etl_pipeline/data'
plot_folder = './plots'
trade_folder = './trade_log'   # ✅ 新增
os.makedirs(plot_folder, exist_ok=True)
os.makedirs(trade_folder, exist_ok=True)  # ✅ 新增

initial_capital = 100000
capital_per_stock = initial_capital / len(symbols)

portfolio_value = pd.DataFrame()
combined_trade_logs = []

for symbol in symbols:
    try:
        df = pd.read_csv(os.path.join(data_folder, f"{symbol}_minute.csv"))
        df['Datetime'] = pd.to_datetime(df['Datetime'])
        df.sort_values('Datetime', inplace=True)
        df = df[~df['Datetime'].duplicated()]  # ✅ 去重保证索引唯一
        df.set_index('Datetime', inplace=True)

        # 策略: 5min 均线上穿 20min 均线买入，下穿卖出
        df['MA5'] = df['close'].rolling(window=5).mean()
        df['MA20'] = df['close'].rolling(window=20).mean()
        df['Signal'] = np.where(df['MA5'] > df['MA20'], 1, 0)
        df['Position'] = df['Signal'].diff()

        position = 0
        cash = capital_per_stock
        shares = 0
        equity_curve = []
        trade_logs = []

        for idx, row in df.iterrows():
            price = row['close']
            if row['Position'] == 1 and cash > 0:
                shares = cash // price
                cash -= shares * price
                trade_logs.append([symbol, idx, 'BUY', price, shares, cash])
            elif row['Position'] == -1 and shares > 0:
                cash += shares * price
                trade_logs.append([symbol, idx, 'SELL', price, shares, cash])
                shares = 0
            equity = cash + shares * price
            equity_curve.append(equity)

        df['Equity'] = equity_curve
        portfolio_value[symbol] = df['Equity']

        # 保存个股交易日志 ✅
        trade_log_df = pd.DataFrame(trade_logs, columns=['Symbol', 'Datetime', 'Action', 'Price', 'Shares', 'Cash_Remaining'])
        trade_log_df.to_csv(os.path.join(trade_folder, f'{symbol}_trade_log.csv'), index=False)

        # 合并到组合日志 ✅
        combined_trade_logs.extend(trade_logs)

        # 绘制个股资金曲线
        plt.figure(figsize=(12, 6))
        plt.plot(df.index, df['Equity'], label='Equity Curve')
        plt.title(f'{symbol} Simulated Trading Equity Curve')
        plt.xlabel('Time')
        plt.ylabel('Equity ($)')
        plt.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(plot_folder, f'{symbol}_equity_curve.png'))
        plt.close()

        print(f"✅ Completed simulation for {symbol}")

    except Exception as e:
        print(f"⚠️ Error processing {symbol}: {e}")

# 组合资金曲线
portfolio_value['Total'] = portfolio_value.sum(axis=1)
plt.figure(figsize=(14, 7))
plt.plot(portfolio_value.index, portfolio_value['Total'], label='Portfolio Total Equity', color='blue')
plt.title('Simulated Portfolio Total Equity Curve (12 Stocks)')
plt.xlabel('Time')
plt.ylabel('Equity ($)')
plt.legend()
plt.tight_layout()
plt.savefig(os.path.join(plot_folder, 'portfolio_total_equity_curve.png'))
plt.close()

# 保存组合交易日志 ✅
combined_trade_log_df = pd.DataFrame(combined_trade_logs, columns=['Symbol', 'Datetime', 'Action', 'Price', 'Shares', 'Cash_Remaining'])
combined_trade_log_df.to_csv(os.path.join(trade_folder, 'combined_trade_log.csv'), index=False)

print("\n✅ All simulations completed: Individual equity curves, portfolio curve, and trade logs saved to 'trade' folder.")
