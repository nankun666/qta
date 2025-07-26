# analyze_daily_data.py

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

symbols = ["SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "GOOGL", "META", "JPM", "BAC", "XOM", "CVX", "AMZN", "WMT", "JNJ"]
data_folder = './data'
plot_folder = './plots'
os.makedirs(plot_folder, exist_ok=True)

combined_df = pd.DataFrame()

for symbol in symbols:
    try:
        df = pd.read_csv(os.path.join(data_folder, f"{symbol}_daily.csv"))
        df['Date'] = pd.to_datetime(df['Date'])
        df.sort_values('Date', inplace=True)
        df.set_index('Date', inplace=True)

        # 计算日收益率
        df['Return'] = df['close'].pct_change()

        # 累计收益率
        df['Cumulative_Return'] = (1 + df['Return']).cumprod() - 1

        # 多窗口年化波动率
        windows = [10, 20, 30, 90, 120, 252]
        for window in windows:
            df[f'Vol_{window}d'] = df['Return'].rolling(window=window).std() * np.sqrt(252)

        # 计算均线
        df['MA10'] = df['close'].rolling(window=10).mean()
        df['MA20'] = df['close'].rolling(window=20).mean()
        df['MA50'] = df['close'].rolling(window=50).mean()
        df['MA200'] = df['close'].rolling(window=200).mean()

        # === 绘制日收益率 + 累计收益率 + 多窗口波动率图 ===
        plt.figure(figsize=(14, 7))
        ax1 = plt.gca()
        ax2 = ax1.twinx()

        ax1.plot(df.index, df['Return'], color='grey', alpha=0.4, label='Daily Return')
        ax1.plot(df.index, df['Cumulative_Return'], color='green', label='Cumulative Return')

        colors = ['red', 'orange', 'blue', 'purple', 'brown', 'black']
        for color, window in zip(colors, windows):
            ax2.plot(df.index, df[f'Vol_{window}d'], color=color, alpha=0.6, label=f'Vol {window}D')

        ax1.set_xlabel('Date')
        ax1.set_ylabel('Return / Cumulative Return')
        ax2.set_ylabel('Annualized Volatility')

        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        plt.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=9)

        plt.title(f"{symbol} Daily Return, Cumulative Return & Rolling Volatility (10,20,30,90,120,252D)")
        plt.tight_layout()
        plt.savefig(os.path.join(plot_folder, f"{symbol}_combined_return_volatility.png"))
        plt.close()

        # === 新增：绘制价格+均线+成交量复合图 ===
        fig, (ax_price, ax_vol) = plt.subplots(2, 1, figsize=(14, 9), sharex=True, gridspec_kw={'height_ratios': [3, 1]})

        # 价格及均线
        ax_price.plot(df.index, df['close'], label='Close Price', color='black')
        ax_price.plot(df.index, df['MA10'], label='MA10')
        ax_price.plot(df.index, df['MA20'], label='MA20')
        ax_price.plot(df.index, df['MA50'], label='MA50')
        ax_price.plot(df.index, df['MA200'], label='MA200')
        ax_price.set_ylabel('Price')
        ax_price.set_title(f'{symbol} Price & Moving Averages')
        ax_price.legend(loc='upper left', fontsize=9)
        ax_price.grid(True)

        # 成交量柱状图
        ax_vol.bar(df.index, df['volume'], color='grey', alpha=0.6)
        ax_vol.set_ylabel('Volume')
        ax_vol.grid(True)

        plt.tight_layout()
        plt.savefig(os.path.join(plot_folder, f"{symbol}_price_volume_ma.png"))
        plt.close()

        # 保存 CSV（收益率 + 波动率）
        output_cols = ['Return', 'Cumulative_Return'] + [f'Vol_{window}d' for window in windows]
        df[output_cols].dropna().to_csv(os.path.join(data_folder, f"{symbol}_returns_volatility.csv"))

        # 合并 Return 列用于后续相关性矩阵
        combined_df[symbol] = df['Return']

        print(f"✅ Processed {symbol}")

    except Exception as e:
        print(f"⚠️ Error processing {symbol}: {e}")

# 相关性矩阵及热力图
combined_df.dropna(inplace=True)
corr_matrix = combined_df.corr()

plt.figure(figsize=(12, 10))
sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', fmt=".2f")
plt.title("Correlation Heatmap of Daily Returns")
plt.tight_layout()
plt.savefig(os.path.join(plot_folder, "correlation_heatmap.png"))
plt.close()

corr_matrix.to_csv(os.path.join(data_folder, "correlation_matrix.csv"))

print("\n✅ Analysis Completed: Combined charts, CSVs, and correlation matrix saved.")
