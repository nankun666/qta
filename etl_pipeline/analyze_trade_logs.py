# analyze_trade_logs.py

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from glob import glob

plt.style.use('seaborn')

trade_folder = './trade'
plot_folder = './plots'
os.makedirs(plot_folder, exist_ok=True)

risk_free_rate = 0.02  # for Sharpe Ratio

def calculate_performance_metrics(df, symbol):
    # Ensure correct data types
    df['Datetime'] = pd.to_datetime(df['Datetime'])
    df.sort_values('Datetime', inplace=True)
    df.set_index('Datetime', inplace=True)

    # Calculate Position (shares held over time)
    df['Signed_Shares'] = np.where(df['Action'] == 'BUY', df['Shares'], -df['Shares'])
    df['Position'] = df['Signed_Shares'].cumsum()

    # Calculate Equity = Cash_Remaining + Position * Price
    df['Equity'] = df['Cash_Remaining'] + df['Position'] * df['Price']

    # Calculate PnL
    df['PnL'] = df['Equity'].diff().fillna(0)

    # Calculate returns based on Equity
    df['Return'] = df['Equity'].pct_change().fillna(0)
    cumulative_return = df['Equity'].iloc[-1] / df['Equity'].iloc[0] - 1
    annualized_return = (1 + cumulative_return) ** (252*390/len(df)) - 1

    annualized_volatility = df['Return'].std() * np.sqrt(252*390)
    sharpe_ratio = (annualized_return - risk_free_rate) / annualized_volatility if annualized_volatility != 0 else np.nan

    # Drawdown
    rolling_max = df['Equity'].cummax()
    drawdown = df['Equity'] / rolling_max - 1
    max_drawdown = drawdown.min()

    # Trade statistics per trade (entry and exit)
    trades = df[df['Action'].isin(['BUY', 'SELL'])]
    wins = trades[trades['PnL'] > 0]['PnL']
    losses = trades[trades['PnL'] < 0]['PnL']
    win_rate = len(wins) / (len(wins) + len(losses)) if (len(wins) + len(losses)) > 0 else np.nan
    avg_win = wins.mean() if not wins.empty else 0
    avg_loss = losses.mean() if not losses.empty else 0
    profit_factor = -avg_win / avg_loss if avg_loss != 0 else np.nan
    trade_count = len(trades)

    metrics = {
        'Symbol': symbol,
        'Total Return (%)': round(cumulative_return * 100, 2),
        'Annualized Return (%)': round(annualized_return * 100, 2),
        'Annualized Volatility (%)': round(annualized_volatility * 100, 2),
        'Sharpe Ratio': round(sharpe_ratio, 2) if not np.isnan(sharpe_ratio) else 'N/A',
        'Max Drawdown (%)': round(max_drawdown * 100, 2),
        'Win Rate (%)': round(win_rate * 100, 2) if not np.isnan(win_rate) else 'N/A',
        'Avg Win ($)': round(avg_win, 2),
        'Avg Loss ($)': round(avg_loss, 2),
        'Profit Factor': round(profit_factor, 2) if not np.isnan(profit_factor) else 'N/A',
        'Trade Count': trade_count
    }

    return metrics, df, drawdown

def plot_equity_and_drawdown(df, drawdown, symbol):
    plt.figure(figsize=(14, 7))
    plt.plot(df.index, df['Equity'], label='Equity Curve', color='green')
    plt.title(f"{symbol} Equity Curve")
    plt.xlabel('Datetime')
    plt.ylabel('Equity ($)')
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(plot_folder, f"{symbol}_equity_curve.png"))
    plt.close()

    plt.figure(figsize=(14, 4))
    plt.plot(drawdown.index, drawdown, label='Drawdown', color='red')
    plt.title(f"{symbol} Drawdown Curve")
    plt.xlabel('Datetime')
    plt.ylabel('Drawdown')
    plt.tight_layout()
    plt.savefig(os.path.join(plot_folder, f"{symbol}_drawdown_curve.png"))
    plt.close()

def plot_pnl_distribution(df, symbol):
    plt.figure(figsize=(8, 5))
    df['PnL'].hist(bins=50, color='skyblue', edgecolor='black')
    plt.title(f"{symbol} Trade PnL Distribution")
    plt.xlabel('PnL ($)')
    plt.ylabel('Frequency')
    plt.tight_layout()
    plt.savefig(os.path.join(plot_folder, f"{symbol}_pnl_distribution.png"))
    plt.close()

all_metrics = []

# Load and analyze all trade logs
log_files = glob(os.path.join(trade_folder, "*_trade_log.csv"))

for file in log_files:
    try:
        symbol = os.path.basename(file).split('_trade_log.csv')[0]
        df = pd.read_csv(file)

        metrics, df_processed, drawdown = calculate_performance_metrics(df, symbol)
        all_metrics.append(metrics)

        plot_equity_and_drawdown(df_processed, drawdown, symbol)
        plot_pnl_distribution(df_processed, symbol)

        print(f"✅ Processed {symbol}")

    except Exception as e:
        print(f"⚠️ Error processing {file}: {e}")

# Save metrics summary
if all_metrics:
    metrics_df = pd.DataFrame(all_metrics)
    metrics_df.to_csv(os.path.join(plot_folder, "trade_performance_summary.csv"), index=False)
    print("\n✅ Trade performance analysis completed. Results saved to plots folder.")
else:
    print("⚠️ No trade logs found for analysis.")
