import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from glob import glob

plt.style.use('seaborn')

trade_folder = './trade_log'
plot_folder = './plots'
strategy_folder = './strategy'

os.makedirs(strategy_folder, exist_ok=True)
os.makedirs(plot_folder, exist_ok=True)

risk_free_rate = 0.02  # for Sharpe Ratio

def calculate_daily_metrics(df, symbol):
    df['Datetime'] = pd.to_datetime(df['Datetime'])
    df.sort_values('Datetime', inplace=True)
    df['Date'] = df['Datetime'].dt.date
    df.set_index('Datetime', inplace=True)

    df['Signed_Shares'] = np.where(df['Action'] == 'BUY', df['Shares'], -df['Shares'])
    df['Position'] = df['Signed_Shares'].cumsum()
    df['Equity'] = df['Cash_Remaining'] + df['Position'] * df['Price']
    df['PnL'] = df['Equity'].diff().fillna(0)
    df['Return'] = df['Equity'].pct_change().fillna(0)
    df['Drawdown'] = df['Equity'] / df['Equity'].cummax() - 1

    daily_metrics = []
    unique_dates = df['Date'].unique()

    for current_date in unique_dates:
        subset = df[df['Date'] <= current_date]
        daily_df = df[df['Date'] == current_date]
        if daily_df.empty:
            continue

        equity_start = subset['Equity'].iloc[0]
        equity_end = subset['Equity'].iloc[-1]
        cumulative_return = equity_end / equity_start - 1
        ann_return = (1 + cumulative_return) ** (252 / len(set(subset['Date']))) - 1
        ann_vol = subset['Return'].std() * np.sqrt(252)
        sharpe = (ann_return - risk_free_rate) / ann_vol if ann_vol != 0 else np.nan
        max_dd = subset['Drawdown'].min()

        trades_so_far = subset[subset['Action'].isin(['BUY', 'SELL'])]
        wins = trades_so_far[trades_so_far['PnL'] > 0]['PnL']
        losses = trades_so_far[trades_so_far['PnL'] < 0]['PnL']
        win_rate = len(wins) / (len(wins) + len(losses)) if (len(wins) + len(losses)) > 0 else np.nan
        avg_win = wins.mean() if not wins.empty else 0
        avg_loss = losses.mean() if not losses.empty else 0
        profit_factor = -avg_win / avg_loss if avg_loss != 0 else np.nan
        trade_count = len(trades_so_far)

        daily_metrics.append({
            'Date': pd.to_datetime(current_date),
            'Symbol': symbol,
            'Total Return': round(cumulative_return, 6),
            'Annualized Return': round(ann_return, 6),
            'Annualized Volatility': round(ann_vol, 6),
            'Sharpe Ratio': round(sharpe, 4) if not np.isnan(sharpe) else np.nan,
            'Max Drawdown': round(max_dd, 6),
            'Win Rate': round(win_rate, 4) if not np.isnan(win_rate) else np.nan,
            'Avg Win': round(avg_win, 4),
            'Avg Loss': round(avg_loss, 4),
            'Profit Factor': round(profit_factor, 4) if not np.isnan(profit_factor) else np.nan,
            'Trade Count': trade_count
        })

    metrics_df = pd.DataFrame(daily_metrics)

    export_df = df[['Equity', 'PnL', 'Drawdown']].copy()
    export_df['Symbol'] = symbol
    export_df.reset_index(inplace=True)

    return metrics_df, df, df['Drawdown'], export_df

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

# =========== 主程序入口 ===========
all_metrics = []
all_equity_data = []

log_files = glob(os.path.join(trade_folder, "*_trade_log.csv"))

for file in log_files:
    try:
        symbol = os.path.basename(file).split('_trade_log.csv')[0]
        df = pd.read_csv(file)

        metrics_df, df_processed, drawdown, export_df = calculate_daily_metrics(df, symbol)

        all_metrics.append(metrics_df)
        all_equity_data.append(export_df)

        # 单独保存每个 symbol 的 daily metrics
        metrics_df.to_csv(os.path.join(strategy_folder, f"{symbol}_daily_metrics.csv"), index=False)

        plot_equity_and_drawdown(df_processed, drawdown, symbol)
        plot_pnl_distribution(df_processed, symbol)

        print(f"✅ Processed {symbol}")

    except Exception as e:
        print(f"⚠️ Error processing {file}: {e}")

# 合并保存
if all_metrics:
    combined_metrics = pd.concat(all_metrics, ignore_index=True)
    combined_metrics.to_csv(os.path.join(strategy_folder, "daily_trade_metrics_all.csv"), index=False)

if all_equity_data:
    combined_df = pd.concat(all_equity_data, ignore_index=True)
    combined_df.to_csv(os.path.join(strategy_folder, "equity_drawdown_pnl_all.csv"), index=False)

print("\n✅ Trade performance analysis completed. Results saved.")
