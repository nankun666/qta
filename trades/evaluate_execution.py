import pandas as pd
import numpy as np
import os
from glob import glob

trade_folder = './trade_log'
market_folder = '../etl_pipeline/data'
output_folder = './execution'
os.makedirs(output_folder, exist_ok=True)

def clean_market_data(market_df):
    # 确保Datetime是datetime类型，转为UTC，精确到分钟
    market_df['Datetime'] = pd.to_datetime(market_df['Datetime'], utc=True).dt.floor('min')
    # 按分钟聚合：成交量求和，价格取成交量加权均价（VWAP）
    market_df['weighted_price'] = market_df['close'] * market_df['volume']
    agg_df = market_df.groupby('Datetime').agg({
        'weighted_price': 'sum',
        'volume': 'sum'
    }).reset_index()
    # 计算VWAP价格
    agg_df['close'] = agg_df['weighted_price'] / agg_df['volume']
    agg_df.drop(columns=['weighted_price'], inplace=True)
    agg_df.set_index('Datetime', inplace=True)
    return agg_df

def calculate_metrics(trade_df, market_df):
    # trade_df 必须包含: Datetime, Action (BUY/SELL), Price, Shares
    trade_df['Datetime'] = pd.to_datetime(trade_df['Datetime'], utc=True).dt.floor('min')
    trade_df.sort_values('Datetime', inplace=True)

    # 计算滑点: (成交价格 - 当分钟市场VWAP价格) * 交易数量
    slippages = []
    vwap_prices = []
    twap_prices = []
    participation_rates = []

    total_market_volume = market_df['volume'].sum()
    total_trade_volume = trade_df['Shares'].sum()

    # TWAP简单按交易均价计算
    twap_price = (trade_df['Price'] * trade_df['Shares']).sum() / total_trade_volume if total_trade_volume > 0 else np.nan

    for idx, row in trade_df.iterrows():
        ts = row['Datetime']
        trade_price = row['Price']
        shares = row['Shares']

        if ts not in market_df.index:
            # 找不到对应行情分钟，跳过或用前一分钟价格填充
            vwap_price = market_df['close'].ffill().loc[ts] if ts in market_df.index else np.nan
        else:
            vwap_price = market_df.loc[ts, 'close']

        slippage = (trade_price - vwap_price) * shares
        slippages.append(slippage)
        vwap_prices.append(vwap_price)

    trade_df['Market_VWAP'] = vwap_prices
    trade_df['Slippage'] = slippages

    # 参与率 = 交易数量 / 市场成交量
    # 注意：需要对应分钟市场成交量
    participation_rates = []
    for idx, row in trade_df.iterrows():
        ts = row['Datetime']
        shares = row['Shares']
        if ts in market_df.index:
            mkt_vol = market_df.loc[ts, 'volume']
            pr = shares / mkt_vol if mkt_vol > 0 else np.nan
        else:
            pr = np.nan
        participation_rates.append(pr)

    trade_df['Participation_Rate'] = participation_rates

    # 汇总执行指标
    total_slippage = trade_df['Slippage'].sum()
    avg_slippage_per_share = total_slippage / total_trade_volume if total_trade_volume > 0 else np.nan

    metrics = {
        'Total Trades': len(trade_df),
        'Total Shares Traded': total_trade_volume,
        'Total Market Volume': total_market_volume,
        'Average Participation Rate': trade_df['Participation_Rate'].mean(),
        'Total Slippage ($)': total_slippage,
        'Average Slippage per Share ($)': avg_slippage_per_share,
        'VWAP of Market': market_df['close'].mean(),
        'TWAP of Trades': twap_price
    }

    return trade_df, metrics

def main():
    trade_files = glob(os.path.join(trade_folder, "*_trade_log.csv"))

    for trade_file in trade_files:
        try:
            symbol = os.path.basename(trade_file).split('_trade_log.csv')[0]

            # 读取交易日志
            trade_df = pd.read_csv(trade_file)
            # 读取对应市场行情
            market_file = os.path.join(market_folder, f"{symbol}_minute.csv")
            if not os.path.exists(market_file):
                print(f"⚠️ Market data for {symbol} not found, skipping.")
                continue
            market_df = pd.read_csv(market_file)

            # 清洗行情数据，聚合去重
            market_df = clean_market_data(market_df)

            # 计算执行指标
            trade_df, metrics = calculate_metrics(trade_df, market_df)

            # 保存带指标的交易日志
            trade_df.to_csv(os.path.join(output_folder, f"{symbol}_trade_metrics.csv"), index=False)

            # 保存指标汇总
            metrics_df = pd.DataFrame([metrics])
            metrics_df.insert(0, 'Symbol', symbol)
            metrics_df.to_csv(os.path.join(output_folder, f"{symbol}_execution_summary.csv"), index=False)

            print(f"✅ Processed {symbol}")

        except Exception as e:
            print(f"⚠️ Error processing {trade_file}: {e}")

if __name__ == "__main__":
    main()
