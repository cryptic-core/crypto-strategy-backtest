import pandas as pd
from scipy import stats
import math
import os
from performence_matrix import calculate_metrics, plot_results

# 定义计算每行的ahr999值的函数
def calculate_ahr999_for_each_row(row, df):
    low_price = row['low']  # 获取该行的最低价
    inception_date = pd.Timestamp('2009-01-03 00:00:00')  # 比特币诞生日期
    day = (row['date'] - inception_date).total_seconds() / (24 * 60 * 60)  # 计算距离比特币诞生的天数
    if day <= 0:
        raise ValueError(f"Invalid day value {day} for date {row['Date']}")  # 如果天数不合理，则抛出异常

    # 计算截止到该行日期的数据的几何平均值
    start_date = row['date'] - pd.Timedelta(days=199)  # 计算起始日期
    window_df = df[(df['date'] >= start_date) & (df['date'] <= row['date'])]  # 获取对应时间窗口的数据
    geomean = stats.gmean(window_df['low'])  # 计算几何平均值

    coinPrice = 10 ** (5.84 * math.log10(day) - 17.01)  # 根据公式计算比特币价格
    ahr_value = (low_price / geomean) * (low_price / coinPrice)  # 计算ahr999值
    return ahr_value

# 定义给数据帧添加ahr999列的函数
def add_ahr999_column_to_dataframe(df, start_date=None, end_date=None):
    df['date'] = pd.to_datetime(df['date'])  # 将日期列转换为日期时间格式
    df.sort_values(by='date', inplace=True)  # 按日期排序

    if start_date is not None:
        df = df[df['date'] >= pd.to_datetime(start_date)]  # 筛选起始日期后的数据
    if end_date is not None:
        df = df[df['date'] <= pd.to_datetime(end_date)]  # 筛选结束日期前的数据

    df['ahr999'] = df.apply(lambda row: calculate_ahr999_for_each_row(row, df), axis=1)  # 计算每行的ahr999值并添加为新列
    return df


# 绘制比特币价格和AHR999指数图
# draw_btc_and_ahr999_plot(btc_data, title="BTC Price and AHR999 Index Over Time")


def run_ahr999_backtest(df):
    # Initialize portfolio tracking
    df['btc_bought'] = 0.0  # Amount of BTC bought each day
    df['usd_invested'] = 0.0  # USD invested each day
    df['total_btc'] = 0.0  # Running total of BTC held
    df['total_usd_invested'] = 0.0  # Running total of USD invested
    df['portfolio_value'] = 0.0  # Current value of holdings
    df['net_pnl'] = 0.0  # Net profit/loss
    
    # Convert date to datetime if it's not already
    start_date = '2015-01-01'
    end_date = '2024-02-20'
    df = add_ahr999_column_to_dataframe(df, start_date, end_date)
    df['date'] = pd.to_datetime(df['date'])
    total_btc = 0.0
    total_usd_invested = 0.0
    in_buy_cycle = False
    trade_count = 0
    buy_threshold = 0.45
    # Iterate through each row
    for index, row in df.iterrows():
        # Check if it's the 15th of the month
        if not in_buy_cycle and row['ahr999'] <= buy_threshold:
            usd_amount = 1000  # ahr999 amount
            btc_bought = usd_amount / row['close']  # Calculate BTC amount bought
            
            # Record the transaction
            df.at[index, 'btc_bought'] = btc_bought
            df.at[index, 'usd_invested'] = usd_amount
            
            # Update running totals
            total_btc += btc_bought
            trade_count += 1
            total_usd_invested += usd_amount
        
        if in_buy_cycle and row['ahr999'] > buy_threshold:
            in_buy_cycle = False
        # Update running totals for each day
        df.at[index, 'total_btc'] = total_btc
        df.at[index, 'total_usd_invested'] = total_usd_invested
        df.at[index, 'portfolio_value'] = total_btc * row['close']
        df.at[index, 'net_pnl'] = (total_btc * row['close']) - total_usd_invested
    
    # Print summary statistics
    print("\nDCA Backtest Results:")
    print(f"Total BTC accumulated: {total_btc:.8f}")
    print(f"Total USD invested: ${total_usd_invested:,.2f}")
    print(f"Total trades: {trade_count}")
    final_value = df['portfolio_value'].iloc[-1]
    final_pnl = df['net_pnl'].iloc[-1]
    print(f"Final portfolio value: ${final_value:,.2f}")
    print(f"Net P&L: ${final_pnl:,.2f}")
    print(f"Return on Investment: {(final_pnl/total_usd_invested)*100:.2f}%")
    
    # Save detailed results to CSV
    df.to_csv('dca_backtest_results.csv', index=False)
    
    return df 

if __name__ == '__main__':
    df = pd.read_csv(os.getcwd()+'/data/candlestick_data/BTC-USDT.csv')
    df = run_ahr999_backtest(df)
    matrix = calculate_metrics(df)
    plot_results(df)
    print(df.head())
