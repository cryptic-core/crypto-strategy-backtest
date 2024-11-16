import sys,os
import requests
import json
import platform
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import asyncio
import aiohttp
from zipfile import ZipFile

coin_list = ['BTCUSDT',"ETHUSDT","SOLUSDT","DOGEUSDT"]

# download open interest data from binance
async def download_openinterest_data():
    for coin in coin_list:
        start_date = datetime(2021,12,1)
        end_date = datetime.now()
        date_list = [start_date + timedelta(days=x) for x in range(0, (end_date - start_date).days)]
        df_oi = pd.DataFrame(index=date_list)
        df_oi['open_interest'] = 0
        df_oi.index = pd.to_datetime(df_oi.index)
        
        for date in date_list:
            file_name = f'{coin}-metrics-{date.strftime("%Y-%m-%d")}.zip'
            download_url = f'https://data.binance.vision/data/futures/um/daily/metrics/{coin}/{file_name}'
            response = requests.get(download_url)
            
            if response.status_code == 200:
                # Save zip file
                with open(file_name, 'wb') as f:
                    f.write(response.content)
                
                csv_name = f'{coin}-metrics-{date.strftime("%Y-%m-%d")}.csv'
                # Extract and read CSV from zip
                with ZipFile(file_name, 'r') as zip_file:
                    csv_name = zip_file.namelist()[0]  # Get the CSV filename from zip
                    with zip_file.open(csv_name) as csv_file:
                        df = pd.read_csv(csv_file)
                        df_oi.loc[date] = df.iloc[-1]['sum_open_interest_value']
                # Optional: Remove zip file after processing
                os.remove(file_name)
        else:
            print(f'Failed to download data for {coin}')
        df_oi.to_csv(f'data/open_interest_data/{coin}-open_interest-data.csv')

async def download_candlestick_data():
    for coin in coin_list:
        df = pd.read_csv(f'data/open_interest_data/{coin}-open_interest-data.csv', index_col=0, parse_dates=True)
        # Convert datetime index to timestamp (milliseconds)
        startTime = int(df.index[0].timestamp() * 1000)
        endTime = int(df.index[-1].timestamp() * 1000)
        t = startTime
        df_candlesticks = pd.DataFrame(
            columns=['timestamp','open', 'high', 'low', 'close', 'volume'],
        )
        while(t <= endTime):
            url = "https://api.binance.com/api/v3/klines"
            # Set the parameters for the API request
            params = {
                'symbol': coin,
                'interval': '1d',
                'startTime': t, 
                'limit': 1000
            }
            # Make the request to the Binance API
            response = requests.get(url, params=params)
            arrobj_r = json.loads(response.text)
                
            for fds in arrobj_r:
                newrow = {
                    'timestamp': fds[0],
                    'open': float(fds[1]),
                    'high': float(fds[2]),
                    'low': float(fds[3]),
                    'close': float(fds[4]),
                    'volume': float(fds[5])
                }
                df_candlesticks = pd.concat([df_candlesticks, pd.DataFrame([newrow])], ignore_index=True)

            t = arrobj_r[-1][0]
        
        # Save the data
        df_candlesticks.index = pd.to_datetime(df_candlesticks['timestamp'], unit='ms')
        df_candlesticks.to_csv(f'data/candlestick_data/{coin}-candlestick-data.csv')

def calculate_benchmark_returns(df, initial_portfolio_size=100):
    df['benchmark_return'] = df['close'].pct_change()
    df['benchmark_cumulative'] = (1 + df['benchmark_return']).cumprod() * initial_portfolio_size
    df.loc[df.index[0], 'benchmark_cumulative'] = initial_portfolio_size  # Set the first value to the initial portfolio size
    return df[['date', 'benchmark_cumulative']]

def manage_positions(df, initial_portfolio_size=100):
    trades = []
    portfolio_size = initial_portfolio_size
    cumulative_returns = []
    current_date = df['date'].iloc[0]
    position = None
    unrealized_profit = 0
    
    for index, row in df.iterrows():
        row_date = row['date']
        while current_date < row_date:
            cumulative_returns.append({
                'date': current_date,
                'cumulative': portfolio_size + unrealized_profit
            })
            current_date += timedelta(days=1)
        
        if position is None:  # No open position
            if row['long_entry']:
                position = {'entry_price': row['close'], 'type': 'long', 'entry_date': row['date'], 'asset': 'BTCUSDT'}
            elif row['short_entry']:
                position = {'entry_price': row['close'], 'type': 'short', 'entry_date': row['date'], 'asset': 'BTCUSDT'}
        else:  # There is an open position
            if position['type'] == 'long':
                unrealized_profit = (row['close'] / position['entry_price'] - 1) * portfolio_size
            elif position['type'] == 'short':
                unrealized_profit = (position['entry_price'] / row['close'] - 1) * portfolio_size

            if (position['type'] == 'long' and row['long_exit']) or (position['type'] == 'short' and row['short_exit']):
                exit_price = row['close']
                trade_return = (exit_price / position['entry_price'] - 1) if position['type'] == 'long' else (position['entry_price'] / exit_price - 1)
                portfolio_size *= (1 + trade_return)
                trades.append({
                    'asset': position['asset'],
                    'entry_date': position['entry_date'],
                    'entry_price': position['entry_price'],
                    'exit_date': row['date'],
                    'exit_price': exit_price,
                    'return': trade_return,
                    'cumulative': portfolio_size,
                    'type': position['type']
                })
                position = None
                unrealized_profit = 0
        
        cumulative_returns.append({
            'date': row['date'],
            'cumulative': portfolio_size + unrealized_profit
        })
        current_date = row_date + timedelta(days=1)
    
    # Fill remaining dates after the last row
    while current_date <= df['date'].iloc[-1]:
        cumulative_returns.append({
            'date': current_date,
            'cumulative': portfolio_size + unrealized_profit
        })
        current_date += timedelta(days=1)
    
    return pd.DataFrame(trades), pd.DataFrame(cumulative_returns)

def calculate_drawdowns(cumulative_returns, column='cumulative', drawdown_col='drawdown'):
    cumulative_returns['peak'] = cumulative_returns[column].cummax()
    cumulative_returns[drawdown_col] = (cumulative_returns[column] / cumulative_returns['peak']) - 1
    cumulative_returns['drawdown_duration'] = (cumulative_returns[drawdown_col] < 0).astype(int).groupby((cumulative_returns[drawdown_col] == 0).cumsum()).cumsum()
    return cumulative_returns

def calculate_metrics(cumulative_returns, benchmark_returns):
    metrics = {}
    
    # Merge the cumulative returns and benchmark returns
    combined_df = pd.merge(cumulative_returns, benchmark_returns, on='date', how='left')
    
    # Fill missing values
    combined_df['benchmark_cumulative'].fillna(method='ffill', inplace=True)
    
    # Calculate daily returns as percentage difference between portfolio values
    combined_df['strategy_return'] = combined_df['cumulative'].pct_change()
    combined_df['benchmark_return'] = combined_df['benchmark_cumulative'].pct_change()
    
    # Calculate drawdowns and durations for both strategy and benchmark
    combined_df = calculate_drawdowns(combined_df, column='cumulative', drawdown_col='strategy_drawdown')
    combined_df = calculate_drawdowns(combined_df, column='benchmark_cumulative', drawdown_col='benchmark_drawdown')
    
    # Calculate metrics
    metrics['Metric'] = ['Strategy', 'Benchmark']
    metrics['Exposure Time (%)'] = [
        combined_df['strategy_return'].ne(combined_df['strategy_return'].shift()).mean() * 100,
        combined_df['benchmark_return'].ne(combined_df['benchmark_return'].shift()).mean() * 100
    ]
    metrics['Total Return (%)'] = [
        (combined_df['cumulative'].iloc[-1] / combined_df['cumulative'].iloc[0] - 1) * 100,
        (combined_df['benchmark_cumulative'].iloc[-1] / combined_df['benchmark_cumulative'].iloc[0] - 1) * 100
    ]
    
    num_days = (combined_df['date'].iloc[-1] - combined_df['date'].iloc[0]).days
    metrics['Annualized Return (%)'] = [
        ((1 + metrics['Total Return (%)'][0] / 100) ** (365 / num_days) - 1) * 100,
        ((1 + metrics['Total Return (%)'][1] / 100) ** (365 / num_days) - 1) * 100
    ]
    metrics['Annualized Volatility (%)'] = [
        combined_df['strategy_return'].std() * np.sqrt(365) * 100,
        combined_df['benchmark_return'].std() * np.sqrt(365) * 100
    ]
    metrics['Sharpe Ratio'] = [
        combined_df['strategy_return'].mean() / combined_df['strategy_return'].std() * np.sqrt(365),
        combined_df['benchmark_return'].mean() / combined_df['benchmark_return'].std() * np.sqrt(365)
    ]
    
    metrics['Max Drawdown (%)'] = [
        combined_df['strategy_drawdown'].min() * 100,
        combined_df['benchmark_drawdown'].min() * 100
    ]
    
    metrics['Return to Max DD Ratio'] = [
        metrics['Total Return (%)'][0] / (abs(metrics['Max Drawdown (%)'][0])),
        metrics['Total Return (%)'][1] / (abs(metrics['Max Drawdown (%)'][1]))
    ]
    
    return pd.DataFrame(metrics)

def backtest_oi_marketcap(coin):
    big_oi_threshold = 2
    num_days = 20
    num_days_hh = 10
    
    # Read both CSVs with datetime index
    df_marketcap = pd.read_csv(f'data/marketcap_data/btc_marketcap_data.csv',index_col=0,parse_dates=True)
    
    # Normalize total_mcap using the first row as base
    first_mcap = df_marketcap['total_mcap'].iloc[0]
    df_marketcap['mcap_norm'] = df_marketcap['total_mcap'] / first_mcap
    
    df_candlesticks = pd.read_csv(f'data/candlestick_data/BTCUSDT-candlestick-data.csv',index_col=0,parse_dates=True)
    df_oi = pd.read_csv(f'data/open_interest_data/BTCUSDT-open_interest-data.csv',index_col=0,parse_dates=True)
    df_oi['oi_norm'] =  df_oi['open_interest'] / df_oi['open_interest'].shift(1).rolling(window=num_days).mean()
    df_oi['bigoi'] = df_oi['oi_norm'] > df_oi['oi_norm'].shift(1).rolling(window=20).max()

    # Merge the dataframes on datetime index
    df_merged = pd.merge(df_candlesticks, 
                        df_oi[['bigoi']], 
                        left_index=True, 
                        right_index=True, 
                        how='inner')
    
    # Add date column from index
    df_merged['date'] = df_merged.index
    
    print(df_merged[df_merged['bigoi']])

    # Create signal column where both conditions are met
    df_merged['long_entry'] = df_merged['bigoi'] & (df_merged['low'] < df_merged['low'].rolling(window=num_days_hh).max().shift(1))
    df_merged['long_exit'] = (df_merged['high'] > df_merged['high'].rolling(window=num_days_hh).max().shift(1))
    df_merged['short_entry'] = df_merged['bigoi'] & (df_merged['high'] > df_merged['high'].rolling(window=num_days_hh).max().shift(1))
    df_merged['short_exit'] = (df_merged['low'] < df_merged['low'].rolling(window=num_days_hh).max().shift(1))

    print(f"\nBig oi increase rate events")
    # Print only rows where signal == 1
    print(df_merged[df_merged['long_entry'] == True])
    print(df_merged[df_merged['long_exit'] == True])
    print(df_merged[df_merged['short_entry'] == True])
    print(df_merged[df_merged['short_exit'] == True])
    # Optionally save the merged data
    return df_merged

if __name__ == '__main__':
    os_name = platform.system()
    mode = int(sys.argv[1])
    if mode == 0:
        asyncio.run(download_openinterest_data())
    elif mode == 1:
        asyncio.run(download_candlestick_data())
    elif mode == 2:
        for coin in coin_list:
            df = backtest_oi_marketcap(coin)
            # Calculate benchmark returns
            benchmark_returns = calculate_benchmark_returns(df)

            trades_df, cumulative_returns = manage_positions(df)
            # Calculate metrics
            metrics = calculate_metrics(cumulative_returns, benchmark_returns)

            # Save trade records
            backtest_filename = os.path.join("backtest", f"backtest_{coin}.csv")
            trades_df.to_csv(backtest_filename, index=False)
            print(f"Saved backtest results for {coin} to {backtest_filename}")

            # Save metrics
            metrics_filename = os.path.join("backtest", f"metrics_{coin}.csv")
            metrics.to_csv(metrics_filename, index=False)
            print(f"Saved metrics for {coin} to {metrics_filename}")
