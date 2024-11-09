import sys,os
import requests
import json
import platform
from datetime import datetime, timedelta
import pandas as pd
import asyncio
import aiohttp
from zipfile import ZipFile

coin_list = ['BTCUSDT',"ETHUSDT","SOLUSDT","DOGEUSDT"]

# We only want the daily(final row) liquidation data each day here
def download_liquidation_data():
    for coin in coin_list:
        start_date = datetime(2023,6,25)
        end_date = datetime(2024,3,31)
        date_list = [start_date + timedelta(days=x) for x in range(0, (end_date - start_date).days)]
        df_liquidations = pd.DataFrame(index=date_list)
        df_liquidations['liquidation_usd_amount'] = 0
        df_liquidations.index = pd.to_datetime(df_liquidations.index)
        
        for date in date_list:
            file_name = f'{coin}-liquidationSnapshot-{date.strftime("%Y-%m-%d")}.zip'
            download_url = f'https://data.binance.vision/data/futures/um/daily/liquidationSnapshot/{coin}/{file_name}'
            response = requests.get(download_url)
            
            if response.status_code == 200:
                # Save zip file
                with open(file_name, 'wb') as f:
                    f.write(response.content)
                
                csv_name = f'{coin}-liquidationSnapshot-{date.strftime("%Y-%m-%d")}.csv'
                # Extract and read CSV from zip
                with ZipFile(file_name, 'r') as zip_file:
                    csv_name = zip_file.namelist()[0]  # Get the CSV filename from zip
                    with zip_file.open(csv_name) as csv_file:
                        df = pd.read_csv(csv_file)
                        df['liquidation_usd_amount'] = df['average_price'] * df['last_fill_quantity']
                        daily_liquidation_sum = df['liquidation_usd_amount'].sum()
                        print(f"{coin} - {date.strftime('%Y-%m-%d')}: ${daily_liquidation_sum:,.2f}")
                        df_liquidations.loc[date] = daily_liquidation_sum
                # Optional: Remove zip file after processing
                os.remove(file_name)
        else:
            print(f'Failed to download data for {coin}')
        df_liquidations.to_csv(f'data/liquidation_data/{coin}-liquidation-data.csv')


async def download_candlestick_data():
    for coin in coin_list:
        df = pd.read_csv(f'data/liquidation_data/{coin}-liquidation-data.csv', index_col=0, parse_dates=True)
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

def backtest_liquidation():
    num_days = 20
    num_days_lowest_low = 10
    for coin in coin_list:
        # Read both CSVs with datetime index
        df_liquidations = pd.read_csv(f'data/liquidation_data/{coin}-liquidation-data.csv', 
                                    index_col=0, 
                                    parse_dates=True)
        df_candlesticks = pd.read_csv(f'data/candlestick_data/{coin}-candlestick-data.csv', 
                                     index_col=0, 
                                     parse_dates=True)
        
        # Merge the dataframes on datetime index
        df_merged = pd.merge(df_liquidations, 
                           df_candlesticks, 
                           left_index=True, 
                           right_index=True, 
                           how='inner')
        
        # Add big_liq column based on 20-day rolling maximum comparison
        df_merged['big_liq'] = (
            df_merged['liquidation_usd_amount'] >= 
            df_merged['liquidation_usd_amount'].rolling(window=num_days, min_periods=1).max()
        ).astype(int)
        
        # Check if current low is the lowest in the previous 20-day window
        df_merged['lowest_low'] = (
            df_merged['low'] <= 
            df_merged['low'].rolling(window=num_days_lowest_low, min_periods=1).min()
        ).astype(int)
        
        # Create signal column where both conditions are met
        df_merged['signal'] = (df_merged['big_liq'] & df_merged['lowest_low']).astype(int)
        
        print(f"\nBig liquidation events for {coin}:")
        # Print only rows where signal == 1
        print(df_merged[df_merged['signal'] == 1])
        
        # Optionally save the merged data
        df_merged.to_csv(f'data/merged/{coin}-merged-data.csv')

if __name__ == '__main__':
    os_name = platform.system()
    mode = int(sys.argv[1])
    if mode == 0:
        download_liquidation_data()
    elif mode == 1:
        asyncio.run(download_candlestick_data())
    elif mode == 2:
        backtest_liquidation()
