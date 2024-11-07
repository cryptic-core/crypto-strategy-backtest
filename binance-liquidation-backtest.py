import sys,os
import requests
import platform
from datetime import datetime, timedelta
import pandas as pd
import io
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

def backtest_liquidation():
    pass

if __name__ == '__main__':
    os_name = platform.system()
    mode = int(sys.argv[1])
    if mode == 0:
        download_liquidation_data()
    elif mode == 1:
        backtest_liquidation()
