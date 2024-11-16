import os
import pandas as pd
import datetime

def process_pkl_csv():
    start_date = datetime.datetime(2021, 12, 1)
    file_path = os.getcwd() + '/data/marketcap_data/raw_cmc_daily_data_concat_washed.pkl'
    df = pd.read_pickle(file_path)
    
    # Filter DataFrame for dates after start_date
    df_filtered = df[df['candle_begin_time'] >= start_date]
    
    # Filter for BTC only and select columns
    df_btc = df_filtered[df_filtered['name'] == 'Bitcoin']
    df_btc = df_btc[['candle_begin_time', 'total_mcap']]
    
    # Set candle_begin_time as index
    df_btc = df_btc.set_index('candle_begin_time')
    
    print("\nBTC market cap data with datetime index:")
    print(df_btc.head())
    df_btc.to_csv(os.getcwd() + '/data/marketcap_data/btc_marketcap_data.csv')


if __name__ == '__main__':
    process_pkl_csv()