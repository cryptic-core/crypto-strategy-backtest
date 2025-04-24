# liquidation buy the dip backtest
import pandas as pd
import os

def process_data():
    candlestick_df = pd.read_csv(os.getcwd()+'/data/candlestick_data/BTC-USDT.csv')
    liquidation_df = pd.read_csv(os.getcwd()+'/data/liquidation_data/liquidation.csv')
    
    # Convert date columns to datetime
    candlestick_df['date'] = pd.to_datetime(candlestick_df['date'])
    liquidation_df['date'] = pd.to_datetime(liquidation_df['date'])
    
    # Find common date range
    start_date = max(candlestick_df['date'].min(), liquidation_df['date'].min())
    end_date = min(candlestick_df['date'].max(), liquidation_df['date'].max())
    
    # Trim both dataframes to common date range
    candlestick_df = candlestick_df[(candlestick_df['date'] >= start_date) & (candlestick_df['date'] <= end_date)]
    liquidation_df = liquidation_df[(liquidation_df['date'] >= start_date) & (liquidation_df['date'] <= end_date)]
    
    # Merge dataframes on date
    merged_df = pd.merge(candlestick_df, liquidation_df, on='date', how='inner')
    # Sort by date to ensure correct rolling window calculation
    merged_df = merged_df.sort_values('date')
    
    return merged_df


initial_balance = 100000
def do_backtest(df,params):
    
    # read params
    tp_pct = params["tp_pct"] or 1.05
    sl_pct = params["sl_pct"] or 0.005
    lookback_window = params["lookback_window"] or 14
    lmax_threshold = params["lmax_threshold"] or 150000000
    buy_pct = params["buy_pct"] or 0.9

    # initialize backtst variables
    net_value = initial_balance
    cur_position = {"entry_price": None, "size": 0}
    trade_records = []

    # lookback window for lmax
    rolling_max = df['longusdamount'].rolling(window=lookback_window, min_periods=1).max()
    df['bLMax'] = (df['longusdamount'] == rolling_max) & (df['longusdamount'] > lmax_threshold)
    
    # Compare current close with close price 14 days ago
    df['close_nd_ago'] = df['close'].shift(lookback_window)  # Get close price from 14 days ago
    df['llw'] = df['close'] < df['close_nd_ago']  # True if current close is lower
    
    for index, row in df.iterrows():
        if row['bLMax'] and row['llw']:
            limit_order_buy = row['low'] * buy_pct
            if df.iloc[index+1]['low'] < limit_order_buy:
                if cur_position["entry_price"] is None:
                    entry_price = row['low'] * buy_pct
                    cur_position["entry_price"] = entry_price
                    cur_position["size"] = net_value / entry_price
                    print(f"{row['date']} : Buy the dip at:${entry_price:,.2f}")
                    continue
        # If we're in position, check if price hits entry target
        if cur_position["entry_price"] is not None:
            
            if row['high'] >= cur_position["entry_price"]*tp_pct:
                #calculate profit
                pnl =  cur_position["entry_price"]*tp_pct - cur_position["entry_price"]
                net_value += pnl * cur_position["size"]
                trade_records.append({
                        "date": row['date'],
                        "net_value": net_value,
                        "pnl": pnl
                })
                print(f"{row['date']} : Take profit at:${cur_position['entry_price']*tp_pct:,.2f}")
                # Reset position tracking
                cur_position["entry_price"] = None
                cur_position["size"] = 0
            
            elif row['low'] <= cur_position["entry_price"]*sl_pct:

                pnl = cur_position["entry_price"]*sl_pct - cur_position["entry_price"]
                net_value += pnl * cur_position["size"]
                trade_records.append({
                        "date": row['date'],
                        "net_value": net_value,
                        "pnl": pnl
                })
                # Reset position tracking
                cur_position["entry_price"] = None
                cur_position["size"] = 0
                
            else:  # record net value and pnl
                pnl = row['close'] - cur_position["entry_price"]
                trade_records.append({
                        "date": row['date'],
                        "net_value": net_value + pnl * cur_position["size"],
                        "pnl": pnl
                })
        else:
            trade_records.append({
                "date": row['date'],
                "net_value": net_value,
                "pnl": 0
            })

    return trade_records

def do_dca_backtest(df,params):
    pass


if __name__ == '__main__':
    params = {
        "tp_pct": 1.15,
        "sl_pct": 0.9,
        "lmax_threshold": 200000000,
        "buy_pct": 0.99,
        "lookback_window": 12
    }
    df = process_data()
    records = do_backtest(df,params)
    df_records = pd.DataFrame(records)
    
