import pandas as pd
import os
from performence_matrix import calculate_metrics, plot_results

def run_dca_backtest(df):
    # Initialize portfolio tracking
    df['btc_bought'] = 0.0  # Amount of BTC bought each day
    df['usd_invested'] = 0.0  # USD invested each day
    df['total_btc'] = 0.0  # Running total of BTC held
    df['total_usd_invested'] = 0.0  # Running total of USD invested
    df['portfolio_value'] = 0.0  # Current value of holdings
    df['net_pnl'] = 0.0  # Net profit/loss
    
    # Convert date to datetime if it's not already
    df['date'] = pd.to_datetime(df['date'])
    
    total_btc = 0.0
    total_usd_invested = 0.0
    
    # Iterate through each row
    for index, row in df.iterrows():
        # Check if it's the 15th of the month
        if row['date'].day == 15:
            usd_amount = 200  # DCA amount
            btc_bought = usd_amount / row['close']  # Calculate BTC amount bought
            
            # Record the transaction
            df.at[index, 'btc_bought'] = btc_bought
            df.at[index, 'usd_invested'] = usd_amount
            
            # Update running totals
            total_btc += btc_bought
            total_usd_invested += usd_amount
        
        # Update running totals for each day
        df.at[index, 'total_btc'] = total_btc
        df.at[index, 'total_usd_invested'] = total_usd_invested
        df.at[index, 'portfolio_value'] = total_btc * row['close']
        df.at[index, 'net_pnl'] = (total_btc * row['close']) - total_usd_invested
    
    # Print summary statistics
    print("\nDCA Backtest Results:")
    print(f"Total BTC accumulated: {total_btc:.8f}")
    print(f"Total USD invested: ${total_usd_invested:,.2f}")
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
    run_dca_backtest(df)
    matrix = calculate_metrics(df)
    plot_results(df)
    print(df.head())

