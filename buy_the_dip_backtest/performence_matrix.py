import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

def calculate_metrics(df):
    # Calculate daily returns
    df['daily_returns'] = df['portfolio_value'].pct_change()
    df['btc_returns'] = df['close'].pct_change()
    
    # Calculate metrics
    risk_free_rate = 0.02  # Assuming 2% annual risk-free rate
    daily_rf_rate = (1 + risk_free_rate) ** (1/365) - 1
    
    # Sharpe Ratio
    excess_returns = df['daily_returns'] - daily_rf_rate
    excess_returns_clean = excess_returns[~(np.isnan(excess_returns) | np.isinf(excess_returns))]  # Remove NaN and inf values
    sharpe_ratio = np.sqrt(365) * (excess_returns_clean.mean() / excess_returns_clean.std())
    

    # Maximum Drawdown
    rolling_max = df['portfolio_value'].cummax()
    drawdowns = (df['portfolio_value'] - rolling_max) / rolling_max
    max_drawdown = drawdowns.min()

    # Maximum Drawdown Of BTC(Benchmark)
    rolling_max_btc = df['close'].cummax()
    drawdowns_btc = (df['close'] - rolling_max_btc) / rolling_max_btc
    max_drawdown_btc = drawdowns_btc.min()
    
    # Sortino Ratio (only considering negative returns)
    negative_returns = df['daily_returns'][df['daily_returns'] < 0]
    negative_returns_clean = negative_returns[~(np.isnan(negative_returns) | np.isinf(negative_returns))]
    neg_std = negative_returns_clean.std(skipna=True)
    if neg_std == 0 or np.isnan(neg_std):
        sortino_ratio = np.nan
    else:
        sortino_ratio = np.sqrt(365) * (excess_returns_clean.mean() - daily_rf_rate) / neg_std
    
    # Calculate Benchmark Metrics
    sortino_ratio_btc = np.nan
    negative_returns_btc = df['btc_returns'][df['btc_returns'] < 0]
    negative_returns_clean_btc = negative_returns_btc[~(np.isnan(negative_returns_btc) | np.isinf(negative_returns_btc))]
    neg_std_btc = negative_returns_clean_btc.std(skipna=True)
    if neg_std_btc == 0 or np.isnan(neg_std_btc):
        sortino_ratio_btc = np.nan
    else:
        sortino_ratio_btc = np.sqrt(365) * (df['btc_returns'].mean(skipna=True) - daily_rf_rate) / neg_std_btc


    # Print metrics in markdown table format
    print("\n| Metric | Value |")
    print("|--------|--------|")
    print(f"| Sharpe Ratio Of Portfolio | {sharpe_ratio:.2f} |")
    print(f"| Maximum Drawdown Of Portfolio | {max_drawdown:.2%} |")
    print(f"| Maximum Drawdown Of BTC(Benchmark) | {max_drawdown_btc:.2%} |")
    print(f"| Sortino Ratio Of Portfolio | {sortino_ratio:.2f} |")
    print(f"| Sortino Ratio Of BTC(Benchmark) | {sortino_ratio_btc:.2f} |")
    
def plot_results(df):
    # Create figure with two y-axes
    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax2 = ax1.twinx()
    
    # Plot portfolio value
    ax1.plot(df['date'], df['portfolio_value'], 'b-', label='Portfolio Value')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Portfolio Value (USD)', color='b')
    ax1.tick_params(axis='y', labelcolor='b')
    
    # Plot BTC log returns
    btc_log_returns = np.log(df['close'] / df['close'].iloc[0])
    ax2.plot(df['date'], btc_log_returns, 'r-', label='BTC Log Returns')
    ax2.set_ylabel('BTC Log Returns', color='r')
    ax2.tick_params(axis='y', labelcolor='r')
    
    # Add title and legend
    plt.title('Portfolio vs BTC Log Returns')
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
    
    # Save plot
    plt.savefig('performence_matrix.png')
    plt.close()
