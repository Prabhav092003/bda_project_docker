"""
generate_timeseries_data.py
Generates realistic multi-day daily NAV time series for mutual funds and their benchmark indices
based on the Kaggle Mutual Funds India dataset.
Outputs:
1. data/time_series/historical_navs.csv (Historical baseline for HDFS data lake)
2. data/time_series/streaming_ticks.jsonl (Daily batches for Kafka streaming)
3. data/time_series/fund_master.json (Master metadata index)
"""

import os
import sys
import json
import re
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

def clean_float(val, default=0.0):
    if pd.isna(val):
        return default
    if isinstance(val, (int, float)):
        return float(val)
    # Remove commas, whitespace, currency signs, %
    s = str(val).replace(',', '').replace('Rs.', '').replace('%', '').strip()
    if s == 'N/A' or s == 'Nil' or s == '-' or not s:
        return default
    try:
        return float(s)
    except ValueError:
        return default

def infer_benchmark(benchmark, category):
    bench_str = str(benchmark).strip() if pd.notna(benchmark) else 'N/A'
    if bench_str != 'N/A' and len(bench_str) > 2:
        return bench_str
    
    cat = str(category).lower()
    if 'liquid' in cat or 'overnight' in cat:
        return 'CRISIL Liquid Debt Index'
    elif 'money market' in cat or 'ultra short' in cat:
        return 'CRISIL Money Market Index'
    elif 'corporate bond' in cat or 'short duration' in cat or 'banking' in cat:
        return 'CRISIL Corporate Bond Index'
    elif 'gilt' in cat or 'debt' in cat:
        return 'CRISIL Composite Bond Fund Index'
    elif 'mid cap' in cat:
        return 'Nifty Midcap 150 - TRI'
    elif 'small cap' in cat:
        return 'Nifty Smallcap 250 - TRI'
    elif 'hybrid' in cat:
        return 'CRISIL Hybrid 35+65 - Aggressive Index'
    elif 'sensex' in cat:
        return 'S&P BSE SENSEX - TRI'
    elif 'nifty 50' in cat or 'large cap' in cat:
        return 'NIFTY 50 - TRI'
    else:
        return 'NIFTY 500 - TRI'

def write_hdfs_baseline(hist_df=None):
    """(Re)build the HDFS-style data lake from the historical baseline (year=YYYY/month=MM)."""
    import shutil
    if hist_df is None:
        hist_df = pd.read_csv(config.HIST_FILE)
    if os.path.isdir(config.HDFS_BASE_DIR):
        shutil.rmtree(config.HDFS_BASE_DIR)
    for (year, month), group in hist_df.groupby([hist_df['Date'].str[:4], hist_df['Date'].str[5:7]]):
        part_dir = os.path.join(config.HDFS_BASE_DIR, f"year={year}", f"month={month}")
        os.makedirs(part_dir, exist_ok=True)
        group.to_csv(os.path.join(part_dir, f"nav_data_{year}_{month}.csv"), index=False)
    print(f"Initialized HDFS data lake partitions under {config.HDFS_BASE_DIR}")


def main():
    print("=" * 60)
    print("Generating Historical & Streaming NAV Time Series Data")
    print("=" * 60)
    
    raw_path = os.path.join(config.DATA_DIR, 'raw', 'Mutual_Funds.csv')
    if not os.path.exists(raw_path):
        raw_path = os.path.join(config.PROJECT_ROOT, 'Mutual_Funds.csv')
        
    df = pd.read_csv(raw_path)
    # Normalise stray whitespace in text columns (e.g. ' Debt - Corporate Bond Fund')
    for col in ('Scheme', 'Category', 'Type'):
        df[col] = df[col].astype(str).str.strip()
    print(f"Loaded {len(df)} schemes from {raw_path}")
    
    # Clean fields
    df['Clean_NAV'] = df['Net_Asset_Value(Rs.)'].apply(lambda x: clean_float(x, 100.0))
    df['Clean_CAGR_1Y'] = df['CAGR% 1 Year'].apply(lambda x: clean_float(x, 12.0))
    df['Clean_Exp_Ratio'] = df['Exp. Ratio(%)'].apply(lambda x: clean_float(x, 1.2))
    df['Resolved_Benchmark'] = df.apply(lambda row: infer_benchmark(row['Benchmark'], row['Category']), axis=1)
    
    # Filter out funds with invalid NAV
    df = df[df['Clean_NAV'] > 0.0].copy()
    print(f"Valid schemes after cleaning: {len(df)}")
    
    # Select top 200 diverse representative schemes across all categories to make
    # MapReduce and streaming fast and responsive, while retaining rich diversity
    categories = df['Category'].unique()
    sample_dfs = []
    for cat in categories:
        sub = df[df['Category'] == cat]
        sample_dfs.append(sub.head(min(len(sub), 10))) # up to 10 per category
    
    sample_df = pd.concat(sample_dfs).drop_duplicates(subset=['Scheme']).head(250)
    print(f"Selected {len(sample_df)} funds across {len(categories)} categories for time-series generation")
    
    # Save Fund Master JSON
    master_records = []
    for _, row in sample_df.iterrows():
        master_records.append({
            'scheme': row['Scheme'],
            'category': row['Category'],
            'type': str(row['Type']),
            'benchmark': row['Resolved_Benchmark'],
            'latest_nav': round(float(row['Clean_NAV']), 2),
            'cagr_1y': round(float(row['Clean_CAGR_1Y']), 2),
            'exp_ratio': round(float(row['Clean_Exp_Ratio']), 2)
        })
    
    os.makedirs(config.TS_DIR, exist_ok=True)
    with open(os.path.join(config.TS_DIR, 'fund_master.json'), 'w') as f:
        json.dump(master_records, f, indent=2)
    print(f"Saved fund master metadata: {len(master_records)} schemes")
    
    # Setup Trading Days: 180 business days ending recently
    num_days = 180
    end_date = datetime(2026, 10, 10)
    dates = []
    cur = end_date
    while len(dates) < num_days:
        if cur.weekday() < 5: # Monday to Friday
            dates.append(cur)
        cur -= timedelta(days=1)
    dates = sorted(dates) # Chronological
    
    # Benchmark base prices and daily volatility
    benchmarks = sample_df['Resolved_Benchmark'].unique().tolist()
    np.random.seed(42)
    
    bench_data = {}
    for bench in benchmarks:
        is_debt = any(k in bench.lower() for k in ['debt', 'liquid', 'bond', 'money', 'overnight', 'gilt'])
        base_val = 5000.0 if is_debt else 20000.0
        daily_drift = (0.07 / 252.0) if is_debt else (0.13 / 252.0)
        daily_vol = (0.015 / np.sqrt(252)) if is_debt else (0.16 / np.sqrt(252))
        
        # Simulate geometric random walk with market cycle
        # e.g., mild correction in mid-period, then recovery
        daily_rets = np.random.normal(daily_drift, daily_vol, num_days)
        # Add market regime factor
        for i in range(num_days):
            if 60 <= i <= 90:
                daily_rets[i] -= 0.0015 # mild market dip
            elif 120 <= i <= 150:
                daily_rets[i] += 0.0012 # rally
                
        series = [base_val]
        for r in daily_rets:
            series.append(series[-1] * (1.0 + r))
        bench_data[bench] = series[1:] # 180 days
    
    # Generate Fund daily NAVs
    # Each fund has beta, idiosyncratic volatility, expense drag, alpha drift
    historical_rows = []
    streaming_rows = []
    
    historical_days = 140 # First 140 days to seed HDFS
    streaming_days = 40   # Remaining 40 days to stream via Kafka
    
    for _, fund in sample_df.iterrows():
        scheme = fund['Scheme']
        cat = fund['Category']
        bench = fund['Resolved_Benchmark']
        base_nav = fund['Clean_NAV']
        cagr = fund['Clean_CAGR_1Y']
        exp_ratio = fund['Clean_Exp_Ratio']
        
        is_debt = any(k in cat.lower() for k in ['debt', 'liquid', 'bond', 'money', 'overnight', 'gilt'])
        fund_drift = (cagr / 100.0) / 252.0
        fund_vol = (0.02 / np.sqrt(252)) if is_debt else (0.17 / np.sqrt(252))
        
        # Calculate daily returns correlated with its benchmark
        bench_series = bench_data[bench]
        bench_rets = [(bench_series[i] - bench_series[i-1]) / bench_series[i-1] if i > 0 else 0.0 for i in range(num_days)]
        
        beta = 0.9 + np.random.uniform(-0.25, 0.35) if not is_debt else 0.95 + np.random.uniform(-0.1, 0.1)
        idio_noise = np.random.normal(0, fund_vol * 0.4, num_days)
        
        fund_rets = [beta * bench_rets[i] + fund_drift + idio_noise[i] - (exp_ratio / 100.0 / 252.0) for i in range(num_days)]
        
        # Build NAV trajectory backwards or forwards so final NAV matches realistic base
        nav_trajectory = [base_nav]
        for r in fund_rets[1:]:
            nav_trajectory.append(max(0.5, nav_trajectory[-1] * (1.0 + r)))
            
        for d_idx, dt in enumerate(dates):
            date_str = dt.strftime('%Y-%m-%d')
            nav_val = round(nav_trajectory[d_idx], 4)
            bench_val = round(bench_data[bench][d_idx], 2)
            
            record = {
                'Scheme': scheme,
                'Category': cat,
                'Date': date_str,
                'NAV': nav_val,
                'Benchmark': bench,
                'Benchmark_Value': bench_val,
                'Exp_Ratio': exp_ratio
            }
            
            if d_idx < historical_days:
                historical_rows.append(record)
            else:
                streaming_rows.append(record)
                
    # Save Historical CSV
    hist_df = pd.DataFrame(historical_rows)
    hist_path = config.HIST_FILE
    hist_df.to_csv(hist_path, index=False)
    print(f"Saved {len(hist_df)} historical records to {hist_path}")
    
    # Save Streaming JSONL grouped by date (each line or tick is a daily update)
    stream_path = config.TICKS_FILE
    with open(stream_path, 'w') as f:
        for row in streaming_rows:
            f.write(json.dumps(row) + '\n')
    print(f"Saved {len(streaming_rows)} stream ticks to {stream_path}")
    
    # Also partition historical data into HDFS storage directly!
    write_hdfs_baseline(hist_df)
    print("=" * 60)

if __name__ == '__main__':
    main()
