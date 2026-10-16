#!/usr/bin/env python3
"""
reducer.py - Hadoop Streaming Reducer
Aggregates time-series NAV data per fund scheme,
computes rolling 30/60/90-day returns vs benchmark index,
alpha (excess return), beta, annualized volatility, and outperformance classification.
"""

import sys
import math
from datetime import datetime

def parse_date(d_str):
    try:
        return datetime.strptime(d_str, '%Y-%m-%d')
    except Exception:
        return None

def find_prior_record(records, target_date, days_back, max_tolerance_days=10):
    """Find the record closest to (target_date - days_back)."""
    ideal_target = target_date.timestamp() - (days_back * 86400)
    best_record = None
    min_diff = float('inf')
    
    for r in records:
        r_ts = r['dt'].timestamp()
        if r_ts <= target_date.timestamp() - ((days_back - max_tolerance_days) * 86400):
            diff = abs(r_ts - ideal_target)
            if diff < min_diff:
                min_diff = diff
                best_record = r
                
    if best_record and min_diff <= (max_tolerance_days * 86400):
        return best_record
    return None

def compute_fund_analytics(scheme, records):
    """
    Computes rolling 30, 60, and 90 day returns and benchmark comparisons.
    records is a list of dicts:
    [{'dt': datetime, 'date_str': str, 'nav': float, 'bench': str, 'bench_val': float, 'category': str, 'exp_ratio': float}]
    """
    if not records or len(records) < 2:
        return
        
    # Sort chronologically
    records = sorted(records, key=lambda x: x['dt'])
    
    # Calculate daily returns for volatility and beta
    daily_rets = []
    bench_daily_rets = []
    for i in range(1, len(records)):
        p_nav = records[i-1]['nav']
        c_nav = records[i]['nav']
        p_bench = records[i-1]['bench_val']
        c_bench = records[i]['bench_val']
        
        r_f = (c_nav - p_nav) / p_nav if p_nav > 0 else 0.0
        r_b = (c_bench - p_bench) / p_bench if p_bench > 0 else 0.0
        daily_rets.append(r_f)
        bench_daily_rets.append(r_b)
        
    # Annualized volatility
    if len(daily_rets) > 1:
        mean_rf = sum(daily_rets) / len(daily_rets)
        var_rf = sum((r - mean_rf) ** 2 for r in daily_rets) / (len(daily_rets) - 1)
        ann_vol = math.sqrt(var_rf) * math.sqrt(252) * 100.0
        
        # Beta calculation: Cov(rf, rb) / Var(rb)
        mean_rb = sum(bench_daily_rets) / len(bench_daily_rets)
        var_rb = sum((r - mean_rb) ** 2 for r in bench_daily_rets) / (len(bench_daily_rets) - 1)
        cov_rf_rb = sum((daily_rets[j] - mean_rf) * (bench_daily_rets[j] - mean_rb) for j in range(len(daily_rets))) / (len(daily_rets) - 1)
        
        beta = (cov_rf_rb / var_rb) if var_rb > 1e-8 else 1.0
        
        # Tracking error
        diff_rets = [daily_rets[j] - bench_daily_rets[j] for j in range(len(daily_rets))]
        mean_diff = sum(diff_rets) / len(diff_rets)
        var_diff = sum((d - mean_diff) ** 2 for d in diff_rets) / (len(diff_rets) - 1)
        tracking_err = math.sqrt(var_diff) * math.sqrt(252) * 100.0
    else:
        ann_vol = 12.0
        beta = 1.0
        tracking_err = 2.5
        
    # Process rolling windows for each day in records
    processed_history = []
    for idx, rec in enumerate(records):
        cur_dt = rec['dt']
        cur_nav = rec['nav']
        cur_bval = rec['bench_val']
        
        # Look back 30, 60, 90 days
        rec_30 = find_prior_record(records[:idx], cur_dt, 30)
        rec_60 = find_prior_record(records[:idx], cur_dt, 60)
        rec_90 = find_prior_record(records[:idx], cur_dt, 90)
        
        # Calculate returns
        def calc_ret(curr, prev):
            if prev and prev > 0:
                return round(((curr - prev) / prev) * 100.0, 2)
            return None
            
        ret_30 = calc_ret(cur_nav, rec_30['nav']) if rec_30 else None
        b_ret_30 = calc_ret(cur_bval, rec_30['bench_val']) if rec_30 else None
        alpha_30 = round(ret_30 - b_ret_30, 2) if (ret_30 is not None and b_ret_30 is not None) else None
        
        ret_60 = calc_ret(cur_nav, rec_60['nav']) if rec_60 else None
        b_ret_60 = calc_ret(cur_bval, rec_60['bench_val']) if rec_60 else None
        alpha_60 = round(ret_60 - b_ret_60, 2) if (ret_60 is not None and b_ret_60 is not None) else None
        
        ret_90 = calc_ret(cur_nav, rec_90['nav']) if rec_90 else None
        b_ret_90 = calc_ret(cur_bval, rec_90['bench_val']) if rec_90 else None
        alpha_90 = round(ret_90 - b_ret_90, 2) if (ret_90 is not None and b_ret_90 is not None) else None
        
        processed_history.append({
            'date': rec['date_str'],
            'nav': cur_nav,
            'bench_val': cur_bval,
            'ret_30': ret_30,
            'bench_ret_30': b_ret_30,
            'alpha_30': alpha_30,
            'ret_60': ret_60,
            'bench_ret_60': b_ret_60,
            'alpha_60': alpha_60,
            'ret_90': ret_90,
            'bench_ret_90': b_ret_90,
            'alpha_90': alpha_90,
            'category': rec['category'],
            'benchmark': rec['bench'],
            'exp_ratio': rec['exp_ratio']
        })
        
        # Emit daily time-series row if at least 30-day window is available
        if ret_30 is not None:
            r30_s = f"{ret_30:.2f}" if ret_30 is not None else "N/A"
            b30_s = f"{b_ret_30:.2f}" if b_ret_30 is not None else "N/A"
            a30_s = f"{alpha_30:.2f}" if alpha_30 is not None else "N/A"
            r60_s = f"{ret_60:.2f}" if ret_60 is not None else "N/A"
            b60_s = f"{b_ret_60:.2f}" if b_ret_60 is not None else "N/A"
            a60_s = f"{alpha_60:.2f}" if alpha_60 is not None else "N/A"
            r90_s = f"{ret_90:.2f}" if ret_90 is not None else "N/A"
            b90_s = f"{b_ret_90:.2f}" if b_ret_90 is not None else "N/A"
            a90_s = f"{alpha_90:.2f}" if alpha_90 is not None else "N/A"
            
            sys.stdout.write(f"DAILY\t{scheme}\t{rec['date_str']}\t{rec['category']}\t{cur_nav:.4f}\t{rec['bench']}\t{cur_bval:.2f}\t{r30_s}\t{b30_s}\t{a30_s}\t{r60_s}\t{b60_s}\t{a60_s}\t{r90_s}\t{b90_s}\t{a90_s}\n")

    # Latest day summary record
    latest = processed_history[-1]
    # Classification basis: 90-day alpha (fall back to the longest available window)
    basis_alpha = next((a for a in (latest['alpha_90'], latest['alpha_60'], latest['alpha_30']) if a is not None), None)
    status = "OUTPERFORMING" if (basis_alpha is not None and basis_alpha >= 0) else "UNDERPERFORMING"
    
    r30_s = f"{latest['ret_30']:.2f}" if latest['ret_30'] is not None else "0.00"
    b30_s = f"{latest['bench_ret_30']:.2f}" if latest['bench_ret_30'] is not None else "0.00"
    a30_s = f"{latest['alpha_30']:.2f}" if latest['alpha_30'] is not None else "0.00"
    
    r60_s = f"{latest['ret_60']:.2f}" if latest['ret_60'] is not None else "0.00"
    b60_s = f"{latest['bench_ret_60']:.2f}" if latest['bench_ret_60'] is not None else "0.00"
    a60_s = f"{latest['alpha_60']:.2f}" if latest['alpha_60'] is not None else "0.00"
    
    r90_s = f"{latest['ret_90']:.2f}" if latest['ret_90'] is not None else "0.00"
    b90_s = f"{latest['bench_ret_90']:.2f}" if latest['bench_ret_90'] is not None else "0.00"
    a90_s = f"{latest['alpha_90']:.2f}" if latest['alpha_90'] is not None else "0.00"
    
    sys.stdout.write(
        f"SUMMARY\t{scheme}\t{latest['date']}\t{latest['category']}\t{latest['nav']:.4f}\t"
        f"{latest['benchmark']}\t{latest['bench_val']:.2f}\t{r30_s}\t{b30_s}\t{a30_s}\t"
        f"{r60_s}\t{b60_s}\t{a60_s}\t{r90_s}\t{b90_s}\t{a90_s}\t{beta:.3f}\t{ann_vol:.2f}\t"
        f"{tracking_err:.2f}\t{latest['exp_ratio']:.2f}\t{status}\n"
    )

def main():
    current_scheme = None
    current_records = []
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
            
        parts = line.split('\t')
        if len(parts) != 2:
            continue
            
        scheme, value_str = parts[0], parts[1]
        v_parts = value_str.split(',')
        if len(v_parts) < 6:
            continue
            
        date_str = v_parts[0]
        dt = parse_date(date_str)
        if not dt:
            continue
            
        try:
            nav = float(v_parts[1])
            bench = v_parts[2]
            bench_val = float(v_parts[3])
            cat = v_parts[4]
            exp_ratio = float(v_parts[5]) if len(v_parts) > 5 else 0.0
        except ValueError:
            continue
            
        rec = {
            'dt': dt,
            'date_str': date_str,
            'nav': nav,
            'bench': bench,
            'bench_val': bench_val,
            'category': cat,
            'exp_ratio': exp_ratio
        }
        
        if current_scheme == scheme:
            current_records.append(rec)
        else:
            if current_scheme is not None and current_records:
                compute_fund_analytics(current_scheme, current_records)
            current_scheme = scheme
            current_records = [rec]
            
    if current_scheme is not None and current_records:
        compute_fund_analytics(current_scheme, current_records)

if __name__ == '__main__':
    main()
