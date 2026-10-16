#!/usr/bin/env python3
"""
mapper.py - Hadoop Streaming Mapper
Reads CSV lines of daily Mutual Fund NAVs from HDFS via STDIN.
Emits: Scheme \t Date,NAV,Benchmark,Benchmark_Value,Category,Exp_Ratio
"""

import sys
import csv

def main():
    # Use standard CSV reader on STDIN
    reader = csv.reader(sys.stdin)
    
    for row in reader:
        if not row or len(row) < 5:
            continue
            
        # Skip the CSV header row (exact match - fund names may legitimately contain 'Scheme')
        if row[0].strip() == 'Scheme' and row[2].strip() == 'Date':
            continue
            
        try:
            scheme = row[0].strip()
            category = row[1].strip() if len(row) > 1 else 'General'
            date_str = row[2].strip() if len(row) > 2 else ''
            nav_str = row[3].strip() if len(row) > 3 else '0.0'
            benchmark = row[4].strip() if len(row) > 4 else 'N/A'
            bench_val_str = row[5].strip() if len(row) > 5 else '0.0'
            exp_ratio_str = row[6].strip() if len(row) > 6 else '0.0'
            
            # Basic validation
            if not scheme or not date_str or not nav_str:
                continue
                
            # The value payload is comma-delimited, so commas inside text fields are neutralised
            category = category.replace(',', ';')
            benchmark = benchmark.replace(',', ';')
            nav = float(nav_str)
            bench_val = float(bench_val_str) if bench_val_str else 0.0
            exp_ratio = float(exp_ratio_str) if exp_ratio_str else 0.0
            
            # Map Output Key: Scheme
            # Map Output Value: Date,NAV,Benchmark,Benchmark_Value,Category,Exp_Ratio
            # Delimited by comma or pipe in value, separated by tab from key
            value_str = f"{date_str},{nav},{benchmark},{bench_val},{category},{exp_ratio}"
            sys.stdout.write(f"{scheme}\t{value_str}\n")
            
        except (ValueError, IndexError):
            continue

if __name__ == '__main__':
    main()
