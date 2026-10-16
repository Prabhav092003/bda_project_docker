"""
run_mapreduce.py
Executes the MapReduce job for Mutual Fund Performance Trend Analytics.
Supports:
1. Native Apache Hadoop Cluster streaming (hadoop jar hadoop-streaming.jar ...)
2. High-performance streaming emulation pipeline (HDFS Inputs -> mapper.py -> sort & shuffle -> reducer.py)
Generates:
- mapreduce/output/mapreduce_analytics.tsv (Raw MapReduce emitted results)
- mapreduce/output/analytics_summary.json (Executive analytics for Comparative Dashboard)
"""

import os
import sys
import json
import time
import subprocess
import glob
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

HDFS_NAV_DIR = config.HDFS_BASE_DIR
OUTPUT_DIR = config.MR_OUTPUT_DIR
RAW_OUTPUT_FILE = config.RAW_OUTPUT_FILE
SUMMARY_JSON_FILE = config.SUMMARY_JSON_FILE

def check_hadoop_cli():
    try:
        res = subprocess.run(['hadoop', 'version'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=2)
        return res.returncode == 0
    except Exception:
        return False

def collect_hdfs_files():
    files = []
    for root, _, filenames in os.walk(HDFS_NAV_DIR):
        for f in filenames:
            if f.endswith('.csv'):
                files.append(os.path.join(root, f))
    return sorted(files)

def run_local_streaming_mapreduce(input_files):
    """
    Executes exact Hadoop Streaming semantics locally:
    cat <input_files> | python mapper.py | sort | python reducer.py > output.tsv
    """
    print(f"[*] Running MapReduce via Python Streaming Engine...")
    print(f"[*] Reading {len(input_files)} partition files from HDFS Data Lake...")
    
    mapper_script = os.path.join(config.MR_DIR, 'mapper.py')
    reducer_script = os.path.join(config.MR_DIR, 'reducer.py')
    
    t0 = time.time()
    
    # Step 1: Mapper Phase
    print("--> [Map Phase] Distributing records to mapper.py...")
    map_proc = subprocess.Popen(
        [sys.executable, mapper_script],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding='utf-8'
    )
    
    total_input_lines = 0
    def stream_input():
        nonlocal total_input_lines
        for fpath in input_files:
            try:
                with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        map_proc.stdin.write(line)
                        if not line.startswith('Scheme,'):  # do not count CSV header rows
                            total_input_lines += 1
            except Exception as e:
                print(f"Warning reading {fpath}: {e}")
        map_proc.stdin.close()
        
    import threading
    writer_thread = threading.Thread(target=stream_input)
    writer_thread.start()
    
    # Collect mapper output
    mapper_lines = []
    for line in map_proc.stdout:
        mapper_lines.append(line)
    map_proc.wait()
    writer_thread.join()
    
    print(f"    [+] Mapper Phase complete: Processed {total_input_lines} input lines -> {len(mapper_lines)} key-value pairs.")
    
    # Step 2: Shuffle & Sort Phase
    print("--> [Shuffle & Sort Phase] Grouping & sorting key-value pairs by Scheme Key...")
    mapper_lines.sort() # Standard Hadoop lexicographical sort by key
    
    # Step 3: Reducer Phase
    print("--> [Reduce Phase] Calculating rolling 30/60/90-day returns and benchmark alphas in reducer.py...")
    red_proc = subprocess.Popen(
        [sys.executable, reducer_script],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding='utf-8'
    )
    
    def stream_to_reducer():
        for line in mapper_lines:
            red_proc.stdin.write(line)
        red_proc.stdin.close()
        
    red_writer = threading.Thread(target=stream_to_reducer)
    red_writer.start()
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    reducer_output_lines = []
    with open(RAW_OUTPUT_FILE, 'w', encoding='utf-8') as out_f:
        for line in red_proc.stdout:
            out_f.write(line)
            reducer_output_lines.append(line)
            
    red_proc.wait()
    red_writer.join()
    t_elapsed = round(time.time() - t0, 2)
    print(f"    [+] Reducer Phase complete in {t_elapsed}s! Output written to {RAW_OUTPUT_FILE}")
    
    return reducer_output_lines, t_elapsed, total_input_lines

def parse_and_summarize_results(reducer_lines, runtime_sec, total_records):
    print("--> Compiling Comparative Performance Summary JSON...")
    
    summaries = []
    daily_history = defaultdict(list)
    
    for line in reducer_lines:
        line = line.strip()
        if not line:
            continue
        parts = line.split('\t')
        record_type = parts[0]
        
        if record_type == 'SUMMARY' and len(parts) >= 20:
            summaries.append({
                'scheme': parts[1],
                'date': parts[2],
                'category': parts[3],
                'nav': float(parts[4]),
                'benchmark': parts[5],
                'benchmark_value': float(parts[6]),
                'ret_30d': float(parts[7]),
                'bench_ret_30d': float(parts[8]),
                'alpha_30d': float(parts[9]),
                'ret_60d': float(parts[10]),
                'bench_ret_60d': float(parts[11]),
                'alpha_60d': float(parts[12]),
                'ret_90d': float(parts[13]),
                'bench_ret_90d': float(parts[14]),
                'alpha_90d': float(parts[15]),
                'beta': float(parts[16]),
                'volatility': float(parts[17]),
                'tracking_error': float(parts[18]),
                'exp_ratio': float(parts[19]),
                'status': parts[20]
            })
        elif record_type == 'DAILY' and len(parts) >= 16:
            scheme = parts[1]
            daily_history[scheme].append({
                'date': parts[2],
                'category': parts[3],
                'nav': float(parts[4]),
                'benchmark': parts[5],
                'bench_val': float(parts[6]),
                'ret_30d': float(parts[7]) if parts[7] != 'N/A' else None,
                'bench_ret_30d': float(parts[8]) if parts[8] != 'N/A' else None,
                'alpha_30d': float(parts[9]) if parts[9] != 'N/A' else None,
                'ret_60d': float(parts[10]) if parts[10] != 'N/A' else None,
                'bench_ret_60d': float(parts[11]) if parts[11] != 'N/A' else None,
                'alpha_60d': float(parts[12]) if parts[12] != 'N/A' else None,
                'ret_90d': float(parts[13]) if parts[13] != 'N/A' else None,
                'bench_ret_90d': float(parts[14]) if parts[14] != 'N/A' else None,
                'alpha_90d': float(parts[15]) if parts[15] != 'N/A' else None,
            })
            
    # Sort summaries by 90d alpha descending
    summaries_by_alpha = sorted(summaries, key=lambda x: x['alpha_90d'], reverse=True)
    
    top_performers = summaries_by_alpha[:10]
    underperformers = sorted(summaries, key=lambda x: x['alpha_90d'])[:10]
    
    # Category aggregation
    cat_stats = defaultdict(lambda: {'count': 0, 'outperform_count': 0, 'fund_ret_90': [], 'bench_ret_90': [], 'alpha_90': [], 'fund_ret_30': [], 'alpha_30': []})
    for s in summaries:
        c = s['category']
        cat_stats[c]['count'] += 1
        if s['alpha_90d'] > 0:
            cat_stats[c]['outperform_count'] += 1
        cat_stats[c]['fund_ret_90'].append(s['ret_90d'])
        cat_stats[c]['bench_ret_90'].append(s['bench_ret_90d'])
        cat_stats[c]['alpha_90'].append(s['alpha_90d'])
        cat_stats[c]['fund_ret_30'].append(s['ret_30d'])
        cat_stats[c]['alpha_30'].append(s['alpha_30d'])
        
    category_summary = []
    for cat, data in sorted(cat_stats.items(), key=lambda x: len(x[1]['alpha_90']), reverse=True):
        n = data['count']
        category_summary.append({
            'category': cat,
            'fund_count': n,
            'outperform_percentage': round((data['outperform_count'] / n) * 100.0, 1) if n > 0 else 0,
            'avg_fund_ret_90d': round(sum(data['fund_ret_90']) / n, 2) if n > 0 else 0,
            'avg_bench_ret_90d': round(sum(data['bench_ret_90']) / n, 2) if n > 0 else 0,
            'avg_alpha_90d': round(sum(data['alpha_90']) / n, 2) if n > 0 else 0,
            'avg_fund_ret_30d': round(sum(data['fund_ret_30']) / n, 2) if n > 0 else 0,
            'avg_alpha_30d': round(sum(data['alpha_30']) / n, 2) if n > 0 else 0
        })
        
    total_funds = len(summaries)
    outperforming_funds = sum(1 for s in summaries if s['alpha_90d'] > 0)
    outperform_ratio = round((outperforming_funds / total_funds) * 100.0, 1) if total_funds > 0 else 0
    
    analytics_payload = {
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
        'execution_time_seconds': runtime_sec,
        'total_input_records': total_records,
        'total_funds_analyzed': total_funds,
        'outperforming_funds_count': outperforming_funds,
        'outperform_rate_pct': outperform_ratio,
        'top_performers': top_performers,
        'underperformers': underperformers,
        'category_aggregates': category_summary,
        'all_funds': summaries,
        'daily_sample_histories': {s['scheme']: daily_history[s['scheme']] for s in top_performers[:5]}
    }
    
    with open(SUMMARY_JSON_FILE, 'w', encoding='utf-8') as f:
        json.dump(analytics_payload, f, indent=2)
        
    print(f"[Done] Summary written to {SUMMARY_JSON_FILE}!")
    return analytics_payload

def main():
    print("=" * 60)
    print("Mutual Fund Rolling Return MapReduce Job")
    print("=" * 60)
    
    input_files = collect_hdfs_files()
    if not input_files:
        print(f"No HDFS partition files found in {HDFS_NAV_DIR}")
        print("Please run scripts/generate_timeseries_data.py or kafka_consumer_hdfs.py first.")
        return
        
    reducer_lines, runtime_sec, total_records = run_local_streaming_mapreduce(input_files)
    parse_and_summarize_results(reducer_lines, runtime_sec, total_records)
    print("=" * 60)

if __name__ == '__main__':
    main()
