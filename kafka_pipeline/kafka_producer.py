"""
kafka_producer.py
Streams daily Mutual Fund NAV ticks and Benchmark levels to Kafka.
Topic: mutual_fund_daily_nav
"""

import os
import sys
import json
import time
import argparse

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from kafka_pipeline.kafka_engine import ResilientKafkaProducer
from kafka_pipeline import stream_state

TOPIC_NAME = config.TOPIC_NAME

def load_stream_ticks(file_path):
    if not os.path.exists(file_path):
        print(f"Error: Streaming ticks file not found at {file_path}")
        print("Please run scripts/generate_timeseries_data.py first.")
        return []
    
    ticks = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                ticks.append(json.loads(line))
    return ticks

def run_producer(delay=1.0, single_day=False, max_ticks=None, topic=TOPIC_NAME):
    producer = ResilientKafkaProducer()
    status = producer.get_status()
    print(f"[*] Kafka Producer initialized in [{status['mode']}]")
    print(f"[*] Target Topic: {topic}")
    
    ticks_file = config.TICKS_FILE
    all_ticks = load_stream_ticks(ticks_file)
    if not all_ticks:
        return 0
    
    if max_ticks:
        all_ticks = all_ticks[:max_ticks]
        
    # Group ticks by date
    ticks_by_date = {}
    for tick in all_ticks:
        d = tick['Date']
        if d not in ticks_by_date:
            ticks_by_date[d] = []
        ticks_by_date[d].append(tick)
        
    dates = sorted(ticks_by_date.keys())
    print(f"[*] Available streaming dates: {len(dates)} days ({len(all_ticks)} total fund ticks)")
    
    # Resume from the persisted cursor so each run streams the next unseen trading day
    state = stream_state.load_state()
    start_idx = state.get('next_day_index', 0)
    if start_idx >= len(dates):
        print("[!] All streaming days have already been published. Reset the pipeline to replay them.")
        return 0
    print(f"[*] Resuming from day {start_idx + 1}/{len(dates)}")

    total_sent = 0
    for idx in range(start_idx, len(dates)):
        date_str = dates[idx]
        batch = ticks_by_date[date_str]
        print(f"--> [Kafka Producer] Streaming Day {idx+1}/{len(dates)}: {date_str} ({len(batch)} fund NAV quotes)...")
        
        for tick in batch:
            key = tick['Scheme']
            producer.send(topic, value=tick, key=key)
            total_sent += 1
            
        producer.flush()
        print(f"    [+] Successfully emitted {len(batch)} records to Kafka topic '{topic}'")
        state['next_day_index'] = idx + 1
        state['total_published'] = state.get('total_published', 0) + len(batch)
        state['last_date'] = date_str
        stream_state.save_state(state)
        
        if single_day:
            print("[-] Single-day mode requested. Stopping producer.")
            break
            
        if idx < len(dates) - 1:
            time.sleep(delay)
            
    producer.close()
    print(f"[Done] Total NAV ticks streamed to Kafka: {total_sent}")
    return total_sent

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Mutual Fund NAV Kafka Producer")
    parser.add_argument('--delay', type=float, default=1.0, help="Delay in seconds between daily batches")
    parser.add_argument('--single-day', action='store_true', help="Stream only 1 business day of quotes")
    parser.add_argument('--max-ticks', type=int, default=None, help="Maximum number of ticks to send")
    parser.add_argument('--topic', type=str, default=TOPIC_NAME, help="Kafka topic name")
    
    args = parser.parse_args()
    run_producer(delay=args.delay, single_day=args.single_day, max_ticks=args.max_ticks, topic=args.topic)
