"""
kafka_consumer_hdfs.py
Kafka-to-HDFS Consumer pipeline.
Subscribes to topic 'mutual_fund_daily_nav', batches incoming stream ticks,
and persists them into time-partitioned HDFS storage (year=YYYY/month=MM/).
If a real HDFS cluster is reachable (WebHDFS), every partition file is mirrored to it.
Ingestion is idempotent: a (Scheme, Date) pair is never written twice.
"""

import os
import sys
import json
import time
import argparse
import pandas as pd
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from kafka_pipeline.kafka_engine import ResilientKafkaConsumer
from kafka_pipeline import hdfs_client, stream_state

TOPIC_NAME = config.TOPIC_NAME
HDFS_BASE_DIR = config.HDFS_BASE_DIR
MANIFEST_PATH = config.MANIFEST_PATH
COLUMNS = ['Scheme', 'Category', 'Date', 'NAV', 'Benchmark', 'Benchmark_Value', 'Exp_Ratio']


def check_hadoop_cluster():
    """Detect a real HDFS cluster (WebHDFS REST endpoint)."""
    return hdfs_client.is_available()


def sync_to_real_hdfs(local_file_path, hdfs_dest_path):
    """Mirror a partition file into the real HDFS cluster (best effort)."""
    try:
        hdfs_client.upload_file(local_file_path, hdfs_dest_path)
        print(f"[Real HDFS] Uploaded {os.path.basename(local_file_path)} -> hdfs:{hdfs_dest_path}")
    except Exception as e:
        print(f"[Real HDFS Sync] Skipped ({e})")


def update_manifest(records_added=0):
    """Rebuild the data-lake manifest from what is actually on disk (always consistent)."""
    partitions = []
    total_size = 0
    total_records = 0
    for root, _, files in os.walk(HDFS_BASE_DIR):
        for file in sorted(files):
            if file.endswith('.csv'):
                fp = os.path.join(root, file)
                total_size += os.path.getsize(fp)
                with open(fp, 'r', encoding='utf-8', errors='ignore') as f:
                    total_records += max(sum(1 for _ in f) - 1, 0)  # minus header
                rel = os.path.relpath(root, HDFS_BASE_DIR).replace(os.sep, '/')
                if rel != '.' and rel not in partitions:
                    partitions.append(rel)

    manifest = {
        'last_updated': time.strftime('%Y-%m-%d %H:%M:%S'),
        'hdfs_logical_root': config.HDFS_LOGICAL_PATH,
        'total_records': total_records,
        'partitions': sorted(partitions),
        'total_size_bytes': total_size,
        'total_size_mb': round(total_size / (1024 * 1024), 2),
        'hadoop_cluster_connected': check_hadoop_cluster(),
    }
    os.makedirs(os.path.dirname(MANIFEST_PATH), exist_ok=True)
    with open(MANIFEST_PATH, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2)
    return manifest


def _write_partition(part_file, df_batch):
    """Append new rows to a partition file, skipping (Scheme, Date) keys already stored."""
    if os.path.exists(part_file):
        existing = pd.read_csv(part_file, usecols=['Scheme', 'Date'])
        seen = set(zip(existing['Scheme'], existing['Date']))
        mask = [(s, d) not in seen for s, d in zip(df_batch['Scheme'], df_batch['Date'])]
        df_batch = df_batch[mask]
        if df_batch.empty:
            return 0
        df_batch.to_csv(part_file, mode='a', header=False, index=False)
    else:
        df_batch.to_csv(part_file, mode='w', header=True, index=False)
    return len(df_batch)


def run_consumer(poll_cycles=5, batch_size=1000):
    print("=" * 60)
    print("Kafka-to-HDFS Consumer Service")
    print(f"Target HDFS Lake: {HDFS_BASE_DIR}")
    print("=" * 60)

    consumer = ResilientKafkaConsumer(topic=TOPIC_NAME, group_id='hdfs_storage_writer')
    has_real_hadoop = check_hadoop_cluster()
    print(f"[*] Real HDFS Cluster (WebHDFS) Detected: {has_real_hadoop}")

    # A real Kafka consumer needs a few seconds to join its group before records flow
    if consumer.is_real_kafka:
        poll_cycles = max(poll_cycles, 8)

    total_consumed = 0
    total_written = 0
    cycle = 0

    while cycle < poll_cycles:
        records = consumer.poll(max_records=batch_size)
        if not records:
            time.sleep(0.5)
            cycle += 1
            continue

        cycle = 0  # reset idle cycle count
        total_consumed += len(records)
        print(f"[<-- Kafka Consumer] Received {len(records)} records from topic '{TOPIC_NAME}'")

        partitioned = defaultdict(list)
        for r in records:
            val = r['value']
            date_str = val.get('Date', '')
            if len(date_str) >= 7:
                partitioned[(date_str[:4], date_str[5:7])].append(val)

        for (year, month), batch_rows in partitioned.items():
            part_dir = os.path.join(HDFS_BASE_DIR, f"year={year}", f"month={month}")
            os.makedirs(part_dir, exist_ok=True)
            part_file = os.path.join(part_dir, f"nav_data_{year}_{month}.csv")

            df_batch = pd.DataFrame(batch_rows)
            for c in COLUMNS:
                if c not in df_batch.columns:
                    df_batch[c] = ''
            df_batch = df_batch[COLUMNS]

            written = _write_partition(part_file, df_batch)
            total_written += written
            print(f"    [+] Ingested {written} new records into HDFS partition: /nav_data/year={year}/month={month}/")

            if has_real_hadoop:
                sync_to_real_hdfs(part_file, f"{config.HDFS_LOGICAL_PATH}/year={year}/month={month}/nav_data_{year}_{month}.csv")

        time.sleep(0.2)

    consumer.close()
    print(f"\n[Done] Consumer session completed. Consumed {total_consumed} messages, wrote {total_written} new records into HDFS.")
    final_manifest = update_manifest()
    print(f"HDFS Data Lake Summary: {final_manifest['total_records']} records across {len(final_manifest['partitions'])} partitions ({final_manifest['total_size_mb']} MB)")
    return total_consumed


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Kafka-to-HDFS Consumer")
    parser.add_argument('--cycles', type=int, default=5, help="Idle poll cycles before stopping")
    parser.add_argument('--batch-size', type=int, default=1000, help="Batch size per poll")
    args = parser.parse_args()
    run_consumer(poll_cycles=args.cycles, batch_size=args.batch_size)
