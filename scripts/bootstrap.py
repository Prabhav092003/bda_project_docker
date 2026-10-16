"""
bootstrap.py
Idempotent first-run setup used by the Docker entrypoint (safe to run on every start):
  1. generate the synthetic time-series data if it is missing
  2. build the HDFS-style data lake from the historical baseline if it is empty
  3. run the MapReduce job once if no analytics exist yet
  4. (cluster profile) wait for the real HDFS NameNode and mirror the lake into it
"""

import os
import sys
import time
import glob
import subprocess

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from kafka_pipeline import hdfs_client
from kafka_pipeline.kafka_consumer_hdfs import update_manifest


def run(script):
    subprocess.run([sys.executable, os.path.join(config.PROJECT_ROOT, script)], check=True)


def sync_lake_to_real_hdfs(max_wait=120):
    if not config.WEBHDFS_URL:
        return
    print(f"[Bootstrap] Waiting for HDFS NameNode at {config.WEBHDFS_URL} ...")
    deadline = time.time() + max_wait
    while time.time() < deadline and not hdfs_client.is_available():
        time.sleep(3)
    if not hdfs_client.is_available():
        print("[Bootstrap] HDFS cluster not reachable - continuing with the local data lake only.")
        return
    files = sorted(glob.glob(os.path.join(config.HDFS_BASE_DIR, 'year=*', 'month=*', '*.csv')))
    for fp in files:
        rel = os.path.relpath(fp, config.HDFS_BASE_DIR).replace(os.sep, '/')
        for attempt in range(10):  # NameNode may still be in safe mode right after start-up
            try:
                hdfs_client.upload_file(fp, f"{config.HDFS_LOGICAL_PATH}/{rel}")
                print(f"[Bootstrap] -> hdfs:{config.HDFS_LOGICAL_PATH}/{rel}")
                break
            except Exception as e:
                if attempt == 9:
                    print(f"[Bootstrap] Could not upload {rel}: {e}")
                time.sleep(3)


def main():
    if not (os.path.exists(config.HIST_FILE) and os.path.exists(config.TICKS_FILE)):
        print("[Bootstrap] Generating time-series data ...")
        run(os.path.join('scripts', 'generate_timeseries_data.py'))

    lake_files = glob.glob(os.path.join(config.HDFS_BASE_DIR, 'year=*', 'month=*', '*.csv'))
    if not lake_files:
        print("[Bootstrap] Building HDFS data lake from historical baseline ...")
        from scripts.generate_timeseries_data import write_hdfs_baseline
        write_hdfs_baseline()
    update_manifest()

    if not os.path.exists(config.SUMMARY_JSON_FILE):
        print("[Bootstrap] Running initial MapReduce job ...")
        run(os.path.join('mapreduce', 'run_mapreduce.py'))

    sync_lake_to_real_hdfs()
    print("[Bootstrap] Ready.")


if __name__ == '__main__':
    main()
