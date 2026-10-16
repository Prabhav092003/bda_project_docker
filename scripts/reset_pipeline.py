"""
reset_pipeline.py
Restores the demo to its starting point:
  * HDFS data lake  -> 140-day historical baseline only (year=YYYY/month=MM partitions)
  * Kafka emulator  -> write-ahead log, consumer offsets and stream cursor cleared
  * MapReduce output -> recomputed on the baseline (optional)
Usable from the CLI (python scripts/reset_pipeline.py) and from the dashboard (Reset button).
"""

import os
import sys
import subprocess

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from kafka_pipeline import stream_state
from kafka_pipeline.kafka_consumer_hdfs import update_manifest


def reset(rerun_mapreduce=True):
    # 1. Make sure the synthetic time-series exists
    if not os.path.exists(config.HIST_FILE) or not os.path.exists(config.TICKS_FILE):
        subprocess.run([sys.executable, os.path.join(config.PROJECT_ROOT, 'scripts', 'generate_timeseries_data.py')], check=True)

    # 2. Reset broker emulator state + stream cursor
    for f in (config.WAL_FILE, config.OFFSET_FILE):
        if os.path.exists(f):
            os.remove(f)
    stream_state.reset_state()

    # 3. Rebuild the data lake from the historical baseline
    from scripts.generate_timeseries_data import write_hdfs_baseline
    write_hdfs_baseline()
    update_manifest()

    # 4. Recompute analytics on the baseline
    if rerun_mapreduce:
        subprocess.run([sys.executable, os.path.join(config.MR_DIR, 'run_mapreduce.py')], check=True)
    print("[Reset] Pipeline restored to the historical baseline.")


if __name__ == '__main__':
    reset(rerun_mapreduce='--no-mapreduce' not in sys.argv)
