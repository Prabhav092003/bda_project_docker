"""
config.py
Central configuration for the Mutual Fund Performance Trend Analytics project.
All paths are absolute (derived from this file), so every module works no matter
which directory it is launched from - locally or inside Docker.
Runtime behaviour can be tuned with environment variables (see docker-compose.yml).
"""

import os

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# --- Data ---
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
TS_DIR = os.path.join(DATA_DIR, 'time_series')
HIST_FILE = os.path.join(TS_DIR, 'historical_navs.csv')
TICKS_FILE = os.path.join(TS_DIR, 'streaming_ticks.jsonl')

# --- Kafka (local broker emulation state) ---
KAFKA_DIR = os.path.join(PROJECT_ROOT, 'kafka_pipeline')
WAL_FILE = os.path.join(KAFKA_DIR, 'wal_stream.log')
OFFSET_FILE = os.path.join(KAFKA_DIR, 'consumer_offsets.json')
STREAM_STATE_FILE = os.path.join(KAFKA_DIR, 'stream_state.json')
TOPIC_NAME = 'mutual_fund_daily_nav'
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

# --- HDFS data lake ---
HDFS_ROOT = os.path.join(PROJECT_ROOT, 'hdfs_storage')
HDFS_BASE_DIR = os.path.join(HDFS_ROOT, 'user', 'bda', 'mutual_funds', 'nav_data')
MANIFEST_PATH = os.path.join(HDFS_ROOT, 'user', 'bda', 'mutual_funds', 'hdfs_manifest.json')
HDFS_LOGICAL_PATH = '/user/bda/mutual_funds/nav_data'
# Optional: WebHDFS endpoint of a real HDFS cluster (docker compose --profile cluster)
WEBHDFS_URL = os.getenv('WEBHDFS_URL', '')

# --- MapReduce ---
MR_DIR = os.path.join(PROJECT_ROOT, 'mapreduce')
MR_OUTPUT_DIR = os.path.join(MR_DIR, 'output')
RAW_OUTPUT_FILE = os.path.join(MR_OUTPUT_DIR, 'mapreduce_analytics.tsv')
SUMMARY_JSON_FILE = os.path.join(MR_OUTPUT_DIR, 'analytics_summary.json')

# --- Dashboard ---
DASHBOARD_PORT = int(os.getenv('PORT', '5050'))
