"""
dashboard/app.py
Flask Web Application for Mutual Fund Performance Trend Analytics.
Provides REST APIs and serves the comparative performance dashboard.
"""

import os
import sys
import json
import time
import subprocess
import threading
from flask import Flask, render_template, jsonify

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import config
from kafka_pipeline.kafka_engine import kafka_reachable
from kafka_pipeline import stream_state
from kafka_pipeline.kafka_producer import run_producer
from kafka_pipeline.kafka_consumer_hdfs import run_consumer, update_manifest

app = Flask(
    __name__,
    template_folder=os.path.join(PROJECT_ROOT, 'dashboard', 'templates'),
    static_folder=os.path.join(PROJECT_ROOT, 'dashboard', 'static')
)

SUMMARY_FILE = config.SUMMARY_JSON_FILE
RAW_MR_FILE = config.RAW_OUTPUT_FILE

# Serialise pipeline operations (the UI can fire overlapping requests)
pipeline_lock = threading.Lock()

# Cache of per-fund daily history, rebuilt only when the MapReduce TSV changes
_history_cache = {'mtime': None, 'data': {}}


def load_summary_data():
    if os.path.exists(SUMMARY_FILE):
        try:
            with open(SUMMARY_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading summary: {e}")
    return {}


def _num(x):
    return float(x) if x != 'N/A' else None


def _build_history_index():
    index = {}
    with open(RAW_MR_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.rstrip('\n').split('\t')
            if parts[0] != 'DAILY' or len(parts) < 16:
                continue
            index.setdefault(parts[1], []).append({
                'date': parts[2], 'category': parts[3], 'nav': float(parts[4]),
                'benchmark': parts[5], 'bench_val': float(parts[6]),
                'ret_30d': _num(parts[7]), 'bench_ret_30d': _num(parts[8]), 'alpha_30d': _num(parts[9]),
                'ret_60d': _num(parts[10]), 'bench_ret_60d': _num(parts[11]), 'alpha_60d': _num(parts[12]),
                'ret_90d': _num(parts[13]), 'bench_ret_90d': _num(parts[14]), 'alpha_90d': _num(parts[15]),
            })
    for rows in index.values():
        rows.sort(key=lambda x: x['date'])
    return index


def load_fund_daily_history(scheme_name):
    """Daily rolling-return points of one fund, parsed from the MapReduce output (cached)."""
    if not os.path.exists(RAW_MR_FILE):
        return []
    mtime = os.path.getmtime(RAW_MR_FILE)
    if _history_cache['mtime'] != mtime:
        try:
            _history_cache['data'] = _build_history_index()
            _history_cache['mtime'] = mtime
        except Exception as e:
            print(f"Error reading TSV: {e}")
            return []
    return _history_cache['data'].get(scheme_name, [])


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/healthz')
def healthz():
    return jsonify({'status': 'ok'})


@app.route('/api/status')
def get_status():
    manifest = update_manifest()
    summary = load_summary_data()
    state = stream_state.load_state()
    real_kafka = kafka_reachable()
    return jsonify({
        'status': 'healthy',
        'hdfs': {
            'partitions_count': len(manifest.get('partitions', [])),
            'partitions': manifest.get('partitions', []),
            'total_size_mb': manifest.get('total_size_mb', 0),
            'total_records': manifest.get('total_records', 0),
            'cluster_connected': manifest.get('hadoop_cluster_connected', False),
        },
        'kafka': {
            'topic': config.TOPIC_NAME,
            'mode': 'Apache Kafka cluster' if real_kafka else 'Local streaming engine',
            'buffered_messages': state.get('total_published', 0),
            'days_streamed': state.get('next_day_index', 0),
            'last_streamed_date': state.get('last_date') or 'N/A',
        },
        'mapreduce': {
            'last_run': summary.get('timestamp', 'Not run yet'),
            'execution_time': summary.get('execution_time_seconds', 0),
            'funds_analyzed': summary.get('total_funds_analyzed', 0),
            'outperform_rate_pct': summary.get('outperform_rate_pct', 0),
            'latest_data_date': (summary.get('all_funds') or [{}])[0].get('date', 'N/A'),
        },
    })


@app.route('/api/summary')
def get_summary():
    return jsonify(load_summary_data())


@app.route('/api/fund/<path:scheme_name>')
def get_fund_detail(scheme_name):
    history = load_fund_daily_history(scheme_name)
    summary = load_summary_data()
    fund_meta = next((f for f in summary.get('all_funds', []) if f['scheme'] == scheme_name), None)
    return jsonify({'scheme': scheme_name, 'metadata': fund_meta, 'history': history})


@app.route('/api/stream_day', methods=['POST'])
def stream_single_day():
    """Publish the NEXT trading day to Kafka, then ingest it into the HDFS data lake."""
    if not pipeline_lock.acquire(blocking=False):
        return jsonify({'success': False, 'busy': True, 'error': 'Another pipeline operation is running.'}), 409
    try:
        sent = run_producer(delay=0.1, single_day=True)
        if sent == 0:
            return jsonify({'success': True, 'exhausted': True, 'records_streamed': 0,
                            'records_ingested_hdfs': 0,
                            'message': 'All streaming days have been published. Use Reset to replay the demo.'})
        consumed = run_consumer(poll_cycles=2, batch_size=1000)
        last = stream_state.load_state().get('last_date')
        return jsonify({
            'success': True,
            'records_streamed': sent,
            'records_ingested_hdfs': consumed,
            'streamed_date': last,
            'message': f"Streamed {sent} NAV quotes for {last} via Kafka and ingested them into HDFS."
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        pipeline_lock.release()


@app.route('/api/run_mapreduce', methods=['POST'])
def trigger_mapreduce():
    if not pipeline_lock.acquire(blocking=False):
        return jsonify({'success': False, 'busy': True, 'error': 'Another pipeline operation is running.'}), 409
    try:
        script = os.path.join(config.MR_DIR, 'run_mapreduce.py')
        subprocess.run([sys.executable, script], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        summary = load_summary_data()
        return jsonify({
            'success': True,
            'execution_time': summary.get('execution_time_seconds', 0),
            'funds_analyzed': summary.get('total_funds_analyzed', 0),
            'outperform_rate_pct': summary.get('outperform_rate_pct', 0),
            'message': "MapReduce rolling returns computation completed successfully!"
        })
    except subprocess.CalledProcessError as e:
        return jsonify({'success': False, 'error': e.stderr}), 500
    finally:
        pipeline_lock.release()


@app.route('/api/reset', methods=['POST'])
def reset_pipeline_api():
    """Restore the demo to the historical baseline (clears streamed days)."""
    if not pipeline_lock.acquire(blocking=False):
        return jsonify({'success': False, 'busy': True, 'error': 'Another pipeline operation is running.'}), 409
    try:
        from scripts.reset_pipeline import reset
        reset(rerun_mapreduce=True)
        return jsonify({'success': True, 'message': 'Pipeline reset to the historical baseline.'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        pipeline_lock.release()


if __name__ == '__main__':
    port = config.DASHBOARD_PORT
    print(f"Starting Mutual Fund Performance Trend Analytics Dashboard on http://127.0.0.1:{port}")
    app.run(host='0.0.0.0', port=port, debug=False, threaded=True)
