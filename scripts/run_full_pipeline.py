"""
run_full_pipeline.py
One-click end-to-end execution of Mutual Fund Performance Trend Analytics.
  1. Verifies / generates the historical time-series baseline and stream feed
  2. Streams the next trading day of NAV quotes through Kafka
  3. Ingests the stream into the partitioned HDFS data lake
  4. Runs the MapReduce rolling 30/60/90-day return job
  5. Launches the comparative dashboard (unless --skip-server)
"""

import os
import sys
import argparse
import subprocess

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


def print_banner():
    print("""
    ╔════════════════════════════════════════════════════════════════════════╗
    ║       MUTUAL FUND PERFORMANCE TREND ANALYTICS (BIG DATA ANALYTICS)     ║
    ║   Kafka Streaming  •  Hadoop HDFS Lake  •  MapReduce Rolling Returns   ║
    ╚════════════════════════════════════════════════════════════════════════╝
    """)


def step(title, *cmd):
    print(f"\n{title}")
    subprocess.run([sys.executable, *cmd], check=True, cwd=config.PROJECT_ROOT)


def main():
    print_banner()
    parser = argparse.ArgumentParser(description="Mutual Fund Big Data Pipeline Orchestrator")
    parser.add_argument('--skip-server', action='store_true', help="Run the data pipeline only, without the web dashboard")
    parser.add_argument('--port', type=int, default=config.DASHBOARD_PORT, help="Dashboard port (default: 5050)")
    args = parser.parse_args()

    step("[STEP 1/5] Preparing data lake & historical baseline...", os.path.join('scripts', 'bootstrap.py'))
    step("[STEP 2/5] Kafka Producer: streaming next day of NAV & benchmark quotes...",
         os.path.join('kafka_pipeline', 'kafka_producer.py'), '--single-day')
    step("[STEP 3/5] Kafka Consumer: ingesting stream into the partitioned HDFS lake...",
         os.path.join('kafka_pipeline', 'kafka_consumer_hdfs.py'), '--cycles', '2')
    step("[STEP 4/5] MapReduce job (Mapper -> Shuffle & Sort -> Reducer)...",
         os.path.join('mapreduce', 'run_mapreduce.py'))

    print("\n" + "=" * 70)
    print(" [✓] PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
    print("=" * 70)

    if not args.skip_server:
        print(f"\n[STEP 5/5] Launching dashboard on http://127.0.0.1:{args.port}  (Ctrl+C to stop)\n")
        env = os.environ.copy()
        env['PORT'] = str(args.port)
        subprocess.run([sys.executable, os.path.join('dashboard', 'app.py')], env=env, cwd=config.PROJECT_ROOT)


if __name__ == '__main__':
    main()
