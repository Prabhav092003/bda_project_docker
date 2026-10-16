#!/bin/sh
# Container start-up: idempotent bootstrap (data -> HDFS lake -> first MapReduce run),
# then serve the dashboard with gunicorn.
set -e
cd "$(dirname "$0")/.."

echo "================================================================"
echo " Mutual Fund Performance Trend Analytics  -  starting container"
echo "================================================================"

python scripts/bootstrap.py

# One worker + threads: the pipeline lock and Kafka-emulator state live in this process.
exec gunicorn \
    --workers 1 --threads 8 --timeout 600 \
    --bind "0.0.0.0:${PORT:-5050}" \
    --access-logfile - \
    dashboard.app:app
