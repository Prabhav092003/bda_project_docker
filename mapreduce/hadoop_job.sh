#!/bin/bash
# ==============================================================================
# Hadoop MapReduce Cluster Execution Script
# Problem Statement: Mutual Fund Performance Trend Analytics
# Rolling 30/60/90-Day Returns vs Benchmark Index
# ==============================================================================

# Always run from the mapreduce/ folder so -files mapper.py,reducer.py resolves
cd "$(dirname "$0")"

# 1. Environment Verification
if [ -z "$HADOOP_HOME" ]; then
    echo "[!] WARNING: HADOOP_HOME environment variable is not set."
    echo "[!] Attempting to locate hadoop-streaming jar in default paths..."
    STREAMING_JAR=$(find / -name "hadoop-streaming*.jar" 2>/dev/null | head -n 1)
else
    STREAMING_JAR=$(find "$HADOOP_HOME" -name "hadoop-streaming*.jar" 2>/dev/null | head -n 1)
fi

if [ -z "$STREAMING_JAR" ]; then
    STREAMING_JAR="/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar"
fi

HDFS_INPUT_DIR="/user/bda/mutual_funds/nav_data/*"
HDFS_OUTPUT_DIR="/user/bda/mutual_funds/output_$(date +%Y%m%d_%H%M%S)"

echo "=================================================================="
echo " Submitting Mutual Fund Analytics MapReduce Job to Apache Hadoop  "
echo "=================================================================="
echo "Streaming Jar: $STREAMING_JAR"
echo "HDFS Input:    $HDFS_INPUT_DIR"
echo "HDFS Output:   $HDFS_OUTPUT_DIR"
echo "Mapper:        mapper.py"
echo "Reducer:       reducer.py"
echo "=================================================================="

# 2. Execute Hadoop Streaming Job
hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="MutualFund_Rolling_Returns_Analytics" \
    -D mapreduce.job.reduces=4 \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input "$HDFS_INPUT_DIR" \
    -output "$HDFS_OUTPUT_DIR"

# 3. Check Status and Display Top Outperforming Funds
if [ $? -eq 0 ]; then
    echo "[+] MapReduce Job Succeeded!"
    echo "Fetching preview from HDFS output..."
    hadoop fs -cat "$HDFS_OUTPUT_DIR/part-*" | grep "^SUMMARY" | head -n 15
else
    echo "[-] MapReduce Job Failed. Check YARN ResourceManager logs."
    exit 1
fi
