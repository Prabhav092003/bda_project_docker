# Mutual Fund Performance Trend Analytics

> **Big Data Analytics (BDA) Project**
> **Stack:** Apache Kafka · Hadoop HDFS · MapReduce (Hadoop Streaming) · Python · Flask · Chart.js · Docker
> **Data:** *Mutual Funds India* (Kaggle) snapshot + simulated daily NAV / benchmark time-series

---

## 🚀 Quick Start (Docker)

**Requirements:** Docker Desktop (or Docker Engine + Compose v2). Nothing else.

```bash
docker compose up --build
```

Open **http://localhost:5050** — that's it. On first start the container automatically
(1) verifies the data, (2) builds the partitioned HDFS-style data lake, (3) runs the MapReduce job,
and (4) serves the dashboard. Stop with `Ctrl+C`, remove with `docker compose down`.

| What you want | Command |
|---|---|
| Run in the background | `docker compose up --build -d` |
| Watch logs | `docker compose logs -f app` |
| Use another port (e.g. 8080) | `DASHBOARD_PORT=8080 docker compose up --build` |
| Fresh demo state | `docker compose down && docker compose up` (or the **↺ Reset Demo** button) |
| Run a CLI step inside the container | `docker compose exec app python kafka_pipeline/kafka_producer.py --single-day` |
| Open a shell | `docker compose exec app sh` |

### Optional: real Apache Kafka + real HDFS containers

```bash
docker compose -f docker-compose.yml -f docker-compose.cluster.yml up --build
```

This adds an Apache Kafka broker (KRaft mode) and an HDFS NameNode + DataNode next to the app.
The dashboard then shows **Apache Kafka cluster** / **Mirrored to real HDFS cluster**, and every
ingested partition appears in the NameNode UI at **http://localhost:9870** → *Utilities → Browse the file system*
→ `/user/bda/mutual_funds/nav_data/`. First start takes a few minutes (image downloads, ~1 GB).

Verify Kafka received the stream:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic mutual_fund_daily_nav --from-beginning --max-messages 3
```

---

## 🎬 Demo Script (3 minutes)

1. **Dashboard overview** — KPIs, rolling-return chart (fund vs benchmark vs alpha), category alpha, risk–return scatter.
2. Note the status cards: Kafka `0 msgs`, HDFS `35,000 records`, data as of **2026-08-14**.
3. Click **⚡ Stream Next Day (Kafka)** → Kafka count goes to 250, HDFS grows, data advances to the next trading day.
4. Click **⚙️ Run MapReduce Job** → analytics refresh; the charts extend by one day.
5. Use **▶ Live Ingestion Loop** to stream continuously (stops automatically after all 40 days).
6. Open the **Big Data Pipeline** tab to explain Kafka → HDFS → Map → Shuffle → Reduce.
7. **↺ Reset Demo** returns to the baseline so you can repeat.

---

## 📌 Problem Statement & Objective

**Problem Statement:**
Retail investors and fund analysts lack an automated, continuously updated, and scalable system to compare mutual fund performance relative to benchmark indices over multiple investment horizons.

**Objective:**
1. Stream daily Net Asset Value (NAV) quotes and benchmark indices via **Apache Kafka**.
2. Store and partition time-series records in a **Hadoop HDFS data lake** (`/user/bda/mutual_funds/nav_data/year=YYYY/month=MM/`).
3. Execute a distributed **MapReduce** job computing rolling **30/60/90-day returns** per fund versus its benchmark, plus alpha (α), beta (β), volatility and tracking error.
4. Provide an interactive **Comparative Performance Dashboard**.

---

## 🧪 About the Data (please read before presenting)

- **Real (from the Kaggle snapshot):** scheme names, categories, benchmark, expense ratio, latest NAV, 1-year CAGR.
- **Simulated:** the daily NAV and benchmark-index time-series. The Kaggle file only holds one NAV per scheme, so
  `scripts/generate_timeseries_data.py` generates **180 business days** with a seeded (`seed=42`) correlated random walk —
  each fund has its own beta, idiosyncratic noise, expense drag and drift derived from its real 1-year CAGR.
  It is deterministic: re-running always produces the same data.
- **Universe:** 250 funds (up to 10 per category) across 25 categories — kept small so the demo is fast.
- **Split:** first 140 days seed the HDFS lake (baseline, 2026-02-02 → 2026-08-14);
  the last 40 days (2026-08-17 → 2026-10-09) are streamed through Kafka one day at a time.

Because the series are simulated, the numbers illustrate the *pipeline*; they are not investment advice or real fund performance.

---

## 🏗️ End-to-End System Architecture

```
                                  [ Daily NAV & Benchmark Feeds ]
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │    Kafka Producer     │
                                    │  (kafka_producer.py)  │
                                    └───────────┬───────────┘
                                                │ Topic: mutual_fund_daily_nav
                                                ▼
                                    ┌───────────────────────┐
                                    │     Kafka Broker      │
                                    │  (Cluster / Emulated) │
                                    └───────────┬───────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │  Kafka-HDFS Consumer  │
                                    │(kafka_consumer_hdfs)  │
                                    └───────────┬───────────┘
                                                │
                                                ▼
                     ┌─────────────────────────────────────────────────────┐
                     │                  Hadoop HDFS Lake                   │
                     │  /user/bda/mutual_funds/nav_data/year=YYYY/month=MM │
                     └──────────────────────────┬──────────────────────────┘
                                                │
                     ┌──────────────────────────┴──────────────────────────┐
                     ▼                                                     ▼
        ┌─────────────────────────┐                           ┌─────────────────────────┐
        │        mapper.py        │                           │        mapper.py        │
        │ Key: Scheme             │                           │ Key: Scheme             │
        │ Val: Date,NAV,Bench,... │                           │ Val: Date,NAV,Bench,... │
        └────────────┬────────────┘                           └────────────┬────────────┘
                     │                                                     │
                     └──────────────────────────┬──────────────────────────┘
                                                │
                                    ┌───────────▼───────────┐
                                    │ Hadoop Shuffle & Sort │
                                    │  (Group by Scheme)    │
                                    └───────────┬───────────┘
                                                │
                     ┌──────────────────────────┴──────────────────────────┐
                     ▼                                                     ▼
        ┌─────────────────────────┐                           ┌─────────────────────────┐
        │       reducer.py        │                           │       reducer.py        │
        │ Chronological Sliding   │                           │ Chronological Sliding   │
        │ Window Rolling Returns  │                           │ Window Rolling Returns  │
        └────────────┬────────────┘                           └────────────┬────────────┘
                     │                                                     │
                     └──────────────────────────┬──────────────────────────┘
                                                │
                                                ▼
                                ┌───────────────────────────────┐
                                │   MapReduce Output TSV/JSON   │
                                │   - Rolling 30/60/90d Return  │
                                │   - Benchmark Alpha & Beta    │
                                │   - Outperformance Rating     │
                                └───────────────┬───────────────┘
                                                │
                                                ▼
                                ┌───────────────────────────────┐
                                │     Comparative Dashboard     │
                                │    (Flask API + Chart.js)     │
                                │    http://127.0.0.1:5050      │
                                └───────────────────────────────┘
```

**Docker view**

```
 docker compose up                          + docker-compose.cluster.yml
┌──────────────────────────────┐            ┌────────────┐  ┌──────────────────────┐
│ app container  :5050         │ ─────────▶ │ Kafka      │  │ HDFS NameNode :9870  │
│  Flask/gunicorn dashboard    │  (cluster  │ (KRaft)    │  │ HDFS DataNode        │
│  Kafka producer / consumer   │   mode)    └────────────┘  └──────────────────────┘
│  HDFS-style lake + MapReduce │ ─────────────────────────▶  WebHDFS mirror of every partition
└──────────────────────────────┘
```

---

## 📐 Mathematical Formulation

### 1. Rolling Fund Return
For a rolling window of $k \in \{30, 60, 90\}$ days:
$$R_{\text{fund}, k}(t) = \frac{\text{NAV}(t) - \text{NAV}(t - k)}{\text{NAV}(t - k)} \times 100\%$$

### 2. Rolling Benchmark Return
$$R_{\text{bench}, k}(t) = \frac{\text{Bench}(t) - \text{Bench}(t - k)}{\text{Bench}(t - k)} \times 100\%$$

### 3. Excess Return (Alpha $\alpha$)
$$\alpha_k(t) = R_{\text{fund}, k}(t) - R_{\text{bench}, k}(t)$$

- **Outperforming:** $\alpha_k(t) \ge 0$
- **Underperforming:** $\alpha_k(t) < 0$

### 4. Beta ($\beta$) — over the full observed window
$$\beta = \frac{\text{Covariance}(R_{\text{fund}}, R_{\text{bench}})}{\text{Variance}(R_{\text{bench}})}$$

### 5. Annualized Volatility ($\sigma$)
$$\sigma = \sqrt{252} \times \sqrt{\frac{1}{N-1} \sum_{i=1}^N (r_i - \bar{r})^2} \times 100\%$$

---

## 📂 Repository File Structure

```
bda project/
├── Dockerfile                        # App image (python:3.12-slim + gunicorn)
├── docker-compose.yml                # Default one-container stack
├── docker-compose.cluster.yml        # Optional: + real Kafka & HDFS containers
├── docker/entrypoint.sh              # Bootstrap + start gunicorn
├── config.py                         # Central absolute paths & environment settings
├── requirements.txt
├── README.md · PROJECT_REPORT.md
├── data/
│   ├── raw/Mutual_Funds.csv          # Kaggle Mutual Funds India dataset
│   └── time_series/                  # historical_navs.csv · streaming_ticks.jsonl · fund_master.json
├── kafka_pipeline/
│   ├── kafka_engine.py               # Dual-mode Kafka client (real cluster or local WAL broker)
│   ├── kafka_producer.py             # Streams the NEXT trading day each run (cursor in stream_state.py)
│   ├── kafka_consumer_hdfs.py        # Idempotent Kafka → partitioned HDFS lake ingestion
│   ├── hdfs_client.py                # WebHDFS client (cluster mode)
│   └── stream_state.py               # Persistent streaming cursor
├── hdfs_storage/user/bda/mutual_funds/nav_data/year=YYYY/month=MM/   # the data lake
├── mapreduce/
│   ├── mapper.py · reducer.py        # Hadoop Streaming mapper / reducer
│   ├── run_mapreduce.py              # Local streaming runner (cat | map | sort | reduce)
│   ├── hadoop_job.sh                 # Submit to a real Hadoop/YARN cluster
│   └── output/                       # mapreduce_analytics.tsv · analytics_summary.json
├── dashboard/                        # Flask app + self-hosted (offline) Chart.js, fonts, CSS, JS
└── scripts/
    ├── generate_timeseries_data.py   # Builds the simulated time-series + baseline lake
    ├── bootstrap.py                  # Idempotent first-run setup (used by Docker)
    ├── reset_pipeline.py             # Restore the demo baseline
    └── run_full_pipeline.py          # One-click run without Docker
```

---

## 💻 Running Without Docker

```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python scripts/run_full_pipeline.py                    # pipeline + dashboard on http://127.0.0.1:5050
```

Individual steps (all work from any directory):
```bash
python scripts/bootstrap.py                            # data + lake + first MapReduce
python kafka_pipeline/kafka_producer.py --single-day   # stream the next trading day
python kafka_pipeline/kafka_consumer_hdfs.py --cycles 2
python mapreduce/run_mapreduce.py
python dashboard/app.py
python scripts/reset_pipeline.py                       # back to the baseline
```

---

## 🌐 Running MapReduce on a Real Hadoop Cluster

```bash
bash mapreduce/hadoop_job.sh
```
or directly:
```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    -D mapreduce.job.name="MutualFund_Rolling_Returns" -D mapreduce.job.reduces=4 \
    -files mapreduce/mapper.py,mapreduce/reducer.py \
    -mapper "python3 mapper.py" -reducer "python3 reducer.py" \
    -input "/user/bda/mutual_funds/nav_data/*" -output "/user/bda/mutual_funds/output"
```
(Hadoop nodes need `python3`. The reducer sorts each fund's records itself, so it does not rely on secondary sort.)

---

## 📊 Dashboard Features

- **Live Pipeline Control Center** — stream the next day, run MapReduce, continuous ingestion loop, reset demo.
- **Rolling Returns Trajectory** — fund vs benchmark 90-day return and excess alpha over time.
- **Category Alpha** — average 90-day alpha by category.
- **Top vs Bottom Alpha** and **Risk–Return Scatter** (volatility vs 90-day return).
- **Fund Explorer** — search, filter by category / status, inspect any fund.
- **Big Data Architecture tab** — Kafka, HDFS, Map and Reduce explained.
- Fully **offline**: Chart.js and fonts are bundled, no CDN needed.

**Status rule:** a fund is *OUTPERFORMING* when its 90-day alpha ≥ 0 (falls back to 60/30-day if 90-day history is not yet available).

---

## 🛠️ Troubleshooting

| Symptom | Fix |
|---|---|
| `port is already allocated` | `DASHBOARD_PORT=8080 docker compose up --build` |
| Page loads but charts are empty | Wait ~30 s on first start (initial MapReduce), then refresh; check `docker compose logs app` |
| Want to replay the 40 streaming days | Click **↺ Reset Demo** |
| `exec docker/entrypoint.sh: no such file` (Windows) | Line endings — the Dockerfile normalises them; run `docker compose build --no-cache` |
| Cluster mode: dashboard says *Local streaming engine* | Kafka still starting — wait a minute; status is re-probed every 30 s |
| Cluster mode: NameNode UI empty | Wait for the DataNode to register (~1 min), then click *Stream Next Day* or restart `app` |
