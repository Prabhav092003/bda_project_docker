# Big Data Analytics Project Report
# Mutual Fund Performance Trend Analytics

**Course / Domain:** Big Data Analytics (BDA)  
**Topic:** Real-Time Financial Streaming & Distributed Rolling Return Analytics  
**Architecture:** Apache Kafka $\rightarrow$ Hadoop HDFS $\rightarrow$ MapReduce $\rightarrow$ Comparative Dashboard  
**Dataset:** Mutual Funds India (Kaggle) & Benchmark Indices Time-Series  

---

## 1. Executive Summary & Problem Definition

### 1.1 Context & Problem Statement
Retail investors and portfolio managers constantly face the challenge of evaluating fund performance dynamically. Traditional reports are static, delayed, and rarely present a continuous comparison of fund Net Asset Value (NAV) trajectories against appropriate benchmark indices (e.g., NIFTY 500, Nifty Midcap 150, CRISIL Corporate Bond Index).

Without automated streaming and distributed aggregation:
- NAV records arrive at high volumes daily across thousands of funds.
- Calculating rolling multi-horizon returns (30-day, 60-day, 90-day) requires sliding-window computations across historical time-series datasets.
- Single-node centralized architectures become bottlenecks as historical data grows.

### 1.2 Project Objective
To engineer an end-to-end distributed Big Data pipeline that:
1. Streams daily NAV quotes and benchmark index updates using **Apache Kafka**.
2. Partitions and stores the time-series records in an **Hadoop HDFS Data Lake** (`/user/bda/mutual_funds/nav_data/year=YYYY/month=MM/`).
3. Executes a distributed **Hadoop MapReduce** job that groups records by fund scheme and executes chronological sliding-window calculations to compute rolling 30/60/90-day returns and benchmark excess returns (Alpha $\alpha$).
4. Exposes results via a responsive, interactive **Comparative Performance Dashboard** with real-time pipeline controls.

---

## 2. Big Data Architecture & Pipeline Design

```
+-----------------------------------------------------------------------------------+
|                            DATA GENERATION & INGESTION                            |
|  - 2,556 Mutual Fund Schemes (Kaggle); 250 funds / 25 categories used                     |
|  - Daily NAV & Benchmark Time-Series Generator (180 Trading Days)                 |
+-----------------------------------------------------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                        TIER 1: APACHE KAFKA STREAMING                             |
|  - Producer: Emits daily NAV tick batches keyed by Scheme Name                    |
|  - Topic: mutual_fund_daily_nav                                                   |
|  - Resilient Dual Engine: Supports live clusters (localhost:9092) and local WAL   |
+-----------------------------------------------------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                        TIER 2: HADOOP HDFS DATA LAKE                              |
|  - Consumer: Ingests stream and partitions into time-based directories            |
|  - Path: /user/bda/mutual_funds/nav_data/year=YYYY/month=MM/                      |
|  - Manifest: Tracks file size, partitions, record counts                          |
+-----------------------------------------------------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                     TIER 3: DISTRIBUTED MAPREDUCE ANALYTICS                       |
|  - Mapper (mapper.py): Emits Key=Scheme, Value=(Date, NAV, Benchmark, Val, Cat)  |
|  - Shuffle & Sort: Lexicographical sorting by Scheme key                          |
|  - Reducer (reducer.py): Rolling 30/60/90d returns, Alpha, Beta, Volatility       |
|  - Output: mapreduce_analytics.tsv & analytics_summary.json                       |
+-----------------------------------------------------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                  TIER 4: COMPARATIVE PERFORMANCE DASHBOARD                        |
|  - Flask REST API Backend (app.py)                                                |
|  - Interactive Glassmorphism UI (Chart.js + Vanilla CSS)                          |
|  - Live Pipeline Controls: Stream Next Day, Trigger MapReduce, Continuous Loop    |
+-----------------------------------------------------------------------------------+
```

---

## 3. Mathematical & Financial Formulation

### 3.1 Rolling Investment Returns
For an investment horizon $k \in \{30, 60, 90\}$ days:

$$\text{Return}_{\text{fund}, k}(t) = \left( \frac{\text{NAV}(t) - \text{NAV}(t - k)}{\text{NAV}(t - k)} \right) \times 100\%$$

$$\text{Return}_{\text{bench}, k}(t) = \left( \frac{\text{Index}(t) - \text{Index}(t - k)}{\text{Index}(t - k)} \right) \times 100\%$$

### 3.2 Excess Return (Alpha $\alpha$)
Alpha measures the value added by fund management over the benchmark:

$$\alpha_k(t) = \text{Return}_{\text{fund}, k}(t) - \text{Return}_{\text{bench}, k}(t)$$

- **Outperformance Criterion:** $\alpha_{90}(t) \ge 0$
- **Underperformance Criterion:** $\alpha_{90}(t) < 0$

### 3.3 Systematic Risk (Beta $\beta$)
Beta measures the sensitivity of the fund's returns relative to market benchmark movements:

$$\beta = \frac{\text{Covariance}(R_{\text{fund}}, R_{\text{bench}})}{\text{Variance}(R_{\text{bench}})}$$

### 3.4 Annualized Volatility ($\sigma$)
Daily standard deviation annualized across 252 trading days:

$$\sigma = \sqrt{252} \times \sqrt{\frac{1}{N-1}\sum_{i=1}^{N}(r_i - \bar{r})^2} \times 100\%$$

---

## 4. MapReduce Algorithm Specification

### 4.1 Mapper (`mapreduce/mapper.py`)
- **Input:** STDIN line from partitioned CSV files in HDFS.
- **Parsing:** Extract `Scheme`, `Date`, `Category`, `NAV`, `Benchmark`, `Benchmark_Value`, `Exp_Ratio`.
- **Output:**
  - Key: `Scheme`
  - Value: `Date,NAV,Benchmark,Benchmark_Value,Category,Exp_Ratio`
- **Complexity:** $O(1)$ per record; $O(N)$ total.

### 4.2 Shuffle & Sort Phase
- Hadoop distributes and groups keys so that all records corresponding to a particular mutual fund scheme arrive consecutively at the assigned reducer task.

### 4.3 Reducer (`mapreduce/reducer.py`)
- **Input:** STDIN sorted by `Scheme`.
- **Sliding Window Processing:**
  1. Accumulates all historical $(Date, NAV, BenchmarkValue)$ points for the fund.
  2. Sorts chronological trajectory by date.
  3. For each date $t_i$, locates prior date records at $t_i - 30\text{d}$, $t_i - 60\text{d}$, and $t_i - 90\text{d}$.
  4. Computes rolling fund returns, benchmark returns, alphas, beta, and annualized volatility.
  5. Emits daily trajectory points (`DAILY\t...`) and fund summary ranking records (`SUMMARY\t...`).
- **Complexity:** $O(M \log M)$ per fund where $M$ is the number of historical days.

---

## 5. Experimental Results & Findings

> **Data note:** the NAV and benchmark time-series are *simulated* (seeded random walk, `seed=42`) around the real Kaggle snapshot (scheme, category, benchmark, expense ratio, 1-year CAGR). Results below validate the **pipeline**; they are not real market performance. Figures are for the baseline lake (140 trading days, 2026-02-02 → 2026-08-14) and move as further days are streamed.

### 5.1 Pipeline Execution Metrics
- **Fund Universe:** 250 funds (up to 10 per category) across 25 categories, sampled from the 2,556-scheme Kaggle dataset.
- **Historical Baseline:** 35,000 daily NAV records partitioned into 7 HDFS directories (`year=2026/month=02` … `month=08`).
- **Streaming Feed:** 40 further trading days (10,000 quotes, 250 per day) published through Kafka one day per run.
- **MapReduce Execution Time:** ≈ 4–5 s on the local streaming runner for the full baseline (35,000 records → 35,000 key-value pairs → 250 fund summaries).
- **Idempotent ingestion:** the consumer skips any `(Scheme, Date)` pair already stored, so replays never create duplicates.

### 5.2 Key Analytical Insights (baseline run)
1. **Outperformance Rate:** 83.2% of funds (208 of 250) show positive 90-day alpha versus their benchmark.
2. **Category Alpha Spread:** Small Cap (+6.3%), Large Cap (+5.1%), Flexi Cap (+4.7%), ELSS (+4.5%) and Large & Mid Cap (+4.4%) show the highest average 90-day alpha; Short Duration Debt is close to its benchmark (+0.4%), while Overseas FoFs and Nifty Next 50 index funds slightly lag (-0.3%).
3. **Liquid / Overnight / Money-Market funds** track their CRISIL benchmarks closely (mean |α₉₀| ≈ 0.65%, max ≈ 1.2%).
4. **Risk Profile:** the 29 funds with β > 1.15 are markedly more volatile (mean σ ≈ 19.9% vs 10.8%) but did **not** earn higher 90-day returns in this window (2.2% vs 2.9%) — higher beta bought risk, not reward, in the simulated regime.

### 5.3 Limitations
- Simulated time-series (see data note); no transaction costs, taxes or survivorship effects.
- Beta, volatility and tracking error are computed over the full observed window, not as rolling statistics; only the *returns and alpha* are rolling (30/60/90 days).
- MapReduce runs through a faithful local Hadoop-Streaming emulation by default; `mapreduce/hadoop_job.sh` submits the identical mapper/reducer to a real YARN cluster.

---

## 6. Deployment (Docker)
- `Dockerfile` — `python:3.12-slim`, dependencies layered first, non-root user, health check on `/healthz`, gunicorn (1 worker, 8 threads).
- `docker/entrypoint.sh` — runs `scripts/bootstrap.py` (generate data → build lake → first MapReduce, all idempotent) then serves the dashboard.
- `docker-compose.yml` — one-command run: `docker compose up --build` → http://localhost:5050.
- `docker-compose.cluster.yml` — adds Apache Kafka (KRaft), HDFS NameNode and DataNode; partitions are mirrored through WebHDFS and visible in the NameNode UI (port 9870).

---

## 7. Conclusion
The implemented architecture demonstrates how Apache Kafka for streaming ingestion, an HDFS-partitioned data lake for storage, and MapReduce for per-fund sliding-window analytics fit together into a scalable, reproducible pipeline for mutual-fund performance analysis. Containerisation makes the whole system runnable with a single command, and the optional cluster profile exercises real Kafka and HDFS services.
