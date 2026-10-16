/* ==========================================================================
   Mutual Fund Performance Trend Analytics - Interactive Dashboard Logic
   Chart.js integration, live pipeline controls, dynamic filtering
   ========================================================================== */

let analyticsData = null;
let currentFundHistory = null;
let selectedFundScheme = null;

// Chart references
let rollingReturnChart = null;
let categoryAlphaChart = null;
let topAlphaChart = null;
let riskReturnChart = null;

let isContinuousStreaming = false;
let streamingInterval = null;

document.addEventListener('DOMContentLoaded', () => {
  initTabs();
  initEventListeners();
  loadDashboardData();
  loadPipelineStatus();
  // Poll status periodically
  setInterval(loadPipelineStatus, 10000);
});

function initTabs() {
  const tabs = document.querySelectorAll('.tab-btn');
  tabs.forEach(tab => {
    tab.addEventListener('click', () => {
      tabs.forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      const target = tab.dataset.tab;
      
      if (target === 'analytics') {
        document.getElementById('analytics-view').style.display = 'block';
        document.getElementById('architecture-view').style.display = 'none';
      } else {
        document.getElementById('analytics-view').style.display = 'none';
        document.getElementById('architecture-view').style.display = 'block';
      }
    });
  });
}

function initEventListeners() {
  // Action Buttons
  document.getElementById('btn-stream-day').addEventListener('click', handleStreamSingleDay);
  document.getElementById('btn-run-mr').addEventListener('click', handleRunMapReduce);
  document.getElementById('btn-toggle-stream').addEventListener('click', toggleContinuousStream);
  document.getElementById('btn-reset').addEventListener('click', handleReset);

  // Table "Inspect" buttons (event delegation - safe for scheme names with quotes)
  document.getElementById('funds-table-body').addEventListener('click', (e) => {
    const btn = e.target.closest('button[data-scheme]');
    if (btn) selectFund(btn.dataset.scheme);
  });
  
  // Table search & filters
  document.getElementById('search-input').addEventListener('input', filterAndRenderTable);
  document.getElementById('filter-category').addEventListener('change', filterAndRenderTable);
  document.getElementById('filter-status').addEventListener('change', filterAndRenderTable);
  
  // Fund selector dropdown for main chart
  document.getElementById('fund-select').addEventListener('change', (e) => {
    loadFundDeepDive(e.target.value);
  });
}

// Show Toast Message
function showToast(message, type = 'info') {
  const toast = document.getElementById('toast');
  const msgEl = document.getElementById('toast-msg');
  msgEl.textContent = message;
  toast.classList.add('show');
  setTimeout(() => {
    toast.classList.remove('show');
  }, 3500);
}

// Pipeline Status API
async function loadPipelineStatus() {
  try {
    const res = await fetch('/api/status');
    const data = await res.json();
    
    // Update HDFS status
    document.getElementById('status-hdfs-records').textContent = `${data.hdfs.total_records.toLocaleString()} records`;
    document.getElementById('status-hdfs-sub').textContent = `${data.hdfs.partitions_count} partitions (${data.hdfs.total_size_mb} MB)`;
    document.getElementById('status-hdfs-mode').textContent = data.hdfs.cluster_connected ? 'Mirrored to real HDFS cluster' : 'Local HDFS-style lake';
    
    // Update Kafka status
    document.getElementById('status-kafka-msgs').textContent = `${data.kafka.buffered_messages.toLocaleString()} msgs published`;
    document.getElementById('status-kafka-sub').textContent = `Topic: ${data.kafka.topic} | ${data.kafka.days_streamed} day(s) streamed`;
    document.getElementById('status-kafka-mode').textContent = data.kafka.mode;
    document.getElementById('footer-asof').textContent = `Data as of: ${data.mapreduce.latest_data_date}`;
    
    // Update MapReduce status
    document.getElementById('status-mr-info').textContent = `${data.mapreduce.funds_analyzed} funds analyzed`;
    document.getElementById('status-mr-sub').textContent = `Last Run: ${data.mapreduce.last_run.split(' ')[1] || 'Ready'}`;
  } catch (err) {
    console.error('Failed to load status:', err);
  }
}

// Main Analytics API
async function loadDashboardData() {
  try {
    const res = await fetch('/api/summary');
    analyticsData = await res.json();
    
    if (!analyticsData || !analyticsData.all_funds) {
      console.warn('No summary data yet.');
      return;
    }
    
    updateKpis(analyticsData);
    populateFundSelector(analyticsData.all_funds);
    populateCategoryFilter(analyticsData.category_aggregates);
    renderCategoryAlphaChart(analyticsData.category_aggregates);
    renderTopAlphaChart(analyticsData.top_performers, analyticsData.underperformers);
    renderRiskReturnChart(analyticsData.all_funds);
    filterAndRenderTable();
    
    // Select initial top performer for deep dive
    if (analyticsData.top_performers && analyticsData.top_performers.length > 0) {
      const topFund = analyticsData.top_performers[0].scheme;
      document.getElementById('fund-select').value = topFund;
      loadFundDeepDive(topFund);
    }
  } catch (err) {
    console.error('Error loading dashboard data:', err);
  }
}

function updateKpis(data) {
  document.getElementById('kpi-funds-count').textContent = data.total_funds_analyzed.toLocaleString();
  const nCats = (data.category_aggregates || []).length;
  document.getElementById('kpi-funds-desc').textContent = `Across ${nCats} Equity, Debt & Hybrid categories`;
  document.getElementById('kpi-outperform-rate').textContent = `${data.outperform_rate_pct}%`;
  
  if (data.top_performers && data.top_performers.length > 0) {
    const top = data.top_performers[0];
    document.getElementById('kpi-top-fund-name').textContent = top.scheme;
    document.getElementById('kpi-top-alpha').textContent = `+${top.alpha_90d}% Alpha (90d)`;
  }
  
  document.getElementById('kpi-runtime').textContent = `${data.execution_time_seconds}s`;
}

function populateFundSelector(funds) {
  const sel = document.getElementById('fund-select');
  sel.innerHTML = '';
  funds.forEach(f => {
    const opt = document.createElement('option');
    opt.value = f.scheme;
    opt.textContent = `${f.scheme} (${f.category})`;
    sel.appendChild(opt);
  });
}

function populateCategoryFilter(categories) {
  const sel = document.getElementById('filter-category');
  sel.innerHTML = '<option value="ALL">All Categories</option>';
  categories.forEach(c => {
    const opt = document.createElement('option');
    opt.value = c.category;
    opt.textContent = `${c.category} (${c.fund_count})`;
    sel.appendChild(opt);
  });
}

// Deep Dive into Fund History
async function loadFundDeepDive(scheme) {
  selectedFundScheme = scheme;
  try {
    const res = await fetch(`/api/fund/${encodeURIComponent(scheme)}`);
    const data = await res.json();
    currentFundHistory = data.history;
    
    if (data.metadata) {
      document.getElementById('fund-meta-display').textContent = 
        `Benchmark: ${data.metadata.benchmark} | Current NAV: ₹${data.metadata.nav.toFixed(2)} | Exp Ratio: ${data.metadata.exp_ratio}% | Beta: ${data.metadata.beta}`;
    }
    
    renderRollingReturnChart(data.history, scheme);
  } catch (err) {
    console.error('Error loading fund deep dive:', err);
  }
}

// Primary Chart: Rolling Return Fund vs Benchmark
function renderRollingReturnChart(history, schemeName) {
  const ctx = document.getElementById('rollingReturnChart').getContext('2d');
  
  if (rollingReturnChart) {
    rollingReturnChart.destroy();
  }
  
  if (!history || history.length === 0) {
    return;
  }
  
  const labels = history.map(h => h.date);
  const fundRet90 = history.map(h => h.ret_90d);
  const benchRet90 = history.map(h => h.bench_ret_90d);
  const alpha90 = history.map(h => h.alpha_90d);
  
  rollingReturnChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: labels,
      datasets: [
        {
          label: 'Fund Rolling 90d Return (%)',
          data: fundRet90,
          borderColor: '#38bdf8',
          backgroundColor: 'rgba(56, 189, 248, 0.12)',
          fill: true,
          borderWidth: 2.5,
          tension: 0.25,
          pointRadius: 1,
          pointHoverRadius: 5
        },
        {
          label: 'Benchmark 90d Return (%)',
          data: benchRet90,
          borderColor: '#f59e0b',
          borderDash: [5, 5],
          backgroundColor: 'transparent',
          borderWidth: 2,
          tension: 0.25,
          pointRadius: 1,
          pointHoverRadius: 5
        },
        {
          label: 'Excess Alpha 90d (%)',
          data: alpha90,
          borderColor: '#10b981',
          backgroundColor: 'rgba(16, 185, 129, 0.08)',
          fill: true,
          borderWidth: 1.5,
          tension: 0.25,
          pointRadius: 0,
          pointHoverRadius: 4
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: 'index',
        intersect: false
      },
      plugins: {
        legend: {
          labels: { color: '#cbd5e1', font: { family: 'Outfit', size: 12 } }
        },
        tooltip: {
          backgroundColor: 'rgba(15, 23, 42, 0.95)',
          titleColor: '#38bdf8',
          bodyColor: '#f8fafc',
          borderColor: 'rgba(56, 189, 248, 0.3)',
          borderWidth: 1,
          callbacks: {
            label: (ctx) => `${ctx.dataset.label}: ${ctx.raw !== null ? ctx.raw + '%' : 'N/A'}`
          }
        }
      },
      scales: {
        x: {
          ticks: { color: '#64748b', maxTicksLimit: 10 },
          grid: { color: 'rgba(255, 255, 255, 0.05)' }
        },
        y: {
          ticks: { 
            color: '#94a3b8',
            callback: (v) => v + '%'
          },
          grid: { color: 'rgba(255, 255, 255, 0.05)' }
        }
      }
    }
  });
}

// Category Alpha Generation Chart
function renderCategoryAlphaChart(categories) {
  const ctx = document.getElementById('categoryAlphaChart').getContext('2d');
  if (categoryAlphaChart) categoryAlphaChart.destroy();
  
  const topCategories = categories.slice(0, 8);
  const labels = topCategories.map(c => c.category.length > 20 ? c.category.slice(0, 18) + '...' : c.category);
  const alphas = topCategories.map(c => c.avg_alpha_90d);
  const colors = alphas.map(a => a >= 0 ? 'rgba(16, 185, 129, 0.75)' : 'rgba(244, 63, 94, 0.75)');
  
  categoryAlphaChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [{
        label: 'Average 90d Alpha (%)',
        data: alphas,
        backgroundColor: colors,
        borderRadius: 6
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false }
      },
      scales: {
        x: {
          ticks: { color: '#94a3b8', font: { size: 10 } },
          grid: { display: false }
        },
        y: {
          ticks: { 
            color: '#94a3b8',
            callback: (v) => v + '%'
          },
          grid: { color: 'rgba(255, 255, 255, 0.05)' }
        }
      }
    }
  });
}

// Top Alpha Generators vs Underperformers
function renderTopAlphaChart(top, under) {
  const ctx = document.getElementById('topAlphaChart').getContext('2d');
  if (topAlphaChart) topAlphaChart.destroy();
  
  const combined = [...(top ? top.slice(0, 5) : []), ...(under ? under.slice(0, 5) : [])];
  const labels = combined.map(f => f.scheme.length > 22 ? f.scheme.slice(0, 20) + '...' : f.scheme);
  const alphas = combined.map(f => f.alpha_90d);
  const colors = alphas.map(a => a >= 0 ? '#10b981' : '#f43f5e');
  
  topAlphaChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [{
        label: 'Rolling 90-Day Alpha vs Benchmark (%)',
        data: alphas,
        backgroundColor: colors,
        borderRadius: 6
      }]
    },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false }
      },
      scales: {
        x: {
          ticks: { 
            color: '#94a3b8',
            callback: (v) => v + '%'
          },
          grid: { color: 'rgba(255, 255, 255, 0.05)' }
        },
        y: {
          ticks: { color: '#cbd5e1', font: { size: 11 } },
          grid: { display: false }
        }
      }
    }
  });
}

// Risk-Return Scatter Plot
function renderRiskReturnChart(funds) {
  const ctx = document.getElementById('riskReturnChart').getContext('2d');
  if (riskReturnChart) riskReturnChart.destroy();
  
  const outData = [];
  const underData = [];
  
  funds.forEach(f => {
    const pt = {
      x: f.volatility,
      y: f.ret_90d,
      scheme: f.scheme,
      alpha: f.alpha_90d,
      category: f.category
    };
    if (f.alpha_90d >= 0) {
      outData.push(pt);
    } else {
      underData.push(pt);
    }
  });
  
  riskReturnChart = new Chart(ctx, {
    type: 'scatter',
    data: {
      datasets: [
        {
          label: 'Outperforming (Alpha ≥ 0)',
          data: outData,
          backgroundColor: 'rgba(16, 185, 129, 0.7)',
          pointHoverRadius: 8
        },
        {
          label: 'Underperforming (Alpha < 0)',
          data: underData,
          backgroundColor: 'rgba(244, 63, 94, 0.7)',
          pointHoverRadius: 8
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          labels: { color: '#cbd5e1' }
        },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const p = ctx.raw;
              return `${p.scheme}: Volatility=${p.x}%, 90d Return=${p.y}%, Alpha=${p.alpha}%`;
            }
          }
        }
      },
      scales: {
        x: {
          title: { display: true, text: 'Annualized Volatility (Risk %)', color: '#94a3b8' },
          ticks: { color: '#94a3b8' },
          grid: { color: 'rgba(255, 255, 255, 0.05)' }
        },
        y: {
          title: { display: true, text: 'Rolling 90-Day Return (%)', color: '#94a3b8' },
          ticks: { color: '#94a3b8' },
          grid: { color: 'rgba(255, 255, 255, 0.05)' }
        }
      }
    }
  });
}

// Table Filter and Render
function filterAndRenderTable() {
  if (!analyticsData || !analyticsData.all_funds) return;
  
  const query = document.getElementById('search-input').value.toLowerCase().trim();
  const selectedCat = document.getElementById('filter-category').value;
  const selectedStatus = document.getElementById('filter-status').value;
  
  let filtered = analyticsData.all_funds.filter(f => {
    const matchQuery = !query || f.scheme.toLowerCase().includes(query) || f.category.toLowerCase().includes(query) || f.benchmark.toLowerCase().includes(query);
    const matchCat = selectedCat === 'ALL' || f.category === selectedCat;
    const matchStatus = selectedStatus === 'ALL' || f.status === selectedStatus;
    return matchQuery && matchCat && matchStatus;
  });
  
  const tbody = document.getElementById('funds-table-body');
  tbody.innerHTML = '';
  
  document.getElementById('table-count-badge').textContent = `Showing ${filtered.length} of ${analyticsData.all_funds.length}`;
  
  filtered.forEach(f => {
    const tr = document.createElement('tr');
    
    const alpha30Class = f.alpha_30d >= 0 ? 'alpha-pos' : 'alpha-neg';
    const alpha30Sign = f.alpha_30d >= 0 ? '+' : '';
    const alpha60Class = f.alpha_60d >= 0 ? 'alpha-pos' : 'alpha-neg';
    const alpha60Sign = f.alpha_60d >= 0 ? '+' : '';
    const alpha90Class = f.alpha_90d >= 0 ? 'alpha-pos' : 'alpha-neg';
    const alpha90Sign = f.alpha_90d >= 0 ? '+' : '';
    
    const isOut = f.status === 'OUTPERFORMING';
    const pillClass = isOut ? 'pill-outperform' : 'pill-underperform';
    
    tr.innerHTML = `
      <td>
        <div class="scheme-cell">
          <span class="scheme-name">${f.scheme}</span>
          <span class="scheme-category">${f.category}</span>
        </div>
      </td>
      <td><span class="benchmark-tag" title="${f.benchmark}">${f.benchmark}</span></td>
      <td><strong>₹${f.nav.toFixed(2)}</strong></td>
      <td>
        <div class="ret-dual">
          <span class="ret-val">${f.ret_30d}%</span>
          <span class="alpha-val ${alpha30Class}">${alpha30Sign}${f.alpha_30d}% α</span>
        </div>
      </td>
      <td>
        <div class="ret-dual">
          <span class="ret-val">${f.ret_60d}%</span>
          <span class="alpha-val ${alpha60Class}">${alpha60Sign}${f.alpha_60d}% α</span>
        </div>
      </td>
      <td>
        <div class="ret-dual">
          <span class="ret-val">${f.ret_90d}%</span>
          <span class="alpha-val ${alpha90Class}">${alpha90Sign}${f.alpha_90d}% α</span>
        </div>
      </td>
      <td>${f.beta.toFixed(2)}</td>
      <td>${f.volatility.toFixed(1)}%</td>
      <td><span class="status-pill ${pillClass}">${f.status}</span></td>
      <td>
        <button class="btn btn-secondary" style="padding: 4px 10px; font-size: 0.76rem;" data-scheme="${f.scheme.replace(/&/g, '&amp;').replace(/"/g, '&quot;')}">Inspect</button>
      </td>
    `;
    tbody.appendChild(tr);
  });
}

function selectFund(scheme) {
  document.getElementById('fund-select').value = scheme;
  loadFundDeepDive(scheme);
  window.scrollTo({ top: 350, behavior: 'smooth' });
}

// Handlers for Live Actions
async function handleStreamSingleDay() {
  const btn = document.getElementById('btn-stream-day');
  btn.disabled = true;
  btn.innerHTML = 'Streaming...';
  
  try {
    const res = await fetch('/api/stream_day', { method: 'POST' });
    const data = await res.json();
    if (data.success && data.exhausted) {
      showToast(data.message);
      if (isContinuousStreaming) toggleContinuousStream();
    } else if (data.success) {
      showToast(`Streamed ${data.records_streamed} quotes for ${data.streamed_date} via Kafka into HDFS. Run MapReduce to refresh analytics.`);
      loadPipelineStatus();
    } else {
      showToast(`Stream error: ${data.error}`, 'error');
    }
  } catch (err) {
    showToast('Failed to trigger Kafka streaming.', 'error');
  } finally {
    btn.disabled = false;
    btn.innerHTML = '⚡ Stream Next Day (Kafka)';
  }
}

async function handleRunMapReduce() {
  const btn = document.getElementById('btn-run-mr');
  btn.disabled = true;
  btn.innerHTML = 'Computing...';
  
  try {
    const res = await fetch('/api/run_mapreduce', { method: 'POST' });
    const data = await res.json();
    if (data.success) {
      showToast(`MapReduce completed in ${data.execution_time}s! Rolling returns updated.`);
      await loadDashboardData();
      await loadPipelineStatus();
    } else {
      showToast(`MapReduce error: ${data.error}`, 'error');
    }
  } catch (err) {
    showToast('Failed to execute MapReduce job.', 'error');
  } finally {
    btn.disabled = false;
    btn.innerHTML = '⚙️ Run MapReduce Job';
  }
}

function toggleContinuousStream() {
  const btn = document.getElementById('btn-toggle-stream');
  if (!isContinuousStreaming) {
    isContinuousStreaming = true;
    btn.textContent = '⏹ Stop Streaming';
    btn.classList.remove('btn-secondary');
    btn.classList.add('btn-emerald');
    showToast('Continuous Kafka streaming mode started!');
    streamingInterval = setInterval(async () => {
      try {
        const r = await fetch('/api/stream_day', { method: 'POST' });
        const d = await r.json();
        if (d.exhausted) {
          showToast(d.message);
          toggleContinuousStream();
        }
      } catch (e) { /* transient error - retry on next tick */ }
      loadPipelineStatus();
    }, 3000);
  } else {
    isContinuousStreaming = false;
    clearInterval(streamingInterval);
    btn.textContent = '▶ Live Ingestion Loop';
    btn.classList.remove('btn-emerald');
    btn.classList.add('btn-secondary');
    showToast('Continuous streaming paused.');
  }
}

async function handleReset() {
  if (!confirm('Reset the demo to the historical baseline? Streamed days will be removed and MapReduce re-run.')) return;
  const btn = document.getElementById('btn-reset');
  btn.disabled = true;
  btn.innerHTML = 'Resetting...';
  if (isContinuousStreaming) toggleContinuousStream();
  try {
    const res = await fetch('/api/reset', { method: 'POST' });
    const data = await res.json();
    if (data.success) {
      showToast(data.message);
      await loadDashboardData();
      await loadPipelineStatus();
    } else {
      showToast(`Reset error: ${data.error}`, 'error');
    }
  } catch (err) {
    showToast('Failed to reset the pipeline.', 'error');
  } finally {
    btn.disabled = false;
    btn.innerHTML = '↺ Reset Demo';
  }
}
