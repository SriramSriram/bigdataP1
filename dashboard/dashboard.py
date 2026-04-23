import pandas as pd
import os

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────
ANOMALIES_PATH   = "../data/processed/stream_output/anomalies/anomalies.csv"
ROLLING_AVG_PATH = "../data/processed/stream_output/rolling_avg/rolling_avg.csv"
OUTPUT_HTML      = "../dashboard/energy_dashboard.html"

def load_data():
    anomalies  = pd.read_csv(ANOMALIES_PATH)
    rolling    = pd.read_csv(ROLLING_AVG_PATH)
    return anomalies, rolling

def build_dashboard(anomalies, rolling):

    # ── Summary Stats ──
    total_anomalies  = len(anomalies)
    high_voltage     = len(anomalies[anomalies["anomaly_flag"] == "high_voltage"])
    high_power       = len(anomalies[anomalies["anomaly_flag"] == "high_power"])
    avg_voltage      = round(anomalies["Voltage"].mean(), 2)
    avg_power        = round(rolling["avg_power"].mean(), 3)
    max_power        = round(rolling["avg_power"].max(), 3)

    # ── Rolling Avg Chart Data ──
    rolling_labels   = list(range(1, len(rolling) + 1))
    rolling_power    = rolling["avg_power"].tolist()
    rolling_voltage  = rolling["avg_voltage"].tolist()

    # ── Anomaly Type Chart ──
    anomaly_counts   = anomalies["anomaly_flag"].value_counts()
    anomaly_labels   = anomaly_counts.index.tolist()
    anomaly_values   = anomaly_counts.values.tolist()

    # ── Anomaly Table (last 20) ──
    table_rows = ""
    for _, row in anomalies.tail(20).iloc[::-1].iterrows():
        badge_color = "#e74c3c" if row["anomaly_flag"] == "high_voltage" else "#e67e22"
        table_rows += f"""
        <tr>
            <td>{row['Timestamp']}</td>
            <td><span style="background:{badge_color};color:white;padding:3px 8px;
                border-radius:4px;font-size:12px">{row['anomaly_flag']}</span></td>
            <td>{row['Voltage']}</td>
            <td>{row['Global_active_power']}</td>
            <td>{row['detected_at']}</td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Energy Pipeline Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ font-family: 'Segoe UI', sans-serif; background:#0f1117; color:#e0e0e0; }}
  header {{ background:linear-gradient(135deg,#1a1f2e,#2d3561);
            padding:24px 32px; border-bottom:1px solid #2a2f45; }}
  header h1 {{ font-size:24px; color:#7c83fd; }}
  header p  {{ font-size:13px; color:#888; margin-top:4px; }}
  .grid-4 {{ display:grid; grid-template-columns:repeat(4,1fr);
              gap:16px; padding:24px 32px 0; }}
  .card {{ background:#1a1f2e; border:1px solid #2a2f45;
           border-radius:12px; padding:20px; }}
  .card .label {{ font-size:12px; color:#888; text-transform:uppercase;
                  letter-spacing:1px; }}
  .card .value {{ font-size:32px; font-weight:700; margin-top:8px; }}
  .card .sub   {{ font-size:12px; color:#666; margin-top:4px; }}
  .blue  {{ color:#7c83fd; }}
  .red   {{ color:#e74c3c; }}
  .orange{{ color:#e67e22; }}
  .green {{ color:#2ecc71; }}
  .charts {{ display:grid; grid-template-columns:2fr 1fr;
             gap:16px; padding:24px 32px 0; }}
  .chart-card {{ background:#1a1f2e; border:1px solid #2a2f45;
                 border-radius:12px; padding:20px; }}
  .chart-card h3 {{ font-size:14px; color:#aaa; margin-bottom:16px; }}
  .table-section {{ padding:24px 32px; }}
  .table-section h3 {{ font-size:16px; color:#aaa; margin-bottom:12px; }}
  table {{ width:100%; border-collapse:collapse;
           background:#1a1f2e; border-radius:12px; overflow:hidden;
           border:1px solid #2a2f45; }}
  th {{ background:#2a2f45; padding:12px 16px; text-align:left;
        font-size:12px; color:#888; text-transform:uppercase; }}
  td {{ padding:12px 16px; font-size:13px; border-bottom:1px solid #2a2f45; }}
  tr:last-child td {{ border-bottom:none; }}
  tr:hover {{ background:#222840; }}
  footer {{ text-align:center; padding:24px; color:#444; font-size:12px; }}
</style>
</head>
<body>

<header>
  <h1>Energy Consumption Pipeline — Live Dashboard</h1>
  <p>UCI Household Power Consumption | Kafka + PySpark + n8n Pipeline</p>
</header>

<!-- SUMMARY CARDS -->
<div class="grid-4">
  <div class="card">
    <div class="label">Total Anomalies</div>
    <div class="value red">{total_anomalies}</div>
    <div class="sub">Detected in stream</div>
  </div>
  <div class="card">
    <div class="label">High Voltage Events</div>
    <div class="value orange">{high_voltage}</div>
    <div class="sub">Voltage &gt; 250V</div>
  </div>
  <div class="card">
    <div class="label">High Power Events</div>
    <div class="value red">{high_power}</div>
    <div class="sub">Power &gt; 6kW</div>
  </div>
  <div class="card">
    <div class="label">Avg Active Power</div>
    <div class="value blue">{avg_power} kW</div>
    <div class="sub">Peak: {max_power} kW</div>
  </div>
</div>

<!-- CHARTS -->
<div class="charts">
  <div class="chart-card">
    <h3>Rolling Average — Active Power & Voltage Over Time</h3>
    <canvas id="rollingChart" height="100"></canvas>
  </div>
  <div class="chart-card">
    <h3>Anomaly Breakdown</h3>
    <canvas id="anomalyPie" height="100"></canvas>
  </div>
</div>

<!-- ANOMALY TABLE -->
<div class="table-section">
  <h3>Recent Anomaly Events (Last 20)</h3>
  <table>
    <thead>
      <tr>
        <th>Timestamp</th>
        <th>Type</th>
        <th>Voltage (V)</th>
        <th>Active Power (kW)</th>
        <th>Detected At</th>
      </tr>
    </thead>
    <tbody>
      {table_rows}
    </tbody>
  </table>
</div>

<footer>
  BigDataP1 Pipeline &nbsp;|&nbsp; Stage 5 Dashboard &nbsp;|&nbsp;
  Built with Kafka · PySpark · n8n · Docker
</footer>

<script>
// Rolling Average Line Chart
new Chart(document.getElementById('rollingChart'), {{
  type: 'line',
  data: {{
    labels: {rolling_labels},
    datasets: [
      {{
        label: 'Avg Power (kW)',
        data: {rolling_power},
        borderColor: '#7c83fd',
        backgroundColor: 'rgba(124,131,253,0.1)',
        tension: 0.4, fill: true, pointRadius: 3
      }},
      {{
        label: 'Avg Voltage (V)',
        data: {rolling_voltage},
        borderColor: '#2ecc71',
        backgroundColor: 'rgba(46,204,113,0.05)',
        tension: 0.4, fill: false, pointRadius: 3,
        yAxisID: 'y2'
      }}
    ]
  }},
  options: {{
    responsive: true,
    plugins: {{ legend: {{ labels: {{ color:'#aaa' }} }} }},
    scales: {{
      x:  {{ ticks: {{ color:'#666' }}, grid: {{ color:'#2a2f45' }} }},
      y:  {{ ticks: {{ color:'#666' }}, grid: {{ color:'#2a2f45' }},
             title: {{ display:true, text:'Power (kW)', color:'#666' }} }},
      y2: {{ position:'right', ticks: {{ color:'#666' }},
             grid: {{ drawOnChartArea:false }},
             title: {{ display:true, text:'Voltage (V)', color:'#666' }} }}
    }}
  }}
}});

// Anomaly Pie Chart
new Chart(document.getElementById('anomalyPie'), {{
  type: 'doughnut',
  data: {{
    labels: {anomaly_labels},
    datasets: [{{
      data: {anomaly_values},
      backgroundColor: ['#e74c3c','#e67e22','#3498db','#9b59b6'],
      borderWidth: 0
    }}]
  }},
  options: {{
    responsive: true,
    plugins: {{
      legend: {{ position:'bottom', labels: {{ color:'#aaa', padding:16 }} }}
    }}
  }}
}});
</script>
</body>
</html>"""

    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Dashboard saved to: {OUTPUT_HTML}")

def main():
    print("Building dashboard...")
    anomalies, rolling = load_data()
    print(f"Loaded {len(anomalies)} anomalies, {len(rolling)} rolling avg records.")
    build_dashboard(anomalies, rolling)
    print("Done! Open energy_dashboard.html in your browser.")

if __name__ == "__main__":
    main()