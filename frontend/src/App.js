import { useState, useEffect } from "react";
import {
  ComposedChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ReferenceArea,
  ResponsiveContainer,
} from "recharts";
import axios from "axios";

const API = process.env.REACT_APP_API_URL;

const INDICATORS = [
  { id: "FEDFUNDS", label: "Fed Funds", color: "#00d4aa" },
  { id: "CPIAUCSL", label: "CPI", color: "#ffa502" },
  { id: "UNRATE", label: "Unemployment", color: "#3d9cf5" },
  { id: "GDP", label: "GDP", color: "#9b59b6" },
  { id: "T10Y2Y", label: "Yield Spread", color: "#ff4757" },
];

const CRISIS_BANDS = [
  { start: "2007-10", end: "2009-06", label: "2008 GFC", color: "#ff4757" },
  { start: "2020-02", end: "2020-09", label: "COVID", color: "#ff4757" },
  { start: "2022-01", end: "2023-06", label: "2022 Hike", color: "#ffa502" },
];

const styles = {
  app: {
    backgroundColor: "#0f1117",
    minHeight: "100vh",
    fontFamily: "'DM Sans', sans-serif",
    color: "#e8eaf0",
  },
  topbar: {
    backgroundColor: "#13141a",
    borderBottom: "1px solid #1e2030",
    padding: "0 32px",
    height: "56px",
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
  },
  logo: {
    fontSize: "16px",
    fontWeight: "600",
    color: "#f1f2f6",
    letterSpacing: "-0.3px",
  },
  logoAccent: { color: "#00d4aa" },
  topbarRight: { display: "flex", alignItems: "center", gap: "24px" },
  liveBadge: {
    display: "flex",
    alignItems: "center",
    gap: "6px",
    fontSize: "12px",
    color: "#2ed573",
  },
  liveDot: {
    width: "7px",
    height: "7px",
    borderRadius: "50%",
    backgroundColor: "#2ed573",
    animation: "pulse 1.5s infinite",
  },
  topbarDate: { fontSize: "12px", color: "#747d8c" },
  main: { padding: "28px 32px", display: "flex", flexDirection: "column", gap: "20px" },
  pageHeader: { display: "flex", alignItems: "flex-end", justifyContent: "space-between" },
  pageTitle: { fontSize: "20px", fontWeight: "600", color: "#f1f2f6", letterSpacing: "-0.4px" },
  pageSub: { fontSize: "12px", color: "#747d8c", marginTop: "4px" },
  kpiRow: {
    display: "grid",
    gridTemplateColumns: "repeat(5, 1fr)",
    gap: "12px",
  },
  kpiCard: {
    backgroundColor: "#13141a",
    border: "1px solid #1e2030",
    borderRadius: "10px",
    padding: "16px 18px",
  },
  kpiLabel: {
    fontSize: "11px",
    color: "#747d8c",
    letterSpacing: "0.8px",
    textTransform: "uppercase",
    marginBottom: "8px",
  },
  kpiValue: {
    fontSize: "24px",
    fontWeight: "600",
    fontFamily: "'DM Mono', monospace",
    color: "#f1f2f6",
  },
  kpiDelta: { fontSize: "12px", marginTop: "6px" },
  kpiSub: { fontSize: "10px", color: "#444", marginTop: "4px" },
  card: {
    backgroundColor: "#13141a",
    border: "1px solid #1e2030",
    borderRadius: "10px",
    padding: "20px 24px",
  },
  cardHeader: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    marginBottom: "16px",
  },
  cardTitle: { fontSize: "13px", fontWeight: "500", color: "#c5c8d4" },
  tag: {
    fontSize: "10px",
    padding: "3px 8px",
    borderRadius: "4px",
    letterSpacing: "0.5px",
    fontFamily: "'DM Mono', monospace",
  },
};

const KPI_META = [
  { id: "FEDFUNDS", label: "Fed Funds Rate", unit: "%", sub: "Monthly · FRED", accentColor: "#00d4aa" },
  { id: "CPIAUCSL", label: "CPI Index", unit: "", sub: "Monthly · FRED", accentColor: "#ffa502" },
  { id: "UNRATE", label: "Unemployment", unit: "%", sub: "Monthly · FRED", accentColor: "#3d9cf5" },
  { id: "T10Y2Y", label: "Yield Spread", unit: "%", sub: "Daily · FRED", accentColor: "#ff4757" },
  { id: "SP500", label: "S&P 500", unit: "", sub: "Daily · Yahoo Finance", accentColor: "#9b59b6" },
];

function KpiCard({ meta, value, delta }) {
  const isPos = delta >= 0;
  const deltaColor = meta.id === "UNRATE"
    ? (isPos ? "#ff4757" : "#2ed573")
    : (isPos ? "#2ed573" : "#ff4757");

  return (
    <div style={{ ...styles.kpiCard, borderTop: `2px solid ${meta.accentColor}` }}>
      <div style={styles.kpiLabel}>{meta.label}</div>
      <div style={styles.kpiValue}>
        {value !== null ? `${Number(value).toFixed(2)}${meta.unit}` : "—"}
      </div>
      {delta !== null && (
        <div style={{ ...styles.kpiDelta, color: deltaColor }}>
          {isPos ? "▲" : "▼"} {Math.abs(delta).toFixed(2)}{meta.unit} prev
        </div>
      )}
      <div style={styles.kpiSub}>{meta.sub}</div>
    </div>
  );
}

function CustomTooltip({ active, payload, label }) {
  if (!active || !payload || !payload.length) return null;
  return (
    <div style={{
      backgroundColor: "#1a1b26",
      border: "1px solid #2a2a3a",
      borderRadius: "8px",
      padding: "10px 14px",
      fontSize: "12px",
      fontFamily: "'DM Mono', monospace",
      boxShadow: "0 4px 20px rgba(0,0,0,0.4)",
    }}>
      <div style={{ color: "#747d8c", marginBottom: "8px", fontSize: "11px" }}>
        {label}
      </div>
      {payload.map((entry) => (
        <div key={entry.name} style={{
          display: "flex",
          justifyContent: "space-between",
          gap: "24px",
          color: entry.color,
          marginBottom: "4px",
        }}>
          <span>{entry.name}</span>
          <span style={{ fontWeight: "500" }}>
            {typeof entry.value === "number" ? entry.value.toLocaleString(undefined, { maximumFractionDigits: 2 }) : "—"}
          </span>
        </div>
      ))}
    </div>
  );
}

export default function App() {
  const [activeIndicators, setActiveIndicators] = useState(["FEDFUNDS"]);
  const [macroData, setMacroData] = useState({});
  const [equityData, setEquityData] = useState([]);
  const [anomalyData, setAnomalyData] = useState([]);
  const [kpiValues, setKpiValues] = useState({});
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchAllData();
  }, []);

  async function fetchAllData() {
    try {
      const [equityRes, anomalyRes, ...macroRes] = await Promise.all([
        axios.get(`${API}/equity?limit=6602`),
        axios.get(`${API}/anomalies`),
        ...INDICATORS.map((ind) =>
          axios.get(`${API}/macro/${ind.id}?limit=7000`)
        ),
      ]);

      const equity = equityRes.data;
      setEquityData(equity);

      const macro = {};
      INDICATORS.forEach((ind, i) => {
        macro[ind.id] = macroRes[i].data;
      });
      setMacroData(macro);
      const anomalies = anomalyRes.data;
      const sortedAnomalies = [...anomalies].sort((a, b) =>
        new Date(b.date) - new Date(a.date)
      );
      setAnomalyData(sortedAnomalies);

      // Extract latest KPI values
      const kpis = {};
      INDICATORS.forEach((ind) => {
        const series = [...(macro[ind.id] || [])].sort((a, b) =>
          new Date(b.date) - new Date(a.date)
        );
        if (series.length >= 2) {
          kpis[ind.id] = {
            value: series[0].value,
            delta: series[0].value - series[1].value,
          };
        }
      });

      if (equity && equity.length >= 2) {
        const sorted = [...equity].sort((a, b) => new Date(b.date) - new Date(a.date));
        kpis["SP500"] = {
          value: sorted[0].close,
          delta: sorted[0].close - sorted[1].close,
        };
      }
      setKpiValues(kpis);
      setLoading(false);
    } catch (err) {
      console.error("Data fetch error:", err);
      setLoading(false);
    }
  }

  function toggleIndicator(id) {
    setActiveIndicators((prev) =>
      prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]
    );
  }

  function normalize(series, key) {
    const values = series.map((r) => r[key]).filter((v) => v != null);
    const min = Math.min(...values);
    const max = Math.max(...values);
    if (max === min) return series;
    return series.map((r) => ({
      ...r,
      [key]: r[key] != null ? ((r[key] - min) / (max - min)) * 100 : null,
    }));
  }

  function buildChartData() {
    const monthMap = {};

    equityData.forEach((row) => {
      const d = row.date?.slice(0, 10);
      if (!d) return;
      const month = d.slice(0, 7); // "2008-03"
      if (!monthMap[month]) monthMap[month] = { date: month };
      monthMap[month]["SP500"] = row.close;
    });

    INDICATORS.forEach((ind) => {
      const series = macroData[ind.id] || [];
      series.forEach((row) => {
        const d = row.date?.slice(0, 10);
        if (!d) return;
        const month = d.slice(0, 7);
        if (!monthMap[month]) monthMap[month] = { date: month };
        monthMap[month][ind.id] = row.value;
      });
    });
    let result = Object.values(monthMap).sort((a, b) =>
      a.date.localeCompare(b.date)
    );

    // Normalize each macro indicator to 0-100 for visual comparability
    INDICATORS.forEach((ind) => {
      result = normalize(result, ind.id);
    });

    return result;
  }

  if (loading) {
    return (
      <div style={{ ...styles.app, display: "flex", alignItems: "center", justifyContent: "center", height: "100vh" }}>
        <div style={{ textAlign: "center" }}>
          <div style={{ fontSize: "13px", color: "#00d4aa", fontFamily: "'DM Mono', monospace", letterSpacing: "2px" }}>LOADING DATA...</div>
          <div style={{ fontSize: "11px", color: "#747d8c", marginTop: "8px" }}>Connecting to Railway API</div>
        </div>
      </div>
    );
  }

  return (
    <div style={styles.app}>
      <style>{`
        @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
        * { box-sizing: border-box; margin: 0; padding: 0; }
      `}</style>

      {/* TOPBAR */}
      <div style={styles.topbar}>
        <div style={styles.logo}>
          Macro<span style={styles.logoAccent}>Equity</span> Dashboard
        </div>
        <div style={styles.topbarRight}>
          <div style={styles.liveBadge}>
            <div style={styles.liveDot} />
            LIVE
          </div>
          <div style={styles.topbarDate}>
            {new Date().toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" })}
          </div>
        </div>
      </div>

      <div style={styles.main}>
        {/* PAGE HEADER */}
        <div style={styles.pageHeader}>
          <div>
            <div style={styles.pageTitle}>Macro + Equity Overview</div>
            <div style={styles.pageSub}>2000–2026 · 14,212 data points · 5 FRED indicators · S&P 500</div>
          </div>
        </div>

        {/* KPI CARDS */}
        <div style={styles.kpiRow}>
          {KPI_META.map((meta) => (
            <KpiCard
              key={meta.id}
              meta={meta}
              value={kpiValues[meta.id]?.value ?? null}
              delta={kpiValues[meta.id]?.delta ?? null}
            />
          ))}
        </div>

        {/* INDICATOR TOGGLES + MAIN CHART */}
        <div style={styles.card}>
          <div style={styles.cardHeader}>
            <div>
              <div style={styles.cardTitle}>S&P 500 vs Macro Indicators — 2000–2026</div>
              <div style={{ fontSize: "11px", color: "#747d8c", marginTop: "4px" }}>
                S&P 500 (actual price) · macro indicators normalized 0–100 for comparison · hover to inspect
              </div>
            </div>
            <span style={{ ...styles.tag, backgroundColor: "rgba(46,213,115,0.1)", color: "#2ed573" }}>
              LIVE
            </span>
          </div>

          {/* TOGGLE BUTTONS */}
          <div style={{ display: "flex", gap: "8px", marginBottom: "20px", flexWrap: "wrap" }}>
            {INDICATORS.map((ind) => {
              const active = activeIndicators.includes(ind.id);
              return (
                <button
                  key={ind.id}
                  onClick={() => toggleIndicator(ind.id)}
                  style={{
                    padding: "6px 14px",
                    fontSize: "11px",
                    fontFamily: "'DM Mono', monospace",
                    borderRadius: "5px",
                    border: `1px solid ${active ? ind.color : "#2a2a3a"}`,
                    backgroundColor: active ? `${ind.color}18` : "transparent",
                    color: active ? ind.color : "#747d8c",
                    cursor: "pointer",
                    letterSpacing: "0.5px",
                    transition: "all 0.15s",
                  }}
                >
                  {ind.label}
                </button>
              );
            })}
          </div>

          {/* CHART */}
          <ResponsiveContainer width="100%" height={340}>
            <ComposedChart
              data={buildChartData()}
              margin={{ top: 10, right: 20, left: 0, bottom: 0 }}
            >
              <CartesianGrid
                strokeDasharray="3 6"
                stroke="#1e2030"
                vertical={false}
              />
              <XAxis
                dataKey="date"
                tick={{ fontSize: 10, fill: "#747d8c", fontFamily: "DM Mono" }}
                tickLine={false}
                axisLine={{ stroke: "#1e2030" }}
                tickFormatter={(d) => d?.slice(0, 4)}
                interval={Math.floor(buildChartData().length / 8)}
              />
              <YAxis
                yAxisId="equity"
                orientation="left"
                tick={{ fontSize: 10, fill: "#747d8c", fontFamily: "DM Mono" }}
                tickLine={false}
                axisLine={false}
                tickFormatter={(v) => v.toLocaleString()}
                width={60}
              />
              <YAxis
                yAxisId="macro"
                orientation="right"
                tick={{ fontSize: 10, fill: "#747d8c", fontFamily: "DM Mono" }}
                tickLine={false}
                axisLine={false}
                domain={[0, 100]}
                tickFormatter={(v) => `${Math.round(v)}`}
                width={35}
              />
              <Tooltip content={<CustomTooltip />} />

              {/* CRISIS BANDS */}
              {CRISIS_BANDS.map((band) => (
                <ReferenceArea
                  key={band.label}
                  x1={band.start}
                  x2={band.end}
                  yAxisId="equity"
                  fill={band.color}
                  fillOpacity={0.07}
                  stroke={band.color}
                  strokeOpacity={0.2}
                  strokeWidth={1}
                  label={{
                    value: band.label,
                    position: "insideTopLeft",
                    fontSize: 9,
                    fill: band.color,
                    fontFamily: "DM Mono",
                    opacity: 0.8,
                  }}
                />
              ))}

              {/* S&P 500 LINE — always visible */}
              <Line
                yAxisId="equity"
                type="monotone"
                dataKey="SP500"
                stroke="#ffffff"
                strokeWidth={1.5}
                dot={false}
                activeDot={{ r: 4, fill: "#ffffff" }}
                name="S&P 500"
                connectNulls
              />

              {/* ACTIVE MACRO INDICATOR LINES */}
              {INDICATORS.map((ind) =>
                activeIndicators.includes(ind.id) ? (
                  <Line
                    key={ind.id}
                    yAxisId="macro"
                    type="monotone"
                    dataKey={ind.id}
                    stroke={ind.color}
                    strokeWidth={1.5}
                    dot={false}
                    activeDot={{ r: 4, fill: ind.color }}
                    name={ind.label}
                    strokeDasharray="5 3"
                    connectNulls
                  />
                ) : null
              )}
            </ComposedChart>
          </ResponsiveContainer>

          {/* LEGEND */}
          <div style={{ display: "flex", gap: "20px", marginTop: "12px", flexWrap: "wrap" }}>
            <div style={{ display: "flex", alignItems: "center", gap: "6px", fontSize: "11px", color: "#747d8c" }}>
              <div style={{ width: "16px", height: "2px", backgroundColor: "#ffffff" }} />
              S&P 500 (left axis)
            </div>
            {INDICATORS.filter((i) => activeIndicators.includes(i.id)).map((ind) => (
              <div key={ind.id} style={{ display: "flex", alignItems: "center", gap: "6px", fontSize: "11px", color: "#747d8c" }}>
                <div style={{ width: "16px", height: "2px", backgroundColor: ind.color, borderTop: "2px dashed " + ind.color }} />
                {ind.label} (right axis)
              </div>
            ))}
          </div>
        </div>

        {/* BOTTOM ROW */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: "16px" }}>

          {/* ANOMALY ALERT FEED */}
          <div style={styles.card}>
            <div style={styles.cardHeader}>
              <div style={styles.cardTitle}>Recent Anomaly Flags</div>
              <span style={{ ...styles.tag, backgroundColor: "rgba(255,71,87,0.1)", color: "#ff4757" }}>
                HIGH / CAUTION
              </span>
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: "8px" }}>
              {anomalyData.slice(0, 6).map((flag, i) => (
                <div key={i} style={{
                  display: "flex",
                  alignItems: "flex-start",
                  gap: "10px",
                  padding: "10px 12px",
                  backgroundColor: "#0f1017",
                  borderRadius: "6px",
                  borderLeft: `3px solid ${flag.severity === "HIGH" ? "#ff4757" : "#ffa502"}`,
                }}>
                  <div style={{ flex: 1 }}>
                    <div style={{
                      fontSize: "12px",
                      fontWeight: "500",
                      color: "#c5c8d4",
                      marginBottom: "2px",
                    }}>
                      {flag.series_id}
                    </div>
                    <div style={{ fontSize: "11px", color: "#747d8c", fontFamily: "'DM Mono', monospace" }}>
                      {flag.date?.slice(0, 10)} · z={Number(flag.zscore).toFixed(2)}σ
                    </div>
                  </div>
                  <div style={{
                    fontSize: "11px",
                    fontWeight: "600",
                    fontFamily: "'DM Mono', monospace",
                    color: flag.severity === "HIGH" ? "#ff4757" : "#ffa502",
                    marginTop: "2px",
                  }}>
                    {flag.severity}
                  </div>
                </div>
              ))}
              {anomalyData.length === 0 && (
                <div style={{ fontSize: "12px", color: "#747d8c", textAlign: "center", padding: "20px 0" }}>
                  No HIGH anomalies found
                </div>
              )}
            </div>
          </div>

          {/* CORRELATION PANEL */}
          <div style={styles.card}>
            <div style={styles.cardHeader}>
              <div style={styles.cardTitle}>Indicator Correlations vs S&P 500</div>
              <span style={{ ...styles.tag, backgroundColor: "rgba(116,125,140,0.15)", color: "#747d8c" }}>
                Pearson
              </span>
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: "4px" }}>
              {[
                { label: "GDP", value: 0.57, pos: true },
                { label: "T10Y2Y", value: 0.44, pos: true },
                { label: "FEDFUNDS", value: 0.26, pos: true },
                { label: "CPIAUCSL", value: -0.18, pos: false },
                { label: "UNRATE", value: -0.31, pos: false },
              ].map((item) => (
                <div key={item.label} style={{
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "space-between",
                  padding: "9px 0",
                  borderBottom: "1px solid #1a1a28",
                }}>
                  <div style={{ fontSize: "11px", color: "#747d8c", width: "80px", fontFamily: "'DM Mono', monospace" }}>
                    {item.label}
                  </div>
                  <div style={{ flex: 1, margin: "0 12px", height: "3px", backgroundColor: "#1e2030", borderRadius: "2px" }}>
                    <div style={{
                      height: "100%",
                      width: `${Math.abs(item.value) * 100}%`,
                      backgroundColor: item.pos ? "#2ed573" : "#ff4757",
                      borderRadius: "2px",
                    }} />
                  </div>
                  <div style={{
                    fontSize: "12px",
                    fontWeight: "600",
                    fontFamily: "'DM Mono', monospace",
                    color: item.pos ? "#2ed573" : "#ff4757",
                    minWidth: "44px",
                    textAlign: "right",
                  }}>
                    {item.value > 0 ? "+" : ""}{item.value.toFixed(2)}
                  </div>
                </div>
              ))}
            </div>
            <div style={{ fontSize: "10px", color: "#444", marginTop: "12px", lineHeight: "1.5" }}>
              Pearson correlation · 2000–2026 · Spearman flagged as robustness check
            </div>
          </div>

          {/* YIELD CURVE STATUS */}
          <div style={styles.card}>
            <div style={styles.cardHeader}>
              <div style={styles.cardTitle}>Yield Curve Status</div>
              <span style={{ ...styles.tag, backgroundColor: "rgba(255,71,87,0.1)", color: "#ff4757" }}>
                INVERTED
              </span>
            </div>
            <div style={{ textAlign: "center", padding: "12px 0 16px" }}>
              <div style={{
                fontSize: "48px",
                fontWeight: "600",
                fontFamily: "'DM Mono', monospace",
                color: "#ff4757",
                lineHeight: 1,
              }}>
                1,009
              </div>
              <div style={{ fontSize: "11px", color: "#747d8c", marginTop: "6px" }}>
                total inversion days across 25 years
              </div>
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: "0px" }}>
              {[
                { label: "Total episodes", value: "20 distinct" },
                { label: "Longest episode", value: "2022–present" },
                { label: "Current spread", value: `${kpiValues["T10Y2Y"]?.value != null ? Number(kpiValues["T10Y2Y"].value).toFixed(2) : "—"}%` },
                { label: "Anomaly flags", value: "431 macro flags" },
                { label: "Crisis recall", value: "3/3 — 100%" },
              ].map((row) => (
                <div key={row.label} style={{
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                  padding: "8px 0",
                  borderBottom: "1px solid #1a1a28",
                  fontSize: "11px",
                }}>
                  <span style={{ color: "#747d8c" }}>{row.label}</span>
                  <span style={{
                    color: "#c5c8d4",
                    fontFamily: "'DM Mono', monospace",
                    fontWeight: "500",
                  }}>{row.value}</span>
                </div>
              ))}
            </div>
          </div>

        </div>

        {/* FOOTER */}
        <div style={{
          borderTop: "1px solid #1e2030",
          paddingTop: "16px",
          paddingBottom: "8px",
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
        }}>
          <div style={{ fontSize: "11px", color: "#444" }}>
            Data: FRED API + Yahoo Finance · Backend: FastAPI + Railway · DB: DuckDB · 14,212 data points
          </div>
          <div style={{ fontSize: "11px", color: "#444", fontFamily: "'DM Mono', monospace" }}>
            Built with Polars · DuckDB · React · Recharts
          </div>
        </div>
      </div>
    </div>
  );
}