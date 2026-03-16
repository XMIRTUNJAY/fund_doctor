/**
 * Fund Doctor — React Frontend
 * Fully integrated with FastAPI backend at http://localhost:8000
 * All data fetched live from API — no mock data.
 */

import { useState, useEffect, useCallback, useRef } from "react";
import {
  AreaChart, Area, LineChart, Line, BarChart, Bar,
  RadarChart, Radar, PolarGrid, PolarAngleAxis,
  XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ReferenceLine, Cell,
} from "recharts";

// ─── Config ──────────────────────────────────────────────────────────────────
const API = "http://localhost:8000/api";

// ─── Design tokens ────────────────────────────────────────────────────────────
const C = {
  bg:"#07090f", surface:"#0c1018", s2:"#111827", s3:"#162032", s4:"#1a2535",
  border:"rgba(255,255,255,0.055)", border2:"rgba(255,255,255,0.10)",
  accent:"#00d4ff", accentDim:"rgba(0,212,255,0.10)", accentGlow:"rgba(0,212,255,0.22)",
  gold:"#f0b429", goldDim:"rgba(240,180,41,0.10)",
  green:"#22c55e", greenDim:"rgba(34,197,94,0.10)",
  red:"#ef4444", redDim:"rgba(239,68,68,0.10)",
  orange:"#f97316",
  purple:"#a78bfa",
  text:"#e2e8f0", sub:"#94a3b8", muted:"#4b5563",
};

const FLAGS = {
  OK:       { color:C.green,  bg:C.greenDim,  label:"Performing" },
  WARNING:  { color:C.gold,   bg:C.goldDim,   label:"Warning"    },
  SERIOUS:  { color:C.orange, bg:"rgba(249,115,22,0.10)", label:"Serious" },
  CRITICAL: { color:C.red,    bg:C.redDim,    label:"Critical"   },
  NO_DATA:  { color:C.muted,  bg:"rgba(75,85,99,0.12)",  label:"No Data" },
  INSUFFICIENT_DATA: { color:C.muted, bg:"rgba(75,85,99,0.12)", label:"Insufficient" },
};

const ALLOC_COLORS = [C.accent, C.gold, C.purple, C.green, C.orange, "#f472b6", "#34d399"];

// ─── Global CSS ───────────────────────────────────────────────────────────────
const CSS = `
@import url('https://fonts.googleapis.com/css2?family=Bricolage+Grotesque:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap');
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html,body{background:${C.bg};color:${C.text};font-family:'Bricolage Grotesque',sans-serif;overflow-x:hidden;font-size:14px;-webkit-font-smoothing:antialiased}
::-webkit-scrollbar{width:3px;height:3px}
::-webkit-scrollbar-thumb{background:rgba(255,255,255,0.08);border-radius:99px}
.mono{font-family:'JetBrains Mono',monospace}
@keyframes fadeUp{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.45}}
@keyframes spin{to{transform:rotate(360deg)}}
.fade{animation:fadeUp 0.35s ease both}
.fade2{animation:fadeUp 0.35s 0.07s ease both}
.fade3{animation:fadeUp 0.35s 0.14s ease both}
.fade4{animation:fadeUp 0.35s 0.21s ease both}
input[type=range]{-webkit-appearance:none;appearance:none;width:100%;height:2px;background:rgba(255,255,255,0.08);border-radius:99px;outline:none;cursor:pointer}
input[type=range]::-webkit-slider-thumb{-webkit-appearance:none;width:14px;height:14px;border-radius:50%;background:${C.accent};cursor:pointer;border:2px solid ${C.bg};box-shadow:0 0 8px ${C.accent}66}
input[type=range]::-moz-range-thumb{width:14px;height:14px;border-radius:50%;background:${C.accent};cursor:pointer;border:2px solid ${C.bg}}
`;

// ─── Utilities ────────────────────────────────────────────────────────────────
const pct   = (v, signed=false) => v==null ? "—" : `${signed&&v>0?"+":""}${(v*100).toFixed(1)}%`;
const num2  = (v) => v==null ? "—" : v.toFixed(2);
const inr   = (v) => `₹${Math.round(v||0).toLocaleString("en-IN")}`;
const short = (s, n=30) => s ? (s.length>n ? s.slice(0,n)+"…" : s) : "";

// ─── API hook ─────────────────────────────────────────────────────────────────
function useAPI(url, deps=[]) {
  const [data, setData]       = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError]     = useState(null);
  const ctrl = useRef(null);

  useEffect(() => {
    if (!url) { setLoading(false); return; }
    ctrl.current?.abort();
    ctrl.current = new AbortController();
    setLoading(true); setError(null);

    fetch(`${API}${url}`, { signal: ctrl.current.signal })
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then(d  => { setData(d); setLoading(false); })
      .catch(e => { if (e.name !== "AbortError") { setError(e.message); setLoading(false); } });

    return () => ctrl.current?.abort();
  }, deps);

  return { data, loading, error };
}

// ─── Shared components ────────────────────────────────────────────────────────
const Spin = () => (
  <div style={{display:"flex",alignItems:"center",justifyContent:"center",padding:40}}>
    <div style={{width:28,height:28,borderRadius:"50%",border:`2px solid ${C.border2}`,borderTopColor:C.accent,animation:"spin 0.7s linear infinite"}}/>
  </div>
);

const ErrBox = ({ msg }) => (
  <div style={{padding:20,background:C.redDim,border:`1px solid ${C.red}44`,borderRadius:10,color:C.red,fontSize:13}}>
    ⚠ {msg || "Could not connect to API. Make sure the backend is running: uvicorn api:app --port 8000"}
  </div>
);

const FlagPill = ({ flag }) => {
  const cfg = FLAGS[flag] || FLAGS.NO_DATA;
  return (
    <span style={{display:"inline-flex",alignItems:"center",gap:5,padding:"3px 10px",borderRadius:99,background:cfg.bg,color:cfg.color,fontSize:10,fontFamily:"JetBrains Mono,monospace",fontWeight:500,letterSpacing:"0.04em",whiteSpace:"nowrap"}}>
      <span style={{width:5,height:5,borderRadius:"50%",background:cfg.color,animation:"pulse 2s infinite"}}/>
      {cfg.label}
    </span>
  );
};

const KPI = ({ label, value, sub, color=C.accent, mono=true, cls="" }) => (
  <div className={cls} style={{background:C.s2,border:`1px solid ${C.border}`,borderRadius:14,padding:"15px 17px",position:"relative",overflow:"hidden"}}>
    <div style={{position:"absolute",top:0,left:0,right:0,height:1,background:`linear-gradient(90deg,transparent,${color}44,transparent)`}}/>
    <p style={{fontSize:10,color:C.muted,textTransform:"uppercase",letterSpacing:"0.08em",marginBottom:7,fontFamily:"JetBrains Mono,monospace"}}>{label}</p>
    <p style={{fontSize:21,fontWeight:700,color,fontFamily:mono?"JetBrains Mono,monospace":"Bricolage Grotesque,sans-serif",letterSpacing:mono?"-0.01em":"normal"}}>{value}</p>
    {sub&&<p style={{fontSize:11,color:C.muted,marginTop:3}}>{sub}</p>}
  </div>
);

const Tag = ({ c, children }) => (
  <span style={{display:"inline-block",padding:"2px 9px",borderRadius:99,background:C.s3,color:c||C.sub,fontSize:10,fontFamily:"JetBrains Mono,monospace",marginRight:5,marginBottom:3}}>{children}</span>
);

const SectionHead = ({ title, sub }) => (
  <div style={{marginBottom:22}}>
    <h2 style={{fontSize:22,fontWeight:700,letterSpacing:"-0.03em",color:C.text}}>{title}</h2>
    {sub&&<p style={{fontSize:13,color:C.sub,marginTop:5,lineHeight:1.5}}>{sub}</p>}
  </div>
);

const Divider = () => <div style={{height:1,background:C.border,margin:"22px 0"}}/>;

const Card = ({ children, style={} }) => (
  <div style={{background:C.s2,border:`1px solid ${C.border}`,borderRadius:16,padding:"22px",...style}}>
    {children}
  </div>
);

const PeriodBtn = ({ active, onClick, children }) => (
  <button onClick={onClick} style={{padding:"3px 10px",borderRadius:7,border:`1px solid ${active?C.accent:C.border}`,background:active?C.accentDim:"transparent",color:active?C.accent:C.muted,fontSize:11,cursor:"pointer",fontFamily:"JetBrains Mono,monospace",transition:"all 0.12s"}}>{children}</button>
);

const ChartTip = ({ active, payload, label, fmt }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{background:C.surface,border:`1px solid ${C.border2}`,borderRadius:10,padding:"9px 13px",backdropFilter:"blur(12px)"}}>
      <p style={{fontSize:10,color:C.muted,marginBottom:5,fontFamily:"JetBrains Mono,monospace"}}>{label}</p>
      {payload.map((p,i) => (
        <p key={i} style={{fontSize:12,fontFamily:"JetBrains Mono,monospace",color:p.color||C.accent,marginBottom:2}}>
          {p.name}: {fmt ? fmt(p.value) : p.value}
        </p>
      ))}
    </div>
  );
};

const tickStyle = { fontSize:10, fill:C.muted, fontFamily:"JetBrains Mono,monospace" };

// ─── Sidebar ─────────────────────────────────────────────────────────────────
const PAGES = [
  {id:"overview",   icon:"◈", label:"Overview"},
  {id:"analysis",   icon:"⟋", label:"Fund Analysis"},
  {id:"comparison", icon:"⇌", label:"Comparison"},
  {id:"radar",      icon:"◉", label:"Risk Radar"},
  {id:"exit",       icon:"⊗", label:"Exit Strategy"},
  {id:"portfolio",  icon:"♦", label:"Portfolio Doctor"},
];

const Sidebar = ({ page, setPage, pipelineStatus }) => {
  const [refreshing, setRefreshing] = useState(false);

  const triggerRefresh = async (mode) => {
    setRefreshing(true);
    await fetch(`${API}/pipeline/trigger?mode=${mode}`, { method:"POST" });
    setTimeout(() => setRefreshing(false), 2000);
  };

  return (
    <aside style={{width:212,minHeight:"100vh",background:C.surface,borderRight:`1px solid ${C.border}`,display:"flex",flexDirection:"column",position:"fixed",top:0,left:0,bottom:0,zIndex:100}}>
      <div style={{padding:"22px 17px 16px",borderBottom:`1px solid ${C.border}`}}>
        <div style={{display:"flex",alignItems:"center",gap:10}}>
          <div style={{width:34,height:34,borderRadius:9,background:`linear-gradient(135deg,${C.accent},${C.gold})`,display:"flex",alignItems:"center",justifyContent:"center",fontSize:17,flexShrink:0}}>💊</div>
          <div>
            <p style={{fontWeight:800,fontSize:15,letterSpacing:"-0.03em",color:C.text}}>Fund Doctor</p>
            <p style={{fontSize:9,color:C.muted,fontFamily:"JetBrains Mono,monospace",letterSpacing:"0.06em",marginTop:1}}>INDIA MF PLATFORM</p>
          </div>
        </div>
      </div>

      <nav style={{padding:"12px 9px",flex:1}}>
        {PAGES.map(p => (
          <button key={p.id} onClick={() => setPage(p.id)}
            style={{width:"100%",display:"flex",alignItems:"center",gap:9,padding:"9px 12px",borderRadius:9,border:"none",background:page===p.id?C.accentDim:"transparent",color:page===p.id?C.accent:C.muted,cursor:"pointer",marginBottom:2,transition:"all 0.12s",fontSize:13,fontWeight:page===p.id?600:400,textAlign:"left"}}>
            <span style={{fontSize:12,opacity:page===p.id?1:0.5,flexShrink:0}}>{p.icon}</span>
            {p.label}
            {page===p.id&&<span style={{marginLeft:"auto",width:3,height:16,borderRadius:99,background:C.accent,flexShrink:0}}/>}
          </button>
        ))}
      </nav>

      {/* Pipeline control */}
      <div style={{padding:"12px 14px",borderTop:`1px solid ${C.border}`}}>
        <p style={{fontSize:9,color:C.muted,fontFamily:"JetBrains Mono,monospace",letterSpacing:"0.06em",marginBottom:8,textTransform:"uppercase"}}>Data Refresh</p>
        <div style={{display:"flex",gap:6,marginBottom:8}}>
          <button onClick={() => triggerRefresh("demo")}
            style={{flex:1,padding:"5px 8px",borderRadius:7,border:`1px solid ${C.border}`,background:C.s3,color:C.sub,fontSize:10,cursor:"pointer",fontFamily:"JetBrains Mono,monospace",transition:"all 0.12s"}}
            onMouseEnter={e=>{e.currentTarget.style.borderColor=C.accent;e.currentTarget.style.color=C.accent}}
            onMouseLeave={e=>{e.currentTarget.style.borderColor=C.border;e.currentTarget.style.color=C.sub}}>
            {refreshing ? "⟳ …" : "Demo"}
          </button>
          <button onClick={() => triggerRefresh("daily")}
            style={{flex:1,padding:"5px 8px",borderRadius:7,border:`1px solid ${C.border}`,background:C.s3,color:C.sub,fontSize:10,cursor:"pointer",fontFamily:"JetBrains Mono,monospace",transition:"all 0.12s"}}
            onMouseEnter={e=>{e.currentTarget.style.borderColor=C.gold;e.currentTarget.style.color=C.gold}}
            onMouseLeave={e=>{e.currentTarget.style.borderColor=C.border;e.currentTarget.style.color=C.sub}}>
            {refreshing ? "⟳ …" : "AMFI Live"}
          </button>
        </div>
        {pipelineStatus && (
          <p style={{fontSize:9,color:C.muted,fontFamily:"JetBrains Mono,monospace",lineHeight:1.5}}>
            {pipelineStatus.fund_count} funds · {(pipelineStatus.nav_rows/1000).toFixed(0)}K NAV rows<br/>
            Last: {pipelineStatus.last_nav || "—"}
          </p>
        )}
      </div>
    </aside>
  );
};

// ══════════════════════════════════════════════════════════════════════════════
// PAGE: OVERVIEW
// ══════════════════════════════════════════════════════════════════════════════
const OverviewPage = ({ setPage }) => {
  const { data, loading, error } = useAPI("/overview", []);

  if (loading) return <Spin/>;
  if (error)   return <ErrBox msg={error}/>;
  if (!data)   return null;

  const { stats, flag_counts, funds } = data;

  return (
    <div>
      {/* Hero */}
      <div style={{marginBottom:38}}>
        <div className="fade" style={{display:"inline-flex",alignItems:"center",gap:6,background:C.accentDim,border:`1px solid ${C.accentGlow}`,borderRadius:99,padding:"4px 14px",marginBottom:14}}>
          <span style={{width:6,height:6,borderRadius:"50%",background:C.accent,animation:"pulse 2s infinite"}}/>
          <span style={{fontSize:10,color:C.accent,fontFamily:"JetBrains Mono,monospace",letterSpacing:"0.08em"}}>LIVE FROM AMFI INDIA</span>
        </div>
        <h1 className="fade2" style={{fontSize:44,fontWeight:800,letterSpacing:"-0.04em",lineHeight:1.06,marginBottom:14}}>
          India Mutual Fund<br/>
          <span style={{background:`linear-gradient(90deg,${C.accent},${C.gold})`,WebkitBackgroundClip:"text",WebkitTextFillColor:"transparent"}}>Intelligence Engine</span>
        </h1>
        <p className="fade3" style={{fontSize:15,color:C.sub,maxWidth:500,lineHeight:1.65}}>
          Analyse every fund in India. Detect underperformers automatically. Know exactly when to exit.
        </p>
      </div>

      {/* Stats */}
      <div className="fade4" style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:12,marginBottom:30}}>
        <KPI label="Funds Tracked"   value={stats.total_funds}                          color={C.accent}/>
        <KPI label="NAV Data Points" value={`${(stats.total_nav_rows/1000).toFixed(0)}K`} color={C.gold}/>
        <KPI label="Benchmarks"      value={stats.total_benchmarks}                     color={C.green}/>
        <KPI label="Last Updated"    value={stats.last_nav || "—"} mono={false}          color={C.sub}/>
      </div>

      {/* Flag health scanner */}
      <SectionHead title="Fund Health Scanner" sub="Click any tile to go to the Underperformance Radar"/>
      <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:12,marginBottom:34}}>
        {["OK","WARNING","SERIOUS","CRITICAL"].map(flag => {
          const cfg = FLAGS[flag];
          return (
            <div key={flag} onClick={() => setPage("radar")}
              style={{background:C.s2,border:`1px solid ${C.border}`,borderRadius:14,padding:"20px",cursor:"pointer",transition:"all 0.18s"}}
              onMouseEnter={e=>{e.currentTarget.style.borderColor=cfg.color+"55";e.currentTarget.style.background=C.s3}}
              onMouseLeave={e=>{e.currentTarget.style.borderColor=C.border;e.currentTarget.style.background=C.s2}}>
              <p className="mono" style={{fontSize:36,fontWeight:600,color:cfg.color,marginBottom:10,letterSpacing:"-0.02em"}}>{flag_counts[flag]||0}</p>
              <FlagPill flag={flag}/>
            </div>
          );
        })}
      </div>

      {/* Fund universe table */}
      <SectionHead title="Fund Universe" sub={`All ${funds.length} tracked funds — live data from AMFI India`}/>
      <Card style={{padding:0,overflow:"hidden"}}>
        <table style={{width:"100%",borderCollapse:"collapse"}}>
          <thead>
            <tr style={{borderBottom:`1px solid ${C.border}`}}>
              {["Fund","AMC","Category","1Y Return","5Y CAGR","Sharpe","ER%","Status"].map(h => (
                <th key={h} style={{padding:"11px 14px",fontSize:10,color:C.muted,fontFamily:"JetBrains Mono,monospace",textAlign:"left",letterSpacing:"0.07em",textTransform:"uppercase",fontWeight:400}}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {funds.map((f, i) => (
              <tr key={f.fund_id} style={{borderBottom:i<funds.length-1?`1px solid ${C.border}`:"none",transition:"background 0.12s",cursor:"pointer"}}
                onMouseEnter={e=>e.currentTarget.style.background=C.s3}
                onMouseLeave={e=>e.currentTarget.style.background="transparent"}>
                <td style={{padding:"11px 14px"}}>
                  <p style={{fontSize:13,color:C.text,fontWeight:600,letterSpacing:"-0.01em"}}>{f.fund_name}</p>
                  {f.fund_manager&&<p style={{fontSize:10,color:C.muted,marginTop:2}}>{f.fund_manager}</p>}
                </td>
                <td style={{padding:"11px 14px",fontSize:12,color:C.sub}}>{f.amc}</td>
                <td style={{padding:"11px 14px"}}><Tag>{(f.category||"").replace("Equity: ","").replace("Index: ","").replace("Hybrid: ","")}</Tag></td>
                <td style={{padding:"11px 14px"}} className="mono">
                  <span style={{color:f.return_1y>0.18?C.green:f.return_1y>0.10?C.text:C.orange,fontSize:12}}>
                    {pct(f.return_1y,true)}
                  </span>
                </td>
                <td style={{padding:"11px 14px"}} className="mono">
                  <span style={{color:f.return_5y>0.15?C.green:f.return_5y>0.10?C.text:C.orange,fontSize:12}}>
                    {pct(f.return_5y,true)}
                  </span>
                </td>
                <td style={{padding:"11px 14px"}} className="mono">
                  <span style={{color:f.sharpe_ratio>1.2?C.green:f.sharpe_ratio>0.7?C.text:C.orange,fontSize:12}}>
                    {num2(f.sharpe_ratio)}
                  </span>
                </td>
                <td style={{padding:"11px 14px"}} className="mono">
                  <span style={{color:f.expense_ratio<0.5?C.green:f.expense_ratio<1.2?C.text:C.orange,fontSize:12}}>
                    {f.expense_ratio!=null?`${f.expense_ratio}%`:"—"}
                  </span>
                </td>
                <td style={{padding:"11px 14px"}}><FlagPill flag={f.flag}/></td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>
    </div>
  );
};

// ══════════════════════════════════════════════════════════════════════════════
// PAGE: FUND ANALYSIS
// ══════════════════════════════════════════════════════════════════════════════
const AnalysisPage = () => {
  const [selId, setSelId]   = useState(null);
  const [period, setPeriod] = useState("5Y");
  const [rollWin, setRollWin] = useState(3);

  const { data: fundsList } = useAPI("/funds", []);
  const { data: an, loading: anLoading } = useAPI(selId ? `/funds/${selId}/analytics` : null, [selId]);
  const { data: navData, loading: navLoading } = useAPI(selId ? `/funds/${selId}/nav?period=${period}&thin=4` : null, [selId, period]);
  const { data: ddData } = useAPI(selId ? `/funds/${selId}/drawdown?period=${period}&thin=4` : null, [selId, period]);
  const { data: rollData } = useAPI(selId ? `/funds/${selId}/rolling?window_years=${rollWin}&thin=4` : null, [selId, rollWin]);

  // Set first fund as default once list loads
  useEffect(() => {
    if (fundsList?.funds?.length && !selId) {
      setSelId(fundsList.funds.find(f => f.fund_id === "120716")?.fund_id || fundsList.funds[0].fund_id);
    }
  }, [fundsList]);

  const chartData = navData?.nav_norm?.map((d, i) => ({
    date: d.date?.slice(2,7),
    fund: d.nav,
    bench: navData.benchmark_norm?.[i]?.nav,
  })) || [];

  const ddChart = ddData?.drawdown || [];
  const rollChart = (rollData?.fund_rolling || []).map((d, i) => ({
    date: d.date?.slice(2,7),
    fund: d.val,
    bench: rollData?.bench_rolling?.[i]?.val,
  }));

  return (
    <div>
      {/* Fund selector */}
      <div className="fade" style={{marginBottom:26}}>
        <p style={{fontSize:10,color:C.muted,fontFamily:"JetBrains Mono,monospace",letterSpacing:"0.08em",textTransform:"uppercase",marginBottom:10}}>Select Fund</p>
        <div style={{display:"flex",gap:7,flexWrap:"wrap"}}>
          {(fundsList?.funds || []).map(f => (
            <button key={f.fund_id} onClick={() => setSelId(f.fund_id)}
              style={{padding:"7px 14px",borderRadius:9,border:`1px solid ${selId===f.fund_id?C.accent:C.border}`,background:selId===f.fund_id?C.accentDim:C.s2,color:selId===f.fund_id?C.accent:C.sub,fontSize:12,cursor:"pointer",transition:"all 0.12s",fontFamily:"Bricolage Grotesque,sans-serif",fontWeight:selId===f.fund_id?600:400}}>
              {f.fund_name.replace(" Fund","").replace(" - Growth","").substring(0,22)}
            </button>
          ))}
        </div>
      </div>

      {(anLoading || navLoading) && <Spin/>}

      {an && !anLoading && (
        <>
          {/* Fund header card */}
          <div className="fade2" style={{background:C.s2,border:`1px solid ${C.border}`,borderRadius:16,padding:"22px 26px",marginBottom:20,position:"relative",overflow:"hidden"}}>
            <div style={{position:"absolute",top:0,left:0,right:0,height:1,background:`linear-gradient(90deg,transparent,${C.accent}44,transparent)`}}/>
            <div style={{position:"absolute",right:0,top:0,width:280,height:280,background:`radial-gradient(circle at 100% 0%,${C.accentDim},transparent 65%)`,pointerEvents:"none"}}/>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",flexWrap:"wrap",gap:14}}>
              <div>
                <h2 style={{fontSize:23,fontWeight:800,letterSpacing:"-0.03em",marginBottom:10}}>{an.fund_name}</h2>
                <div style={{display:"flex",flexWrap:"wrap",gap:4,marginBottom:8}}>
                  <Tag>{an.amc}</Tag>
                  <Tag>{an.category}</Tag>
                  <Tag c={C.accent}>Benchmark: {an.benchmark}</Tag>
                  {an.expense_ratio&&<Tag>ER: {an.expense_ratio}%</Tag>}
                  <FlagPill flag={fundsList?.funds?.find(f=>f.fund_id===selId)?.flag||"OK"}/>
                </div>
              </div>
              <div style={{textAlign:"right"}}>
                <p style={{fontSize:10,color:C.muted,fontFamily:"JetBrains Mono,monospace",marginBottom:4}}>LATEST NAV</p>
                <p className="mono" style={{fontSize:30,fontWeight:600,color:C.accent}}>₹{an.nav_latest?.toFixed(2)}</p>
                <p style={{fontSize:10,color:C.muted,marginTop:3}}>{an.nav_end_date} · {an.nav_count?.toLocaleString()} data points</p>
              </div>
            </div>
          </div>

          {/* Returns */}
          <div className="fade3" style={{display:"grid",gridTemplateColumns:"repeat(5,1fr)",gap:10,marginBottom:12}}>
            {[["1Y Return",an.return_1y,true],["3Y CAGR",an.return_3y,true],["5Y CAGR",an.return_5y,true],["10Y CAGR",an.return_10y,true],["Inception",an.return_inception,true]].map(([l,v,s]) => (
              <div key={l} style={{background:C.s2,border:`1px solid ${C.border}`,borderRadius:12,padding:"13px 15px"}}>
                <p style={{fontSize:10,color:C.muted,fontFamily:"JetBrains Mono,monospace",textTransform:"uppercase",letterSpacing:"0.06em",marginBottom:5}}>{l}</p>
                <p className="mono" style={{fontSize:18,fontWeight:600,color:v>0?C.green:v<0?C.red:C.text}}>{pct(v,s)}</p>
              </div>
            ))}
          </div>

          {/* Risk metrics */}
          <div className="fade4" style={{display:"grid",gridTemplateColumns:"repeat(5,1fr)",gap:10,marginBottom:26}}>
            {[["Volatility",pct(an.volatility),C.sub],["Max DD",pct(an.max_drawdown),C.red],["Sharpe",num2(an.sharpe_ratio),an.sharpe_ratio>1.2?C.green:C.sub],["Sortino",num2(an.sortino_ratio),C.sub],["Beta",num2(an.beta),C.sub],["Alpha",pct(an.alpha,true),an.alpha>0?C.green:C.red]].slice(0,5).map(([l,v,c]) => (
              <div key={l} style={{background:C.s2,border:`1px solid ${C.border}`,borderRadius:12,padding:"13px 15px"}}>
                <p style={{fontSize:10,color:C.muted,fontFamily:"JetBrains Mono,monospace",textTransform:"uppercase",letterSpacing:"0.06em",marginBottom:5}}>{l}</p>
                <p className="mono" style={{fontSize:18,fontWeight:600,color:c}}>{v}</p>
              </div>
            ))}
          </div>

          {/* NAV chart */}
          <Card style={{marginBottom:14}}>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:18}}>
              <h3 style={{fontSize:15,fontWeight:700}}>NAV Growth — ₹100 Invested</h3>
              <div style={{display:"flex",gap:6}}>
                {["1Y","3Y","5Y","10Y","ALL"].map(p => <PeriodBtn key={p} active={period===p} onClick={()=>setPeriod(p)}>{p}</PeriodBtn>)}
              </div>
            </div>
            {navLoading ? <Spin/> : (
              <ResponsiveContainer width="100%" height={250}>
                <AreaChart data={chartData}>
                  <defs>
                    <linearGradient id="g1" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor={C.accent} stopOpacity={0.18}/><stop offset="95%" stopColor={C.accent} stopOpacity={0}/></linearGradient>
                    <linearGradient id="g2" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor={C.gold} stopOpacity={0.08}/><stop offset="95%" stopColor={C.gold} stopOpacity={0}/></linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke={C.border}/>
                  <XAxis dataKey="date" tick={tickStyle} interval={Math.floor(chartData.length/6)} tickLine={false}/>
                  <YAxis tick={tickStyle} tickFormatter={v=>`₹${v}`} width={56} tickLine={false}/>
                  <Tooltip content={<ChartTip fmt={v=>`₹${v}`}/>}/>
                  <Area type="monotone" dataKey="fund" stroke={C.accent} strokeWidth={2} fill="url(#g1)" name={an.fund_name?.split(" ").slice(0,2).join(" ")} dot={false}/>
                  <Area type="monotone" dataKey="bench" stroke={C.gold} strokeWidth={1.5} strokeDasharray="5 3" fill="url(#g2)" name={an.benchmark} dot={false}/>
                </AreaChart>
              </ResponsiveContainer>
            )}
          </Card>

          {/* Drawdown */}
          <Card style={{marginBottom:14}}>
            <h3 style={{fontSize:15,fontWeight:700,marginBottom:18}}>Drawdown Analysis</h3>
            <ResponsiveContainer width="100%" height={170}>
              <AreaChart data={ddChart}>
                <defs><linearGradient id="ddg" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor={C.red} stopOpacity={0.22}/><stop offset="95%" stopColor={C.red} stopOpacity={0}/></linearGradient></defs>
                <CartesianGrid strokeDasharray="3 3" stroke={C.border}/>
                <XAxis dataKey="date" tick={tickStyle} interval={Math.floor((ddChart.length||1)/6)} tickLine={false} tickFormatter={v=>v?.slice?.(2,7)||v}/>
                <YAxis tick={tickStyle} tickFormatter={v=>`${v}%`} width={50} tickLine={false}/>
                <Tooltip content={<ChartTip fmt={v=>`${v}%`}/>}/>
                <ReferenceLine y={0} stroke={C.border2}/>
                <Area type="monotone" dataKey="dd" stroke={C.red} strokeWidth={1.5} fill="url(#ddg)" name="Drawdown%" dot={false}/>
              </AreaChart>
            </ResponsiveContainer>
          </Card>

          {/* Rolling returns */}
          <Card>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:18}}>
              <h3 style={{fontSize:15,fontWeight:700}}>{rollWin}Y Rolling CAGR</h3>
              <div style={{display:"flex",gap:6}}>
                {[1,3,5].map(w => <PeriodBtn key={w} active={rollWin===w} onClick={()=>setRollWin(w)}>{w}Y</PeriodBtn>)}
              </div>
            </div>
            <ResponsiveContainer width="100%" height={210}>
              <LineChart data={rollChart}>
                <CartesianGrid strokeDasharray="3 3" stroke={C.border}/>
                <XAxis dataKey="date" tick={tickStyle} interval={Math.floor((rollChart.length||1)/6)} tickLine={false}/>
                <YAxis tick={tickStyle} tickFormatter={v=>`${v}%`} width={50} tickLine={false}/>
                <Tooltip content={<ChartTip fmt={v=>`${v?.toFixed?.(1)||v}%`}/>}/>
                <ReferenceLine y={0} stroke={C.border2}/>
                <Line type="monotone" dataKey="fund" stroke={C.accent} strokeWidth={2} name="Fund" dot={false}/>
                <Line type="monotone" dataKey="bench" stroke={C.gold} strokeWidth={1.5} strokeDasharray="5 3" name={an.benchmark} dot={false}/>
              </LineChart>
            </ResponsiveContainer>
          </Card>
        </>
      )}
    </div>
  );
};

// ══════════════════════════════════════════════════════════════════════════════
// PAGE: COMPARISON
// ══════════════════════════════════════════════════════════════════════════════
const ComparisonPage = () => {
  const [idA, setIdA] = useState("120716");
  const [idB, setIdB] = useState("120503");
  const [period, setPeriod] = useState("5Y");

  const { data: fundsList } = useAPI("/funds", []);
  const { data: cmpData, loading } = useAPI(
    idA && idB ? `/comparison?fund_a=${idA}&fund_b=${idB}&period=${period}&thin=4` : null,
    [idA, idB, period]
  );

  const FundPicker = ({ id, setId, color, label }) => (
    <div style={{background:C.s2,border:`1px solid ${C.border}`,borderRadius:14,padding:"16px"}}>
      <p style={{fontSize:10,color,fontFamily:"JetBrains Mono,monospace",letterSpacing:"0.08em",textTransform:"uppercase",marginBottom:12}}>{label}</p>
      <div style={{display:"flex",flexDirection:"column",gap:4}}>
        {(fundsList?.funds||[]).map(f => (
          <button key={f.fund_id} onClick={() => setId(f.fund_id)}
            style={{padding:"7px 11px",borderRadius:8,border:`1px solid ${id===f.fund_id?color:C.border}`,background:id===f.fund_id?color+"18":"transparent",color:id===f.fund_id?color:C.sub,fontSize:12,cursor:"pointer",textAlign:"left",transition:"all 0.12s"}}>
            {f.fund_name.substring(0,34)}
          </button>
        ))}
      </div>
    </div>
  );

  const radarData = cmpData ? [
    {m:"Sharpe",   A:Math.min(((cmpData.fund_a?.sharpe_ratio)||0)/2,1), B:Math.min(((cmpData.fund_b?.sharpe_ratio)||0)/2,1)},
    {m:"Sortino",  A:Math.min(((cmpData.fund_a?.sortino_ratio)||0)/3,1),B:Math.min(((cmpData.fund_b?.sortino_ratio)||0)/3,1)},
    {m:"Low Vol",  A:1-Math.min(Math.abs((cmpData.fund_a?.volatility)||0.25)/0.4,1), B:1-Math.min(Math.abs((cmpData.fund_b?.volatility)||0.25)/0.4,1)},
    {m:"Low DD",   A:1-Math.min(Math.abs((cmpData.fund_a?.max_drawdown)||0.3)/0.6,1), B:1-Math.min(Math.abs((cmpData.fund_b?.max_drawdown)||0.3)/0.6,1)},
    {m:"Alpha",    A:Math.min(((cmpData.fund_a?.alpha)||0)/0.1+0.5,1), B:Math.min(((cmpData.fund_b?.alpha)||0)/0.1+0.5,1)},
    {m:"Low Cost", A:1-Math.min(((cmpData.fund_a?.expense_ratio)||1)/2,1), B:1-Math.min(((cmpData.fund_b?.expense_ratio)||1)/2,1)},
  ] : [];

  return (
    <div>
      <SectionHead title="Fund Comparison" sub="Side-by-side analysis — live data from backend"/>
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16,marginBottom:26}}>
        <FundPicker id={idA} setId={setIdA} color={C.accent} label="Fund A"/>
        <FundPicker id={idB} setId={setIdB} color={C.purple} label="Fund B"/>
      </div>

      {loading && <Spin/>}

      {cmpData && !loading && (
        <>
          {/* Period selector */}
          <div style={{display:"flex",gap:6,marginBottom:18}}>
            {["1Y","3Y","5Y","10Y","ALL"].map(p => <PeriodBtn key={p} active={period===p} onClick={()=>setPeriod(p)}>{p}</PeriodBtn>)}
          </div>

          {/* Comparison table */}
          <Card style={{padding:0,overflow:"hidden",marginBottom:16}}>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",borderBottom:`1px solid ${C.border}`}}>
              <div style={{padding:"11px 14px",fontSize:10,color:C.muted,fontFamily:"JetBrains Mono,monospace",textTransform:"uppercase",letterSpacing:"0.07em"}}>Metric</div>
              <div style={{padding:"11px 14px",fontSize:11,color:C.accent,fontFamily:"JetBrains Mono,monospace",borderLeft:`1px solid ${C.border}`,fontWeight:600}}>{cmpData.fund_a.fund_name?.substring(0,28)}</div>
              <div style={{padding:"11px 14px",fontSize:11,color:C.purple,fontFamily:"JetBrains Mono,monospace",borderLeft:`1px solid ${C.border}`,fontWeight:600}}>{cmpData.fund_b.fund_name?.substring(0,28)}</div>
            </div>
            {(cmpData.comparison_table||[]).map((row,i) => (
              <div key={row.key} style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",borderBottom:i<cmpData.comparison_table.length-1?`1px solid ${C.border}`:"none"}}>
                <div style={{padding:"9px 14px",fontSize:12,color:C.sub}}>{row.label}</div>
                <div style={{padding:"9px 14px",fontFamily:"JetBrains Mono,monospace",fontSize:12,color:row.winner==="A"?C.green:C.text,borderLeft:`1px solid ${C.border}`,display:"flex",alignItems:"center",gap:6}}>
                  {row.key.includes("return")||row.key==="alpha"||row.key==="volatility"||row.key==="max_drawdown" ? pct(row.a,row.key==="alpha"||row.key.includes("return")) : row.key==="expense_ratio" ? `${row.a}%` : num2(row.a)}
                  {row.winner==="A"&&<span style={{fontSize:9,color:C.green}}>▲</span>}
                </div>
                <div style={{padding:"9px 14px",fontFamily:"JetBrains Mono,monospace",fontSize:12,color:row.winner==="B"?C.green:C.text,borderLeft:`1px solid ${C.border}`,display:"flex",alignItems:"center",gap:6}}>
                  {row.key.includes("return")||row.key==="alpha"||row.key==="volatility"||row.key==="max_drawdown" ? pct(row.b,row.key==="alpha"||row.key.includes("return")) : row.key==="expense_ratio" ? `${row.b}%` : num2(row.b)}
                  {row.winner==="B"&&<span style={{fontSize:9,color:C.green}}>▲</span>}
                </div>
              </div>
            ))}
          </Card>

          {/* NAV comparison chart */}
          <Card style={{marginBottom:16}}>
            <h3 style={{fontSize:15,fontWeight:700,marginBottom:18}}>Normalised NAV — Base ₹100</h3>
            <ResponsiveContainer width="100%" height={230}>
              <LineChart data={cmpData.nav_chart}>
                <CartesianGrid strokeDasharray="3 3" stroke={C.border}/>
                <XAxis dataKey="date" tick={tickStyle} interval={Math.floor((cmpData.nav_chart?.length||1)/6)} tickLine={false}/>
                <YAxis tick={tickStyle} tickFormatter={v=>`₹${v}`} width={56} tickLine={false}/>
                <Tooltip content={<ChartTip fmt={v=>`₹${v}`}/>}/>
                <Line type="monotone" dataKey="fund_a" stroke={C.accent} strokeWidth={2} name={cmpData.fund_a.fund_name?.substring(0,20)} dot={false}/>
                <Line type="monotone" dataKey="fund_b" stroke={C.purple} strokeWidth={2} name={cmpData.fund_b.fund_name?.substring(0,20)} dot={false}/>
              </LineChart>
            </ResponsiveContainer>
          </Card>

          {/* Radar */}
          <Card>
            <h3 style={{fontSize:15,fontWeight:700,marginBottom:18}}>Risk-Return Profile Radar</h3>
            <ResponsiveContainer width="100%" height={300}>
              <RadarChart data={radarData} cx="50%" cy="50%" outerRadius="72%">
                <PolarGrid stroke={C.border2}/>
                <PolarAngleAxis dataKey="m" tick={{fontSize:11,fill:C.sub,fontFamily:"JetBrains Mono,monospace"}}/>
                <Radar name={cmpData.fund_a.fund_name?.substring(0,20)} dataKey="A" stroke={C.accent} fill={C.accent} fillOpacity={0.15} strokeWidth={2}/>
                <Radar name={cmpData.fund_b.fund_name?.substring(0,20)} dataKey="B" stroke={C.purple} fill={C.purple} fillOpacity={0.12} strokeWidth={2}/>
                <Tooltip content={<ChartTip fmt={v=>v?.toFixed?.(2)}/>}/>
              </RadarChart>
            </ResponsiveContainer>
            {cmpData.overlap?.overlap_pct!=null && (
              <div style={{marginTop:16,padding:"12px 16px",background:C.s3,borderRadius:10,display:"flex",alignItems:"center",gap:12}}>
                <span style={{fontSize:11,color:C.muted}}>Holdings Overlap:</span>
                <span className="mono" style={{fontSize:14,color:cmpData.overlap.overlap_pct>30?C.orange:C.green,fontWeight:600}}>{cmpData.overlap.overlap_pct}%</span>
                {cmpData.overlap.count_common>0&&<span style={{fontSize:11,color:C.muted}}>{cmpData.overlap.count_common} common stocks</span>}
              </div>
            )}
          </Card>
        </>
      )}
    </div>
  );
};

// ══════════════════════════════════════════════════════════════════════════════
// PAGE: UNDERPERFORMANCE RADAR
// ══════════════════════════════════════════════════════════════════════════════
const RadarPage = () => {
  const [filter, setFilter] = useState(["WARNING","SERIOUS","CRITICAL"]);
  const { data, loading, error } = useAPI("/radar", []);

  if (loading) return <Spin/>;
  if (error)   return <ErrBox msg={error}/>;
  if (!data)   return null;

  const { flag_counts, funds } = data;
  const visible = filter.length ? funds.filter(f => filter.includes(f.flag)) : funds;
  const barData = [...funds].sort((a,b) => (a.excess_return_pct||0)-(b.excess_return_pct||0));

  return (
    <div>
      <SectionHead title="Underperformance Radar" sub="Every fund scanned against its benchmark — live from backend"/>

      {/* Flag count tiles */}
      <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:12,marginBottom:28}}>
        {["OK","WARNING","SERIOUS","CRITICAL"].map(flag => {
          const cfg = FLAGS[flag]; const on = filter.includes(flag);
          return (
            <div key={flag}
              onClick={() => flag!=="OK"&&setFilter(p=>p.includes(flag)?p.filter(x=>x!==flag):[...p,flag])}
              style={{background:C.s2,border:`1px solid ${on||flag==="OK"?cfg.color+"44":C.border}`,borderRadius:14,padding:"18px 20px",cursor:flag==="OK"?"default":"pointer",transition:"all 0.18s"}}>
              <p className="mono" style={{fontSize:34,fontWeight:600,color:cfg.color,marginBottom:8,letterSpacing:"-0.02em"}}>{flag_counts[flag]||0}</p>
              <FlagPill flag={flag}/>
              {flag!=="OK"&&<p style={{fontSize:10,color:C.muted,marginTop:6,fontFamily:"JetBrains Mono,monospace"}}>{on?"✓ shown":"click to filter"}</p>}
            </div>
          );
        })}
      </div>

      {/* Excess return bar */}
      <Card style={{marginBottom:18}}>
        <h3 style={{fontSize:15,fontWeight:700,marginBottom:18}}>5-Year Excess Return vs Benchmark (%)</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={barData} layout="vertical" margin={{left:8,right:40}}>
            <CartesianGrid strokeDasharray="3 3" stroke={C.border} horizontal={false}/>
            <XAxis type="number" tick={tickStyle} tickFormatter={v=>`${v}%`} tickLine={false}/>
            <YAxis type="category" dataKey="fund_name" tick={{fontSize:10,fill:C.sub,fontFamily:"JetBrains Mono,monospace"}} width={155} tickLine={false}
              tickFormatter={v=>v?.substring?.(0,22)||v}/>
            <Tooltip content={<ChartTip fmt={v=>`${v}%`}/>}/>
            <ReferenceLine x={0} stroke={C.border2} strokeWidth={1.5}/>
            <Bar dataKey="excess_return_pct" name="Excess Return %" radius={[0,4,4,0]}>
              {barData.map((d,i) => <Cell key={i} fill={d.excess_return_pct>=0?C.green:C.red}/>)}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </Card>

      {/* Filter pills */}
      <div style={{display:"flex",gap:8,marginBottom:14,flexWrap:"wrap",alignItems:"center"}}>
        <span style={{fontSize:11,color:C.muted,marginRight:4}}>Filter:</span>
        {["OK","WARNING","SERIOUS","CRITICAL"].map(f => {
          const cfg=FLAGS[f]; const on=filter.includes(f);
          return (
            <button key={f} onClick={()=>setFilter(p=>p.includes(f)?p.filter(x=>x!==f):[...p,f])}
              style={{padding:"4px 12px",borderRadius:99,border:`1px solid ${on?cfg.color+"55":C.border}`,background:on?cfg.bg:"transparent",color:on?cfg.color:C.muted,fontSize:11,cursor:"pointer",fontFamily:"JetBrains Mono,monospace",transition:"all 0.12s"}}>
              {f}
            </button>
          );
        })}
        <span style={{fontSize:11,color:C.muted,marginLeft:8}}>{visible.length} fund{visible.length!==1?"s":""} shown</span>
      </div>

      {/* Detail table */}
      <Card style={{padding:0,overflow:"hidden"}}>
        <table style={{width:"100%",borderCollapse:"collapse"}}>
          <thead>
            <tr style={{borderBottom:`1px solid ${C.border}`}}>
              {["Fund","Category","Fund 5Y","Bench 5Y","Excess","Sharpe","Rolling Underperf","Status"].map(h => (
                <th key={h} style={{padding:"11px 14px",fontSize:10,color:C.muted,fontFamily:"JetBrains Mono,monospace",textAlign:"left",letterSpacing:"0.07em",textTransform:"uppercase",fontWeight:400}}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {visible.length===0 ? (
              <tr><td colSpan={8} style={{padding:"32px",textAlign:"center",color:C.muted,fontSize:13}}>No funds match selected filters</td></tr>
            ) : visible.map((d,i) => (
              <tr key={d.fund_id} style={{borderBottom:i<visible.length-1?`1px solid ${C.border}`:"none",transition:"background 0.12s"}}
                onMouseEnter={e=>e.currentTarget.style.background=C.s3}
                onMouseLeave={e=>e.currentTarget.style.background="transparent"}>
                <td style={{padding:"10px 14px",fontSize:13,color:C.text,fontWeight:600}}>{short(d.fund_name,26)}</td>
                <td style={{padding:"10px 14px"}}><Tag>{(d.category||"").replace("Equity: ","")}</Tag></td>
                <td style={{padding:"10px 14px"}} className="mono"><span style={{fontSize:12,color:C.text}}>{pct(d.fund_5y,true)}</span></td>
                <td style={{padding:"10px 14px"}} className="mono"><span style={{fontSize:12,color:C.muted}}>{pct(d.bench_5y,true)}</span></td>
                <td style={{padding:"10px 14px"}} className="mono"><span style={{fontSize:12,color:d.excess_return_pct!=null&&d.excess_return_pct>=0?C.green:C.red}}>{d.excess_return_pct!=null?`${d.excess_return_pct>0?"+":""}${d.excess_return_pct}%`:"—"}</span></td>
                <td style={{padding:"10px 14px"}} className="mono"><span style={{fontSize:12,color:d.sharpe_ratio>1.2?C.green:d.sharpe_ratio>0.7?C.text:C.orange}}>{num2(d.sharpe_ratio)}</span></td>
                <td style={{padding:"10px 14px"}} className="mono"><span style={{fontSize:12,color:C.muted}}>{d.pct_rolling_underperf!=null?`${(d.pct_rolling_underperf*100).toFixed(0)}%`:"—"}</span></td>
                <td style={{padding:"10px 14px"}}><FlagPill flag={d.flag}/></td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>
    </div>
  );
};

// ══════════════════════════════════════════════════════════════════════════════
// PAGE: EXIT STRATEGY
// ══════════════════════════════════════════════════════════════════════════════
const ExitPage = () => {
  const [selId, setSelId]   = useState("120503");
  const [months, setMonths] = useState(24);
  const [amount, setAmount] = useState(50000);
  const [queried, setQueried] = useState(false);

  const { data: fundsList } = useAPI("/funds", []);
  const { data, loading, error } = useAPI(
    queried ? `/exit/${selId}?holding_months=${months}&invested_amount=${amount}` : null,
    [selId, months, amount, queried]
  );

  const selFund = fundsList?.funds?.find(f=>f.fund_id===selId);

  const REC_META = {
    EXIT:   {color:C.red,    bg:C.redDim,    icon:"⊗"},
    SWITCH: {color:C.orange, bg:"rgba(249,115,22,0.1)", icon:"⇌"},
    WATCH:  {color:C.gold,   bg:C.goldDim,   icon:"◉"},
    HOLD:   {color:C.green,  bg:C.greenDim,  icon:"✓"},
  };

  const rec  = data?.assessment?.recommendation;
  const meta = REC_META[rec] || REC_META.HOLD;

  return (
    <div>
      <SectionHead title="Exit Strategy Engine" sub="Live backend analysis — Hold, Watch, Switch, or Exit with tax notes"/>

      {/* Input panel */}
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:16,marginBottom:22}}>
        {/* Fund picker */}
        <div style={{background:C.s2,border:`1px solid ${C.border}`,borderRadius:14,padding:"17px"}}>
          <p style={{fontSize:10,color:C.muted,fontFamily:"JetBrains Mono,monospace",letterSpacing:"0.08em",textTransform:"uppercase",marginBottom:11}}>Select Fund</p>
          <div style={{display:"flex",flexDirection:"column",gap:4,maxHeight:330,overflowY:"auto"}}>
            {(fundsList?.funds||[]).map(f => (
              <button key={f.fund_id} onClick={()=>{setSelId(f.fund_id);setQueried(false);}}
                style={{display:"block",padding:"8px 11px",borderRadius:8,border:`1px solid ${selId===f.fund_id?C.accent:C.border}`,background:selId===f.fund_id?C.accentDim:"transparent",color:selId===f.fund_id?C.accent:C.sub,fontSize:12,cursor:"pointer",textAlign:"left",transition:"all 0.12s",marginBottom:2}}>
                {f.fund_name.substring(0,32)}
                <FlagPill flag={f.flag}/>
              </button>
            ))}
          </div>
        </div>

        {/* Sliders */}
        <div style={{background:C.s2,border:`1px solid ${C.border}`,borderRadius:14,padding:"17px",display:"flex",flexDirection:"column",gap:22}}>
          <div>
            <p style={{fontSize:10,color:C.muted,fontFamily:"JetBrains Mono,monospace",textTransform:"uppercase",letterSpacing:"0.08em",marginBottom:10}}>Holding Period</p>
            <div style={{display:"flex",justifyContent:"space-between",marginBottom:8}}>
              <span style={{fontSize:12,color:C.sub}}>Duration</span>
              <span className="mono" style={{fontSize:14,color:C.accent}}>{months} months</span>
            </div>
            <input type="range" min={1} max={120} value={months} onChange={e=>{setMonths(+e.target.value);setQueried(false);}}/>
            <div style={{display:"flex",justifyContent:"space-between",fontSize:10,fontFamily:"JetBrains Mono,monospace",marginTop:5}}>
              <span style={{color:C.muted}}>1mo</span>
              <span style={{color:months<12?C.orange:C.green,fontWeight:600}}>{months<12?"⚠ STCG (15%)":"✓ LTCG (10%)"}</span>
              <span style={{color:C.muted}}>10yr</span>
            </div>
          </div>
          <div>
            <p style={{fontSize:10,color:C.muted,fontFamily:"JetBrains Mono,monospace",textTransform:"uppercase",letterSpacing:"0.08em",marginBottom:10}}>Amount Invested</p>
            <div style={{display:"flex",justifyContent:"space-between",marginBottom:8}}>
              <span style={{fontSize:12,color:C.sub}}>Principal</span>
              <span className="mono" style={{fontSize:14,color:C.accent}}>{inr(amount)}</span>
            </div>
            <input type="range" min={5000} max={1000000} step={5000} value={amount} onChange={e=>{setAmount(+e.target.value);setQueried(false);}}/>
            <div style={{display:"flex",justifyContent:"space-between",fontSize:10,color:C.muted,marginTop:5,fontFamily:"JetBrains Mono,monospace"}}><span>₹5K</span><span>₹10L</span></div>
          </div>
        </div>

        {/* Preview + CTA */}
        <div style={{background:C.s2,border:`1px solid ${C.border}`,borderRadius:14,padding:"17px",display:"flex",flexDirection:"column"}}>
          <p style={{fontSize:10,color:C.muted,fontFamily:"JetBrains Mono,monospace",textTransform:"uppercase",letterSpacing:"0.08em",marginBottom:13}}>Selected Fund</p>
          {selFund && (
            <>
              <p style={{fontSize:14,color:C.text,fontWeight:700,marginBottom:4,letterSpacing:"-0.01em"}}>{selFund.fund_name.substring(0,34)}</p>
              <p style={{fontSize:11,color:C.muted,marginBottom:14}}>{selFund.category} · {selFund.amc}</p>
              <div style={{display:"flex",flexDirection:"column",gap:7,flex:1}}>
                {[["5Y CAGR",pct(selFund.return_5y,true),selFund.return_5y>0.15?C.green:C.orange],["Sharpe",num2(selFund.sharpe_ratio),selFund.sharpe_ratio>1?C.green:C.orange],["ER",`${selFund.expense_ratio}%`,selFund.expense_ratio<1?C.green:C.orange]].map(([l,v,c])=>(
                  <div key={l} style={{display:"flex",justifyContent:"space-between"}}>
                    <span style={{fontSize:11,color:C.muted}}>{l}</span>
                    <span className="mono" style={{fontSize:11,color:c}}>{v}</span>
                  </div>
                ))}
                <div style={{display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                  <span style={{fontSize:11,color:C.muted}}>Status</span>
                  <FlagPill flag={selFund.flag}/>
                </div>
              </div>
            </>
          )}
          <button onClick={() => setQueried(true)} disabled={loading}
            style={{marginTop:16,width:"100%",padding:"12px",borderRadius:10,border:`1px solid ${C.accent}`,background:queried&&!loading?C.accent:C.accentDim,color:queried&&!loading?C.bg:C.accent,fontSize:12,fontWeight:700,cursor:"pointer",fontFamily:"JetBrains Mono,monospace",letterSpacing:"0.06em",transition:"all 0.18s",opacity:loading?0.6:1}}>
            {loading ? "Analysing …" : queried ? "✓ ANALYSIS DONE" : "ANALYSE EXIT →"}
          </button>
        </div>
      </div>

      {error && <ErrBox msg={error}/>}

      {data && queried && !loading && (
        <div className="fade">
          {/* Recommendation hero */}
          <div style={{background:C.s2,border:`1px solid ${meta.color}44`,borderRadius:16,padding:"24px",marginBottom:16,position:"relative",overflow:"hidden"}}>
            <div style={{position:"absolute",inset:0,background:`radial-gradient(circle at 15% 50%,${meta.color}07,transparent 55%)`,pointerEvents:"none"}}/>
            <div style={{display:"flex",alignItems:"center",gap:18,flexWrap:"wrap"}}>
              <div style={{width:66,height:66,borderRadius:15,background:meta.bg,border:`1px solid ${meta.color}44`,display:"flex",alignItems:"center",justifyContent:"center",fontSize:30,flexShrink:0}}>{meta.icon}</div>
              <div>
                <p style={{fontSize:10,color:C.muted,fontFamily:"JetBrains Mono,monospace",letterSpacing:"0.08em",textTransform:"uppercase",marginBottom:5}}>Recommendation</p>
                <p style={{fontFamily:"Bricolage Grotesque,sans-serif",fontSize:38,fontWeight:800,color:meta.color,letterSpacing:"-0.04em"}}>{rec}</p>
              </div>
              <div style={{marginLeft:"auto",textAlign:"right"}}>
                <FlagPill flag={data.assessment.flag}/>
              </div>
            </div>
          </div>

          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16,marginBottom:16}}>
            <Card>
              <p style={{fontSize:10,color:C.muted,fontFamily:"JetBrains Mono,monospace",textTransform:"uppercase",letterSpacing:"0.08em",marginBottom:12}}>Reasons</p>
              {(data.assessment.reasons||[]).map((r,i) => (
                <div key={i} style={{display:"flex",gap:10,marginBottom:10,fontSize:13,color:C.sub,lineHeight:1.6}}>
                  <span style={{color:meta.color,flexShrink:0,marginTop:2}}>→</span>{r}
                </div>
              ))}
              {data.assessment.details && (
                <p style={{fontSize:11,color:C.muted,marginTop:10,borderTop:`1px solid ${C.border}`,paddingTop:10}}>{data.assessment.details}</p>
              )}
            </Card>
            <Card>
              <p style={{fontSize:10,color:C.muted,fontFamily:"JetBrains Mono,monospace",textTransform:"uppercase",letterSpacing:"0.08em",marginBottom:12}}>Tax & Financial Notes</p>
              {(data.assessment.tax_notes||[]).map((t,i) => (
                <div key={i} style={{display:"flex",gap:10,marginBottom:10,fontSize:13,color:C.sub,lineHeight:1.6}}>
                  <span style={{color:C.gold,flexShrink:0,marginTop:2}}>→</span>{t}
                </div>
              ))}
            </Card>
          </div>

          {/* Replacements */}
          {(rec==="SWITCH"||rec==="EXIT") && data.replacements?.length > 0 && (
            <Card>
              <p style={{fontSize:10,color:C.muted,fontFamily:"JetBrains Mono,monospace",textTransform:"uppercase",letterSpacing:"0.08em",marginBottom:16}}>
                Recommended Replacements — Same Category
              </p>
              <div style={{display:"grid",gridTemplateColumns:`repeat(${Math.min(data.replacements.length,3)},1fr)`,gap:12}}>
                {data.replacements.map((rf,ri) => (
                  <div key={rf.fund_id} style={{background:C.s3,border:`1px solid ${ri===0?C.accent+"44":C.border}`,borderRadius:12,padding:"15px",position:"relative"}}>
                    {ri===0&&<div style={{position:"absolute",top:-1,left:12,right:12,height:2,background:`linear-gradient(90deg,${C.accent},${C.gold})`}}/>}
                    {ri===0&&<span style={{position:"absolute",top:10,right:12,fontSize:9,color:C.accent,fontFamily:"JetBrains Mono,monospace",background:C.accentDim,padding:"2px 7px",borderRadius:99}}>TOP PICK</span>}
                    <p style={{fontSize:13,color:C.text,fontWeight:700,marginBottom:3,paddingRight:60,letterSpacing:"-0.01em"}}>{rf.fund_name?.substring(0,30)}</p>
                    <p style={{fontSize:11,color:C.muted,marginBottom:12}}>{rf.fund_id}</p>
                    <div style={{display:"flex",flexDirection:"column",gap:6}}>
                      {[["5Y CAGR",pct(rf.return_5y,true),C.green],["Sharpe",num2(rf.sharpe_ratio),C.text],["ER",`${rf.expense_ratio}%`,C.sub]].map(([l,v,c])=>(
                        <div key={l} style={{display:"flex",justifyContent:"space-between"}}>
                          <span style={{fontSize:11,color:C.muted}}>{l}</span>
                          <span className="mono" style={{fontSize:11,color:c}}>{v}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </Card>
          )}
          {(rec==="SWITCH"||rec==="EXIT") && (!data.replacements || data.replacements.length===0) && (
            <div style={{padding:18,background:C.goldDim,border:`1px solid ${C.gold}44`,borderRadius:12,color:C.gold,fontSize:13}}>
              No peer funds found in the same category with sufficient data for comparison.
            </div>
          )}
        </div>
      )}
    </div>
  );
};

// ══════════════════════════════════════════════════════════════════════════════
// PAGE: PORTFOLIO DOCTOR
// ══════════════════════════════════════════════════════════════════════════════
const PortfolioPage = () => {
  const { data, loading, error } = useAPI("/portfolio", []);

  if (loading) return <Spin/>;
  if (error)   return <ErrBox msg={error}/>;
  if (!data)   return null;

  const scoreColor = data.health_score>=70?C.green:data.health_score>=45?C.gold:C.red;
  const scoreLabel = data.health_score>=70?"Healthy":data.health_score>=45?"Needs Attention":"Critical";

  const catEntries = Object.entries(data.category_allocation||{})
    .sort((a,b)=>b[1]-a[1])
    .map(([name,pctVal])=>({name,pct:pctVal}));

  return (
    <div>
      <SectionHead title="Portfolio Doctor" sub="Live backend analysis — health score, allocation, recommendations"/>

      {/* Health score + KPIs */}
      <div style={{display:"grid",gridTemplateColumns:"200px 1fr",gap:16,marginBottom:22}}>
        <div style={{background:C.s2,border:`1px solid ${C.border}`,borderRadius:16,padding:"26px 20px",display:"flex",flexDirection:"column",alignItems:"center",justifyContent:"center"}}>
          <p style={{fontSize:10,color:C.muted,fontFamily:"JetBrains Mono,monospace",letterSpacing:"0.08em",textTransform:"uppercase",marginBottom:16}}>Health Score</p>
          <div style={{width:126,height:126,borderRadius:"50%",background:`conic-gradient(${scoreColor} ${data.health_score*3.6}deg,${C.s3} 0)`,display:"flex",alignItems:"center",justifyContent:"center",marginBottom:14,boxShadow:`0 0 30px ${scoreColor}25`}}>
            <div style={{width:92,height:92,borderRadius:"50%",background:C.s2,display:"flex",alignItems:"center",justifyContent:"center"}}>
              <p className="mono" style={{fontSize:30,fontWeight:700,color:scoreColor}}>{data.health_score}</p>
            </div>
          </div>
          <p style={{fontSize:14,color:scoreColor,fontWeight:700,letterSpacing:"-0.02em"}}>{scoreLabel}</p>
        </div>

        <div style={{display:"grid",gridTemplateColumns:"repeat(2,1fr)",gap:12}}>
          <KPI label="Total Invested"    value={inr(data.total_invested)}   color={C.sub}    cls="fade"/>
          <KPI label="Current Value"     value={inr(data.total_current)}    color={C.accent} cls="fade2"/>
          <KPI label="Total Gain / Loss" value={inr(data.total_gain)} sub={pct(data.total_gain_pct,true)} color={data.total_gain>=0?C.green:C.red} cls="fade3"/>
          <KPI label="Avg Expense Ratio" value={`${(data.avg_er||0).toFixed(2)}%`} color={data.avg_er>1?C.orange:C.green} cls="fade4"/>
        </div>
      </div>

      {/* Allocation + flags */}
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16,marginBottom:18}}>
        <Card>
          <h3 style={{fontSize:15,fontWeight:700,marginBottom:18}}>Category Allocation</h3>
          {catEntries.map((e,i) => (
            <div key={e.name} style={{marginBottom:13}}>
              <div style={{display:"flex",justifyContent:"space-between",marginBottom:5}}>
                <span style={{fontSize:12,color:C.sub}}>{e.name}</span>
                <span className="mono" style={{fontSize:12,color:ALLOC_COLORS[i%ALLOC_COLORS.length]}}>{pct(e.pct)}</span>
              </div>
              <div style={{height:5,borderRadius:99,background:C.s3,overflow:"hidden"}}>
                <div style={{width:`${e.pct*100}%`,height:"100%",background:ALLOC_COLORS[i%ALLOC_COLORS.length],borderRadius:99,transition:"width 0.7s ease"}}/>
              </div>
            </div>
          ))}
        </Card>

        <Card>
          <h3 style={{fontSize:15,fontWeight:700,marginBottom:18}}>Fund Status</h3>
          <div style={{display:"flex",flexDirection:"column",gap:8}}>
            {Object.entries(data.underperformance_flags||{}).map(([fid, flag]) => {
              const holding = data.holdings?.find(h=>h.fund_id===fid);
              return (
                <div key={fid} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"10px 12px",borderRadius:9,background:C.s3,border:`1px solid ${C.border}`}}>
                  <div>
                    <p style={{fontSize:12,color:C.text,fontWeight:600,marginBottom:2}}>{holding?.fund_name?.substring(0,28)||fid}</p>
                    <p style={{fontSize:10,color:C.muted}}>{holding?.category?.replace("Equity: "," ")||""}</p>
                  </div>
                  <div style={{display:"flex",flexDirection:"column",alignItems:"flex-end",gap:4}}>
                    <FlagPill flag={flag}/>
                    {holding?.current_value&&holding?.amount_invested&&(
                      <span className="mono" style={{fontSize:10,color:holding.current_value>=holding.amount_invested?C.green:C.red}}>
                        {pct((holding.current_value-holding.amount_invested)/holding.amount_invested,true)}
                      </span>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </Card>
      </div>

      {/* Recommendations */}
      <Card style={{marginBottom:18}}>
        <h3 style={{fontSize:15,fontWeight:700,marginBottom:16}}>💡 Smart Recommendations</h3>
        {(data.recommendations||[]).map((r,i) => (
          <div key={i} style={{display:"flex",gap:12,padding:"12px 0",borderBottom:i<data.recommendations.length-1?`1px solid ${C.border}`:"none",alignItems:"flex-start"}}>
            <div style={{width:22,height:22,borderRadius:"50%",background:C.accentDim,border:`1px solid ${C.accentGlow}`,display:"flex",alignItems:"center",justifyContent:"center",flexShrink:0,marginTop:1}}>
              <span className="mono" style={{fontSize:9,color:C.accent}}>{i+1}</span>
            </div>
            <p style={{fontSize:13,color:C.sub,lineHeight:1.65}}>{r}</p>
          </div>
        ))}
        {(!data.recommendations||data.recommendations.length===0) && (
          <p style={{fontSize:13,color:C.green}}>✓ Portfolio looks healthy across all metrics. Keep monitoring quarterly.</p>
        )}
      </Card>

      {/* Holdings table */}
      <Card style={{padding:0,overflow:"hidden"}}>
        <table style={{width:"100%",borderCollapse:"collapse"}}>
          <thead>
            <tr style={{borderBottom:`1px solid ${C.border}`}}>
              {["Fund","Category","Invested","Current Value","Gain / Loss","Status"].map(h => (
                <th key={h} style={{padding:"11px 14px",fontSize:10,color:C.muted,fontFamily:"JetBrains Mono,monospace",textAlign:"left",letterSpacing:"0.07em",textTransform:"uppercase",fontWeight:400}}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {(data.holdings||[]).map((h,i) => {
              const gain = (h.current_value||0)-(h.amount_invested||0);
              const gainPct = h.amount_invested>0 ? gain/h.amount_invested : 0;
              const flag = data.underperformance_flags?.[h.fund_id]||"OK";
              return (
                <tr key={h.fund_id} style={{borderBottom:i<data.holdings.length-1?`1px solid ${C.border}`:"none",transition:"background 0.12s"}}
                  onMouseEnter={e=>e.currentTarget.style.background=C.s3}
                  onMouseLeave={e=>e.currentTarget.style.background="transparent"}>
                  <td style={{padding:"11px 14px"}}>
                    <p style={{fontSize:13,color:C.text,fontWeight:600,letterSpacing:"-0.01em"}}>{h.fund_name?.substring(0,28)||h.fund_id}</p>
                    <p style={{fontSize:10,color:C.muted,marginTop:2}}>{h.purchase_date}</p>
                  </td>
                  <td style={{padding:"11px 14px"}}><Tag>{(h.category||"").replace("Equity: "," ")}</Tag></td>
                  <td style={{padding:"11px 14px"}} className="mono"><span style={{fontSize:12,color:C.sub}}>{inr(h.amount_invested)}</span></td>
                  <td style={{padding:"11px 14px"}} className="mono"><span style={{fontSize:12,color:C.text}}>{inr(h.current_value)}</span></td>
                  <td style={{padding:"11px 14px"}}>
                    <p className="mono" style={{fontSize:12,color:gain>=0?C.green:C.red}}>{inr(gain)}</p>
                    <p className="mono" style={{fontSize:10,color:gain>=0?C.green:C.red,marginTop:2}}>{pct(gainPct,true)}</p>
                  </td>
                  <td style={{padding:"11px 14px"}}><FlagPill flag={flag}/></td>
                </tr>
              );
            })}
            {/* Totals */}
            <tr style={{borderTop:`1px solid ${C.border2}`,background:C.s3}}>
              <td style={{padding:"12px 14px"}} colSpan={2}><span style={{fontSize:12,color:C.text,fontWeight:700,fontFamily:"JetBrains Mono,monospace"}}>PORTFOLIO TOTAL</span></td>
              <td style={{padding:"12px 14px"}} className="mono"><span style={{fontSize:12,color:C.text,fontWeight:700}}>{inr(data.total_invested)}</span></td>
              <td style={{padding:"12px 14px"}} className="mono"><span style={{fontSize:12,color:C.accent,fontWeight:700}}>{inr(data.total_current)}</span></td>
              <td style={{padding:"12px 14px"}}>
                <p className="mono" style={{fontSize:12,color:data.total_gain>=0?C.green:C.red,fontWeight:700}}>{inr(data.total_gain)}</p>
                <p className="mono" style={{fontSize:10,color:data.total_gain>=0?C.green:C.red,marginTop:2}}>{pct(data.total_gain_pct,true)}</p>
              </td>
              <td style={{padding:"12px 14px"}}><span className="mono" style={{fontSize:10,color:C.muted}}>ER: {(data.avg_er||0).toFixed(2)}% wtd.</span></td>
            </tr>
          </tbody>
        </table>
      </Card>

      <p style={{fontSize:10,color:C.muted,marginTop:14,fontStyle:"italic"}}>⚠ For educational purposes only. Not financial advice. Consult a SEBI-registered investment advisor.</p>
    </div>
  );
};

// ══════════════════════════════════════════════════════════════════════════════
// ROOT APP
// ══════════════════════════════════════════════════════════════════════════════
export default function App() {
  const [page, setPage] = useState("overview");
  const { data: pipelineStatus } = useAPI("/pipeline/status", []);

  useEffect(() => {
    const el = document.createElement("style");
    el.textContent = CSS;
    document.head.appendChild(el);
    return () => document.head.removeChild(el);
  }, []);

  const PAGE_MAP = {
    overview:   <OverviewPage setPage={setPage}/>,
    analysis:   <AnalysisPage/>,
    comparison: <ComparisonPage/>,
    radar:      <RadarPage/>,
    exit:       <ExitPage/>,
    portfolio:  <PortfolioPage/>,
  };

  return (
    <div style={{display:"flex",minHeight:"100vh",background:C.bg}}>
      <Sidebar page={page} setPage={setPage} pipelineStatus={pipelineStatus}/>
      <main style={{marginLeft:212,flex:1,padding:"34px 40px 80px",minHeight:"100vh",maxWidth:1150}}>
        {PAGE_MAP[page]}
      </main>
    </div>
  );
}
