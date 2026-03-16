/**
 * Fund Doctor — Premium React App (Part 1)
 * Tokens · Utils · Shared Components · Sidebar · Overview
 */
import { useState, useEffect, useRef, useMemo, useCallback } from "react";
import {
  AreaChart, Area, LineChart, Line, BarChart, Bar,
  ScatterChart, Scatter, RadarChart, Radar, PolarGrid, PolarAngleAxis,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  ReferenceLine, Cell, ComposedChart,
} from "recharts";

// ─── Config ──────────────────────────────────────────────────────────────────
const API = "http://localhost:8000/api";

// ─── Design tokens ────────────────────────────────────────────────────────────
const T = {
  bg:"#060810", surface:"#0a0d16", s2:"#0f1420", s3:"#141926", s4:"#19202e", s5:"#1e2638",
  b0:"rgba(255,255,255,0.04)", b1:"rgba(255,255,255,0.07)", b2:"rgba(255,255,255,0.12)", b3:"rgba(255,255,255,0.20)",
  cyan:"#00e5ff", cyanDim:"rgba(0,229,255,0.08)", cyanGlow:"rgba(0,229,255,0.18)",
  gold:"#ffc107", goldDim:"rgba(255,193,7,0.08)",
  emerald:"#00e676", emerDim:"rgba(0,230,118,0.08)",
  rose:"#ff5252", roseDim:"rgba(255,82,82,0.08)",
  amber:"#ff9800", violet:"#b388ff",
  tx:"#f0f4ff", t1:"#c8d4e8", t2:"#7a8ba8", t3:"#445568",
};

// ─── Ratio explainers ────────────────────────────────────────────────────────
const RATIO_INFO = {
  "Sharpe Ratio":    "Excess return per unit of total risk. Higher = better. >1.0 is good, >2.0 is excellent.",
  "Sortino Ratio":   "Like Sharpe but only penalises downside volatility. Better for equity funds. >1.5 is good.",
  "Calmar Ratio":    "Annual return divided by max drawdown. Shows recovery efficiency. >1.0 is healthy.",
  "Information Ratio":"How efficiently a manager generates active return above benchmark. >0.5 is skilled.",
  "Treynor Ratio":   "Excess return per unit of market (systematic) risk. Higher = better beta-adjusted performance.",
  "Omega Ratio":     "Ratio of all gains above target to all losses below it. >1.0 means gains exceed losses.",
  "VaR 95%":         "Value at Risk: the maximum loss on a single day in 95% of historical scenarios.",
  "CVaR 95%":        "Expected Shortfall: average daily loss in the worst 5% of historical days.",
  "Ulcer Index":     "Combines depth and duration of drawdowns. Lower is better. Unlike max-DD it catches sustained pain.",
  "Beta":            "Fund's sensitivity to benchmark moves. Beta=1 means it moves with the index. <0.8 means lower volatility.",
  "Alpha":           "Return generated above what Beta alone would predict. Positive alpha = genuine manager skill.",
  "Max Drawdown":    "Largest peak-to-trough fall in the fund's history. Shows worst-case loss scenario.",
  "Volatility":      "Annualised standard deviation of daily returns. Higher = more price swings.",
  "Up Capture":      "% of benchmark upside this fund captures in rising markets. >100% = outperforms on the way up.",
  "Down Capture":    "% of benchmark loss this fund suffers in falling markets. <100% = better downside protection.",
  "Win Rate":        "% of rolling monthly periods this fund beat its benchmark. >50% = consistent outperformer.",
  "Batting Average": "% of rolling 12-month periods the fund beat benchmark. >55% is considered good active management.",
};

const FLAG = {
  OK:       {color:T.emerald, bg:T.emerDim,  label:"Performing"},
  WARNING:  {color:T.gold,    bg:T.goldDim,  label:"Warning"},
  SERIOUS:  {color:T.amber,   bg:"rgba(255,152,0,.10)", label:"Serious"},
  CRITICAL: {color:T.rose,    bg:T.roseDim,  label:"Critical"},
  NO_DATA:  {color:T.t3,      bg:"rgba(68,85,104,.12)", label:"No Data"},
  INSUFFICIENT_DATA:{color:T.t3,bg:"rgba(68,85,104,.12)",label:"Limited"},
};
const GRADE = {
  "A+":{color:T.emerald,bg:T.emerDim},
  "A": {color:"#69f0ae",bg:"rgba(105,240,174,.08)"},
  "B+":{color:T.cyan,   bg:T.cyanDim},
  "B": {color:T.t1,     bg:"rgba(200,212,232,.06)"},
  "C": {color:T.gold,   bg:T.goldDim},
  "D": {color:T.rose,   bg:T.roseDim},
};

// ─── Global CSS ───────────────────────────────────────────────────────────────
const CSS = `
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&family=Inter:wght@300;400;500&display=swap');
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html,body{background:#060810;color:#f0f4ff;font-family:'Inter',sans-serif;line-height:1.6;-webkit-font-smoothing:antialiased;overflow-x:hidden;font-size:13px}
::-webkit-scrollbar{width:2px;height:2px}::-webkit-scrollbar-thumb{background:rgba(255,255,255,.08);border-radius:4px}
.syne{font-family:'Syne',sans-serif}.mono{font-family:'JetBrains Mono',monospace;letter-spacing:-.02em}
@keyframes fadeUp{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}
@keyframes spin{to{transform:rotate(360deg)}}
@keyframes shimmer{0%{background-position:-200% 0}100%{background-position:200% 0}}
.fade{animation:fadeUp .3s ease both}.fade2{animation:fadeUp .3s .06s ease both}
.fade3{animation:fadeUp .3s .12s ease both}.fade4{animation:fadeUp .3s .18s ease both}
.fade5{animation:fadeUp .3s .24s ease both}.fade6{animation:fadeUp .3s .30s ease both}
.skel{background:linear-gradient(90deg,#0f1420 25%,#141926 50%,#0f1420 75%);background-size:200% 100%;animation:shimmer 1.4s infinite;border-radius:6px}
input[type=range]{-webkit-appearance:none;appearance:none;width:100%;height:2px;background:rgba(255,255,255,.08);border-radius:99px;outline:none;cursor:pointer}
input[type=range]::-webkit-slider-thumb{-webkit-appearance:none;width:14px;height:14px;border-radius:50%;background:#00e5ff;border:2px solid #060810;cursor:pointer;box-shadow:0 0 10px rgba(0,229,255,.25)}
input[type=range]::-moz-range-thumb{width:14px;height:14px;border-radius:50%;background:#00e5ff;border:2px solid #060810}
.tooltip-box{position:absolute;bottom:calc(100% + 6px);left:50%;transform:translateX(-50%);background:#0f1420;border:1px solid rgba(255,255,255,.12);border-radius:8px;padding:8px 12px;width:220px;font-size:11px;color:#c8d4e8;line-height:1.5;z-index:200;pointer-events:none;white-space:normal;text-align:left}
.info-wrap{position:relative;display:inline-flex;align-items:center;gap:4px}
.info-wrap:hover .tooltip-box{display:block!important}
`;

// ─── Utils ───────────────────────────────────────────────────────────────────
const pct  = (v,s=false)=>v==null?"—":`${s&&v>0?"+":""}${(v*100).toFixed(1)}%`;
const n2   = (v)=>v==null?"—":v.toFixed(2);
const inr  = (v)=>v==null?"—":`₹${Math.round(v).toLocaleString("en-IN")}`;
const shrt = (s,n=28)=>s?(s.length>n?s.slice(0,n)+"…":s):"";

// ─── API hook ─────────────────────────────────────────────────────────────────
function useAPI(url, deps=[]) {
  const [data,setData]=[useState(null)[0],useState(null)[1]];
  const [loading,setLoading]=useState(!!url);
  const [error,setError]=useState(null);
  const [_data,_setData]=useState(null);
  const ref=useRef(null);
  useEffect(()=>{
    if(!url){setLoading(false);_setData(null);return;}
    ref.current?.abort();ref.current=new AbortController();
    setLoading(true);setError(null);_setData(null);
    fetch(`${API}${url}`,{signal:ref.current.signal})
      .then(r=>{if(!r.ok)throw new Error(`HTTP ${r.status}`);return r.json();})
      .then(d=>{_setData(d);setLoading(false);})
      .catch(e=>{if(e.name!=="AbortError"){setError(e.message);setLoading(false);}});
    return()=>ref.current?.abort();
  },deps);
  return{data:_data,loading,error};
}

// ─── Shared atoms ─────────────────────────────────────────────────────────────
const Spinner=({size=24})=>(
  <div style={{display:"flex",alignItems:"center",justifyContent:"center",padding:"32px 0"}}>
    <div style={{width:size,height:size,borderRadius:"50%",border:`1.5px solid ${T.b2}`,borderTopColor:T.cyan,animation:"spin .7s linear infinite"}}/>
  </div>
);
const Err=({msg})=>(
  <div style={{padding:"14px 16px",background:T.roseDim,border:`1px solid ${T.rose}33`,borderRadius:10,color:T.rose,fontSize:12,fontFamily:"JetBrains Mono,monospace",lineHeight:1.5}}>
    ⚠ {msg||"Backend not reachable — run: uvicorn api:app --port 8000"}
  </div>
);

const InfoIcon=({metric})=>{
  const tip=RATIO_INFO[metric];
  if(!tip) return null;
  return(
    <span className="info-wrap" style={{cursor:"help"}}>
      <span style={{width:14,height:14,borderRadius:"50%",background:T.b2,color:T.t3,fontSize:9,display:"inline-flex",alignItems:"center",justifyContent:"center",fontFamily:"JetBrains Mono,monospace",border:`1px solid ${T.b1}`,flexShrink:0,lineHeight:1}}>i</span>
      <span className="tooltip-box" style={{display:"none"}}>{tip}</span>
    </span>
  );
};

const FlagBadge=({flag})=>{
  const f=FLAG[flag]||FLAG.NO_DATA;
  return(
    <span style={{display:"inline-flex",alignItems:"center",gap:5,padding:"3px 9px",borderRadius:99,background:f.bg,color:f.color,fontSize:10,fontFamily:"JetBrains Mono,monospace",fontWeight:500,letterSpacing:".05em",whiteSpace:"nowrap"}}>
      <span style={{width:5,height:5,borderRadius:"50%",background:f.color,flexShrink:0,animation:"pulse 2s infinite"}}/>
      {f.label}
    </span>
  );
};
const GradeBadge=({grade,large=false})=>{
  const g=GRADE[grade]||GRADE["B"];
  return(
    <span style={{padding:large?"5px 14px":"3px 10px",background:g.bg,color:g.color,border:`1px solid ${g.color}33`,borderRadius:8,fontFamily:"JetBrains Mono,monospace",fontWeight:600,fontSize:large?26:13,display:"inline-block"}}>
      {grade}
    </span>
  );
};
const DataBadge=({badge,completeness,days})=>{
  const ok=badge==="reliable";
  return(
    <div style={{display:"inline-flex",alignItems:"center",gap:6,padding:"3px 10px",borderRadius:99,background:ok?T.emerDim:T.goldDim,border:`1px solid ${ok?T.emerald:T.gold}33`}}>
      <span style={{fontSize:9,color:ok?T.emerald:T.gold,fontFamily:"JetBrains Mono,monospace",letterSpacing:".06em"}}>{ok?"✓ RELIABLE":"⚠ LIMITED"}</span>
      {completeness!=null&&<span style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace"}}>{completeness?.toFixed(0)}% · {days}d ago</span>}
    </div>
  );
};
const Tag=({children,color})=>(
  <span style={{display:"inline-block",padding:"2px 8px",borderRadius:99,background:T.s3,color:color||T.t2,fontSize:10,fontFamily:"JetBrains Mono,monospace",marginRight:4,marginBottom:3,border:`1px solid ${T.b1}`}}>
    {children}
  </span>
);
const Kpi=({label,value,sub,color=T.cyan,mono=true,cls="",metric})=>(
  <div className={cls} style={{background:T.s2,border:`1px solid ${T.b1}`,borderRadius:12,padding:"13px 15px",position:"relative",overflow:"hidden"}}>
    <div style={{position:"absolute",top:0,left:0,right:0,height:1,background:`linear-gradient(90deg,transparent,${color}44,transparent)`}}/>
    <div style={{display:"flex",alignItems:"center",gap:4,marginBottom:5}}>
      <p style={{fontSize:10,color:T.t3,textTransform:"uppercase",letterSpacing:".08em",fontFamily:"JetBrains Mono,monospace"}}>{label}</p>
      {metric&&<InfoIcon metric={metric}/>}
    </div>
    <p style={{fontSize:21,fontWeight:600,color,fontFamily:mono?"JetBrains Mono,monospace":"Syne,sans-serif",lineHeight:1}}>{value}</p>
    {sub&&<p style={{fontSize:10,color:T.t3,marginTop:4,fontFamily:"JetBrains Mono,monospace"}}>{sub}</p>}
  </div>
);
const Card=({children,style={}})=>(
  <div style={{background:T.s2,border:`1px solid ${T.b1}`,borderRadius:14,...style}}>{children}</div>
);
const CardHead=({title,sub,right,pad="16px 18px"})=>(
  <div style={{padding:pad,display:"flex",justifyContent:"space-between",alignItems:"flex-start",gap:12,borderBottom:`1px solid ${T.b0}`}}>
    <div>
      <h3 style={{fontFamily:"Syne,sans-serif",fontSize:14,fontWeight:700,letterSpacing:"-.02em",color:T.tx,marginBottom:sub?3:0}}>{title}</h3>
      {sub&&<p style={{fontSize:11,color:T.t2,lineHeight:1.4}}>{sub}</p>}
    </div>
    {right&&<div style={{flexShrink:0}}>{right}</div>}
  </div>
);
const Divider=()=><div style={{height:1,background:T.b0,margin:"16px 0"}}/>;
const PeriodPill=({active,onClick,children})=>(
  <button onClick={onClick} style={{padding:"3px 10px",borderRadius:7,border:`1px solid ${active?T.cyan:T.b1}`,background:active?T.cyanDim:"transparent",color:active?T.cyan:T.t2,fontSize:10,cursor:"pointer",fontFamily:"JetBrains Mono,monospace",transition:"all .12s"}}>{children}</button>
);
const ScoreRing=({score,size=100,stroke=7,color=T.cyan})=>{
  const r=(size-stroke)/2,c=size/2,circ=2*Math.PI*r,dash=(score/100)*circ;
  return(
    <svg width={size} height={size} style={{transform:"rotate(-90deg)"}}>
      <circle cx={c} cy={c} r={r} fill="none" stroke={T.s3} strokeWidth={stroke}/>
      <circle cx={c} cy={c} r={r} fill="none" stroke={color} strokeWidth={stroke}
        strokeDasharray={`${dash} ${circ}`} strokeLinecap="round" style={{transition:"stroke-dasharray .8s ease"}}/>
    </svg>
  );
};
const ProgressBar=({pct:p,color=T.cyan,height=4})=>(
  <div style={{height,background:T.s3,borderRadius:99,overflow:"hidden"}}>
    <div style={{height:"100%",width:`${Math.max(0,Math.min(100,p||0))}%`,background:color,borderRadius:99,transition:"width .8s ease"}}/>
  </div>
);
const MetricRow=({label,value,color=T.t1,metric,right})=>(
  <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"7px 0",borderBottom:`1px solid ${T.b0}`}}>
    <div style={{display:"flex",alignItems:"center",gap:5}}>
      <span style={{fontSize:12,color:T.t2}}>{label}</span>
      {metric&&<InfoIcon metric={metric}/>}
    </div>
    <div style={{display:"flex",alignItems:"center",gap:8}}>
      {right&&<span style={{fontSize:11,color:T.t3}}>{right}</span>}
      <span className="mono" style={{fontSize:12,color,fontWeight:500}}>{value}</span>
    </div>
  </div>
);
const CT=({active,payload,label,fmt})=>{
  if(!active||!payload?.length)return null;
  return(
    <div style={{background:T.surface,border:`1px solid ${T.b2}`,borderRadius:9,padding:"9px 13px",backdropFilter:"blur(16px)"}}>
      <p style={{fontSize:10,color:T.t3,marginBottom:5,fontFamily:"JetBrains Mono,monospace"}}>{label}</p>
      {payload.map((p,i)=><p key={i} style={{fontSize:11,fontFamily:"JetBrains Mono,monospace",color:p.color||T.cyan,marginBottom:2}}>{p.name}: {fmt?fmt(p.value):p.value}</p>)}
    </div>
  );
};
const TS={fontSize:9,fill:T.t3,fontFamily:"JetBrains Mono,monospace"};

// Fund selector component (search + browse all ~1500 funds)
const FundSelector=({value,onChange,label="Select Fund",multi=false,selected=[]})=>{
  const [search,setSearch]=useState("");
  const [open,setOpen]=useState(false);
  const {data:funds}=useAPI("/funds",[]);
  const ref=useRef();
  useEffect(()=>{
    const h=(e)=>{if(ref.current&&!ref.current.contains(e.target))setOpen(false);};
    document.addEventListener("mousedown",h);return()=>document.removeEventListener("mousedown",h);
  },[]);
  const filtered=(funds?.funds||[])
    .filter(f=>!search||(f.fund_name.toLowerCase().includes(search.toLowerCase())||f.amc?.toLowerCase().includes(search.toLowerCase())))
    .slice(0,80);
  const sel=multi?selected:value;
  const selFund=!multi&&(funds?.funds||[]).find(f=>f.fund_id===value);
  return(
    <div ref={ref} style={{position:"relative",width:"100%"}}>
      <div onClick={()=>setOpen(o=>!o)} style={{background:T.s2,border:`1px solid ${open?T.cyan:T.b1}`,borderRadius:10,padding:"9px 13px",cursor:"pointer",display:"flex",justifyContent:"space-between",alignItems:"center",gap:8,transition:"all .12s"}}>
        <div style={{flex:1,minWidth:0}}>
          {!multi&&selFund?(
            <div>
              <p style={{fontSize:12,color:T.tx,fontWeight:500,marginBottom:1}}>{shrt(selFund.fund_name,36)}</p>
              <p style={{fontSize:10,color:T.t3}}>{selFund.amc} · <FlagBadge flag={selFund.flag}/></p>
            </div>
          ):(
            <span style={{fontSize:12,color:multi&&selected.length?T.tx:T.t3}}>
              {multi?`${selected.length} fund${selected.length!==1?"s":""} selected`:(label)}
            </span>
          )}
        </div>
        <span style={{color:T.t3,fontSize:10,flexShrink:0}}>{open?"▲":"▼"}</span>
      </div>
      {open&&(
        <div style={{position:"absolute",top:"calc(100% + 4px)",left:0,right:0,background:T.s2,border:`1px solid ${T.b2}`,borderRadius:10,zIndex:500,maxHeight:320,display:"flex",flexDirection:"column",boxShadow:`0 16px 48px rgba(0,0,0,.6)`}}>
          <div style={{padding:"8px",borderBottom:`1px solid ${T.b1}`,flexShrink:0}}>
            <input placeholder="Search fund name or AMC…" value={search} onChange={e=>setSearch(e.target.value)} autoFocus
              style={{width:"100%",background:T.s3,border:`1px solid ${T.b1}`,borderRadius:7,padding:"7px 11px",color:T.tx,fontSize:12,outline:"none",fontFamily:"Inter,sans-serif"}}/>
          </div>
          <div style={{overflowY:"auto",flex:1}}>
            {filtered.length===0?<p style={{padding:"16px",textAlign:"center",color:T.t3,fontSize:12}}>No funds found</p>:
            filtered.map(f=>{
              const isSelected=multi?selected.includes(f.fund_id):f.fund_id===value;
              return(
                <div key={f.fund_id} onClick={()=>{
                  if(multi){
                    const next=isSelected?selected.filter(x=>x!==f.fund_id):[...selected,f.fund_id];
                    onChange(next);
                  }else{onChange(f.fund_id);setOpen(false);}
                }} style={{padding:"9px 13px",cursor:"pointer",background:isSelected?T.cyanDim:"transparent",borderBottom:`1px solid ${T.b0}`,transition:"background .1s"}}
                onMouseEnter={e=>!isSelected&&(e.currentTarget.style.background=T.s3)}
                onMouseLeave={e=>!isSelected&&(e.currentTarget.style.background="transparent")}>
                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",gap:8}}>
                    <div style={{flex:1,minWidth:0}}>
                      <p style={{fontSize:12,color:isSelected?T.cyan:T.tx,fontWeight:isSelected?600:400,marginBottom:2}}>{f.fund_name}</p>
                      <p style={{fontSize:10,color:T.t3}}>{f.amc} · {(f.category||"").replace(/^(Equity|Index|Hybrid|Debt|FOF): /,"")}</p>
                    </div>
                    {isSelected&&<span style={{color:T.cyan,fontSize:14,flexShrink:0}}>✓</span>}
                  </div>
                </div>
              );
            })}
          </div>
          {multi&&selected.length>0&&(
            <div style={{padding:"8px 12px",borderTop:`1px solid ${T.b1}`,display:"flex",justifyContent:"space-between",alignItems:"center",flexShrink:0}}>
              <span style={{fontSize:11,color:T.t2}}>{selected.length} selected</span>
              <button onClick={()=>{onChange([]);}} style={{fontSize:10,color:T.rose,background:"none",border:"none",cursor:"pointer",fontFamily:"JetBrains Mono,monospace"}}>Clear all</button>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

// ─── Sidebar ─────────────────────────────────────────────────────────────────
const PAGES=[
  {id:"overview",  icon:"⬡",label:"Overview"},
  {id:"scorecard", icon:"◈",label:"Fund Scorecard"},
  {id:"category",  icon:"◉",label:"Category Picks"},
  {id:"exit",      icon:"⊗",label:"Exit Engine"},
  {id:"portfolio", icon:"♦",label:"Portfolio Doctor"},
  {id:"overlap",   icon:"⊙",label:"Overlap Detector"},
  {id:"analytics", icon:"⟋",label:"Analytics Pack"},
];

const Sidebar=({page,setPage,pipe})=>{
  const[refreshing,setRefreshing]=useState(false);
  const doRefresh=async(mode)=>{setRefreshing(true);await fetch(`${API}/pipeline/trigger?mode=${mode}`,{method:"POST"});setTimeout(()=>setRefreshing(false),2000);};
  return(
    <aside style={{width:200,minHeight:"100vh",background:T.surface,borderRight:`1px solid ${T.b1}`,display:"flex",flexDirection:"column",position:"fixed",top:0,left:0,bottom:0,zIndex:100}}>
      <div style={{padding:"20px 16px 16px",borderBottom:`1px solid ${T.b1}`}}>
        <div style={{display:"flex",alignItems:"center",gap:9}}>
          <div style={{width:32,height:32,borderRadius:8,background:`linear-gradient(135deg,${T.cyan},${T.violet})`,display:"flex",alignItems:"center",justifyContent:"center",fontSize:15,flexShrink:0}}>💊</div>
          <div>
            <p style={{fontFamily:"Syne,sans-serif",fontWeight:800,fontSize:14,letterSpacing:"-.03em",color:T.tx}}>Fund Doctor</p>
            <p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",letterSpacing:".06em",marginTop:1}}>INDIA MF · ANALYTICS</p>
          </div>
        </div>
      </div>
      <nav style={{padding:"9px 8px",flex:1,overflowY:"auto"}}>
        {PAGES.map(p=>(
          <button key={p.id} onClick={()=>setPage(p.id)} style={{width:"100%",display:"flex",alignItems:"center",gap:8,padding:"7px 11px",borderRadius:8,border:"none",background:page===p.id?T.cyanDim:"transparent",color:page===p.id?T.cyan:T.t2,cursor:"pointer",marginBottom:1,transition:"all .12s",fontSize:12,fontWeight:page===p.id?600:400,textAlign:"left",fontFamily:"Inter,sans-serif"}}>
            <span style={{fontSize:11,opacity:page===p.id?1:.4,flexShrink:0}}>{p.icon}</span>
            {p.label}
            {page===p.id&&<span style={{marginLeft:"auto",width:2,height:14,borderRadius:99,background:T.cyan,flexShrink:0}}/>}
          </button>
        ))}
      </nav>
      <div style={{padding:"11px 13px",borderTop:`1px solid ${T.b1}`}}>
        <p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",textTransform:"uppercase",letterSpacing:".06em",marginBottom:7}}>Data Pipeline</p>
        <div style={{display:"flex",gap:5,marginBottom:7}}>
          {["demo","daily"].map(mode=>(
            <button key={mode} onClick={()=>doRefresh(mode)} style={{flex:1,padding:"5px 0",borderRadius:6,border:`1px solid ${T.b1}`,background:T.s3,color:T.t2,fontSize:9,cursor:"pointer",fontFamily:"JetBrains Mono,monospace",transition:"all .12s"}}
              onMouseEnter={e=>{e.currentTarget.style.borderColor=T.cyan;e.currentTarget.style.color=T.cyan}}
              onMouseLeave={e=>{e.currentTarget.style.borderColor=T.b1;e.currentTarget.style.color=T.t2}}>
              {refreshing?"…":mode==="demo"?"Demo":"AMFI"}
            </button>
          ))}
        </div>
        {pipe&&<p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",lineHeight:1.6}}>{pipe.fund_count} funds · {pipe.last_nav||"—"}</p>}
      </div>
    </aside>
  );
};

// ══════════════════════════════════════════════════════════════════════════════
// PAGE: OVERVIEW
// ══════════════════════════════════════════════════════════════════════════════
const OverviewPage=({setPage})=>{
  const{data,loading,error}=useAPI("/overview",[]);
  if(loading)return<Spinner/>;if(error)return<Err msg={error}/>;if(!data)return null;
  const{stats,flag_counts,funds}=data;
  return(
    <div>
      <div style={{marginBottom:38}}>
        <div className="fade" style={{display:"inline-flex",alignItems:"center",gap:6,background:T.cyanDim,border:`1px solid ${T.cyanGlow}`,borderRadius:99,padding:"4px 12px",marginBottom:14}}>
          <span style={{width:5,height:5,borderRadius:"50%",background:T.cyan,animation:"pulse 2s infinite"}}/>
          <span style={{fontSize:9,color:T.cyan,fontFamily:"JetBrains Mono,monospace",letterSpacing:".08em"}}>LIVE · AMFI INDIA</span>
        </div>
        <h1 className="syne fade2" style={{fontSize:46,fontWeight:800,letterSpacing:"-.04em",lineHeight:1.02,marginBottom:14}}>India MF<br/><span style={{background:`linear-gradient(90deg,${T.cyan},${T.violet})`,WebkitBackgroundClip:"text",WebkitTextFillColor:"transparent"}}>Intelligence Engine</span></h1>
        <p className="fade3" style={{fontSize:14,color:T.t1,maxWidth:440,lineHeight:1.7}}>Professional-grade analytics for every Indian mutual fund. Know what you own. Know when to exit.</p>
      </div>
      <div className="fade4" style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:10,marginBottom:30}}>
        <Kpi label="Funds Tracked" value={stats.total_funds} color={T.cyan}/>
        <Kpi label="NAV Data Points" value={`${((stats.total_nav_rows||0)/1000).toFixed(0)}K`} color={T.violet}/>
        <Kpi label="Benchmarks" value={stats.total_benchmarks||4} color={T.emerald}/>
        <Kpi label="Last Refresh" value={stats.last_nav||"—"} mono={false} color={T.t2}/>
      </div>
      <div style={{marginBottom:12}}><h2 className="syne" style={{fontSize:17,fontWeight:700,letterSpacing:"-.03em",marginBottom:4}}>Fund Health Scanner</h2><p style={{fontSize:12,color:T.t2,marginBottom:14}}>Automated underperformance detection · click to explore</p></div>
      <div className="fade5" style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:10,marginBottom:34}}>
        {["OK","WARNING","SERIOUS","CRITICAL"].map(flag=>{
          const f=FLAG[flag];
          return(
            <div key={flag} onClick={()=>setPage("scorecard")} style={{background:T.s2,border:`1px solid ${T.b1}`,borderRadius:12,padding:"16px",cursor:"pointer",transition:"all .18s",position:"relative",overflow:"hidden"}}
              onMouseEnter={e=>{e.currentTarget.style.borderColor=f.color+"44";e.currentTarget.style.background=T.s3}}
              onMouseLeave={e=>{e.currentTarget.style.borderColor=T.b1;e.currentTarget.style.background=T.s2}}>
              <div style={{position:"absolute",top:0,right:0,width:50,height:50,background:`radial-gradient(circle,${f.color}08,transparent 70%)`,pointerEvents:"none"}}/>
              <p className="mono" style={{fontSize:36,fontWeight:600,color:f.color,lineHeight:1,marginBottom:8}}>{flag_counts[flag]||0}</p>
              <FlagBadge flag={flag}/>
            </div>
          );
        })}
      </div>
      <div style={{marginBottom:12}}><h2 className="syne" style={{fontSize:17,fontWeight:700,letterSpacing:"-.03em",marginBottom:4}}>Fund Universe</h2><p style={{fontSize:12,color:T.t2,marginBottom:14}}>All {funds?.length||0} funds — click header to sort</p></div>
      <Card className="fade6" style={{overflow:"hidden"}}>
        <table style={{width:"100%",borderCollapse:"collapse"}}>
          <thead><tr style={{borderBottom:`1px solid ${T.b1}`}}>
            {["Fund","Category","1Y","5Y","Sharpe","ER%","Status"].map(h=>(
              <th key={h} style={{padding:"9px 13px",fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",textAlign:"left",letterSpacing:".07em",textTransform:"uppercase",fontWeight:400}}>{h}</th>
            ))}
          </tr></thead>
          <tbody>
            {(funds||[]).map((f,i)=>(
              <tr key={f.fund_id} style={{borderBottom:i<funds.length-1?`1px solid ${T.b0}`:"none",cursor:"pointer",transition:"background .1s"}}
                onMouseEnter={e=>e.currentTarget.style.background=T.s3}
                onMouseLeave={e=>e.currentTarget.style.background="transparent"}>
                <td style={{padding:"9px 13px"}}><p style={{fontSize:12,color:T.tx,fontWeight:500,marginBottom:1}}>{shrt(f.fund_name,30)}</p>{f.amc&&<p style={{fontSize:10,color:T.t3}}>{f.amc}</p>}</td>
                <td style={{padding:"9px 13px"}}><Tag>{(f.category||"").replace(/^(Equity|Index|Hybrid|Debt|FOF): /,"")}</Tag></td>
                <td style={{padding:"9px 13px"}} className="mono"><span style={{color:f.return_1y>0.18?T.emerald:f.return_1y>0.08?T.t1:T.rose,fontSize:11}}>{pct(f.return_1y,true)}</span></td>
                <td style={{padding:"9px 13px"}} className="mono"><span style={{color:f.return_5y>0.15?T.emerald:f.return_5y>0.08?T.t1:T.rose,fontSize:11}}>{pct(f.return_5y,true)}</span></td>
                <td style={{padding:"9px 13px"}} className="mono"><span style={{color:f.sharpe_ratio>1.2?T.emerald:f.sharpe_ratio>0.7?T.t1:T.amber,fontSize:11}}>{n2(f.sharpe_ratio)}</span></td>
                <td style={{padding:"9px 13px"}} className="mono"><span style={{color:f.expense_ratio<0.5?T.emerald:f.expense_ratio<1.2?T.t1:T.amber,fontSize:11}}>{f.expense_ratio!=null?`${f.expense_ratio}%`:"—"}</span></td>
                <td style={{padding:"9px 13px"}}><FlagBadge flag={f.flag}/></td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>
    </div>
  );
};

export { T, FLAG, GRADE, RATIO_INFO, CSS, pct, n2, inr, shrt, useAPI, Spinner, Err, InfoIcon, FlagBadge, GradeBadge, DataBadge, Tag, Kpi, Card, CardHead, Divider, PeriodPill, ScoreRing, ProgressBar, MetricRow, CT, TS, FundSelector, Sidebar, OverviewPage };
