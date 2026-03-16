/**
 * Fund Doctor — Pages: Overlap Detector · Analytics Pack
 */
import { useState, useEffect } from "react";
import {
  AreaChart, Area, LineChart, Line, BarChart, Bar, ComposedChart,
  ScatterChart, Scatter, RadarChart, Radar, PolarGrid, PolarAngleAxis,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine, Cell,
} from "recharts";
import { T, pct, n2, inr, shrt, useAPI, Spinner, Err, InfoIcon, FlagBadge, GradeBadge, Tag, Kpi, Card, CardHead, Divider, ProgressBar, PeriodPill, MetricRow, CT, TS, FundSelector } from "./shared.jsx";

// ══════════════════════════════════════════════════════════════════════════════
// OVERLAP DETECTOR
// ══════════════════════════════════════════════════════════════════════════════
export const OverlapPage=()=>{
  const[selected,setSelected]=useState([]);
  const[go,setGo]=useState(false);
  const overlapUrl=go&&selected.length>=2?`/overlap?fund_ids=${selected.join(",")}`:null;
  const{data,loading,error}=useAPI(overlapUrl,[selected.join(","),go]);

  const OVERLAP_COLOR=(pct)=>pct>65?T.rose:pct>35?T.amber:T.emerald;
  const OVERLAP_LABEL=(pct)=>pct>65?"High overlap — redundant":pct>35?"Moderate overlap":"Low overlap — good diversification";

  return(
    <div>
      <div style={{marginBottom:20}}>
        <h2 className="syne" style={{fontSize:22,fontWeight:700,letterSpacing:"-.03em",marginBottom:5}}>Portfolio Overlap Detector</h2>
        <p style={{fontSize:12,color:T.t2}}>Select 2–6 funds to see how much they overlap — based on stock holdings or return correlation</p>
      </div>

      <div className="fade" style={{marginBottom:16}}>
        <Card>
          <CardHead title="Select Funds to Compare" sub="Search and pick 2–6 mutual funds from 1,500+ available"/>
          <div style={{padding:"14px"}}>
            <FundSelector multi selected={selected} onChange={(ids)=>{setSelected(ids);setGo(false);}} label="Search any mutual fund…"/>
            {selected.length>0&&(
              <div style={{marginTop:12,display:"flex",flexWrap:"wrap",gap:6}}>
                {selected.map(id=>{
                  return(
                    <div key={id} style={{display:"flex",alignItems:"center",gap:6,background:T.cyanDim,border:`1px solid ${T.cyan}33`,borderRadius:8,padding:"4px 10px"}}>
                      <span style={{fontSize:11,color:T.cyan,fontFamily:"JetBrains Mono,monospace"}}>{id}</span>
                      <button onClick={()=>{setSelected(s=>s.filter(x=>x!==id));setGo(false);}} style={{background:"none",border:"none",color:T.t3,cursor:"pointer",fontSize:12,lineHeight:1,padding:0}}>×</button>
                    </div>
                  );
                })}
              </div>
            )}
            <div style={{marginTop:14,display:"flex",gap:10,alignItems:"center"}}>
              <button onClick={()=>selected.length>=2&&setGo(true)} disabled={selected.length<2||loading}
                style={{padding:"10px 24px",borderRadius:10,border:`1px solid ${selected.length>=2?T.cyan:T.b1}`,background:selected.length>=2?T.cyanDim:"transparent",color:selected.length>=2?T.cyan:T.t3,fontSize:12,fontWeight:700,cursor:selected.length>=2?"pointer":"not-allowed",fontFamily:"JetBrains Mono,monospace",letterSpacing:".06em",transition:"all .18s",opacity:loading?.6:1}}>
                {loading?"ANALYSING …":"ANALYSE OVERLAP →"}
              </button>
              {selected.length<2&&<span style={{fontSize:11,color:T.t3}}>Select at least 2 funds</span>}
            </div>
          </div>
        </Card>
      </div>

      {error&&<Err msg={error}/>}

      {data&&go&&!loading&&(
        <div className="fade">
          {/* Summary */}
          <div style={{background:T.s2,border:`1px solid ${T.b1}`,borderRadius:14,padding:"16px 20px",marginBottom:14,display:"flex",gap:20,flexWrap:"wrap",alignItems:"center"}}>
            <div>
              <p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginBottom:3}}>DIVERSIFICATION SCORE</p>
              <p className="mono" style={{fontSize:28,fontWeight:700,color:data.diversification_score>70?T.emerald:data.diversification_score>45?T.gold:T.rose,lineHeight:1}}>{data.diversification_score}</p>
              <p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginTop:3}}>/100 · higher = better diversified</p>
            </div>
            <div>
              <p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginBottom:3}}>AVG OVERLAP</p>
              <p className="mono" style={{fontSize:22,fontWeight:600,color:OVERLAP_COLOR(data.avg_overlap_pct),lineHeight:1}}>{data.avg_overlap_pct?.toFixed(0)}%</p>
            </div>
            <div style={{flex:1,minWidth:200}}>
              <p style={{fontSize:13,color:T.t1,lineHeight:1.6}}>{data.interpretation}</p>
              <p style={{fontSize:10,color:T.t3,marginTop:4,fontFamily:"JetBrains Mono,monospace"}}>Method: {data.method==="holdings_jaccard"?"Stock holdings (Jaccard similarity)":"Return correlation (holdings not available)"}</p>
            </div>
            {data.high_overlap_warning&&(
              <div style={{background:T.roseDim,border:`1px solid ${T.rose}44`,borderRadius:10,padding:"10px 14px"}}>
                <p style={{fontSize:12,color:T.rose,fontWeight:600}}>⚠ High Overlap Warning</p>
                <p style={{fontSize:11,color:T.t2,marginTop:3}}>Multiple funds own the same stocks.<br/>Consider consolidating.</p>
              </div>
            )}
          </div>

          {/* Pairwise matrix */}
          <div style={{marginBottom:14}}>
            <h3 className="syne" style={{fontSize:15,fontWeight:700,letterSpacing:"-.02em",marginBottom:12}}>Pairwise Overlap Matrix</h3>
            <div style={{overflowX:"auto"}}>
              <table style={{borderCollapse:"collapse",minWidth:400}}>
                <thead>
                  <tr>
                    <th style={{width:40}}/>
                    {(data.fund_ids||[]).map(id=>(
                      <th key={id} style={{padding:"6px 10px",fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",textAlign:"center",maxWidth:120}}>
                        <div style={{transform:"rotate(-20deg)",whiteSpace:"nowrap",overflow:"hidden",textOverflow:"ellipsis",maxWidth:100}}>{shrt(data.fund_names?.[id]||id,18)}</div>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {(data.fund_ids||[]).map((rowId)=>(
                    <tr key={rowId}>
                      <td style={{padding:"6px 10px",fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",whiteSpace:"nowrap",maxWidth:120,overflow:"hidden",textOverflow:"ellipsis"}}>{shrt(data.fund_names?.[rowId]||rowId,18)}</td>
                      {(data.fund_ids||[]).map(colId=>{
                        const cell=data.matrix?.[rowId]?.[colId];
                        const p=cell?.overlap_pct||0;
                        const isSelf=rowId===colId;
                        const bg=isSelf?T.s4:p>65?T.roseDim:p>35?"rgba(255,152,0,.08)":T.emerDim;
                        const col=isSelf?T.t3:OVERLAP_COLOR(p);
                        return(
                          <td key={colId} style={{padding:"8px 12px",textAlign:"center",background:bg,border:`1px solid ${T.b0}`}}>
                            <span className="mono" style={{fontSize:11,color:col,fontWeight:isSelf?400:600}}>{isSelf?"—":`${p}%`}</span>
                          </td>
                        );
                      })}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Pair details */}
          <div style={{display:"flex",flexDirection:"column",gap:10}}>
            {(data.pairs||[]).map((pair,i)=>{
              const col=OVERLAP_COLOR(pair.overlap_pct||0);
              return(
                <Card key={`${pair.fund_a}-${pair.fund_b}`}>
                  <div style={{padding:"13px 17px"}}>
                    <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",gap:12,marginBottom:12}}>
                      <div style={{flex:1}}>
                        <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:6,flexWrap:"wrap"}}>
                          <span style={{fontSize:11,color:T.t2,fontFamily:"JetBrains Mono,monospace"}}>{shrt(pair.fund_a_name,28)}</span>
                          <span style={{color:T.t3}}>vs</span>
                          <span style={{fontSize:11,color:T.t2,fontFamily:"JetBrains Mono,monospace"}}>{shrt(pair.fund_b_name,28)}</span>
                        </div>
                        <div style={{height:6,background:T.s3,borderRadius:99,overflow:"hidden",width:"100%"}}>
                          <div style={{height:"100%",width:`${pair.overlap_pct||0}%`,background:col,borderRadius:99,transition:"width .8s ease"}}/>
                        </div>
                      </div>
                      <div style={{textAlign:"right",flexShrink:0}}>
                        <p className="mono" style={{fontSize:22,color:col,fontWeight:700,lineHeight:1}}>{pair.overlap_pct?.toFixed(0)}%</p>
                        <p style={{fontSize:9,color:T.t3,marginTop:3,fontFamily:"JetBrains Mono,monospace"}}>{OVERLAP_LABEL(pair.overlap_pct||0)}</p>
                        {pair.common_count>0&&<p style={{fontSize:9,color:T.t3,marginTop:1,fontFamily:"JetBrains Mono,monospace"}}>{pair.common_count} common stocks</p>}
                      </div>
                    </div>
                    {pair.common_stocks?.length>0&&(
                      <div>
                        <p style={{fontSize:10,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginBottom:8,textTransform:"uppercase",letterSpacing:".06em"}}>Common Holdings</p>
                        <div style={{display:"flex",flexWrap:"wrap",gap:6}}>
                          {pair.common_stocks.slice(0,12).map(s=>(
                            <div key={s.stock} style={{background:T.s3,border:`1px solid ${T.b1}`,borderRadius:7,padding:"4px 10px"}}>
                              <p style={{fontSize:11,color:T.tx,fontWeight:500,textTransform:"capitalize"}}>{s.stock}</p>
                              {s.sector&&<p style={{fontSize:9,color:T.t3}}>{s.sector}</p>}
                              {s.weight_a&&<p style={{fontSize:9,color:T.t2,fontFamily:"JetBrains Mono,monospace"}}>A:{s.weight_a?.toFixed(1)}% B:{s.weight_b?.toFixed(1)||"—"}%</p>}
                            </div>
                          ))}
                          {pair.common_stocks.length>12&&<span style={{fontSize:10,color:T.t3,alignSelf:"center"}}>+{pair.common_stocks.length-12} more</span>}
                        </div>
                      </div>
                    )}
                    {pair.method==="return_correlation"&&<p style={{fontSize:10,color:T.t3,marginTop:8,fontStyle:"italic"}}>⚠ Holdings data not available — using return correlation ({pair.correlation?.toFixed(3)||"—"}) as proxy</p>}
                  </div>
                </Card>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
};

// ══════════════════════════════════════════════════════════════════════════════
// ANALYTICS PACK — free-form, all metrics, ratio explainers
// ══════════════════════════════════════════════════════════════════════════════
export const AnalyticsPage=()=>{
  const[selId,setSelId]=useState(null);
  const[period,setPeriod]=useState("5Y");
  const[rollWin,setRollWin]=useState(3);
  const[tab,setTab]=useState("charts");

  const{data:navData,loading:nl}=useAPI(selId?`/funds/${selId}/nav?period=${period}&thin=4`:null,[selId,period]);
  const{data:an}=useAPI(selId?`/funds/${selId}/analytics`:null,[selId]);
  const{data:pmFull,loading:pmLoading}=useAPI(selId?`/funds/${selId}/pm-analytics`:null,[selId]);
  const{data:rollData}=useAPI(selId?`/funds/${selId}/rolling?window_years=${rollWin}&thin=4`:null,[selId,rollWin]);
  const{data:calData}=useAPI(selId?`/funds/${selId}/calendar-returns`:null,[selId]);
  const{data:capData}=useAPI(selId?`/funds/${selId}/capture-ratio`:null,[selId]);
  const{data:bwData}=useAPI(selId?`/funds/${selId}/best-worst`:null,[selId]);
  const{data:stData}=useAPI(selId?`/funds/${selId}/stress-test`:null,[selId]);
  const{data:ovData}=useAPI("/overview",[]);

  const chartData=(navData?.nav_norm||[]).map((d,i)=>({date:d.date?.slice(2,7),fund:d.nav,bench:navData?.benchmark_norm?.[i]?.nav}));
  const ddData=(navData?.nav||[]).reduce((acc,d,i,arr)=>{
    let peak=arr[0]?.nav||1;for(let j=0;j<=i;j++)if(arr[j].nav>peak)peak=arr[j].nav;
    acc.push({date:d.date?.slice(2,7),dd:+((d.nav-peak)/peak*100).toFixed(2)});return acc;
  },[]).filter((_,i)=>i%4===0);
  const spreadData=(rollData?.fund_rolling||[]).map((d,i)=>({date:d.date?.slice(2,7),spread:rollData?.bench_rolling?.[i]?.val!=null?+(d.val-rollData.bench_rolling[i].val).toFixed(2):null,fund:d.val,bench:rollData?.bench_rolling?.[i]?.val}));
  const rqData=(ovData?.funds||[]).filter(f=>f.volatility!=null&&f.return_5y!=null).map(f=>({name:shrt(f.fund_name.replace(" Fund",""),16),x:+(f.volatility*100).toFixed(1),y:+(f.return_5y*100).toFixed(1),isSel:f.fund_id===selId}));
  const calChart=(calData?.annual_returns||[]).map(r=>({year:String(r.year),fund:r.fund!=null?+(r.fund*100).toFixed(1):null,bench:r.benchmark!=null?+(r.benchmark*100).toFixed(1):null,excess:r.excess!=null?+(r.excess*100).toFixed(1):null}));

  const TABS=["charts","risk","returns","stress","tools"];

  return(
    <div>
      <div style={{marginBottom:20}}><h2 className="syne" style={{fontSize:22,fontWeight:700,letterSpacing:"-.03em",marginBottom:5}}>Analytics Pack</h2><p style={{fontSize:12,color:T.t2}}>Complete analytics toolkit — charts, ratios, stress tests, calendar returns, best/worst periods</p></div>

      {/* Fund selector */}
      <div className="fade" style={{marginBottom:16}}>
        <FundSelector value={selId} onChange={setSelId} label="Search any mutual fund for deep analytics…"/>
      </div>

      {selId&&(
        <>
          {/* Tab selector */}
          <div style={{display:"flex",gap:6,marginBottom:16,flexWrap:"wrap"}}>
            {TABS.map(t=>(
              <button key={t} onClick={()=>setTab(t)} style={{padding:"6px 14px",borderRadius:8,border:`1px solid ${tab===t?T.cyan:T.b1}`,background:tab===t?T.cyanDim:T.s2,color:tab===t?T.cyan:T.t2,fontSize:11,cursor:"pointer",transition:"all .12s",fontFamily:"Inter,sans-serif",fontWeight:tab===t?600:400,textTransform:"capitalize"}}>
                {t==="charts"?"Charts & NAV":t==="risk"?"Risk Metrics":t==="returns"?"Attribution":t==="stress"?"Stress Test":"Period Analysis"}
              </button>
            ))}
            <div style={{marginLeft:"auto",display:"flex",gap:5,alignItems:"center"}}>
              {tab==="charts"&&["1Y","3Y","5Y","ALL"].map(p=><PeriodPill key={p} active={period===p} onClick={()=>setPeriod(p)}>{p}</PeriodPill>)}
            </div>
          </div>

          {/* ── CHARTS ───────────────────────────────────────────────────── */}
          {tab==="charts"&&(
            <div style={{display:"flex",flexDirection:"column",gap:12}}>
              {nl&&<Spinner/>}
              <Card>
                <CardHead title="NAV Growth — ₹100 Invested" sub={`${an?.fund_name||""} vs ${navData?.benchmark||""}`}/>
                <div style={{padding:"4px 6px 14px"}}>
                  <ResponsiveContainer width="100%" height={220}>
                    <AreaChart data={chartData}>
                      <defs>
                        <linearGradient id="gf" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor={T.cyan} stopOpacity={.15}/><stop offset="95%" stopColor={T.cyan} stopOpacity={0}/></linearGradient>
                        <linearGradient id="gb" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor={T.gold} stopOpacity={.06}/><stop offset="95%" stopColor={T.gold} stopOpacity={0}/></linearGradient>
                      </defs>
                      <CartesianGrid strokeDasharray="3 3" stroke={T.b0}/>
                      <XAxis dataKey="date" tick={TS} interval={Math.floor(chartData.length/6)} tickLine={false} axisLine={{stroke:T.b1}}/>
                      <YAxis tick={TS} tickFormatter={v=>`₹${v}`} width={52} tickLine={false} axisLine={false}/>
                      <Tooltip content={<CT fmt={v=>`₹${v}`}/>}/>
                      <Area type="monotone" dataKey="fund" stroke={T.cyan} strokeWidth={2} fill="url(#gf)" name="Fund" dot={false}/>
                      <Area type="monotone" dataKey="bench" stroke={T.gold} strokeWidth={1.5} strokeDasharray="5 3" fill="url(#gb)" name="Benchmark" dot={false}/>
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              </Card>
              <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
                <Card>
                  <CardHead title="Drawdown Analysis" sub="Peak-to-trough decline over time"/>
                  <div style={{padding:"4px 6px 14px"}}>
                    <ResponsiveContainer width="100%" height={180}>
                      <AreaChart data={ddData}>
                        <defs><linearGradient id="gdd" x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor={T.rose} stopOpacity={.2}/><stop offset="95%" stopColor={T.rose} stopOpacity={0}/></linearGradient></defs>
                        <CartesianGrid strokeDasharray="3 3" stroke={T.b0}/>
                        <XAxis dataKey="date" tick={TS} interval={Math.floor(ddData.length/5)} tickLine={false} axisLine={{stroke:T.b1}}/>
                        <YAxis tick={TS} tickFormatter={v=>`${v}%`} width={40} tickLine={false} axisLine={false}/>
                        <Tooltip content={<CT fmt={v=>`${v}%`}/>}/>
                        <ReferenceLine y={0} stroke={T.b2}/>
                        <Area type="monotone" dataKey="dd" stroke={T.rose} strokeWidth={1.5} fill="url(#gdd)" name="Drawdown%" dot={false}/>
                      </AreaChart>
                    </ResponsiveContainer>
                  </div>
                </Card>
                <Card>
                  <div style={{padding:"14px 18px 0",display:"flex",justifyContent:"space-between",alignItems:"center",borderBottom:`1px solid ${T.b0}`,paddingBottom:12,marginBottom:4}}>
                    <div><p style={{fontFamily:"Syne,sans-serif",fontSize:14,fontWeight:700,marginBottom:2}}>{rollWin}Y Rolling Spread</p><p style={{fontSize:11,color:T.t2}}>Fund minus benchmark</p></div>
                    <div style={{display:"flex",gap:5}}>{[1,3,5].map(w=><PeriodPill key={w} active={rollWin===w} onClick={()=>setRollWin(w)}>{w}Y</PeriodPill>)}</div>
                  </div>
                  <div style={{padding:"4px 6px 14px"}}>
                    <ResponsiveContainer width="100%" height={162}>
                      <ComposedChart data={spreadData}>
                        <CartesianGrid strokeDasharray="3 3" stroke={T.b0}/>
                        <XAxis dataKey="date" tick={TS} interval={Math.floor(spreadData.length/5)} tickLine={false} axisLine={{stroke:T.b1}}/>
                        <YAxis tick={TS} tickFormatter={v=>`${v}%`} width={40} tickLine={false} axisLine={false}/>
                        <Tooltip content={<CT fmt={v=>`${v?.toFixed?.(1)||v}%`}/>}/>
                        <ReferenceLine y={0} stroke={T.b2} strokeWidth={1.5}/>
                        <Bar dataKey="spread" name="Spread %" radius={[2,2,0,0]}>
                          {spreadData.map((d,i)=><Cell key={i} fill={d.spread>=0?T.emerald:T.rose} opacity={0.7}/>)}
                        </Bar>
                      </ComposedChart>
                    </ResponsiveContainer>
                  </div>
                </Card>
              </div>
              <Card>
                <CardHead title="Risk-Return Quadrant" sub="All funds — volatility (x) vs 5Y CAGR (y) · highlighted = selected fund"/>
                <div style={{padding:"4px 6px 16px"}}>
                  <ResponsiveContainer width="100%" height={250}>
                    <ScatterChart>
                      <CartesianGrid strokeDasharray="3 3" stroke={T.b0}/>
                      <XAxis dataKey="x" tick={TS} tickFormatter={v=>`${v}%`} name="Volatility" label={{value:"Volatility →",position:"insideBottom",offset:-4,style:{...TS,fill:T.t3}}} height={34} tickLine={false} axisLine={{stroke:T.b1}}/>
                      <YAxis dataKey="y" tick={TS} tickFormatter={v=>`${v}%`} name="5Y Return" label={{value:"5Y Return →",angle:-90,position:"insideLeft",style:{...TS,fill:T.t3}}} width={44} tickLine={false} axisLine={false}/>
                      <Tooltip content={({active,payload})=>{if(!active||!payload?.length)return null;const d=payload[0]?.payload;return<div style={{background:T.surface,border:`1px solid ${T.b2}`,borderRadius:9,padding:"9px 13px"}}><p style={{fontSize:11,color:T.tx,fontWeight:600,marginBottom:4}}>{d?.name}</p><p style={{fontSize:10,color:T.t2,fontFamily:"JetBrains Mono,monospace"}}>Vol: {d?.x}% · Return: {d?.y}%</p></div>;}}/>
                      <Scatter data={rqData} shape={(p)=>{const d=p.payload;const col=d.isSel?T.cyan:T.violet;return<circle cx={p.cx} cy={p.cy} r={d.isSel?7:5} fill={col} fillOpacity={d.isSel?1:0.6} stroke={d.isSel?T.cyan:T.b2} strokeWidth={d.isSel?2:1}/>;}}/>
                      <ReferenceLine y={rqData.reduce((s,d)=>s+d.y,0)/Math.max(rqData.length,1)} stroke={T.b2} strokeDasharray="4 4"/>
                    </ScatterChart>
                  </ResponsiveContainer>
                </div>
              </Card>
            </div>
          )}

          {/* ── RISK METRICS ──────────────────────────────────────────────── */}
          {tab==="risk"&&(
            pmLoading?<Spinner/>:(
              <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:12}}>
                <Card>
                  <CardHead title="Return Metrics"/>
                  <div style={{padding:"4px 16px 14px"}}>
                    {[["1Y Return",pct(pmFull?.return_1y,true),pmFull?.return_1y>0?T.emerald:T.rose,""],["3Y CAGR",pct(pmFull?.return_3y,true),T.t1,""],["5Y CAGR",pct(pmFull?.return_5y,true),pmFull?.return_5y>0.15?T.emerald:T.t1,""],["10Y CAGR",pct(pmFull?.return_10y,true),T.t1,""],["Alpha",pct(pmFull?.alpha,true),pmFull?.alpha>0?T.emerald:T.rose,"Alpha"],["Beta",n2(pmFull?.beta),T.t2,"Beta"]].map(([l,v,c,m])=><MetricRow key={l} label={l} value={v} color={c} metric={m||undefined}/>)}
                  </div>
                </Card>
                <Card>
                  <CardHead title="Risk Ratios"/>
                  <div style={{padding:"4px 16px 14px"}}>
                    {[["Sharpe Ratio",n2(pmFull?.sharpe_ratio),pmFull?.sharpe_ratio>1.2?T.emerald:pmFull?.sharpe_ratio>0.7?T.t1:T.amber,"Sharpe Ratio"],["Sortino Ratio",n2(pmFull?.sortino_ratio),pmFull?.sortino_ratio>1.5?T.emerald:T.t1,"Sortino Ratio"],["Calmar Ratio",n2(pmFull?.calmar_ratio),pmFull?.calmar_ratio>1?T.emerald:T.t1,"Calmar Ratio"],["Information Ratio",n2(pmFull?.information_ratio),pmFull?.information_ratio>0.5?T.emerald:T.t1,"Information Ratio"],["Treynor Ratio",n2(pmFull?.treynor_ratio),T.t1,"Treynor Ratio"],["Omega Ratio",n2(pmFull?.omega_ratio),pmFull?.omega_ratio>1?T.emerald:T.amber,"Omega Ratio"]].map(([l,v,c,m])=><MetricRow key={l} label={l} value={v} color={c} metric={m}/>)}
                  </div>
                </Card>
                <Card>
                  <CardHead title="Tail Risk Metrics"/>
                  <div style={{padding:"4px 16px 14px"}}>
                    {[["Max Drawdown",pct(pmFull?.max_drawdown),T.rose,"Max Drawdown"],["Volatility",pct(pmFull?.volatility),T.t1,"Volatility"],["VaR 95%",pmFull?.var_95!=null?`${(pmFull.var_95*100).toFixed(2)}%`:"—",T.rose,"VaR 95%"],["CVaR 95%",pmFull?.cvar_95!=null?`${(pmFull.cvar_95*100).toFixed(2)}%`:"—",T.rose,"CVaR 95%"],["Ulcer Index",n2(pmFull?.ulcer_index),T.amber,"Ulcer Index"]].map(([l,v,c,m])=><MetricRow key={l} label={l} value={v} color={c} metric={m}/>)}
                    <div style={{marginTop:12,padding:"10px",background:T.s3,borderRadius:9}}>
                      <p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginBottom:5,textTransform:"uppercase",letterSpacing:".06em"}}>Up/Down Capture</p>
                      {[["Up Capture",capData?.up_capture_pct,T.emerald,"Up Capture"],["Down Capture",capData?.down_capture_pct,T.rose,"Down Capture"],["Capture Ratio",capData?.capture_ratio,capData?.capture_ratio>1?T.emerald:T.amber,""]].map(([l,v,c,m])=>(
                        <div key={l} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"4px 0",borderBottom:`1px solid ${T.b0}`}}>
                          <div style={{display:"flex",alignItems:"center",gap:4}}><span style={{fontSize:11,color:T.t2}}>{l}</span>{m&&<span style={{position:"relative",display:"inline-flex",alignItems:"center",gap:4,cursor:"help"}}><span style={{width:12,height:12,borderRadius:"50%",background:T.b2,color:T.t3,fontSize:8,display:"inline-flex",alignItems:"center",justifyContent:"center",fontFamily:"JetBrains Mono,monospace",border:`1px solid ${T.b1}`}}>i</span></span>}</div>
                          <span className="mono" style={{fontSize:11,color:c,fontWeight:500}}>{v!=null?`${v}%`:"—"}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                </Card>
              </div>
            )
          )}

          {/* ── ATTRIBUTION ──────────────────────────────────────────────── */}
          {tab==="returns"&&(
            <div style={{display:"flex",flexDirection:"column",gap:12}}>
              {calChart.length>0&&(
                <Card>
                  <CardHead title="Calendar Year Returns" sub="Year-by-year fund vs benchmark — see how it behaved in 2020, 2022"/>
                  <div style={{padding:"4px 6px 14px"}}>
                    <ResponsiveContainer width="100%" height={220}>
                      <ComposedChart data={calChart} margin={{left:0,right:10}}>
                        <CartesianGrid strokeDasharray="3 3" stroke={T.b0}/>
                        <XAxis dataKey="year" tick={TS} tickLine={false} axisLine={{stroke:T.b1}}/>
                        <YAxis tick={TS} tickFormatter={v=>`${v}%`} width={42} tickLine={false} axisLine={false}/>
                        <Tooltip content={<CT fmt={v=>`${v}%`}/>}/>
                        <ReferenceLine y={0} stroke={T.b2}/>
                        <Bar dataKey="fund" name="Fund %" fill={T.cyan} fillOpacity={0.8} radius={[3,3,0,0]}/>
                        <Bar dataKey="bench" name="Benchmark %" fill={T.gold} fillOpacity={0.5} radius={[3,3,0,0]}/>
                      </ComposedChart>
                    </ResponsiveContainer>
                  </div>
                </Card>
              )}
              <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
                {/* Win rates */}
                <Card>
                  <CardHead title="Win Rate vs Benchmark"/>
                  <div style={{padding:"14px 16px"}}>
                    <p style={{fontSize:11,color:T.t2,marginBottom:14}}>% of rolling periods the fund beat its benchmark</p>
                    {pmFull?.win_rates&&Object.entries(pmFull.win_rates).map(([window,stats])=>(
                      <div key={window} style={{marginBottom:12}}>
                        <div style={{display:"flex",justifyContent:"space-between",marginBottom:4}}>
                          <span style={{fontSize:11,color:T.t2}}>{window} rolling</span>
                          <span className="mono" style={{fontSize:12,color:stats.win_rate>=60?T.emerald:stats.win_rate>=40?T.gold:T.rose,fontWeight:600}}>{stats.win_rate?.toFixed(0)}%</span>
                        </div>
                        <ProgressBar pct={stats.win_rate||0} color={stats.win_rate>=60?T.emerald:stats.win_rate>=40?T.gold:T.rose}/>
                        <p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginTop:3}}>{stats.wins}/{stats.total} periods</p>
                      </div>
                    ))}
                  </div>
                </Card>
                {/* Best/Worst */}
                <Card>
                  <CardHead title="Best & Worst Periods"/>
                  <div style={{padding:"14px 16px"}}>
                    {bwData?.periods&&Object.entries(bwData.periods).map(([window,stats])=>(
                      <div key={window} style={{marginBottom:12,padding:"10px",background:T.s3,borderRadius:9}}>
                        <p style={{fontSize:10,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginBottom:7}}>{window} window</p>
                        <div style={{display:"flex",justifyContent:"space-between"}}>
                          <div>
                            <p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginBottom:2}}>BEST</p>
                            <p className="mono" style={{fontSize:14,color:T.emerald,fontWeight:600}}>{pct(stats.best,true)}</p>
                            <p style={{fontSize:9,color:T.t3}}>{stats.best_date}</p>
                          </div>
                          <div style={{textAlign:"right"}}>
                            <p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginBottom:2}}>WORST</p>
                            <p className="mono" style={{fontSize:14,color:T.rose,fontWeight:600}}>{pct(stats.worst,true)}</p>
                            <p style={{fontSize:9,color:T.t3}}>{stats.worst_date}</p>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </Card>
              </div>
            </div>
          )}

          {/* ── STRESS TEST ──────────────────────────────────────────────── */}
          {tab==="stress"&&(
            <div style={{display:"flex",flexDirection:"column",gap:10}}>
              <div style={{display:"grid",gridTemplateColumns:"repeat(2,1fr)",gap:10,marginBottom:4}}>
                <Kpi label="Crises Outperformed" value={stData?.crises_outperformed!=null?`${stData.crises_outperformed}/${stData.crises_tracked}`:"—"} color={T.emerald}/>
                <Kpi label="Periods Tested" value={stData?.crises_tracked||"—"} color={T.t2}/>
              </div>
              {Object.entries(stData?.stress_results||{}).map(([scenario,result])=>{
                if(!result.available)return null;
                const outperf=result.outperformed;
                const col=outperf?T.emerald:T.rose;
                return(
                  <Card key={scenario}>
                    <div style={{padding:"14px 18px"}}>
                      <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",gap:12,flexWrap:"wrap"}}>
                        <div>
                          <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:4}}>
                            <span style={{fontSize:12,color:col}}>{outperf?"✓":"✗"}</span>
                            <h4 style={{fontFamily:"Syne,sans-serif",fontSize:14,fontWeight:700,color:T.tx}}>{scenario}</h4>
                            <span style={{fontSize:9,color:col,fontFamily:"JetBrains Mono,monospace",background:`${col}15`,padding:"2px 7px",borderRadius:99}}>{outperf?"OUTPERFORMED":"UNDERPERFORMED"}</span>
                          </div>
                          <p style={{fontSize:11,color:T.t2,marginBottom:3}}>{result.description}</p>
                          <p style={{fontSize:10,color:T.t3,fontFamily:"JetBrains Mono,monospace"}}>{result.period}</p>
                        </div>
                        <div style={{display:"flex",gap:16}}>
                          <div style={{textAlign:"center"}}><p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginBottom:2}}>FUND</p><p className="mono" style={{fontSize:18,color:result.fund_return>=0?T.emerald:T.rose,fontWeight:600}}>{pct(result.fund_return,true)}</p></div>
                          <div style={{textAlign:"center"}}><p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginBottom:2}}>BENCH</p><p className="mono" style={{fontSize:18,color:T.t2,fontWeight:600}}>{pct(result.bench_return,true)}</p></div>
                          <div style={{textAlign:"center"}}><p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginBottom:2}}>EXCESS</p><p className="mono" style={{fontSize:18,color:col,fontWeight:600}}>{pct(result.excess_return,true)}</p></div>
                          <div style={{textAlign:"center"}}><p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginBottom:2}}>MAX DD</p><p className="mono" style={{fontSize:18,color:T.amber,fontWeight:600}}>{pct(result.max_drawdown)}</p></div>
                        </div>
                      </div>
                    </div>
                  </Card>
                );
              })}
              {(!stData||Object.values(stData?.stress_results||{}).every(r=>!r.available))&&<p style={{color:T.t3,fontSize:13,padding:20}}>Insufficient historical data for stress tests (need data going back to 2008)</p>}
            </div>
          )}

          {/* ── PERIOD ANALYSIS ──────────────────────────────────────────── */}
          {tab==="tools"&&(
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12}}>
              <Card>
                <CardHead title="All Return Metrics"/>
                <div style={{padding:"4px 16px 14px"}}>
                  {[["1Y Return",pct(pmFull?.return_1y,true),T.t1,""],["3Y CAGR",pct(pmFull?.return_3y,true),T.t1,""],["5Y CAGR",pct(pmFull?.return_5y,true),pmFull?.return_5y>0.15?T.emerald:T.t1,""],["10Y CAGR",pct(pmFull?.return_10y,true),T.t1,""],["Alpha",pct(pmFull?.alpha,true),pmFull?.alpha>0?T.emerald:T.rose,"Alpha"],["Beta",n2(pmFull?.beta),T.t2,"Beta"],["Up Capture",capData?.up_capture_pct!=null?`${capData.up_capture_pct}%`:"—",T.emerald,"Up Capture"],["Down Capture",capData?.down_capture_pct!=null?`${capData.down_capture_pct}%`:"—",T.rose,"Down Capture"],["Batting Avg",pmFull?.batting_average!=null?`${pmFull.batting_average}%`:"—",pmFull?.batting_average>55?T.emerald:T.t1,"Batting Average"],["Consistency %",pmFull?.consistency_pct!=null?`${pmFull.consistency_pct}%`:"—",T.cyan,""],["Years Outperformed",pmFull?.years_outperformed!=null?`${pmFull.years_outperformed}/${pmFull.years_tracked}y`:"—",T.t1,""],].map(([l,v,c,m])=><MetricRow key={l} label={l} value={v} color={c} metric={m||undefined}/>)}
                </div>
              </Card>
              <Card>
                <CardHead title="All Risk Metrics"/>
                <div style={{padding:"4px 16px 14px"}}>
                  {[["Sharpe Ratio",n2(pmFull?.sharpe_ratio),pmFull?.sharpe_ratio>1.2?T.emerald:T.t1,"Sharpe Ratio"],["Sortino Ratio",n2(pmFull?.sortino_ratio),T.t1,"Sortino Ratio"],["Calmar Ratio",n2(pmFull?.calmar_ratio),pmFull?.calmar_ratio>1?T.emerald:T.t1,"Calmar Ratio"],["Info Ratio",n2(pmFull?.information_ratio),T.t1,"Information Ratio"],["Treynor Ratio",n2(pmFull?.treynor_ratio),T.t1,"Treynor Ratio"],["Omega Ratio",n2(pmFull?.omega_ratio),T.t1,"Omega Ratio"],["VaR 95%",pmFull?.var_95!=null?`${(pmFull.var_95*100).toFixed(2)}%`:"—",T.rose,"VaR 95%"],["CVaR 95%",pmFull?.cvar_95!=null?`${(pmFull.cvar_95*100).toFixed(2)}%`:"—",T.rose,"CVaR 95%"],["Ulcer Index",n2(pmFull?.ulcer_index),T.amber,"Ulcer Index"],["Max Drawdown",pct(pmFull?.max_drawdown),T.rose,"Max Drawdown"],["Volatility",pct(pmFull?.volatility),T.t1,"Volatility"],["Recovery Days",pmFull?.avg_recovery_days!=null?`${pmFull.avg_recovery_days}d avg`:"—",T.t2,""],["Quality Score",pmFull?.quality_score!=null?`${Math.round(pmFull.quality_score)}/100`:"—",T.cyan,""]].map(([l,v,c,m])=><MetricRow key={l} label={l} value={v} color={c} metric={m||undefined}/>)}
                </div>
              </Card>
            </div>
          )}
        </>
      )}
      {!selId&&<div style={{padding:"50px 0",textAlign:"center",color:T.t3,fontSize:13}}>Select a fund above to begin analysis</div>}
    </div>
  );
};
