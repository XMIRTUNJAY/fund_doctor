/**
 * Fund Doctor — Pages: Scorecard · Category · Exit · Portfolio
 */
import { useState, useEffect } from "react";
import { T, FLAG, GRADE, pct, n2, inr, shrt, useAPI, Spinner, Err, InfoIcon, FlagBadge, GradeBadge, DataBadge, Tag, Kpi, Card, CardHead, Divider, ProgressBar, MetricRow, ScoreRing, FundSelector } from "./shared.jsx";

// ══════════════════════════════════════════════════════════════════════════════
// SCORECARD — any of 1500+ funds
// ══════════════════════════════════════════════════════════════════════════════
export const ScorecardPage=()=>{
  const[selId,setSelId]=useState(null);
  const{data:sc,loading,error}=useAPI(selId?`/funds/${selId}/scorecard`:null,[selId]);
  const gradeColor=sc?(GRADE[sc.grade]||GRADE["B"]).color:T.cyan;

  return(
    <div>
      <div style={{marginBottom:22}}>
        <h2 className="syne" style={{fontSize:22,fontWeight:700,letterSpacing:"-.03em",marginBottom:5}}>Fund Health Scorecard</h2>
        <p style={{fontSize:12,color:T.t2}}>Search and analyse any of 1,500+ AMFI-registered mutual funds</p>
      </div>
      <div className="fade" style={{marginBottom:20}}>
        <FundSelector value={selId} onChange={setSelId} label="Search any mutual fund…"/>
      </div>
      {loading&&<Spinner/>}
      {error&&<Err msg={error}/>}
      {sc&&!loading&&(
        <>
          {/* Hero */}
          <div className="fade2" style={{background:T.s2,border:`1px solid ${T.b1}`,borderRadius:16,padding:"22px 26px",marginBottom:14,position:"relative",overflow:"hidden"}}>
            <div style={{position:"absolute",inset:0,background:`radial-gradient(ellipse at 80% 50%,${gradeColor}06,transparent 60%)`,pointerEvents:"none"}}/>
            <div style={{position:"absolute",top:0,left:0,right:0,height:1,background:`linear-gradient(90deg,transparent,${gradeColor}55,transparent)`}}/>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",flexWrap:"wrap",gap:16,position:"relative"}}>
              <div style={{flex:1,minWidth:0}}>
                <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:10,flexWrap:"wrap"}}>
                  <DataBadge badge={sc.data_badge} completeness={sc.data_completeness} days={sc.data_gap_days}/>
                  <FlagBadge flag={sc.flag}/>
                </div>
                <h2 className="syne" style={{fontSize:21,fontWeight:800,letterSpacing:"-.03em",color:T.tx,marginBottom:6,lineHeight:1.2}}>{sc.fund_name}</h2>
                <div style={{display:"flex",flexWrap:"wrap",gap:4}}>
                  <Tag>{sc.amc}</Tag><Tag>{sc.category}</Tag>
                  {sc.benchmark&&<Tag color={T.cyan}>vs {sc.benchmark}</Tag>}
                  {sc.expense_ratio!=null&&<Tag>ER: {sc.expense_ratio}%</Tag>}
                  {sc.risk_score&&<Tag>Risk {sc.risk_score}/9</Tag>}
                  {sc.horizon_min&&<Tag>Min {sc.horizon_min}yr hold</Tag>}
                </div>
              </div>
              <div style={{display:"flex",alignItems:"center",gap:14,flexShrink:0}}>
                <div style={{position:"relative",display:"flex",alignItems:"center",justifyContent:"center"}}>
                  <ScoreRing score={sc.quality_score} size={86} stroke={6} color={gradeColor}/>
                  <div style={{position:"absolute",display:"flex",flexDirection:"column",alignItems:"center"}}>
                    <span className="mono" style={{fontSize:20,fontWeight:600,color:gradeColor,lineHeight:1}}>{Math.round(sc.quality_score)}</span>
                    <span style={{fontSize:7,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginTop:1}}>SCORE</span>
                  </div>
                </div>
                <div style={{textAlign:"center"}}>
                  <GradeBadge grade={sc.grade} large/>
                  <p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginTop:4}}>GRADE</p>
                </div>
              </div>
            </div>
          </div>

          {/* Returns */}
          <div className="fade3" style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:10,marginBottom:10}}>
            {[["1Y Return",sc.return_1y,true],["3Y CAGR",sc.return_3y,true],["5Y CAGR",sc.return_5y,true],["10Y CAGR",sc.return_10y,true]].map(([l,v,s])=>(
              <div key={l} style={{background:T.s2,border:`1px solid ${T.b1}`,borderRadius:12,padding:"12px 14px"}}>
                <p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",textTransform:"uppercase",letterSpacing:".07em",marginBottom:5}}>{l}</p>
                <p className="mono" style={{fontSize:20,fontWeight:600,color:v>0?T.emerald:v<0?T.rose:T.t1,lineHeight:1}}>{pct(v,s)}</p>
                {l==="5Y CAGR"&&sc.bench_5y!=null&&<p style={{fontSize:9,color:T.t3,marginTop:4,fontFamily:"JetBrains Mono,monospace"}}>bench {pct(sc.bench_5y,true)} · <span style={{color:sc.excess_5y>=0?T.emerald:T.rose}}>{pct(sc.excess_5y,true)}</span></p>}
              </div>
            ))}
          </div>

          {/* Risk + Consistency */}
          <div className="fade4" style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:10,marginBottom:10}}>
            <Card>
              <CardHead title="Risk Metrics"/>
              <div style={{padding:"4px 16px 14px"}}>
                {[["Sharpe Ratio",sc.sharpe_ratio!=null?n2(sc.sharpe_ratio):"—",sc.sharpe_ratio>1.2?T.emerald:sc.sharpe_ratio>0.7?T.t1:T.amber,"Sharpe Ratio"],
                  ["Max Drawdown",pct(sc.max_drawdown),T.rose,"Max Drawdown"],
                  ["Volatility",pct(sc.volatility),T.t1,"Volatility"],
                  ["Sortino Ratio",sc.sortino_ratio!=null?n2(sc.sortino_ratio):"—",sc.sortino_ratio>1.5?T.emerald:T.t1,"Sortino Ratio"],
                  ["Beta",n2(sc.beta),T.t2,"Beta"],
                  ["Alpha",pct(sc.alpha,true),sc.alpha>0?T.emerald:T.rose,"Alpha"],
                ].map(([l,v,c,m])=><MetricRow key={l} label={l} value={v} color={c} metric={m}/>)}
              </div>
            </Card>
            <Card>
              <CardHead title="Consistency vs Benchmark"/>
              <div style={{padding:"14px 16px"}}>
                <p style={{fontSize:11,color:T.t2,marginBottom:14,lineHeight:1.5}}>% of rolling periods this fund beat its benchmark index</p>
                {[["Rolling 1-Year windows",sc.consistency_1y_pct],["Rolling 3-Year windows",sc.consistency_3y_pct]].map(([l,v])=>(
                  v!=null&&<div key={l} style={{marginBottom:14}}>
                    <div style={{display:"flex",justifyContent:"space-between",marginBottom:5}}>
                      <span style={{fontSize:11,color:T.t2}}>{l}</span>
                      <span className="mono" style={{fontSize:12,color:v>=60?T.emerald:v>=40?T.gold:T.rose,fontWeight:600}}>{v?.toFixed(0)}%</span>
                    </div>
                    <ProgressBar pct={v||0} color={v>=60?T.emerald:v>=40?T.gold:T.rose}/>
                  </div>
                ))}
                <Divider/>
                <p style={{fontSize:10,color:T.t3,marginBottom:10,fontFamily:"JetBrains Mono,monospace",textTransform:"uppercase",letterSpacing:".06em"}}>Quality Score Breakdown</p>
                {Object.entries(sc.quality_breakdown||{}).map(([k,v])=>(
                  <div key={k} style={{marginBottom:9}}>
                    <div style={{display:"flex",justifyContent:"space-between",marginBottom:3}}>
                      <span style={{fontSize:10,color:T.t2,textTransform:"capitalize"}}>{k.replace(/_/g," ")}</span>
                      <span className="mono" style={{fontSize:10,color:T.t2}}>{v?.toFixed(0)}/100</span>
                    </div>
                    <ProgressBar pct={v||0} color={T.cyan} height={3}/>
                  </div>
                ))}
              </div>
            </Card>
          </div>

          {/* NAV meta */}
          <div className="fade5" style={{background:T.s2,border:`1px solid ${T.b1}`,borderRadius:12,padding:"13px 17px",display:"flex",justifyContent:"space-between",alignItems:"center",flexWrap:"wrap",gap:12}}>
            <div style={{display:"flex",gap:22,flexWrap:"wrap"}}>
              {[["LATEST NAV",`₹${sc.nav_latest?.toFixed(2)}`,T.cyan],["AS OF",sc.nav_end_date,T.t1],["HISTORY START",sc.nav_start_date,T.t2],["DATA POINTS",sc.nav_count?.toLocaleString(),T.t2]].map(([l,v,c])=>(
                <div key={l}><p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginBottom:2}}>{l}</p><p className="mono" style={{fontSize:l==="LATEST NAV"?18:12,color:c,fontWeight:l==="LATEST NAV"?600:400}}>{v||"—"}</p></div>
              ))}
            </div>
            <DataBadge badge={sc.data_badge} completeness={sc.data_completeness} days={sc.data_gap_days}/>
          </div>
        </>
      )}
      {!selId&&<div style={{padding:"60px 0",textAlign:"center",color:T.t3,fontSize:13}}>Select a fund above to see its scorecard</div>}
    </div>
  );
};

// ══════════════════════════════════════════════════════════════════════════════
// CATEGORY PICKS
// ══════════════════════════════════════════════════════════════════════════════
export const CategoryPage=()=>{
  const CATS=["Equity: Large Cap","Equity: Mid Cap","Equity: Small Cap","Equity: Flexi Cap","Equity: ELSS","Hybrid: Balanced Advantage","Hybrid: Aggressive","Index: Large Cap","Debt: Liquid","Debt: Short Duration"];
  const[selCat,setSelCat]=useState(CATS[0]);
  const slug=selCat.toLowerCase().replace(": ","-").replace(/ /g,"-");
  const{data,loading,error}=useAPI(`/category/${encodeURIComponent(slug)}/top-picks`,[selCat]);
  const ALLOC_COLORS=[T.cyan,T.violet,T.gold,T.emerald,T.amber,"#f48fb1"];

  return(
    <div>
      <div style={{marginBottom:20}}><h2 className="syne" style={{fontSize:22,fontWeight:700,letterSpacing:"-.03em",marginBottom:5}}>Category Top Picks</h2><p style={{fontSize:12,color:T.t2}}>Best and worst funds within each SEBI category — ranked by quality score</p></div>
      <div className="fade" style={{display:"flex",gap:6,flexWrap:"wrap",marginBottom:20}}>
        {CATS.map(cat=>(
          <button key={cat} onClick={()=>setSelCat(cat)} style={{padding:"5px 11px",borderRadius:8,border:`1px solid ${selCat===cat?T.cyan:T.b1}`,background:selCat===cat?T.cyanDim:T.s2,color:selCat===cat?T.cyan:T.t2,fontSize:11,cursor:"pointer",transition:"all .12s",fontFamily:"Inter,sans-serif",fontWeight:selCat===cat?600:400}}>
            {cat.replace(/^(Equity|Index|Hybrid|Debt|FOF): /,"")}
          </button>
        ))}
      </div>
      {loading&&<Spinner/>}
      {error&&<Err msg={error}/>}
      {data&&!loading&&(
        <>
          <div className="fade2" style={{background:T.s2,border:`1px solid ${T.b1}`,borderRadius:12,padding:"13px 17px",marginBottom:16,display:"flex",gap:18,flexWrap:"wrap",alignItems:"center"}}>
            <div><p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginBottom:2}}>CATEGORY</p><p style={{fontFamily:"Syne,sans-serif",fontSize:14,fontWeight:700}}>{data.category}</p></div>
            <div><p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginBottom:2}}>FUNDS</p><p className="mono" style={{fontSize:15,color:T.cyan,fontWeight:600}}>{data.total_funds}</p></div>
            <div><p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginBottom:2}}>BENCHMARK</p><p style={{fontSize:12,color:T.t1}}>{data.benchmark}</p></div>
            <div><p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginBottom:2}}>RISK</p><p className="mono" style={{fontSize:12,color:T.gold,fontWeight:600}}>{data.risk_score}/9</p></div>
            {data.score_range?.max!=null&&<div><p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginBottom:2}}>SCORE RANGE</p><p className="mono" style={{fontSize:12,color:T.t1}}>{data.score_range.min?.toFixed(0)}–{data.score_range.max?.toFixed(0)}</p></div>}
          </div>
          <div style={{marginBottom:10}}>
            <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:12}}>
              <div style={{width:7,height:7,borderRadius:"50%",background:T.emerald}}/>
              <h3 className="syne" style={{fontSize:15,fontWeight:700,letterSpacing:"-.02em"}}>Top Picks</h3>
              <span style={{fontSize:10,color:T.t3,fontFamily:"JetBrains Mono,monospace"}}>Best funds by quality score</span>
            </div>
            <div style={{display:"flex",flexDirection:"column",gap:8}}>
              {(data.top_picks||[]).map((f,i)=>(
                <div key={f.fund_id} className={`fade${i+2}`} style={{background:T.s2,border:`1px solid ${i===0?T.emerald+"33":T.b1}`,borderRadius:12,padding:"13px 17px",position:"relative",overflow:"hidden",transition:"all .15s"}}
                  onMouseEnter={e=>{e.currentTarget.style.borderColor=T.b2;e.currentTarget.style.background=T.s3}}
                  onMouseLeave={e=>{e.currentTarget.style.borderColor=i===0?T.emerald+"33":T.b1;e.currentTarget.style.background=T.s2}}>
                  {i===0&&<div style={{position:"absolute",top:0,left:0,right:0,height:1,background:`linear-gradient(90deg,transparent,${T.emerald}55,transparent)`}}/>}
                  <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",gap:14}}>
                    <div style={{flex:1,minWidth:0}}>
                      <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:5,flexWrap:"wrap"}}>
                        <span className="mono" style={{fontSize:10,color:T.t3}}>#{i+1}</span>
                        {i===0&&<span style={{fontSize:9,color:T.emerald,fontFamily:"JetBrains Mono,monospace",background:T.emerDim,padding:"2px 7px",borderRadius:99}}>TOP PICK</span>}
                        <FlagBadge flag={f.flag}/>
                      </div>
                      <p style={{fontSize:13,color:T.tx,fontWeight:600,marginBottom:2}}>{f.fund_name}</p>
                      <p style={{fontSize:10,color:T.t3,marginBottom:10}}>{f.amc}</p>
                      <div style={{display:"grid",gridTemplateColumns:"repeat(5,1fr)",gap:5}}>
                        {Object.entries(f.breakdown||{}).map(([k,v],ci)=>(
                          <div key={k}>
                            <p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginBottom:2,textTransform:"capitalize"}}>{k.split("_")[0]}</p>
                            <div style={{height:3,background:T.s3,borderRadius:99,overflow:"hidden",marginBottom:2}}>
                              <div style={{height:"100%",width:`${v||0}%`,background:ALLOC_COLORS[ci%ALLOC_COLORS.length],borderRadius:99}}/>
                            </div>
                            <p className="mono" style={{fontSize:8,color:T.t2}}>{v?.toFixed(0)}</p>
                          </div>
                        ))}
                      </div>
                    </div>
                    <div style={{display:"flex",flexDirection:"column",alignItems:"flex-end",gap:7,flexShrink:0}}>
                      <GradeBadge grade={f.grade}/>
                      {[["5Y CAGR",f.return_5y,f.return_5y>0.15?T.emerald:T.t1],["3Y CAGR",f.return_3y,T.t1],["vs Bench",f.excess_5y,f.excess_5y>=0?T.emerald:T.rose],["Sharpe",f.sharpe_ratio!=null?n2(f.sharpe_ratio):null,T.t2]].map(([l,v,c])=>(
                        v!=null&&<div key={l} style={{display:"flex",gap:6,alignItems:"center"}}>
                          <span style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace"}}>{l}</span>
                          <span className="mono" style={{fontSize:11,color:c,fontWeight:600}}>{l==="Sharpe"?v:pct(v,true)}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              ))}
              {(!data.top_picks||data.top_picks.length===0)&&<p style={{color:T.t3,fontSize:12,padding:20}}>No funds in this category in current dataset. Run bootstrap_amfi.py to load all funds.</p>}
            </div>
          </div>
          {data.red_flags?.length>0&&(
            <div style={{marginTop:22}}>
              <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:12}}>
                <div style={{width:7,height:7,borderRadius:"50%",background:T.rose}}/>
                <h3 className="syne" style={{fontSize:15,fontWeight:700,letterSpacing:"-.02em"}}>Red Flags — Avoid or Watch</h3>
              </div>
              <div style={{display:"flex",flexDirection:"column",gap:8}}>
                {data.red_flags.map((f)=>(
                  <div key={f.fund_id} style={{background:T.roseDim,border:`1px solid ${T.rose}22`,borderRadius:12,padding:"12px 16px",display:"flex",justifyContent:"space-between",alignItems:"center",gap:12,flexWrap:"wrap"}}>
                    <div><div style={{display:"flex",alignItems:"center",gap:8,marginBottom:4}}><FlagBadge flag={f.flag}/><GradeBadge grade={f.grade}/></div><p style={{fontSize:12,color:T.tx,fontWeight:500}}>{shrt(f.fund_name,36)}</p><p style={{fontSize:10,color:T.t3,marginTop:2}}>{f.amc}</p></div>
                    <div style={{display:"flex",gap:14,flexWrap:"wrap"}}>
                      {[["5Y CAGR",pct(f.return_5y,true),"return_5y"],["vs Bench",pct(f.excess_5y,true),"excess"],["Sharpe",n2(f.sharpe_ratio),"sharpe"],["ER",`${f.expense_ratio||"—"}%`,"er"]].map(([l,v])=>(
                        <div key={l} style={{textAlign:"right"}}><p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginBottom:2}}>{l}</p><p className="mono" style={{fontSize:11,color:T.t1,fontWeight:500}}>{v}</p></div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
};

// ══════════════════════════════════════════════════════════════════════════════
// EXIT ENGINE
// ══════════════════════════════════════════════════════════════════════════════
const TriggerRow=({trigger})=>{
  const fired=trigger.fired;
  const col=fired?(trigger.severity==="high"?T.rose:trigger.severity==="medium"?T.amber:T.gold):T.emerald;
  return(
    <div style={{display:"flex",alignItems:"flex-start",gap:11,padding:"9px 0",borderBottom:`1px solid ${T.b0}`}}>
      <div style={{width:20,height:20,borderRadius:"50%",background:`${col}15`,border:`1px solid ${col}44`,display:"flex",alignItems:"center",justifyContent:"center",flexShrink:0,marginTop:1}}>
        <span style={{fontSize:8,color:col}}>{fired?"✕":"✓"}</span>
      </div>
      <div style={{flex:1,minWidth:0}}>
        <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:2}}>
          <span style={{fontSize:12,color:fired?col:T.t1,fontWeight:fired?500:400}}>{trigger.trigger}</span>
          <span className="mono" style={{fontSize:11,color:col,fontWeight:600}}>{trigger.value}</span>
        </div>
        <p style={{fontSize:11,color:T.t2,lineHeight:1.5}}>{trigger.detail}</p>
      </div>
    </div>
  );
};

export const ExitPage=()=>{
  const[selId,setSelId]=useState(null);
  const[months,setMonths]=useState(24);
  const[amount,setAmount]=useState(50000);
  const[go,setGo]=useState(false);
  const{data,loading,error}=useAPI(go&&selId?`/funds/${selId}/exit-explained?holding_months=${months}&invested_amount=${amount}`:null,[selId,months,amount,go]);
  const REC={EXIT:{color:T.rose,bg:T.roseDim,icon:"⊗"},SWITCH:{color:T.amber,bg:"rgba(255,152,0,.1)",icon:"⇌"},WATCH:{color:T.gold,bg:T.goldDim,icon:"◉"},HOLD:{color:T.emerald,bg:T.emerDim,icon:"✓"}};
  const rec=data?REC[data.recommendation]||REC.HOLD:null;
  return(
    <div>
      <div style={{marginBottom:20}}><h2 className="syne" style={{fontSize:22,fontWeight:700,letterSpacing:"-.03em",marginBottom:5}}>Exit Strategy Engine</h2><p style={{fontSize:12,color:T.t2}}>Search any fund — every trigger condition explained with clear reasoning</p></div>
      <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:12,marginBottom:20}}>
        <Card>
          <CardHead title="Select Fund" pad="12px 15px"/>
          <div style={{padding:"10px"}}><FundSelector value={selId} onChange={(id)=>{setSelId(id);setGo(false);}} label="Search any mutual fund…"/></div>
        </Card>
        <Card>
          <CardHead title="Investment Details" pad="12px 15px"/>
          <div style={{padding:"14px",display:"flex",flexDirection:"column",gap:18}}>
            <div>
              <div style={{display:"flex",justifyContent:"space-between",marginBottom:7}}><span style={{fontSize:11,color:T.t2}}>Holding Period</span><span className="mono" style={{fontSize:13,color:T.cyan,fontWeight:600}}>{months} months</span></div>
              <input type="range" min={1} max={120} value={months} onChange={e=>{setMonths(+e.target.value);setGo(false);}}/>
              <div style={{display:"flex",justifyContent:"space-between",marginTop:4,fontSize:9,fontFamily:"JetBrains Mono,monospace"}}><span style={{color:T.t3}}>1 mo</span><span style={{color:months<12?T.amber:T.emerald,fontWeight:600}}>{months<12?"STCG (15%)":"LTCG (10%)"}</span><span style={{color:T.t3}}>10 yr</span></div>
            </div>
            <div>
              <div style={{display:"flex",justifyContent:"space-between",marginBottom:7}}><span style={{fontSize:11,color:T.t2}}>Amount Invested</span><span className="mono" style={{fontSize:13,color:T.cyan,fontWeight:600}}>{inr(amount)}</span></div>
              <input type="range" min={5000} max={1000000} step={5000} value={amount} onChange={e=>{setAmount(+e.target.value);setGo(false);}}/>
              <div style={{display:"flex",justifyContent:"space-between",marginTop:4,fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace"}}><span>₹5K</span><span>₹10L</span></div>
            </div>
          </div>
        </Card>
        <Card>
          <CardHead title="Analyse" pad="12px 15px"/>
          <div style={{padding:"14px",display:"flex",flexDirection:"column",height:"calc(100% - 44px)"}}>
            {!selId&&<p style={{fontSize:11,color:T.t3,marginBottom:"auto"}}>Select a fund to begin analysis</p>}
            <button onClick={()=>selId&&setGo(true)} disabled={loading||!selId}
              style={{marginTop:"auto",width:"100%",padding:"12px",borderRadius:10,border:`1px solid ${selId?T.cyan:T.b1}`,background:selId?(go&&!loading?T.cyan:T.cyanDim):"transparent",color:selId?go&&!loading?T.bg:T.cyan:T.t3,fontSize:12,fontWeight:700,cursor:selId?"pointer":"not-allowed",fontFamily:"JetBrains Mono,monospace",letterSpacing:".06em",transition:"all .18s",opacity:loading?.6:1}}>
              {loading?"ANALYSING …":go?"✓ RE-ANALYSE":"ANALYSE EXIT →"}
            </button>
          </div>
        </Card>
      </div>
      {error&&<Err msg={error}/>}
      {data&&go&&!loading&&(
        <div className="fade">
          <div style={{background:T.s2,border:`1px solid ${rec.color}44`,borderRadius:16,padding:"20px 24px",marginBottom:12,position:"relative",overflow:"hidden"}}>
            <div style={{position:"absolute",inset:0,background:`radial-gradient(ellipse at 20% 50%,${rec.color}07,transparent 55%)`,pointerEvents:"none"}}/>
            <div style={{display:"flex",alignItems:"center",gap:16,flexWrap:"wrap"}}>
              <div style={{width:60,height:60,borderRadius:13,background:rec.bg,border:`1px solid ${rec.color}44`,display:"flex",alignItems:"center",justifyContent:"center",fontSize:26,flexShrink:0}}>{rec.icon}</div>
              <div><p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",letterSpacing:".08em",textTransform:"uppercase",marginBottom:4}}>Recommendation</p><p className="syne" style={{fontSize:34,fontWeight:800,color:rec.color,letterSpacing:"-.04em",lineHeight:1}}>{data.recommendation}</p></div>
              <div style={{marginLeft:"auto",display:"flex",flexDirection:"column",gap:5,alignItems:"flex-end"}}><FlagBadge flag={data.flag}/><span style={{fontSize:11,color:T.t3,fontFamily:"JetBrains Mono,monospace"}}>{data.fired_count}/{data.triggers?.length||0} triggers fired</span></div>
            </div>
          </div>
          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:12,marginBottom:12}}>
            <Card><CardHead title="Trigger Conditions" sub="Every factor evaluated"/><div style={{padding:"2px 16px 12px"}}>{(data.triggers||[]).map((t,i)=><TriggerRow key={i} trigger={t}/>)}</div></Card>
            <div style={{display:"flex",flexDirection:"column",gap:12}}>
              <Card><CardHead title="Tax Impact"/>
                <div style={{padding:"12px 16px",display:"flex",flexDirection:"column",gap:7}}>
                  {(data.tax_notes||[]).map((t,i)=><div key={i} style={{display:"flex",gap:9,fontSize:12,color:T.t1,lineHeight:1.6}}><span style={{color:T.gold,flexShrink:0}}>→</span>{t}</div>)}
                </div>
              </Card>
              <Card><CardHead title="Fund Metrics"/>
                <div style={{padding:"10px 16px"}}>
                  {[["5Y CAGR",pct(data.fund_metrics?.return_5y,true),T.cyan,""],["vs Benchmark",pct((data.fund_metrics?.return_5y||0)-(data.fund_metrics?.bench_5y||0),true),(data.fund_metrics?.return_5y||0)>=(data.fund_metrics?.bench_5y||0)?T.emerald:T.rose,""],["Sharpe Ratio",n2(data.fund_metrics?.sharpe_ratio),T.t1,"Sharpe Ratio"],["Max Drawdown",pct(data.fund_metrics?.max_drawdown),T.rose,"Max Drawdown"],["Expense Ratio",`${data.fund_metrics?.expense_ratio||"—"}%`,data.fund_metrics?.expense_ratio>1.5?T.amber:T.t1,""]].map(([l,v,c,m])=>(
                    <div key={l} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"6px 0",borderBottom:`1px solid ${T.b0}`}}>
                      <div style={{display:"flex",alignItems:"center",gap:4}}><span style={{fontSize:11,color:T.t2}}>{l}</span></div>
                      <span className="mono" style={{fontSize:11,color:c,fontWeight:500}}>{v}</span>
                    </div>
                  ))}
                </div>
              </Card>
            </div>
          </div>
          {(data.recommendation==="SWITCH"||data.recommendation==="EXIT")&&data.replacements?.length>0&&(
            <Card><CardHead title="Recommended Replacements" sub={`Same category alternatives`}/>
              <div style={{padding:"12px",display:"grid",gridTemplateColumns:`repeat(${Math.min(3,data.replacements.length)},1fr)`,gap:10}}>
                {data.replacements.map((r,i)=>(
                  <div key={r.fund_id} style={{background:T.s3,border:`1px solid ${i===0?T.cyan+"44":T.b1}`,borderRadius:11,padding:"13px",position:"relative"}}>
                    {i===0&&<div style={{position:"absolute",top:-1,left:10,right:10,height:2,background:`linear-gradient(90deg,${T.cyan},${T.violet})`}}/>}
                    {i===0&&<span style={{position:"absolute",top:8,right:10,fontSize:8,color:T.cyan,fontFamily:"JetBrains Mono,monospace",background:T.cyanDim,padding:"2px 6px",borderRadius:99}}>BEST</span>}
                    <p style={{fontSize:12,color:T.tx,fontWeight:600,marginBottom:2,paddingRight:40}}>{shrt(r.fund_name,26)}</p>
                    <p style={{fontSize:10,color:T.t3,marginBottom:10}}>{r.fund_id}</p>
                    {r.grade&&<div style={{marginBottom:8}}><GradeBadge grade={r.grade}/></div>}
                    {[["5Y CAGR",pct(r.return_5y,true)],["Sharpe",n2(r.sharpe_ratio)],["ER",`${r.expense_ratio||"—"}%`]].map(([l,v])=>(
                      <div key={l} style={{display:"flex",justifyContent:"space-between",padding:"4px 0",borderBottom:`1px solid ${T.b0}`}}>
                        <span style={{fontSize:10,color:T.t3}}>{l}</span><span className="mono" style={{fontSize:10,color:T.t1}}>{v}</span>
                      </div>
                    ))}
                  </div>
                ))}
              </div>
            </Card>
          )}
        </div>
      )}
      {!selId&&<div style={{padding:"50px 0",textAlign:"center",color:T.t3,fontSize:13}}>Select a fund above to begin exit analysis</div>}
    </div>
  );
};

// ══════════════════════════════════════════════════════════════════════════════
// PORTFOLIO DOCTOR
// ══════════════════════════════════════════════════════════════════════════════
export const PortfolioPage=()=>{
  const{data,loading,error}=useAPI("/portfolio/doctor-lite",[]);
  if(loading)return<Spinner/>;if(error)return<Err msg={error}/>;if(!data)return null;
  const score=data.health_score;
  const scoreColor=score>=70?T.emerald:score>=45?T.gold:T.rose;
  const scoreLabel=score>=70?"Healthy":score>=45?"Needs Attention":"Critical";
  const URGENCY_COLOR={critical:T.rose,high:T.amber,medium:T.gold,low:T.cyan,none:T.emerald};
  const URGENCY_ICON={critical:"🔴",high:"🔶",medium:"⚠️",low:"💡",none:"✅"};
  const catColors=[T.cyan,T.violet,T.gold,T.emerald,T.amber,"#f48fb1","#80cbc4"];
  const catEntries=Object.entries(data.concentration?.allocation||{}).sort((a,b)=>b[1]-a[1]);
  return(
    <div>
      <div style={{marginBottom:20}}><h2 className="syne" style={{fontSize:22,fontWeight:700,letterSpacing:"-.03em",marginBottom:5}}>Portfolio Doctor</h2><p style={{fontSize:12,color:T.t2}}>3 key numbers · 3 priority actions · complete portfolio health</p></div>
      <div className="fade" style={{display:"grid",gridTemplateColumns:"auto 1fr 1fr 1fr",gap:12,marginBottom:14}}>
        <div style={{background:T.s2,border:`1px solid ${T.b1}`,borderRadius:14,padding:"20px 18px",display:"flex",flexDirection:"column",alignItems:"center",justifyContent:"center",minWidth:140}}>
          <div style={{position:"relative",marginBottom:10,display:"flex",alignItems:"center",justifyContent:"center"}}>
            <ScoreRing score={score} size={106} stroke={7} color={scoreColor}/>
            <div style={{position:"absolute",display:"flex",flexDirection:"column",alignItems:"center"}}><p className="mono" style={{fontSize:28,fontWeight:700,color:scoreColor,lineHeight:1}}>{score}</p><p style={{fontSize:7,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginTop:1}}>/ 100</p></div>
          </div>
          <p className="syne" style={{fontSize:13,fontWeight:700,color:scoreColor}}>{scoreLabel}</p>
          <p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",marginTop:2}}>HEALTH SCORE</p>
        </div>
        <div style={{background:T.s2,border:`1px solid ${T.b1}`,borderRadius:14,padding:"16px"}}>
          <p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",textTransform:"uppercase",letterSpacing:".07em",marginBottom:5}}>Concentration Risk</p>
          <p className="mono" style={{fontSize:24,fontWeight:600,color:data.concentration.level==="high"?T.rose:data.concentration.level==="medium"?T.gold:T.emerald,lineHeight:1,marginBottom:8}}>{data.concentration.level?.toUpperCase()}</p>
          <p style={{fontSize:11,color:T.t1,marginBottom:11}}>{data.concentration.top_pct?.toFixed(0)}% in {shrt(data.concentration.top_category||"",22)}</p>
          <div style={{display:"flex",flexDirection:"column",gap:5}}>
            {catEntries.slice(0,4).map(([cat,p],i)=>(
              <div key={cat}>
                <div style={{display:"flex",justifyContent:"space-between",marginBottom:2}}>
                  <span style={{fontSize:10,color:T.t2}}>{cat.replace(/^(Equity|Index|Hybrid|Debt|FOF): /,"")}</span>
                  <span className="mono" style={{fontSize:10,color:catColors[i%catColors.length]}}>{p?.toFixed(1)}%</span>
                </div>
                <div style={{height:3,background:T.s3,borderRadius:99,overflow:"hidden"}}>
                  <div style={{height:"100%",width:`${p||0}%`,background:catColors[i%catColors.length],borderRadius:99}}/>
                </div>
              </div>
            ))}
          </div>
        </div>
        <div style={{background:T.s2,border:`1px solid ${T.b1}`,borderRadius:14,padding:"16px"}}>
          <p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",textTransform:"uppercase",letterSpacing:".07em",marginBottom:5}}>Expense Drag</p>
          <p className="mono" style={{fontSize:24,fontWeight:600,color:data.expense_drag.level==="high"?T.amber:T.emerald,lineHeight:1,marginBottom:6}}>{data.avg_er?.toFixed(2)}%<span style={{fontSize:11,color:T.t3,fontWeight:400}}> avg/yr</span></p>
          <p style={{fontSize:11,color:T.t1,marginBottom:10}}>Annual cost: {inr(data.expense_drag.annual_drag_rupees)}</p>
          <div style={{height:4,background:T.s3,borderRadius:99,overflow:"hidden",marginBottom:5}}><div style={{height:"100%",width:`${data.expense_drag.score||0}%`,background:data.expense_drag.level==="high"?T.amber:T.emerald,borderRadius:99}}/></div>
          <p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace"}}>{data.expense_drag.level==="high"?"Above average — switch to Direct":"Reasonable"}</p>
          <div style={{marginTop:12,display:"flex",flexDirection:"column",gap:4}}>
            {[["Invested",inr(data.total_invested),T.t2],["Current",inr(data.total_current),T.cyan],["Gain/Loss",`${inr(data.total_gain)} (${pct(data.total_gain_pct,true)})`,(data.total_gain||0)>=0?T.emerald:T.rose]].map(([l,v,c])=>(
              <div key={l} style={{display:"flex",justifyContent:"space-between"}}><span style={{fontSize:10,color:T.t3}}>{l}</span><span className="mono" style={{fontSize:10,color:c,fontWeight:500}}>{v}</span></div>
            ))}
          </div>
        </div>
        <div style={{background:T.s2,border:`1px solid ${T.b1}`,borderRadius:14,padding:"16px"}}>
          <p style={{fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",textTransform:"uppercase",letterSpacing:".07em",marginBottom:8}}>Fund Status</p>
          <div style={{display:"flex",gap:6,marginBottom:12,flexWrap:"wrap"}}>
            {Object.entries(data.flag_summary||{}).map(([fl,cnt])=>(cnt>0&&<div key={fl} style={{display:"flex",alignItems:"center",gap:4}}><FlagBadge flag={fl.toUpperCase()}/><span className="mono" style={{fontSize:11,color:T.t1,fontWeight:600}}>{cnt}</span></div>))}
          </div>
          <div style={{display:"flex",flexDirection:"column",gap:6,overflowY:"auto",maxHeight:180}}>
            {(data.holdings||[]).map(h=>(
              <div key={h.fund_id} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"7px 0",borderBottom:`1px solid ${T.b0}`}}>
                <div><p style={{fontSize:11,color:T.tx,fontWeight:500,marginBottom:2}}>{shrt(h.fund_name,20)}</p><FlagBadge flag={h.flag}/></div>
                <div style={{textAlign:"right"}}><p className="mono" style={{fontSize:11,color:T.cyan}}>{inr(h.current_value)}</p><p className="mono" style={{fontSize:9,color:(h.gain_pct||0)>=0?T.emerald:T.rose}}>{pct(h.gain_pct,true)}</p></div>
              </div>
            ))}
          </div>
        </div>
      </div>
      <div className="fade2" style={{marginBottom:14}}>
        <h3 className="syne" style={{fontSize:15,fontWeight:700,letterSpacing:"-.02em",marginBottom:10}}>Priority Actions</h3>
        <div style={{display:"grid",gridTemplateColumns:"repeat(3,1fr)",gap:10}}>
          {(data.actions||[]).slice(0,3).map((a,i)=>{
            const col=URGENCY_COLOR[a.urgency]||T.cyan;
            return(
              <div key={i} style={{background:T.s2,border:`1px solid ${col}33`,borderRadius:12,padding:"14px 16px",position:"relative",overflow:"hidden"}}>
                <div style={{position:"absolute",top:0,left:0,right:0,height:2,background:`linear-gradient(90deg,${col},transparent)`}}/>
                <div style={{display:"flex",alignItems:"center",gap:7,marginBottom:7}}>
                  <span style={{fontSize:13}}>{URGENCY_ICON[a.urgency]}</span>
                  <span style={{fontSize:9,color:col,fontFamily:"JetBrains Mono,monospace",background:`${col}15`,padding:"2px 7px",borderRadius:99,letterSpacing:".05em"}}>{a.urgency.toUpperCase()}</span>
                </div>
                <p style={{fontSize:13,color:T.tx,fontWeight:600,marginBottom:5,lineHeight:1.3}}>{a.title}</p>
                <p style={{fontSize:11,color:T.t2,lineHeight:1.6}}>{a.detail}</p>
              </div>
            );
          })}
        </div>
      </div>
      <Card className="fade3" style={{overflow:"hidden"}}>
        <CardHead title="Portfolio Holdings" sub={`${data.n_funds} funds · all values as of today`}/>
        <table style={{width:"100%",borderCollapse:"collapse"}}>
          <thead><tr style={{borderBottom:`1px solid ${T.b1}`}}>
            {["Fund","Category","Invested","Current","Gain/Loss","ER%","Status"].map(h=>(
              <th key={h} style={{padding:"8px 13px",fontSize:9,color:T.t3,fontFamily:"JetBrains Mono,monospace",textAlign:"left",letterSpacing:".07em",textTransform:"uppercase",fontWeight:400}}>{h}</th>
            ))}
          </tr></thead>
          <tbody>
            {(data.holdings||[]).map((h,i)=>(
              <tr key={h.fund_id} style={{borderBottom:i<data.holdings.length-1?`1px solid ${T.b0}`:"none",transition:"background .1s"}}
                onMouseEnter={e=>e.currentTarget.style.background=T.s3}
                onMouseLeave={e=>e.currentTarget.style.background="transparent"}>
                <td style={{padding:"9px 13px"}}><p style={{fontSize:12,color:T.tx,fontWeight:500}}>{shrt(h.fund_name,26)}</p></td>
                <td style={{padding:"9px 13px"}}><Tag>{(h.category||"").replace(/^(Equity|Index|Hybrid|Debt|FOF): /,"")}</Tag></td>
                <td style={{padding:"9px 13px"}} className="mono"><span style={{fontSize:11,color:T.t2}}>{inr(h.amount_invested)}</span></td>
                <td style={{padding:"9px 13px"}} className="mono"><span style={{fontSize:11,color:T.t1}}>{inr(h.current_value)}</span></td>
                <td style={{padding:"9px 13px"}}><p className="mono" style={{fontSize:11,color:(h.gain||0)>=0?T.emerald:T.rose,fontWeight:500}}>{inr(h.gain)}</p><p className="mono" style={{fontSize:9,color:(h.gain_pct||0)>=0?T.emerald:T.rose}}>{pct(h.gain_pct,true)}</p></td>
                <td style={{padding:"9px 13px"}} className="mono"><span style={{fontSize:11,color:h.expense_ratio>1.5?T.amber:T.t2}}>{h.expense_ratio!=null?`${h.expense_ratio}%`:"—"}</span></td>
                <td style={{padding:"9px 13px"}}><FlagBadge flag={h.flag}/></td>
              </tr>
            ))}
            <tr style={{background:T.s3,borderTop:`1px solid ${T.b2}`}}>
              <td colSpan={2} style={{padding:"9px 13px"}}><span className="mono" style={{fontSize:11,color:T.tx,fontWeight:700}}>TOTAL</span></td>
              <td style={{padding:"9px 13px"}} className="mono"><span style={{fontSize:11,color:T.t1,fontWeight:700}}>{inr(data.total_invested)}</span></td>
              <td style={{padding:"9px 13px"}} className="mono"><span style={{fontSize:11,color:T.cyan,fontWeight:700}}>{inr(data.total_current)}</span></td>
              <td style={{padding:"9px 13px"}}><p className="mono" style={{fontSize:11,color:(data.total_gain||0)>=0?T.emerald:T.rose,fontWeight:700}}>{inr(data.total_gain)}</p><p className="mono" style={{fontSize:9,color:(data.total_gain_pct||0)>=0?T.emerald:T.rose}}>{pct(data.total_gain_pct,true)}</p></td>
              <td style={{padding:"9px 13px"}} className="mono"><span style={{fontSize:10,color:T.t3}}>{data.avg_er?.toFixed(2)}% wtd</span></td><td/>
            </tr>
          </tbody>
        </table>
      </Card>
      <p style={{fontSize:10,color:T.t3,marginTop:10,fontStyle:"italic"}}>⚠ For educational purposes only. Not investment advice. Consult a SEBI-registered advisor.</p>
    </div>
  );
};
