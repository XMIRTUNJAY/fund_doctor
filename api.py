"""
Fund Doctor — FastAPI Backend
==============================
Serves all data for the React frontend.
Endpoints cover every dashboard page:
  /api/overview          — stats + fund universe table
  /api/funds             — all funds list
  /api/funds/{id}        — single fund metadata
  /api/funds/{id}/nav    — NAV history (filterable by period)
  /api/funds/{id}/analytics  — full metrics bundle
  /api/funds/{id}/rolling    — rolling returns
  /api/funds/{id}/underperformance
  /api/funds/{id}/exit   — exit strategy assessment
  /api/comparison        — side-by-side two funds
  /api/radar             — underperformance scan all funds
  /api/portfolio         — portfolio doctor (default user)
  /api/benchmarks/{name} — benchmark history
  /api/pipeline/status   — last update timestamp
  /api/pipeline/trigger  — trigger daily refresh (POST)

Run:
    uvicorn api:app --reload --port 8000
"""

import sys
import json
import math
import logging
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional, List

sys.path.insert(0, str(Path(__file__).parent))

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import pandas as pd

from database.db import get_connection, initialize_database
from engine.analytics import (
    load_all_funds, load_fund_info, load_nav, load_benchmark,
    compute_fund_analytics, compute_benchmark_analytics,
    detect_underperformance, portfolio_analytics,
    rolling_returns, calculate_overlap,
)
from engine.exit_strategy import assess_exit, find_replacement_funds
from pipeline.ingest import seed_demo_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("api")

# ── App setup ─────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Fund Doctor API",
    description="India Mutual Fund Intelligence Platform — REST API",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Bootstrap on startup ──────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    initialize_database()
    funds = load_all_funds()
    if funds.empty:
        log.info("No fund data found — seeding demo data …")
        seed_demo_data()
    log.info("Fund Doctor API ready. %d funds in database.", len(load_all_funds()))


# ── Helpers ───────────────────────────────────────────────────────────────────
def _safe(v):
    """Convert numpy/pandas types and NaN/Inf to JSON-safe values."""
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return round(v, 6)
    if hasattr(v, "item"):          # numpy scalar
        return _safe(v.item())
    if isinstance(v, (pd.Timestamp, datetime, date)):
        return str(v)[:10]
    if isinstance(v, dict):
        return {k: _safe(vv) for k, vv in v.items()}
    if isinstance(v, list):
        return [_safe(x) for x in v]
    return v


def _nav_to_list(nav: pd.Series, thin: int = 1) -> list:
    """Convert NAV Series to [{date, nav}] list, optionally thinned."""
    if nav.empty:
        return []
    step = max(1, thin)
    return [
        {"date": str(idx.date()), "nav": round(float(v), 4)}
        for idx, v in list(zip(nav.index, nav.values))[::step]
        if not (math.isnan(float(v)) or math.isinf(float(v)))
    ]


def _period_to_days(period: str) -> Optional[int]:
    mapping = {"1W": 7, "1M": 30, "3M": 90, "6M": 180, "1Y": 365,
               "3Y": 1095, "5Y": 1825, "10Y": 3650}
    return mapping.get(period.upper())


# ══════════════════════════════════════════════════════════════════════════════
# OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/overview")
def get_overview():
    """Stats, fund universe table, flag counts — drives Overview page."""
    funds_df = load_all_funds()
    if funds_df.empty:
        return {"funds": [], "stats": {}, "flag_counts": {}}

    flag_counts = {"OK": 0, "WARNING": 0, "SERIOUS": 0, "CRITICAL": 0}
    funds_list  = []

    for _, row in funds_df.iterrows():
        an  = compute_fund_analytics(row["fund_id"])
        up  = detect_underperformance(row["fund_id"])
        flag = up.get("flag", "NO_DATA")
        if flag in flag_counts:
            flag_counts[flag] += 1

        funds_list.append({
            "fund_id":      row["fund_id"],
            "fund_name":    row["fund_name"],
            "amc":          row.get("amc", ""),
            "category":     row.get("category", ""),
            "benchmark":    row.get("benchmark", ""),
            "expense_ratio":_safe(row.get("expense_ratio")),
            "aum":          _safe(row.get("aum")),
            "fund_manager": row.get("fund_manager", ""),
            "risk_level":   row.get("risk_level", ""),
            "return_1y":    _safe(an.get("return_1y")),
            "return_3y":    _safe(an.get("return_3y")),
            "return_5y":    _safe(an.get("return_5y")),
            "sharpe_ratio": _safe(an.get("sharpe_ratio")),
            "nav_latest":   _safe(an.get("nav_latest")),
            "flag":         flag,
        })

    conn     = get_connection()
    n_nav    = conn.execute("SELECT COUNT(*) FROM nav_history").fetchone()[0]
    n_bench  = conn.execute("SELECT COUNT(DISTINCT index_name) FROM benchmark_history").fetchone()[0]
    last_upd = conn.execute("SELECT MAX(updated_at) FROM fund_master").fetchone()[0]
    conn.close()

    total_aum = sum(f.get("aum") or 0 for f in funds_list)

    return {
        "stats": {
            "total_funds":      len(funds_list),
            "total_nav_rows":   n_nav,
            "total_benchmarks": n_bench,
            "total_aum":        total_aum,
            "last_updated":     last_upd,
        },
        "flag_counts": flag_counts,
        "funds":       funds_list,
    }


# ══════════════════════════════════════════════════════════════════════════════
# FUND CATALOGUE
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/funds")
def list_funds(category: Optional[str] = None, amc: Optional[str] = None):
    """List all funds with optional filtering."""
    df = load_all_funds()
    if category:
        df = df[df["category"].str.contains(category, case=False, na=False)]
    if amc:
        df = df[df["amc"].str.contains(amc, case=False, na=False)]
    return {"funds": df.to_dict(orient="records")}


@app.get("/api/funds/{fund_id}")
def get_fund(fund_id: str):
    """Fund metadata."""
    info = load_fund_info(fund_id)
    if not info:
        raise HTTPException(404, f"Fund {fund_id} not found")
    return _safe(info)


# ══════════════════════════════════════════════════════════════════════════════
# FUND ANALYSIS (drives Fund Analysis page)
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/funds/{fund_id}/analytics")
def get_fund_analytics(fund_id: str):
    """Full analytics bundle — returns, risk, alpha/beta."""
    an = compute_fund_analytics(fund_id)
    if "error" in an:
        raise HTTPException(404, an["error"])
    return _safe(an)


@app.get("/api/funds/{fund_id}/nav")
def get_nav(
    fund_id: str,
    period:  str = Query("5Y", description="1W|1M|3M|6M|1Y|3Y|5Y|10Y|ALL"),
    thin:    int = Query(5,   description="Keep every Nth point to reduce payload"),
):
    """
    NAV history for a fund.
    Returns [{date, nav}] list, optionally sliced by period and thinned.
    Also returns benchmark series for the same window.
    """
    nav  = load_nav(fund_id)
    info = load_fund_info(fund_id)

    if nav.empty:
        raise HTTPException(404, f"No NAV data for fund {fund_id}")

    # Slice by period
    if period.upper() != "ALL":
        days = _period_to_days(period)
        if days:
            cutoff = pd.Timestamp.today() - pd.Timedelta(days=days)
            nav    = nav[nav.index >= cutoff]

    bench_name = info.get("benchmark", "Nifty 50")
    bench = load_benchmark(bench_name)
    if not bench.empty and period.upper() != "ALL":
        days = _period_to_days(period)
        if days:
            cutoff = pd.Timestamp.today() - pd.Timedelta(days=days)
            bench  = bench[bench.index >= cutoff]

    # Normalise both to 100 for comparison
    nav_list   = _nav_to_list(nav, thin)
    bench_list = _nav_to_list(bench, thin)

    # Compute normalised (base=100) versions
    def normalise(series_list):
        if not series_list:
            return []
        base = series_list[0]["nav"]
        return [{"date": d["date"], "nav": round(d["nav"] / base * 100, 2)} for d in series_list] if base > 0 else series_list

    return {
        "fund_id":        fund_id,
        "fund_name":      info.get("fund_name", fund_id),
        "benchmark":      bench_name,
        "period":         period,
        "nav":            nav_list,
        "benchmark_nav":  bench_list,
        "nav_norm":       normalise(nav_list),
        "benchmark_norm": normalise(bench_list),
    }


@app.get("/api/funds/{fund_id}/drawdown")
def get_drawdown(fund_id: str, period: str = Query("5Y"), thin: int = Query(5)):
    """Drawdown series for drawdown chart."""
    nav = load_nav(fund_id)
    if nav.empty:
        raise HTTPException(404, f"No NAV data for {fund_id}")

    if period.upper() != "ALL":
        days = _period_to_days(period)
        if days:
            nav = nav[nav.index >= pd.Timestamp.today() - pd.Timedelta(days=days)]

    peak   = nav.cummax()
    dd     = ((nav - peak) / peak * 100).round(2)
    result = [{"date": str(idx.date()), "dd": float(v)}
              for idx, v in zip(dd.index, dd.values)
              if not (math.isnan(float(v)) or math.isinf(float(v)))]
    return {"fund_id": fund_id, "drawdown": result[::thin]}


@app.get("/api/funds/{fund_id}/rolling")
def get_rolling(
    fund_id:      str,
    window_years: int = Query(3, ge=1, le=10),
    thin:         int = Query(5),
):
    """Rolling CAGR for fund and its benchmark."""
    nav  = load_nav(fund_id)
    info = load_fund_info(fund_id)
    if nav.empty:
        raise HTTPException(404, f"No NAV data for {fund_id}")

    bench_name = info.get("benchmark", "Nifty 50")
    bench      = load_benchmark(bench_name)

    fund_roll  = rolling_returns(nav, window_years)
    bench_roll = rolling_returns(bench, window_years)

    def to_list(series):
        return [{"date": str(idx.date()), "val": round(float(v)*100, 2)}
                for idx, v in zip(series.index, series.values)
                if not (math.isnan(float(v)) or math.isinf(float(v)))]

    fr = to_list(fund_roll)[::thin]
    br = to_list(bench_roll)[::thin]

    return {
        "fund_id":       fund_id,
        "benchmark":     bench_name,
        "window_years":  window_years,
        "fund_rolling":  fr,
        "bench_rolling": br,
    }


@app.get("/api/funds/{fund_id}/underperformance")
def get_underperformance(fund_id: str):
    """Underperformance flag + details for a single fund."""
    result = detect_underperformance(fund_id)
    return _safe(result)


# ══════════════════════════════════════════════════════════════════════════════
# FUND COMPARISON
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/comparison")
def compare_funds(
    fund_a: str = Query(..., description="Fund ID A"),
    fund_b: str = Query(..., description="Fund ID B"),
    period: str = Query("5Y"),
    thin:   int = Query(5),
):
    """Side-by-side comparison — drives Comparison page."""
    an_a = compute_fund_analytics(fund_a)
    an_b = compute_fund_analytics(fund_b)

    if "error" in an_a:
        raise HTTPException(404, f"Fund {fund_a}: {an_a['error']}")
    if "error" in an_b:
        raise HTTPException(404, f"Fund {fund_b}: {an_b['error']}")

    # NAV series for chart
    nav_a  = load_nav(fund_a)
    nav_b  = load_nav(fund_b)

    if period.upper() != "ALL":
        days = _period_to_days(period)
        if days:
            cutoff = pd.Timestamp.today() - pd.Timedelta(days=days)
            nav_a  = nav_a[nav_a.index >= cutoff]
            nav_b  = nav_b[nav_b.index >= cutoff]

    min_len = min(len(nav_a), len(nav_b))
    nav_a   = nav_a.iloc[-min_len:]
    nav_b   = nav_b.iloc[-min_len:]

    base_a  = nav_a.iloc[0] if len(nav_a) > 0 else 1
    base_b  = nav_b.iloc[0] if len(nav_b) > 0 else 1

    chart = [
        {
            "date": str(nav_a.index[i].date()),
            "fund_a": round(float(nav_a.iloc[i] / base_a * 100), 2),
            "fund_b": round(float(nav_b.iloc[i] / base_b * 100), 2),
        }
        for i in range(0, min_len, max(1, thin))
    ]

    # Overlap (if holdings data exists)
    overlap = calculate_overlap(fund_a, fund_b)

    metrics = [
        ("return_1y",     "1Y Return"),
        ("return_3y",     "3Y CAGR"),
        ("return_5y",     "5Y CAGR"),
        ("return_10y",    "10Y CAGR"),
        ("volatility",    "Volatility"),
        ("max_drawdown",  "Max Drawdown"),
        ("sharpe_ratio",  "Sharpe Ratio"),
        ("sortino_ratio", "Sortino Ratio"),
        ("beta",          "Beta"),
        ("alpha",         "Alpha"),
        ("expense_ratio", "Expense Ratio"),
    ]

    comparison_table = []
    for key, label in metrics:
        va = _safe(an_a.get(key))
        vb = _safe(an_b.get(key))
        # determine winner (higher = better for all except volatility, drawdown)
        lower_is_better = key in ("volatility", "max_drawdown", "expense_ratio")
        winner = None
        if va is not None and vb is not None:
            if lower_is_better:
                winner = "A" if abs(va) < abs(vb) else ("B" if abs(vb) < abs(va) else None)
            else:
                winner = "A" if va > vb else ("B" if vb > va else None)
        comparison_table.append({"key": key, "label": label, "a": va, "b": vb, "winner": winner})

    return {
        "fund_a": _safe(an_a),
        "fund_b": _safe(an_b),
        "nav_chart": chart,
        "comparison_table": comparison_table,
        "overlap": _safe(overlap),
    }


# ══════════════════════════════════════════════════════════════════════════════
# UNDERPERFORMANCE RADAR (drives Radar page)
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/radar")
def get_radar():
    """Scan ALL funds — flag counts, excess return bar chart, detail table."""
    funds_df    = load_all_funds()
    flag_counts = {"OK": 0, "WARNING": 0, "SERIOUS": 0, "CRITICAL": 0, "NO_DATA": 0}
    results     = []

    for _, row in funds_df.iterrows():
        up   = detect_underperformance(row["fund_id"])
        an   = compute_fund_analytics(row["fund_id"])
        flag = up.get("flag", "NO_DATA")
        if flag in flag_counts:
            flag_counts[flag] += 1
        else:
            flag_counts["NO_DATA"] = flag_counts.get("NO_DATA", 0) + 1

        excess = None
        if up.get("fund_5y") is not None and up.get("bench_5y") is not None:
            excess = round((up["fund_5y"] - up["bench_5y"]) * 100, 2)

        results.append({
            "fund_id":             row["fund_id"],
            "fund_name":           row["fund_name"],
            "category":            row.get("category", ""),
            "flag":                flag,
            "fund_5y":             _safe(up.get("fund_5y")),
            "bench_5y":            _safe(up.get("bench_5y")),
            "excess_return_pct":   excess,
            "sharpe_ratio":        _safe(an.get("sharpe_ratio")),
            "pct_rolling_underperf": _safe(up.get("pct_rolling_underperf")),
            "details":             up.get("details", ""),
        })

    results.sort(key=lambda x: (x["excess_return_pct"] or 0))

    return {
        "flag_counts": flag_counts,
        "funds":       results,
        "total_funds": len(results),
    }


# ══════════════════════════════════════════════════════════════════════════════
# EXIT STRATEGY (drives Exit page)
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/exit/{fund_id}")
def get_exit_strategy(
    fund_id:        str,
    holding_months: int   = Query(24, ge=1, le=360),
    invested_amount: float = Query(50000, ge=0),
):
    """Full exit assessment + replacement recommendations."""
    result  = assess_exit(fund_id, holding_months, invested_amount)
    replac  = find_replacement_funds(fund_id)

    return {
        "assessment":    _safe(result),
        "replacements":  _safe(replac),
    }


# ══════════════════════════════════════════════════════════════════════════════
# PORTFOLIO DOCTOR (drives Portfolio page)
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/portfolio")
def get_portfolio(user_id: str = Query("default")):
    """Full portfolio health analysis."""
    pa = portfolio_analytics(user_id)
    if "error" in pa:
        raise HTTPException(404, pa["error"])

    # portfolio_df contains pandas objects — serialise manually
    portfolio_df = pa.pop("portfolio_df", pd.DataFrame())
    holdings = []
    if not portfolio_df.empty:
        for _, row in portfolio_df.iterrows():
            holdings.append({
                "fund_id":        row["fund_id"],
                "fund_name":      row.get("fund_name", ""),
                "category":       row.get("category", ""),
                "expense_ratio":  _safe(row.get("expense_ratio")),
                "amount_invested":_safe(row.get("amount_invested")),
                "current_value":  _safe(row.get("current_value")),
                "purchase_date":  str(row.get("purchase_date", "")),
            })

    return {**_safe(pa), "holdings": holdings}


@app.post("/api/portfolio/fund")
async def add_portfolio_fund(body: dict):
    """Add a fund to the default user's portfolio."""
    required = ["fund_id", "amount_invested"]
    for k in required:
        if k not in body:
            raise HTTPException(422, f"Missing field: {k}")

    conn = get_connection()
    try:
        conn.execute("""
            INSERT INTO portfolio_user (user_id, fund_id, amount_invested, purchase_date, purchase_nav)
            VALUES (?, ?, ?, ?, ?)
        """, (
            body.get("user_id", "default"),
            body["fund_id"],
            body["amount_invested"],
            body.get("purchase_date"),
            body.get("purchase_nav"),
        ))
        conn.commit()
    finally:
        conn.close()

    return {"status": "ok", "message": "Fund added to portfolio"}


@app.delete("/api/portfolio/fund/{fund_id}")
async def remove_portfolio_fund(fund_id: str, user_id: str = Query("default")):
    """Remove a fund from the portfolio."""
    conn = get_connection()
    try:
        conn.execute("DELETE FROM portfolio_user WHERE user_id=? AND fund_id=?", (user_id, fund_id))
        conn.commit()
    finally:
        conn.close()
    return {"status": "ok"}


# ══════════════════════════════════════════════════════════════════════════════
# BENCHMARKS
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/benchmarks")
def list_benchmarks():
    """Available benchmark names."""
    conn     = get_connection()
    rows     = conn.execute("SELECT DISTINCT index_name FROM benchmark_history").fetchall()
    conn.close()
    return {"benchmarks": [r[0] for r in rows]}


@app.get("/api/benchmarks/{index_name}")
def get_benchmark(
    index_name: str,
    period:     str = Query("5Y"),
    thin:       int = Query(5),
):
    """Benchmark NAV history."""
    bench = load_benchmark(index_name)
    if bench.empty:
        raise HTTPException(404, f"No data for benchmark {index_name}")

    if period.upper() != "ALL":
        days = _period_to_days(period)
        if days:
            bench = bench[bench.index >= pd.Timestamp.today() - pd.Timedelta(days=days)]

    an = compute_benchmark_analytics(index_name)
    return {
        "index_name": index_name,
        "analytics":  _safe(an),
        "history":    _nav_to_list(bench, thin),
    }


# ══════════════════════════════════════════════════════════════════════════════
# DATA PIPELINE STATUS + TRIGGER
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/pipeline/status")
def pipeline_status():
    """Last update timestamp and row counts."""
    conn = get_connection()
    nav_rows   = conn.execute("SELECT COUNT(*) FROM nav_history").fetchone()[0]
    fund_count = conn.execute("SELECT COUNT(*) FROM fund_master").fetchone()[0]
    last_nav   = conn.execute("SELECT MAX(date) FROM nav_history").fetchone()[0]
    last_upd   = conn.execute("SELECT MAX(updated_at) FROM fund_master").fetchone()[0]
    conn.close()
    return {
        "fund_count":  fund_count,
        "nav_rows":    nav_rows,
        "last_nav":    last_nav,
        "last_updated":last_upd,
        "status":      "ok",
    }


_pipeline_running = False

@app.post("/api/pipeline/trigger")
async def trigger_pipeline(background_tasks: BackgroundTasks, mode: str = Query("daily")):
    """
    Trigger a data refresh in the background.
    mode: 'daily' (latest NAV only) | 'demo' (re-seed demo data)
    """
    global _pipeline_running
    if _pipeline_running:
        return {"status": "already_running", "message": "Pipeline is already running"}

    def run_pipeline(mode: str):
        global _pipeline_running
        _pipeline_running = True
        try:
            if mode == "demo":
                seed_demo_data()
                log.info("Demo data re-seeded.")
            elif mode == "daily":
                try:
                    from pipeline.amfi_client import daily_update
                    daily_update()
                except Exception as e:
                    log.error("Daily update failed: %s — falling back to demo seed", e)
                    seed_demo_data()
            log.info("Pipeline finished.")
        finally:
            _pipeline_running = False

    background_tasks.add_task(run_pipeline, mode)
    return {"status": "started", "mode": mode, "message": f"Pipeline ({mode}) started in background"}


# ══════════════════════════════════════════════════════════════════════════════
# HEALTH CHECK
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/health")
def health():
    return {"status": "ok", "version": "2.0.0", "timestamp": datetime.utcnow().isoformat()}


# ── Dev runner ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)


# ══════════════════════════════════════════════════════════════════════════════
# ADVANCED ANALYTICS ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

from engine.advanced_analytics import (
    compute_quality_score, compute_peer_ranking, compute_real_returns,
    sip_monte_carlo, recommend_funds_for_goal, compute_tax_harvest_calendar,
    compute_portfolio_overlap_matrix, get_this_weeks_categories, WEEKLY_SCHEDULE,
)
from engine.classification import CATEGORIES, RISK_GROUPS, GOAL_CATEGORY_MAP


@app.get("/api/funds/{fund_id}/quality")
def get_quality_score(fund_id: str):
    """Proprietary 0-100 quality score with breakdown by dimension."""
    r = compute_quality_score(fund_id)
    if "error" in r:
        raise HTTPException(404, r["error"])
    return _safe(r)


@app.get("/api/funds/{fund_id}/peers")
def get_peer_ranking(fund_id: str):
    """Percentile rank vs all funds in same SEBI category."""
    r = compute_peer_ranking(fund_id)
    if "error" in r:
        raise HTTPException(404, r["error"])
    return _safe(r)


@app.get("/api/funds/{fund_id}/real-returns")
def get_real_returns(fund_id: str):
    """Inflation-adjusted (real) returns using RBI CPI data."""
    r = compute_real_returns(fund_id)
    if "error" in r:
        raise HTTPException(404, r["error"])
    return _safe(r)


@app.get("/api/funds/{fund_id}/sip-projection")
def get_sip_projection(
    fund_id:     str,
    monthly_sip: float = Query(10000, description="Monthly SIP amount in ₹"),
    years:       int   = Query(10,    description="Investment horizon in years", ge=1, le=40),
    simulations: int   = Query(3000,  description="Monte Carlo simulations", ge=100, le=10000),
):
    """
    Monte Carlo SIP projection using this fund's actual historical daily returns.
    Returns P10/P25/P50/P75/P90 outcomes — honest range of possibilities.
    """
    r = sip_monte_carlo(fund_id, monthly_sip=monthly_sip, years=years, simulations=simulations)
    if "error" in r:
        raise HTTPException(404, r["error"])
    return _safe(r)


@app.get("/api/goals/recommend")
def goal_recommend(
    goal:         str   = Query(...,   description="Goal name from: " + str(list(GOAL_CATEGORY_MAP.keys()))),
    target:       float = Query(...,   description="Target amount in ₹"),
    years:        int   = Query(...,   description="Years to goal", ge=1, le=50),
    monthly_sip:  float = Query(10000, description="Monthly SIP capacity"),
    risk:         int   = Query(5,     description="Risk appetite 1-9", ge=1, le=9),
    top_n:        int   = Query(5,     description="Number of recommendations"),
):
    """
    Goal-based fund recommender.
    Filters by goal constraints + risk appetite, ranks by composite score.
    """
    r = recommend_funds_for_goal(goal, target, years, monthly_sip, risk, top_n)
    if "error" in r:
        raise HTTPException(404, r["error"])
    return _safe(r)


@app.get("/api/goals/list")
def list_goals():
    """All supported goal types with metadata."""
    return {
        "goals": [
            {"name": k, "risk_max": v.get("risk_max", 9),
             "asset_classes": v.get("asset_class", []),
             "description": {
                 "Emergency Fund": "Park 6 months expenses safely",
                 "Short Term Savings": "Goals within 1-3 years",
                 "Wealth Building": "Long-term capital growth",
                 "Retirement": "Build retirement corpus over 15-30 years",
                 "Child Education": "Save for child's education 7-15 years away",
                 "Tax Saving": "ELSS — save tax under Section 80C",
                 "Home Purchase": "Down payment or full purchase in 3-7 years",
                 "Monthly Income": "Generate regular income from investments",
                 "Inflation Beating": "Beat inflation with moderate risk",
             }.get(k, k)}
            for k, v in GOAL_CATEGORY_MAP.items()
        ]
    }


@app.get("/api/categories")
def list_categories():
    """All 42 SEBI fund categories with risk metadata."""
    return {
        "categories": [
            {
                "name": k,
                **{kk: vv for kk, vv in v.items()}
            }
            for k, v in CATEGORIES.items()
        ],
        "risk_groups": {
            k: {"risk_range": v["risk_range"], "color": v["color"],
                "description": v["description"], "category_count": len(v["categories"])}
            for k, v in RISK_GROUPS.items()
        },
        "total_categories": len(CATEGORIES),
    }


@app.get("/api/portfolio/tax-harvest")
def get_tax_harvest(user_id: str = Query("default")):
    """
    Tax harvesting calendar — when to sell each fund to minimise tax.
    Shows LTCG vs STCG, days to threshold, tax savings possible.
    """
    r = compute_tax_harvest_calendar(user_id)
    if "error" in r:
        raise HTTPException(404, r["error"])
    return _safe(r)


@app.get("/api/portfolio/overlap")
def get_overlap_matrix(user_id: str = Query("default")):
    """
    Portfolio overlap matrix — pairwise holdings overlap.
    Uses Jaccard similarity on stock holdings, or return correlation as fallback.
    """
    r = compute_portfolio_overlap_matrix(user_id)
    if "error" in r:
        raise HTTPException(404, r["error"])
    return _safe(r)


@app.get("/api/pipeline/schedule")
def get_pipeline_schedule():
    """Weekly refresh schedule — which categories refresh in which week."""
    week_num, this_week_cats = get_this_weeks_categories()
    return {
        "current_week": week_num,
        "this_week_categories": this_week_cats,
        "full_schedule": WEEKLY_SCHEDULE,
        "description": "Categories rotate weekly: Week 1→Equity, Week 2→Hybrid, Week 3→Debt, Week 4→Index/FOF",
    }


@app.get("/api/screener")
def screen_funds(
    category:      Optional[str]  = None,
    asset_class:   Optional[str]  = None,
    risk_min:      int  = Query(1,  ge=1, le=9),
    risk_max:      int  = Query(9,  ge=1, le=9),
    return_5y_min: float = Query(0.0),
    sharpe_min:    float = Query(0.0),
    er_max:        float = Query(3.0),
    flag:          Optional[str]  = None,
    sort_by:       str  = Query("return_5y", description="return_5y|sharpe_ratio|quality_score|alpha"),
    limit:         int  = Query(50, ge=1, le=200),
):
    """
    Advanced fund screener with full filter support.
    Combine category, risk band, return threshold, expense ratio, underperf flag.
    """
    all_f = load_all_funds()

    # Apply filters
    if category:
        all_f = all_f[all_f["category"].str.contains(category, case=False, na=False)]
    if asset_class:
        # filter by asset class prefix
        all_f = all_f[all_f["category"].str.startswith(asset_class, na=False)]

    # Risk filter via classification
    if risk_min > 1 or risk_max < 9:
        from engine.classification import get_category_info
        def in_risk_range(cat):
            rs = get_category_info(cat).get("risk_score", 6)
            return risk_min <= rs <= risk_max
        all_f = all_f[all_f["category"].apply(in_risk_range)]

    results = []
    for _, row in all_f.iterrows():
        an  = compute_fund_analytics(row["fund_id"])
        up  = detect_underperformance(row["fund_id"])
        if "error" in an:
            continue

        fund_flag = up.get("flag", "NO_DATA")
        r5y       = an.get("return_5y") or 0
        sr        = an.get("sharpe_ratio") or 0
        er        = an.get("expense_ratio") or (row.get("expense_ratio") or 99)

        # Apply metric filters
        if r5y < return_5y_min: continue
        if sr < sharpe_min:     continue
        if er > er_max:         continue
        if flag and fund_flag != flag: continue

        results.append({
            "fund_id":       row["fund_id"],
            "fund_name":     row["fund_name"],
            "amc":           row.get("amc",""),
            "category":      row.get("category",""),
            "flag":          fund_flag,
            "return_1y":     _safe(an.get("return_1y")),
            "return_3y":     _safe(an.get("return_3y")),
            "return_5y":     _safe(an.get("return_5y")),
            "sharpe_ratio":  _safe(an.get("sharpe_ratio")),
            "alpha":         _safe(an.get("alpha")),
            "max_drawdown":  _safe(an.get("max_drawdown")),
            "volatility":    _safe(an.get("volatility")),
            "expense_ratio": _safe(er),
        })

    # Sort
    sort_key = sort_by if sort_by in ("return_5y","sharpe_ratio","alpha") else "return_5y"
    results.sort(key=lambda x: x.get(sort_key) or 0, reverse=True)

    return {
        "total":   len(results),
        "filters": {"category": category, "risk_min": risk_min, "risk_max": risk_max,
                    "return_5y_min": return_5y_min, "er_max": er_max, "flag": flag},
        "funds":   results[:limit],
    }


# ══════════════════════════════════════════════════════════════════════════════
# PM-GRADE ANALYTICS ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

from engine.pm_analytics import (
    calmar_ratio, treynor_ratio, information_ratio, omega_ratio,
    var_95, cvar_95, ulcer_index,
    calendar_year_returns, up_down_capture, win_rate_vs_benchmark, best_worst_periods,
    correlation_matrix, efficient_frontier, swp_calculator,
    lumpsum_vs_sip, sip_stepup_calculator,
    index_vs_active_scorecard, amc_scorecard, direct_vs_regular_gap,
    stress_test, drawdown_recovery_time, delay_cost_calculator,
    full_pm_analytics, load_nav, load_benchmark,
)


@app.get("/api/funds/{fund_id}/pm-analytics")
def get_pm_analytics(fund_id: str):
    """
    Complete PM-grade analytics bundle — 39 metrics in one call.
    Returns base + advanced + risk-adjusted + attribution + stress test.
    """
    r = full_pm_analytics(fund_id)
    if "error" in r:
        raise HTTPException(404, r["error"])
    return _safe(r)


@app.get("/api/funds/{fund_id}/risk-metrics")
def get_risk_metrics(fund_id: str):
    """Extended risk metrics: Calmar, Treynor, Info Ratio, Omega, VaR, CVaR, Ulcer."""
    nav   = load_nav(fund_id)
    info  = load_fund_info(fund_id)
    bench = load_benchmark(info.get("benchmark", "Nifty 50"))
    if nav.empty:
        raise HTTPException(404, f"No NAV data for {fund_id}")
    return _safe({
        "fund_id":          fund_id,
        "fund_name":        info.get("fund_name", fund_id),
        "calmar_ratio":     calmar_ratio(nav),
        "treynor_ratio":    treynor_ratio(nav, bench),
        "information_ratio":information_ratio(nav, bench),
        "omega_ratio":      omega_ratio(nav),
        "var_95":           var_95(nav),
        "cvar_95":          cvar_95(nav),
        "ulcer_index":      ulcer_index(nav),
    })


@app.get("/api/funds/{fund_id}/calendar-returns")
def get_calendar_returns(fund_id: str):
    """Year-by-year performance table vs benchmark. Essential for 2020 crash analysis."""
    r = calendar_year_returns(fund_id)
    if "error" in r:
        raise HTTPException(404, r["error"])
    return _safe(r)


@app.get("/api/funds/{fund_id}/capture-ratio")
def get_capture_ratio(fund_id: str):
    """Up/down capture ratios — how much of benchmark upside/downside the fund captures."""
    r = up_down_capture(fund_id)
    if "error" in r:
        raise HTTPException(404, r["error"])
    return _safe(r)


@app.get("/api/funds/{fund_id}/win-rate")
def get_win_rate(fund_id: str):
    """Win rate and batting average vs benchmark across multiple rolling windows."""
    r = win_rate_vs_benchmark(fund_id)
    if "error" in r:
        raise HTTPException(404, r["error"])
    return _safe(r)


@app.get("/api/funds/{fund_id}/best-worst")
def get_best_worst(fund_id: str):
    """Best and worst 1M/3M/6M/1Y/3Y return periods in history."""
    r = best_worst_periods(fund_id)
    if "error" in r:
        raise HTTPException(404, r["error"])
    return _safe(r)


@app.get("/api/funds/{fund_id}/stress-test")
def get_stress_test(fund_id: str):
    """Performance during GFC 2008, COVID 2020, Demonetisation, NBFC Crisis etc."""
    r = stress_test(fund_id)
    if "error" in r:
        raise HTTPException(404, r["error"])
    return _safe(r)


@app.get("/api/funds/{fund_id}/drawdown-recovery")
def get_drawdown_recovery(fund_id: str):
    """Historical drawdown episodes and days to recovery."""
    r = drawdown_recovery_time(fund_id)
    if "error" in r:
        raise HTTPException(404, r["error"])
    return _safe(r)


@app.get("/api/tools/efficient-frontier")
def get_efficient_frontier(
    fund_ids:      str = Query(..., description="Comma-separated fund IDs"),
    n_portfolios:  int = Query(1000, ge=100, le=5000),
):
    """Markowitz efficient frontier for selected funds."""
    ids = [f.strip() for f in fund_ids.split(",") if f.strip()]
    if len(ids) < 2:
        raise HTTPException(400, "Need at least 2 fund IDs")
    r = efficient_frontier(ids, n_portfolios=n_portfolios)
    if "error" in r:
        raise HTTPException(404, r["error"])
    return _safe(r)


@app.get("/api/tools/correlation")
def get_correlation_matrix(fund_ids: str = Query(..., description="Comma-separated fund IDs")):
    """Return correlation matrix for given funds."""
    ids = [f.strip() for f in fund_ids.split(",") if f.strip()]
    r = correlation_matrix(ids)
    if "error" in r:
        raise HTTPException(404, r["error"])
    return _safe(r)


@app.get("/api/tools/swp")
def get_swp(
    fund_id:            str   = Query(...),
    corpus:             float = Query(..., description="Initial corpus in ₹"),
    monthly_withdrawal: float = Query(..., description="Monthly withdrawal in ₹"),
    years:              int   = Query(20, ge=1, le=40),
    simulations:        int   = Query(2000, ge=100, le=10000),
):
    """SWP Monte Carlo — retirement drawdown sustainability analysis."""
    r = swp_calculator(fund_id, corpus, monthly_withdrawal, years, simulations)
    if "error" in r:
        raise HTTPException(404, r["error"])
    return _safe(r)


@app.get("/api/tools/lumpsum-vs-sip")
def get_lumpsum_vs_sip(
    fund_id:      str   = Query(...),
    total_amount: float = Query(...),
    years:        int   = Query(5, ge=1, le=20),
):
    """Compare lumpsum vs SIP strategy using actual fund history."""
    r = lumpsum_vs_sip(fund_id, total_amount, years)
    if "error" in r:
        raise HTTPException(404, r["error"])
    return _safe(r)


@app.get("/api/tools/sip-stepup")
def get_sip_stepup(
    fund_id:     str   = Query(...),
    initial_sip: float = Query(10000),
    stepup_pct:  float = Query(0.10, description="Annual step-up, e.g. 0.10 = 10%"),
    years:       int   = Query(10, ge=1, le=40),
    simulations: int   = Query(2000),
):
    """Step-up SIP calculator — corpus with annual SIP increase vs flat SIP."""
    r = sip_stepup_calculator(fund_id, initial_sip, stepup_pct, years, simulations)
    if "error" in r:
        raise HTTPException(404, r["error"])
    return _safe(r)


@app.get("/api/tools/delay-cost")
def get_delay_cost(
    fund_id:      str   = Query(...),
    monthly_sip:  float = Query(10000),
    total_years:  int   = Query(20, ge=5, le=40),
    delay_years:  int   = Query(5,  ge=1, le=20),
):
    """Cost of delaying SIP start in rupees — most powerful investor education tool."""
    r = delay_cost_calculator(fund_id, monthly_sip, total_years, delay_years)
    if "error" in r:
        raise HTTPException(404, r["error"])
    return _safe(r)


@app.get("/api/tools/direct-vs-regular")
def get_direct_vs_regular(
    fund_id_direct:  str = Query(..., description="Direct plan fund ID"),
    fund_id_regular: str = Query(..., description="Regular plan fund ID"),
    years:           int = Query(5, ge=1, le=20),
):
    """Quantify rupee cost of Regular vs Direct plan over N years."""
    r = direct_vs_regular_gap(fund_id_direct, fund_id_regular, years)
    if "error" in r:
        raise HTTPException(404, r["error"])
    return _safe(r)


@app.get("/api/intelligence/index-vs-active")
def get_index_vs_active():
    """SPIVA India: % of active funds beating index by category over 1Y/3Y/5Y."""
    return _safe(index_vs_active_scorecard())


@app.get("/api/intelligence/amc-scorecard")
def get_amc_scorecard():
    """AMC quality rankings — which fund house consistently delivers best funds."""
    return _safe(amc_scorecard())


@app.get("/api/portfolio/correlation")
def get_portfolio_correlation(user_id: str = Query("default")):
    """Correlation matrix for all funds in user's portfolio."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT DISTINCT fund_id FROM portfolio_user WHERE user_id=?", (user_id,)
    ).fetchall()
    conn.close()
    fund_ids = [r[0] for r in rows]
    if len(fund_ids) < 2:
        raise HTTPException(404, "Need at least 2 funds in portfolio")
    r = correlation_matrix(fund_ids)
    return _safe(r)


# ══════════════════════════════════════════════════════════════════════════════
# 80/20 PREMIUM FEATURE ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

from engine.analytics import rolling_returns as _rolling_returns
from engine.classification import get_category_info as _get_cat_info


@app.get("/api/funds/{fund_id}/scorecard")
def get_fund_scorecard(fund_id: str):
    """
    Single-screen Fund Health Scorecard — the primary decision card.
    Grade A+→D, returns, risk, consistency, data quality, status.
    """
    an   = compute_fund_analytics(fund_id)
    if "error" in an:
        raise HTTPException(404, an["error"])

    qs   = compute_quality_score(fund_id)
    up   = detect_underperformance(fund_id)
    info = load_fund_info(fund_id)
    nav  = load_nav(fund_id)
    bench_name = info.get("benchmark", "Nifty 50")
    bench = load_benchmark(bench_name)

    # Consistency: % of rolling 1Y/3Y periods beating benchmark
    def rolling_beat_pct(window_years):
        f_roll = _rolling_returns(nav, window_years)
        b_roll = _rolling_returns(bench, window_years)
        aligned = pd.DataFrame({"f": f_roll, "b": b_roll}).dropna()
        return round((aligned["f"] > aligned["b"]).mean() * 100, 1) if len(aligned) > 5 else None

    # Data quality
    conn = get_connection()
    nav_count = conn.execute("SELECT COUNT(*) FROM nav_history WHERE fund_id=?", (fund_id,)).fetchone()[0]
    last_nav  = conn.execute("SELECT MAX(date) FROM nav_history WHERE fund_id=?", (fund_id,)).fetchone()[0]
    conn.close()
    expected   = 252 * 5
    completeness_pct = round(min(100, nav_count / expected * 100), 1)
    from datetime import date as _date
    gap_days = (_date.today() - _date.fromisoformat(last_nav)).days if last_nav else 999
    data_badge = "reliable" if gap_days <= 7 and completeness_pct >= 90 else "limited"

    # Excess return vs benchmark
    bench_5y = up.get("bench_5y")
    ret_5y   = an.get("return_5y")
    excess   = _safe(round(ret_5y - bench_5y, 4)) if ret_5y is not None and bench_5y is not None else None

    # Category info
    cat_meta = _get_cat_info(info.get("category", ""))

    return _safe({
        "fund_id":      fund_id,
        "fund_name":    an["fund_name"],
        "amc":          info.get("amc", ""),
        "category":     info.get("category", ""),
        "benchmark":    bench_name,
        "expense_ratio":info.get("expense_ratio"),
        "aum":          info.get("aum"),
        "fund_manager": info.get("fund_manager", ""),

        # Grade & Score
        "grade":         qs.get("grade"),
        "quality_score": qs.get("quality_score"),
        "quality_breakdown": qs.get("breakdown"),
        "flag":          up.get("flag", "OK"),
        "flag_details":  up.get("details", ""),

        # Returns
        "return_1y":  an.get("return_1y"),
        "return_3y":  an.get("return_3y"),
        "return_5y":  ret_5y,
        "return_10y": an.get("return_10y"),
        "bench_5y":   bench_5y,
        "excess_5y":  excess,

        # Risk
        "sharpe_ratio": an.get("sharpe_ratio"),
        "max_drawdown": an.get("max_drawdown"),
        "volatility":   an.get("volatility"),
        "sortino_ratio":an.get("sortino_ratio"),
        "beta":         an.get("beta"),
        "alpha":        an.get("alpha"),

        # Consistency
        "consistency_1y_pct": rolling_beat_pct(1),
        "consistency_3y_pct": rolling_beat_pct(3),
        "pct_rolling_underperf": up.get("pct_rolling_underperf"),

        # NAV info
        "nav_latest":     an.get("nav_latest"),
        "nav_start_date": an.get("nav_start_date"),
        "nav_end_date":   an.get("nav_end_date"),
        "nav_count":      nav_count,

        # Data quality
        "data_badge":         data_badge,
        "data_completeness":  completeness_pct,
        "last_updated":       last_nav,
        "data_gap_days":      gap_days,

        # Category context
        "risk_score":   cat_meta.get("risk_score"),
        "horizon_min":  cat_meta.get("horizon_min"),
    })


@app.get("/api/category/{category_slug}/top-picks")
def get_category_top_picks(
    category_slug: str,
    top_n:  int = Query(5, ge=3, le=10),
    flag:   Optional[str] = None,
):
    """
    Top picks + red flags for a category.
    Returns top N by quality score + bottom quartile flagged funds.
    """
    # Decode slug (e.g. "equity-large-cap" → "Equity: Large Cap")
    cat_name = category_slug.replace("-", ": ", 1).title().replace("Cap", "Cap")
    # Fallback: try direct match
    conn = get_connection()
    cats = [r[0] for r in conn.execute("SELECT DISTINCT category FROM fund_master").fetchall()]
    conn.close()
    matched = next((c for c in cats if c.lower().replace(" ", "-").replace(":", "") ==
                   category_slug.lower().replace(":", "")), None)
    if not matched:
        # fuzzy match
        matched = next((c for c in cats if category_slug.lower().replace("-","") in c.lower().replace(" ","")), None)
    if not matched:
        raise HTTPException(404, f"Category '{category_slug}' not found. Available: {cats[:5]}")

    all_f = load_all_funds()
    cat_f = all_f[all_f["category"] == matched]
    if cat_f.empty:
        return {"category": matched, "top_picks": [], "red_flags": [], "total_funds": 0}

    scored = []
    for _, row in cat_f.iterrows():
        an  = compute_fund_analytics(row["fund_id"])
        qs  = compute_quality_score(row["fund_id"])
        up  = detect_underperformance(row["fund_id"])
        if "error" in an:
            continue
        scored.append({
            "fund_id":      row["fund_id"],
            "fund_name":    row["fund_name"],
            "amc":          row.get("amc", ""),
            "grade":        qs.get("grade"),
            "quality_score":qs.get("quality_score"),
            "breakdown":    qs.get("breakdown"),
            "flag":         up.get("flag", "OK"),
            "return_5y":    _safe(an.get("return_5y")),
            "return_3y":    _safe(an.get("return_3y")),
            "sharpe_ratio": _safe(an.get("sharpe_ratio")),
            "max_drawdown": _safe(an.get("max_drawdown")),
            "expense_ratio":_safe(an.get("expense_ratio") or row.get("expense_ratio")),
            "alpha":        _safe(an.get("alpha")),
            "bench_5y":     _safe(up.get("bench_5y")),
            "excess_5y":    _safe((an.get("return_5y") or 0) - (up.get("bench_5y") or 0)) if an.get("return_5y") and up.get("bench_5y") else None,
        })

    scored.sort(key=lambda x: x["quality_score"] or 0, reverse=True)
    total = len(scored)
    bottom_quartile_threshold = total // 4

    top_picks  = scored[:top_n]
    red_flags  = [s for s in scored[-bottom_quartile_threshold:] if s["flag"] in ("WARNING","SERIOUS","CRITICAL")]
    red_flags.sort(key=lambda x: {"CRITICAL":0,"SERIOUS":1,"WARNING":2}.get(x["flag"],3))

    cat_meta = _get_cat_info(matched)
    return _safe({
        "category":    matched,
        "total_funds": total,
        "risk_score":  cat_meta.get("risk_score"),
        "benchmark":   cat_meta.get("benchmark"),
        "description": cat_meta.get("description"),
        "top_picks":   top_picks[:top_n],
        "red_flags":   red_flags[:5],
        "score_range": {
            "max": scored[0]["quality_score"] if scored else None,
            "min": scored[-1]["quality_score"] if scored else None,
            "median": sorted([s["quality_score"] or 0 for s in scored])[total//2] if scored else None,
        },
    })


@app.get("/api/funds/{fund_id}/exit-explained")
def get_exit_explained(
    fund_id:        str,
    holding_months: int   = Query(24, ge=1, le=360),
    invested_amount:float = Query(50000.0, ge=0),
):
    """
    Exit Decision with full explainability — every trigger condition shown.
    HOLD/WATCH/SWITCH/EXIT + why + replacements + tax.
    """
    from engine.exit_strategy import assess_exit, find_replacement_funds
    from engine.analytics import rolling_returns as rr, load_nav, load_benchmark, load_fund_info, calculate_cagr

    result = assess_exit(fund_id, holding_months, invested_amount)
    reps   = find_replacement_funds(fund_id, top_n=3)
    an     = compute_fund_analytics(fund_id)
    up     = detect_underperformance(fund_id)
    info   = load_fund_info(fund_id)
    nav    = load_nav(fund_id)
    bench  = load_benchmark(info.get("benchmark", "Nifty 50"))

    # Build explicit trigger conditions
    triggers = []

    r5y    = an.get("return_5y")
    b5y    = up.get("bench_5y")
    sharpe = an.get("sharpe_ratio")
    er     = an.get("expense_ratio") or info.get("expense_ratio") or 1.5

    if r5y is not None and b5y is not None:
        diff = (r5y - b5y) * 100
        triggers.append({
            "trigger":   "5Y Return vs Benchmark",
            "value":     f"{r5y:.1%} vs {b5y:.1%}",
            "fired":     r5y < b5y,
            "severity":  "high" if r5y < b5y - 0.03 else "medium",
            "detail":    f"{diff:+.1f}% {'below' if diff<0 else 'above'} benchmark",
        })

    pct_under = up.get("pct_rolling_underperf")
    if pct_under is not None:
        triggers.append({
            "trigger":  "Rolling 1Y Consistency vs Benchmark",
            "value":    f"{pct_under:.0%} of rolling windows underperformed",
            "fired":    pct_under > 0.5,
            "severity": "high" if pct_under > 0.65 else "medium",
            "detail":   f"Beat benchmark in only {(1-pct_under):.0%} of rolling 1-year windows",
        })

    if sharpe is not None:
        triggers.append({
            "trigger":  "Sharpe Ratio",
            "value":    f"{sharpe:.2f}",
            "fired":    sharpe < 0.7,
            "severity": "medium",
            "detail":   "Below 0.7 = poor risk-adjusted return" if sharpe < 0.7 else "Acceptable risk-adjusted return",
        })

    if er:
        triggers.append({
            "trigger":  "Expense Ratio",
            "value":    f"{er:.2f}%",
            "fired":    er > 1.5,
            "severity": "low" if er <= 2.0 else "medium",
            "detail":   f"{'High cost drag' if er>1.5 else 'Acceptable'} — avg active fund ~1.2%",
        })

    # 3-consecutive-year check
    consec_under = 0
    for yr in [1, 2, 3]:
        fr = calculate_cagr(nav, yr)
        br = calculate_cagr(bench, yr)
        if fr is not None and br is not None and fr < br:
            consec_under += 1
    if consec_under >= 3:
        triggers.append({
            "trigger":  "Multi-Year Underperformance",
            "value":    f"{consec_under}/3 years underperformed",
            "fired":    True,
            "severity": "high",
            "detail":   "Underperforming benchmark across 1Y, 2Y, and 3Y windows",
        })

    # Enriched replacements
    enriched_reps = []
    for r in reps:
        r_an  = compute_fund_analytics(r["fund_id"])
        r_qs  = compute_quality_score(r["fund_id"])
        enriched_reps.append({**r,
            "grade":         r_qs.get("grade"),
            "quality_score": r_qs.get("quality_score"),
            "return_3y":     _safe(r_an.get("return_3y")),
            "sharpe_ratio":  _safe(r.get("sharpe_ratio")),
            "max_drawdown":  _safe(r_an.get("max_drawdown")),
        })

    return _safe({
        "fund_id":        fund_id,
        "fund_name":      result.get("fund_name", fund_id),
        "recommendation": result["recommendation"],
        "flag":           result["flag"],
        "holding_months": holding_months,
        "invested_amount":invested_amount,
        "triggers":       triggers,
        "fired_count":    sum(1 for t in triggers if t["fired"]),
        "reasons":        result["reasons"],
        "tax_notes":      result["tax_notes"],
        "replacements":   enriched_reps,
        "fund_metrics": {
            "return_5y":    _safe(r5y),
            "bench_5y":     _safe(b5y),
            "sharpe_ratio": _safe(sharpe),
            "expense_ratio":_safe(er),
            "max_drawdown": _safe(an.get("max_drawdown")),
        },
    })


@app.get("/api/portfolio/doctor-lite")
def get_portfolio_doctor_lite(user_id: str = Query("default")):
    """
    Portfolio Doctor Lite — 3 numbers + 3 actions.
    Health score, concentration risk, expense drag, top actions.
    """
    from engine.analytics import portfolio_analytics
    pa = portfolio_analytics(user_id)
    if "error" in pa:
        raise HTTPException(404, pa["error"])

    cat_alloc = pa.get("category_allocation", {})
    flags     = pa.get("underperformance_flags", {})

    # Top category concentration
    top_cat     = max(cat_alloc, key=cat_alloc.get) if cat_alloc else None
    top_pct     = cat_alloc.get(top_cat, 0) if top_cat else 0
    conc_score  = round(min(100, top_pct * 150), 0)   # 67%+ concentration = 100 risk
    conc_level  = "high" if top_pct > 0.60 else "medium" if top_pct > 0.40 else "low"

    # Expense drag
    avg_er   = pa.get("avg_er", 1.0)
    er_score = round(min(100, max(0, (avg_er - 0.2) / 2.3 * 100)), 0)
    er_level = "high" if avg_er > 1.2 else "ok"

    # Flag summary
    crit  = sum(1 for f in flags.values() if f == "CRITICAL")
    ser   = sum(1 for f in flags.values() if f == "SERIOUS")
    warn  = sum(1 for f in flags.values() if f == "WARNING")

    # Build exactly 3 prioritised actions
    actions = []
    if crit > 0:
        actions.append({
            "priority": 1, "type": "exit",
            "title":   f"Exit {crit} critical fund{'s' if crit>1 else ''}",
            "detail":  f"{crit} fund{'s are' if crit>1 else ' is'} critically underperforming. Review exit strategy immediately.",
            "urgency": "critical",
        })
    if ser > 0 and len(actions) < 3:
        actions.append({
            "priority": 2, "type": "review",
            "title":   f"Review {ser} serious underperformer{'s' if ser>1 else ''}",
            "detail":  f"Consider switching to better funds in the same category.",
            "urgency": "high",
        })
    if conc_level == "high" and len(actions) < 3:
        actions.append({
            "priority": 3, "type": "diversify",
            "title":   f"Diversify from {top_cat}",
            "detail":  f"{top_pct:.0%} of portfolio in one category. Add mid cap or hybrid exposure.",
            "urgency": "medium",
        })
    if er_level == "high" and len(actions) < 3:
        actions.append({
            "priority": 4, "type": "cost",
            "title":   "Reduce expense ratio drag",
            "detail":  f"Avg ER {avg_er:.2f}% costs ~₹{round(pa.get('total_current',100000)*avg_er/100):,}/yr. Switch to direct/index plans.",
            "urgency": "low",
        })
    if warn > 0 and len(actions) < 3:
        actions.append({
            "priority": 5, "type": "watch",
            "title":   f"Monitor {warn} flagged fund{'s' if warn>1 else ''}",
            "detail":  "These funds are lagging benchmarks — set a review date.",
            "urgency": "low",
        })
    if len(actions) < 3:
        actions.append({
            "priority": 6, "type": "healthy",
            "title":   "Portfolio is in good shape",
            "detail":  "Keep investing consistently. Review every quarter.",
            "urgency": "none",
        })

    # Holdings with enriched data
    portfolio_df = pa.get("portfolio_df", pd.DataFrame())
    holdings = []
    if not portfolio_df.empty:
        for _, row in portfolio_df.iterrows():
            flag = flags.get(row["fund_id"], "OK")
            gain = (row.get("current_value",0) or 0) - (row.get("amount_invested",0) or 0)
            holdings.append({
                "fund_id":       row["fund_id"],
                "fund_name":     row.get("fund_name","")[:35],
                "category":      row.get("category",""),
                "amount_invested":_safe(row.get("amount_invested")),
                "current_value": _safe(row.get("current_value")),
                "gain":          _safe(gain),
                "gain_pct":      _safe(gain/row.get("amount_invested",1) if row.get("amount_invested",0)>0 else 0),
                "expense_ratio": _safe(row.get("expense_ratio")),
                "flag":          flag,
            })

    return _safe({
        "user_id":         user_id,
        "health_score":    pa.get("health_score"),
        "total_invested":  pa.get("total_invested"),
        "total_current":   pa.get("total_current"),
        "total_gain":      pa.get("total_gain"),
        "total_gain_pct":  pa.get("total_gain_pct"),
        "n_funds":         pa.get("n_funds"),
        "avg_er":          pa.get("avg_er"),

        "concentration": {
            "score":    conc_score,
            "level":    conc_level,
            "top_category": top_cat,
            "top_pct":  round(top_pct * 100, 1),
            "allocation": {k: round(v*100, 1) for k, v in cat_alloc.items()},
        },
        "expense_drag": {
            "score":     er_score,
            "level":     er_level,
            "avg_er":    round(avg_er, 3),
            "annual_drag_rupees": round((pa.get("total_current",0) or 0) * avg_er / 100),
        },
        "flag_summary": {
            "critical": crit, "serious": ser,
            "warning": warn, "ok": sum(1 for f in flags.values() if f=="OK"),
        },
        "actions":     actions[:3],
        "holdings":    holdings,
    })


# ══════════════════════════════════════════════════════════════════════════════
# OVERLAP ANALYSIS — stock-level + return-correlation
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/api/overlap")
def get_overlap_analysis(
    fund_ids: str = Query(..., description="Comma-separated fund IDs, min 2"),
):
    """
    Multi-fund overlap matrix.
    Uses stock holdings (Jaccard) when available, return correlation as fallback.
    Also returns common stocks list for each pair.
    """
    ids = [f.strip() for f in fund_ids.split(",") if f.strip()]
    if len(ids) < 2:
        raise HTTPException(400, "Need at least 2 fund IDs")

    conn    = get_connection()
    names   = {}
    holdings= {}

    for fid in ids:
        row = conn.execute("SELECT fund_name FROM fund_master WHERE fund_id=?", (fid,)).fetchone()
        names[fid] = row[0] if row else fid
        h = conn.execute(
            "SELECT stock_name, sector, weight FROM fund_holdings WHERE fund_id=? ORDER BY weight DESC LIMIT 50",
            (fid,)
        ).fetchall()
        holdings[fid] = [{"stock": r[0].lower().strip(), "sector": r[1], "weight": r[2]} for r in h]
    conn.close()

    has_holdings = any(len(h) > 0 for h in holdings.values())

    # Build pairwise matrix
    pairs  = []
    matrix = {a: {b: None for b in ids} for a in ids}

    for i, fa in enumerate(ids):
        matrix[fa][fa] = {"overlap_pct": 100, "method": "self", "common_stocks": []}
        for fb in ids[i+1:]:
            if has_holdings and holdings[fa] and holdings[fb]:
                set_a = {s["stock"] for s in holdings[fa]}
                set_b = {s["stock"] for s in holdings[fb]}
                union = set_a | set_b
                inter = set_a & set_b
                pct   = round(len(inter) / len(union) * 100, 1) if union else 0.0
                # Get stock details for common names
                common_detail = []
                for s in holdings[fa]:
                    if s["stock"] in inter:
                        wb = next((x["weight"] for x in holdings[fb] if x["stock"]==s["stock"]), None)
                        common_detail.append({
                            "stock":    s["stock"].title(),
                            "sector":   s.get("sector",""),
                            "weight_a": s["weight"],
                            "weight_b": wb,
                        })
                common_detail.sort(key=lambda x: -(x["weight_a"] or 0))
                result = {
                    "overlap_pct":    pct,
                    "method":         "holdings_jaccard",
                    "common_stocks":  common_detail[:15],
                    "total_a":        len(set_a),
                    "total_b":        len(set_b),
                    "common_count":   len(inter),
                }
            else:
                # Return correlation fallback
                nav_a = load_nav(fa); nav_b = load_nav(fb)
                if nav_a.empty or nav_b.empty:
                    pct, result = 0.0, {"overlap_pct":0,"method":"no_data","common_stocks":[]}
                else:
                    import pandas as _pd
                    combined = _pd.DataFrame({"a":nav_a.pct_change(),"b":nav_b.pct_change()}).dropna()
                    corr     = float(combined["a"].corr(combined["b"])) if len(combined)>30 else 0.0
                    pct      = round(max(0, corr)*100, 1)
                    result   = {
                        "overlap_pct":   pct,
                        "method":        "return_correlation",
                        "correlation":   round(corr, 4),
                        "common_stocks": [],
                        "note":          "No holdings data — using return correlation as proxy",
                    }

            matrix[fa][fb] = result
            matrix[fb][fa] = result
            level = "high" if pct>65 else "medium" if pct>35 else "low"
            pairs.append({
                "fund_a": fa, "fund_b": fb,
                "fund_a_name": names.get(fa,""),
                "fund_b_name": names.get(fb,""),
                **_safe(result),
                "overlap_level": level,
            })

    pairs.sort(key=lambda x: -(x.get("overlap_pct") or 0))

    # Diversification score: average pairwise overlap (lower = better diversified)
    avg_overlap = sum(p.get("overlap_pct",0) or 0 for p in pairs) / max(len(pairs),1)
    div_score   = round(max(0, 100 - avg_overlap), 1)

    return _safe({
        "fund_ids":     ids,
        "fund_names":   names,
        "matrix":       matrix,
        "pairs":        pairs,
        "method":       "holdings_jaccard" if has_holdings else "return_correlation",
        "diversification_score": div_score,
        "avg_overlap_pct": round(avg_overlap, 1),
        "high_overlap_warning": avg_overlap > 50,
        "interpretation": (
            f"Average overlap: {avg_overlap:.0f}%. "
            + ("High redundancy — consider consolidating."  if avg_overlap>65
               else "Moderate overlap — reasonable diversification." if avg_overlap>35
               else "Well diversified — low redundancy between funds.")
        ),
    })
