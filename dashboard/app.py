"""
Fund Doctor — Mutual Fund Analyzer & Comparison Platform
Main Streamlit Dashboard
"""

import sys
import os
from pathlib import Path

# Ensure project root is on path
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

from database.db import get_connection, initialize_database
from engine.analytics import (
    load_nav, load_benchmark, load_all_funds, load_fund_info,
    compute_fund_analytics, compute_benchmark_analytics,
    rolling_returns, detect_underperformance, portfolio_analytics,
    calculate_overlap,
)
from engine.exit_strategy import assess_exit, find_replacement_funds
from pipeline.ingest import seed_demo_data

# ══════════════════════════════════════════════════════════════════════════════
# Page config
# ══════════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Fund Doctor",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ══════════════════════════════════════════════════════════════════════════════
# CSS — dark financial terminal aesthetic
# ══════════════════════════════════════════════════════════════════════════════

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=Syne:wght@400;600;800&display=swap');

html, body, [class*="css"] {
    font-family: 'Syne', sans-serif;
    background-color: #0a0e1a;
    color: #e2e8f0;
}

/* Sidebar */
[data-testid="stSidebar"] {
    background: #0d1220 !important;
    border-right: 1px solid #1e2a3a;
}

/* Metric cards */
[data-testid="stMetric"] {
    background: #111827;
    border: 1px solid #1e2a3a;
    border-radius: 10px;
    padding: 16px !important;
}
[data-testid="stMetricValue"] {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.5rem !important;
    color: #38bdf8 !important;
}
[data-testid="stMetricDelta"] svg { display: none; }

/* Headers */
h1 { color: #38bdf8; font-weight: 800; font-size: 2rem; letter-spacing: -1px; }
h2 { color: #94a3b8; font-weight: 600; font-size: 1.3rem; }
h3 { color: #64748b; font-weight: 400; }

/* Selectbox */
[data-testid="stSelectbox"] > div > div {
    background: #111827 !important;
    border: 1px solid #1e2a3a !important;
    color: #e2e8f0 !important;
}

/* Dataframe */
[data-testid="stDataFrame"] { border: 1px solid #1e2a3a; border-radius: 8px; }

/* Buttons */
.stButton > button {
    background: #1e3a5f;
    color: #38bdf8;
    border: 1px solid #38bdf8;
    border-radius: 6px;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.85rem;
    transition: all 0.2s;
}
.stButton > button:hover { background: #38bdf8; color: #0a0e1a; }

/* Flag badges */
.flag-ok       { color: #22c55e; font-weight: 700; }
.flag-warning  { color: #f59e0b; font-weight: 700; }
.flag-serious  { color: #f97316; font-weight: 700; }
.flag-critical { color: #ef4444; font-weight: 700; }

/* Section divider */
.divider { border-top: 1px solid #1e2a3a; margin: 20px 0; }

/* Pill tag */
.pill {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 999px;
    font-size: 0.75rem;
    font-family: 'IBM Plex Mono', monospace;
    background: #1e2a3a;
    color: #94a3b8;
    margin-right: 6px;
}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

PLOTLY_LAYOUT = dict(
    paper_bgcolor="#0a0e1a",
    plot_bgcolor="#111827",
    font_color="#94a3b8",
    font_family="IBM Plex Mono",
    xaxis=dict(gridcolor="#1e2a3a", showgrid=True),
    yaxis=dict(gridcolor="#1e2a3a", showgrid=True),
    margin=dict(l=20, r=20, t=40, b=20),
)

def fmt_pct(v):
    return f"{v*100:+.2f}%" if v is not None else "N/A"

def fmt_num(v, decimals=2):
    return f"{v:.{decimals}f}" if v is not None else "N/A"

def flag_html(flag):
    classes = {"OK": "flag-ok", "WARNING": "flag-warning",
                "SERIOUS": "flag-serious", "CRITICAL": "flag-critical",
                "NO_DATA": "flag-warning", "INSUFFICIENT_DATA": "flag-warning"}
    icons = {"OK": "✅", "WARNING": "⚠️", "SERIOUS": "🔶", "CRITICAL": "🔴",
             "NO_DATA": "❓", "INSUFFICIENT_DATA": "❓"}
    cls  = classes.get(flag, "flag-warning")
    icon = icons.get(flag, "❓")
    return f'<span class="{cls}">{icon} {flag}</span>'

def health_color(score):
    if score >= 70: return "#22c55e"
    if score >= 45: return "#f59e0b"
    return "#ef4444"


@st.cache_data(ttl=300)
def cached_nav(fund_id):          return load_nav(fund_id)
@st.cache_data(ttl=300)
def cached_bench(name):           return load_benchmark(name)
@st.cache_data(ttl=60)
def cached_all_funds():           return load_all_funds()
@st.cache_data(ttl=300)
def cached_analytics(fund_id):   return compute_fund_analytics(fund_id)
@st.cache_data(ttl=300)
def cached_underperf(fund_id):   return detect_underperformance(fund_id)
@st.cache_data(ttl=300)
def cached_portfolio(uid):        return portfolio_analytics(uid)


# ══════════════════════════════════════════════════════════════════════════════
# Bootstrap
# ══════════════════════════════════════════════════════════════════════════════

initialize_database()
all_funds = cached_all_funds()

if all_funds.empty:
    st.warning("No fund data found. Seeding demo data …")
    seed_demo_data()
    st.cache_data.clear()
    all_funds = cached_all_funds()


# ══════════════════════════════════════════════════════════════════════════════
# Sidebar
# ══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("## 💊 Fund Doctor")
    st.markdown('<p style="color:#64748b;font-size:0.8rem;font-family:IBM Plex Mono">Mutual Fund Intelligence Platform</p>', unsafe_allow_html=True)
    st.markdown("---")

    page = st.radio("Navigation", [
        "🏠 Overview",
        "📈 Fund Analysis",
        "⚖️ Fund Comparison",
        "🚨 Underperformance Radar",
        "🚪 Exit Strategy",
        "🩺 Portfolio Doctor",
    ], label_visibility="collapsed")

    st.markdown("---")
    if st.button("🔄 Reload Demo Data"):
        seed_demo_data()
        st.cache_data.clear()
        st.rerun()

    st.markdown('<p style="color:#1e2a3a;font-size:0.7rem">Fund Doctor v1.0 · MVP</p>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# Page: Overview
# ══════════════════════════════════════════════════════════════════════════════

if page == "🏠 Overview":
    st.markdown("# 💊 Fund Doctor")
    st.markdown("### India Mutual Fund Intelligence Platform")
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    col1, col2, col3, col4 = st.columns(4)
    conn = get_connection()
    n_funds = conn.execute("SELECT COUNT(*) FROM fund_master").fetchone()[0]
    n_nav   = conn.execute("SELECT COUNT(*) FROM nav_history").fetchone()[0]
    n_bench = conn.execute("SELECT COUNT(DISTINCT index_name) FROM benchmark_history").fetchone()[0]
    n_port  = conn.execute("SELECT COUNT(*) FROM portfolio_user WHERE user_id='default'").fetchone()[0]
    conn.close()

    with col1: st.metric("Funds in Database", f"{n_funds:,}")
    with col2: st.metric("NAV Data Points",   f"{n_nav:,}")
    with col3: st.metric("Benchmarks",        f"{n_bench}")
    with col4: st.metric("Portfolio Funds",   f"{n_port}")

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    # Quick comparison table
    st.markdown("### Fund Universe Overview")
    if not all_funds.empty:
        display_cols = ["fund_name", "amc", "category", "benchmark", "expense_ratio"]
        available    = [c for c in display_cols if c in all_funds.columns]
        st.dataframe(
            all_funds[available].rename(columns={
                "fund_name": "Fund", "amc": "AMC", "category": "Category",
                "benchmark": "Benchmark", "expense_ratio": "Expense Ratio (%)"
            }),
            use_container_width=True,
            hide_index=True,
        )

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    st.markdown("### Platform Modules")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**📈 Fund Analysis**\nDetailed NAV history, CAGR, rolling returns, risk metrics, benchmark comparison.")
    with c2:
        st.markdown("**⚖️ Fund Comparison**\nSide-by-side comparison of 2+ funds with performance and risk tables.")
    with c3:
        st.markdown("**🩺 Portfolio Doctor**\nHealth score, overlap detection, sector allocation, personalised recommendations.")


# ══════════════════════════════════════════════════════════════════════════════
# Page: Fund Analysis
# ══════════════════════════════════════════════════════════════════════════════

elif page == "📈 Fund Analysis":
    st.markdown("# 📈 Fund Analysis")

    fund_options = dict(zip(all_funds["fund_name"], all_funds["fund_id"]))
    fund_name    = st.selectbox("Select Fund", list(fund_options.keys()))
    fund_id      = fund_options[fund_name]

    analytics = cached_analytics(fund_id)
    nav       = cached_nav(fund_id)

    if nav.empty:
        st.error("No NAV data available for this fund.")
        st.stop()

    info       = load_fund_info(fund_id)
    bench_name = info.get("benchmark", "Nifty 50")
    bench      = cached_bench(bench_name)

    # ── Fund header ──────────────────────────────────────────────────────────
    st.markdown(f"## {fund_name}")
    pills = "".join([
        f'<span class="pill">{info.get("amc","")}</span>',
        f'<span class="pill">{info.get("category","")}</span>',
        f'<span class="pill">ER: {info.get("expense_ratio","N/A")}%</span>',
        f'<span class="pill">Benchmark: {bench_name}</span>',
    ])
    st.markdown(pills, unsafe_allow_html=True)
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    # ── Return metrics ───────────────────────────────────────────────────────
    st.markdown("### Returns")
    bench_analytics = compute_benchmark_analytics(bench_name)
    c1, c2, c3, c4, c5 = st.columns(5)
    periods = [("1Y", "return_1y"), ("3Y CAGR", "return_3y"), ("5Y CAGR", "return_5y"),
               ("10Y CAGR", "return_10y"), ("Inception", "return_inception")]
    for col, (label, key) in zip([c1, c2, c3, c4, c5], periods):
        val   = analytics.get(key)
        bench_val = bench_analytics.get(key)
        delta = None
        if val is not None and bench_val is not None:
            delta = f"vs bench {fmt_pct(bench_val)}"
        col.metric(label, fmt_pct(val), delta)

    # ── Risk metrics ─────────────────────────────────────────────────────────
    st.markdown("### Risk Metrics")
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: st.metric("Volatility",    fmt_pct(analytics.get("volatility")))
    with c2: st.metric("Max Drawdown",  fmt_pct(analytics.get("max_drawdown")))
    with c3: st.metric("Sharpe Ratio",  fmt_num(analytics.get("sharpe_ratio")))
    with c4: st.metric("Sortino Ratio", fmt_num(analytics.get("sortino_ratio")))
    with c5: st.metric("Beta",          fmt_num(analytics.get("beta")))

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    # ── NAV growth chart ─────────────────────────────────────────────────────
    st.markdown("### NAV Growth vs Benchmark")

    fig = go.Figure()
    # Normalise to 100
    nav_norm = nav / nav.iloc[0] * 100
    fig.add_trace(go.Scatter(
        x=nav_norm.index, y=nav_norm.values,
        name=fund_name[:35], line=dict(color="#38bdf8", width=2)
    ))
    if not bench.empty:
        bench_aligned = bench.reindex(nav.index, method="ffill").dropna()
        bench_norm = bench_aligned / bench_aligned.iloc[0] * 100
        fig.add_trace(go.Scatter(
            x=bench_norm.index, y=bench_norm.values,
            name=bench_name, line=dict(color="#f59e0b", width=1.5, dash="dot")
        ))
    fig.update_layout(title="Growth of ₹100 Invested", **PLOTLY_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

    # ── Drawdown chart ───────────────────────────────────────────────────────
    st.markdown("### Drawdown Analysis")
    roll_max = nav.cummax()
    drawdown = (nav - roll_max) / roll_max * 100

    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=drawdown.index, y=drawdown.values,
        fill="tozeroy", name="Drawdown %",
        line=dict(color="#ef4444"), fillcolor="rgba(239,68,68,0.15)"
    ))
    fig2.update_layout(title="Drawdown (%)", **PLOTLY_LAYOUT)
    st.plotly_chart(fig2, use_container_width=True)

    # ── Rolling returns ──────────────────────────────────────────────────────
    st.markdown("### Rolling Returns")
    roll_period = st.select_slider("Window", options=[1, 3, 5], value=3, format_func=lambda x: f"{x}Y")

    fig3 = go.Figure()
    fund_roll  = rolling_returns(nav, roll_period) * 100
    fig3.add_trace(go.Scatter(x=fund_roll.index, y=fund_roll.values,
                               name="Fund", line=dict(color="#38bdf8")))
    if not bench.empty:
        bench_roll = rolling_returns(bench, roll_period) * 100
        fig3.add_trace(go.Scatter(x=bench_roll.index, y=bench_roll.values,
                                   name=bench_name, line=dict(color="#f59e0b", dash="dot")))
    fig3.update_layout(title=f"{roll_period}Y Rolling CAGR (%)", **PLOTLY_LAYOUT)
    st.plotly_chart(fig3, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# Page: Fund Comparison
# ══════════════════════════════════════════════════════════════════════════════

elif page == "⚖️ Fund Comparison":
    st.markdown("# ⚖️ Fund Comparison")

    fund_options = dict(zip(all_funds["fund_name"], all_funds["fund_id"]))
    names        = list(fund_options.keys())

    col1, col2 = st.columns(2)
    with col1:
        fn_a = st.selectbox("Fund A", names, index=0, key="fa")
    with col2:
        fn_b = st.selectbox("Fund B", names, index=min(1, len(names)-1), key="fb")

    fid_a = fund_options[fn_a]
    fid_b = fund_options[fn_b]

    an_a = cached_analytics(fid_a)
    an_b = cached_analytics(fid_b)
    nav_a = cached_nav(fid_a)
    nav_b = cached_nav(fid_b)

    if nav_a.empty or nav_b.empty:
        st.error("One or both funds have no NAV data.")
        st.stop()

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    # ── Comparison table ─────────────────────────────────────────────────────
    metrics = [
        ("1Y Return",     "return_1y",     "pct"),
        ("3Y CAGR",       "return_3y",     "pct"),
        ("5Y CAGR",       "return_5y",     "pct"),
        ("Volatility",    "volatility",    "pct"),
        ("Max Drawdown",  "max_drawdown",  "pct"),
        ("Sharpe Ratio",  "sharpe_ratio",  "num"),
        ("Sortino Ratio", "sortino_ratio", "num"),
        ("Beta",          "beta",          "num"),
        ("Alpha",         "alpha",         "pct"),
        ("Expense Ratio", "expense_ratio", "num"),
    ]

    rows = []
    for label, key, fmt in metrics:
        v_a = an_a.get(key)
        v_b = an_b.get(key)
        formatter = fmt_pct if fmt == "pct" else lambda x: fmt_num(x)
        rows.append({
            "Metric":  label,
            fn_a[:30]: formatter(v_a),
            fn_b[:30]: formatter(v_b),
        })

    df_cmp = pd.DataFrame(rows)
    st.dataframe(df_cmp, use_container_width=True, hide_index=True)

    # ── Overlap ──────────────────────────────────────────────────────────────
    overlap = calculate_overlap(fid_a, fid_b)
    if overlap.get("overlap_pct") is not None:
        st.metric("Holdings Overlap", f"{overlap['overlap_pct']}%")
        if overlap["common_stocks"]:
            st.write("Common holdings:", ", ".join(overlap["common_stocks"]))

    # ── Side-by-side NAV chart ────────────────────────────────────────────────
    st.markdown("### Normalised NAV (Base = 100)")
    fig = go.Figure()
    na_norm = nav_a / nav_a.iloc[0] * 100
    nb_norm = nav_b / nav_b.iloc[0] * 100
    fig.add_trace(go.Scatter(x=na_norm.index, y=na_norm.values, name=fn_a[:35],
                              line=dict(color="#38bdf8")))
    fig.add_trace(go.Scatter(x=nb_norm.index, y=nb_norm.values, name=fn_b[:35],
                              line=dict(color="#a78bfa")))
    fig.update_layout(**PLOTLY_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

    # ── Risk radar ────────────────────────────────────────────────────────────
    st.markdown("### Risk Profile Radar")
    categories = ["Sharpe", "Sortino", "Low Volatility", "Low Drawdown", "Alpha"]

    def safe_radar(an):
        sharpe   = min(max((an.get("sharpe_ratio") or 0) / 2, 0), 1)
        sortino  = min(max((an.get("sortino_ratio") or 0) / 3, 0), 1)
        low_vol  = 1 - min(abs(an.get("volatility") or 0.25) / 0.4, 1)
        low_dd   = 1 - min(abs(an.get("max_drawdown") or -0.3) / 0.6, 1)
        alpha    = min(max((an.get("alpha") or 0) / 0.1 + 0.5, 0), 1)
        return [sharpe, sortino, low_vol, low_dd, alpha]

    vals_a = safe_radar(an_a)
    vals_b = safe_radar(an_b)

    fig_r = go.Figure()
    for vals, name, color in [(vals_a, fn_a[:25], "#38bdf8"), (vals_b, fn_b[:25], "#a78bfa")]:
        fig_r.add_trace(go.Scatterpolar(
            r=vals + [vals[0]], theta=categories + [categories[0]],
            fill="toself", name=name,
            line=dict(color=color), fillcolor=color.replace(")", ",0.15)").replace("rgb", "rgba") if "rgb" in color else color + "26"
        ))
    fig_r.update_layout(polar=dict(bgcolor="#111827"), **PLOTLY_LAYOUT)
    st.plotly_chart(fig_r, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# Page: Underperformance Radar
# ══════════════════════════════════════════════════════════════════════════════

elif page == "🚨 Underperformance Radar":
    st.markdown("# 🚨 Underperformance Radar")
    st.markdown("Scans all funds and flags those lagging their benchmarks.")
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    fund_ids = all_funds["fund_id"].tolist()

    with st.spinner("Analysing all funds …"):
        results = [cached_underperf(fid) for fid in fund_ids]

    df_res = pd.DataFrame(results)
    if df_res.empty:
        st.info("No underperformance data available.")
        st.stop()

    # Summary counts
    flag_counts = df_res["flag"].value_counts()
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("✅ OK",       flag_counts.get("OK", 0))
    with c2: st.metric("⚠️ Warning",  flag_counts.get("WARNING", 0))
    with c3: st.metric("🔶 Serious",  flag_counts.get("SERIOUS", 0))
    with c4: st.metric("🔴 Critical", flag_counts.get("CRITICAL", 0))

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    # Filter
    filter_flag = st.multiselect("Filter by Flag", ["OK", "WARNING", "SERIOUS", "CRITICAL"],
                                  default=["WARNING", "SERIOUS", "CRITICAL"])
    filtered = df_res[df_res["flag"].isin(filter_flag)] if filter_flag else df_res

    display = filtered[["fund_name", "flag", "fund_5y", "bench_5y", "pct_rolling_underperf", "details"]].copy()
    display.columns = ["Fund", "Flag", "Fund 5Y Return", "Benchmark 5Y", "Rolling Underperf %", "Details"]
    display["Fund 5Y Return"] = display["Fund 5Y Return"].apply(lambda x: fmt_pct(x) if x else "N/A")
    display["Benchmark 5Y"]   = display["Benchmark 5Y"].apply(lambda x: fmt_pct(x) if x else "N/A")
    display["Rolling Underperf %"] = display["Rolling Underperf %"].apply(lambda x: f"{x:.0%}" if x else "N/A")

    st.dataframe(display, use_container_width=True, hide_index=True)

    # Bar chart
    df_chart = df_res[df_res["fund_5y"].notna() & df_res["bench_5y"].notna()].copy()
    if not df_chart.empty:
        df_chart["excess"] = (df_chart["fund_5y"] - df_chart["bench_5y"]) * 100
        df_chart = df_chart.sort_values("excess")
        colors   = ["#ef4444" if e < 0 else "#22c55e" for e in df_chart["excess"]]

        fig = go.Figure(go.Bar(
            x=df_chart["fund_name"].str[:30],
            y=df_chart["excess"],
            marker_color=colors,
            text=df_chart["excess"].apply(lambda x: f"{x:+.1f}%"),
            textposition="outside",
        ))
        fig.update_layout(title="5Y Excess Return vs Benchmark (%)", **PLOTLY_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# Page: Exit Strategy
# ══════════════════════════════════════════════════════════════════════════════

elif page == "🚪 Exit Strategy":
    st.markdown("# 🚪 Exit Strategy Engine")
    st.markdown("Evaluate whether to hold, watch, switch, or exit a fund.")
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    fund_options = dict(zip(all_funds["fund_name"], all_funds["fund_id"]))
    fund_name    = st.selectbox("Select Fund to Evaluate", list(fund_options.keys()))
    fund_id      = fund_options[fund_name]

    holding_months  = st.slider("Holding Period (months)", 1, 120, 24)
    invested_amount = st.number_input("Amount Invested (₹)", min_value=1000, value=50000, step=1000)

    if st.button("🔍 Analyse Exit"):
        with st.spinner("Running exit analysis …"):
            result = assess_exit(fund_id, holding_months, invested_amount)

        st.markdown(f"### Recommendation: {flag_html(result['recommendation'])}", unsafe_allow_html=True)
        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Reasons**")
            for r in result["reasons"]:
                st.markdown(f"- {r}")
        with col2:
            st.markdown("**Tax Notes**")
            for t in result["tax_notes"]:
                st.markdown(f"- {t}")

        if result["recommendation"] in ("SWITCH", "EXIT"):
            st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
            st.markdown("### 🔁 Recommended Replacement Funds")
            replacements = find_replacement_funds(fund_id)
            if replacements:
                df_rep = pd.DataFrame(replacements)
                df_rep["return_5y"]    = df_rep["return_5y"].apply(fmt_pct)
                df_rep["sharpe_ratio"] = df_rep["sharpe_ratio"].apply(fmt_num)
                df_rep.rename(columns={
                    "fund_name": "Fund", "return_5y": "5Y CAGR",
                    "sharpe_ratio": "Sharpe", "expense_ratio": "ER (%)"
                }, inplace=True)
                st.dataframe(df_rep[["Fund", "5Y CAGR", "Sharpe", "ER (%)"]], use_container_width=True, hide_index=True)
            else:
                st.info("No peer funds found in the same category for comparison.")


# ══════════════════════════════════════════════════════════════════════════════
# Page: Portfolio Doctor
# ══════════════════════════════════════════════════════════════════════════════

elif page == "🩺 Portfolio Doctor":
    st.markdown("# 🩺 Portfolio Doctor")
    st.markdown("Comprehensive health analysis of your mutual fund portfolio.")
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    with st.spinner("Diagnosing portfolio …"):
        pa = cached_portfolio("default")

    if "error" in pa:
        st.error(pa["error"])
        st.stop()

    # ── Health score gauge ────────────────────────────────────────────────────
    score = pa["health_score"]
    color = health_color(score)

    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        domain={"x": [0, 1], "y": [0, 1]},
        title={"text": "Portfolio Health Score", "font": {"size": 20, "color": "#94a3b8"}},
        gauge={
            "axis":      {"range": [0, 100], "tickcolor": "#64748b"},
            "bar":       {"color": color},
            "bgcolor":   "#111827",
            "bordercolor": "#1e2a3a",
            "steps": [
                {"range": [0, 40],   "color": "rgba(239,68,68,0.15)"},
                {"range": [40, 70],  "color": "rgba(245,158,11,0.15)"},
                {"range": [70, 100], "color": "rgba(34,197,94,0.15)"},
            ],
        },
        number={"font": {"color": color, "size": 48}},
    ))
    fig_gauge.update_layout(paper_bgcolor="#0a0e1a", height=300, margin=dict(l=30, r=30, t=30, b=0))
    st.plotly_chart(fig_gauge, use_container_width=True)

    # ── Summary metrics ───────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("Total Invested",  f"₹{pa['total_invested']:,.0f}")
    with c2: st.metric("Current Value",   f"₹{pa['total_current']:,.0f}")
    with c3: st.metric("Total Gain/Loss", f"₹{pa['total_gain']:,.0f}", fmt_pct(pa["total_gain_pct"]))
    with c4: st.metric("Avg Expense Ratio", f"{pa['avg_er']:.2f}%")

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    # ── Category allocation donut ─────────────────────────────────────────────
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("### Sector Allocation")
        cat_alloc = pa["category_allocation"]
        if cat_alloc:
            fig_pie = go.Figure(go.Pie(
                labels=list(cat_alloc.keys()),
                values=list(cat_alloc.values()),
                hole=0.55,
                marker_colors=px.colors.qualitative.Set2,
                textinfo="label+percent",
                textfont_size=11,
            ))
            fig_pie.update_layout(paper_bgcolor="#0a0e1a", font_color="#94a3b8",
                                   showlegend=False, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig_pie, use_container_width=True)

    with col_b:
        st.markdown("### Fund Performance Flags")
        flags = pa["underperformance_flags"]
        for fid, flag in flags.items():
            fname = all_funds[all_funds["fund_id"] == fid]["fund_name"].values
            fname = fname[0][:40] if len(fname) > 0 else fid
            st.markdown(f"{flag_html(flag)} &nbsp; {fname}", unsafe_allow_html=True)

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    # ── Recommendations ───────────────────────────────────────────────────────
    st.markdown("### 💡 Recommendations")
    if pa["recommendations"]:
        for rec in pa["recommendations"]:
            st.markdown(f"- {rec}")
    else:
        st.success("Your portfolio looks healthy! No major concerns detected.")

    # ── Portfolio table ───────────────────────────────────────────────────────
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    st.markdown("### Portfolio Holdings")
    pf = pa.get("portfolio_df", pd.DataFrame())
    if not pf.empty:
        pf_display = pf[["fund_name", "category", "amount_invested", "current_value", "purchase_date"]].copy()
        pf_display.columns = ["Fund", "Category", "Invested (₹)", "Current (₹)", "Purchase Date"]
        pf_display["Invested (₹)"] = pf_display["Invested (₹)"].apply(lambda x: f"₹{x:,.0f}")
        pf_display["Current (₹)"]  = pf_display["Current (₹)"].apply(lambda x: f"₹{x:,.0f}")
        st.dataframe(pf_display, use_container_width=True, hide_index=True)
