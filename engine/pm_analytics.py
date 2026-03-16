"""
PM-Grade Analytics Engine
===========================
Adds professional portfolio manager metrics missing from the base engine:

Risk-Adjusted Metrics:
  - Calmar Ratio, Omega Ratio, Information Ratio, Treynor Ratio
  - VaR (95%), CVaR / Expected Shortfall, Ulcer Index

Return Attribution:
  - Calendar Year Returns (year-by-year performance table)
  - Up/Down Capture Ratio (market participation analysis)
  - Win Rate vs Benchmark, Batting Average
  - Best/Worst Period Analysis

Portfolio Construction:
  - Correlation Matrix (all portfolio funds)
  - Efficient Frontier (Markowitz MPT)
  - Rebalancing Calculator
  - SWP Calculator (retirement drawdown)
  - Lumpsum vs SIP Comparison (historical simulation)
  - SIP Step-Up Calculator

Market Intelligence:
  - Index vs Active Scorecard (SPIVA India equivalent)
  - AMC Scorecard (best fund house by quality)
  - Direct vs Regular Plan cost gap
  - Drawdown Recovery Time

Investor Behaviour Tools:
  - Delay Cost Calculator
  - SIP Pause Impact
  - Rupee Cost Averaging Visualiser data

Risk Management:
  - Stress Test (2008, 2015, 2020, 2022 scenarios)
  - Drawdown Recovery Timeline
  - Concentration / Liquidity Risk Score
"""

import math
import numpy as np
import pandas as pd
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.db import get_connection
from engine.analytics import (
    load_nav, load_benchmark, load_all_funds, load_fund_info,
    compute_fund_analytics, calculate_cagr, rolling_returns,
    RISK_FREE_RATE, TRADING_DAYS,
)

# ── known Indian market stress periods ───────────────────────────────────────
STRESS_SCENARIOS = {
    "GFC 2008":        ("2008-01-01", "2009-03-31", "Global Financial Crisis — Nifty -60%"),
    "Euro Crisis 2011":("2011-01-01", "2011-12-31", "European debt crisis spillover"),
    "Taper Tantrum 2013":("2013-05-01","2013-08-31","Fed tapering — INR crash, FII exodus"),
    "Demonetisation 2016":("2016-11-08","2016-12-31","Sudden cash ban — liquidity shock"),
    "NBFC Crisis 2018":("2018-09-01", "2019-03-31","IL&FS default — credit market freeze"),
    "COVID Crash 2020":("2020-01-20", "2020-03-31","Fastest -40% crash in history"),
    "Rate Hike 2022":  ("2022-01-01", "2022-06-30","RBI/Fed rate hikes — growth scare"),
}


# ══════════════════════════════════════════════════════════════════════════════
# RISK-ADJUSTED METRICS
# ══════════════════════════════════════════════════════════════════════════════

def calmar_ratio(nav: pd.Series) -> Optional[float]:
    """CAGR / abs(Max Drawdown). Higher = better drawdown-adjusted return."""
    cagr = calculate_cagr(nav, min(3, len(nav) / TRADING_DAYS))
    if cagr is None or cagr <= 0:
        return None
    from engine.analytics import calculate_max_drawdown
    dd = calculate_max_drawdown(nav)
    if dd is None or dd == 0:
        return None
    return round(cagr / abs(dd), 4)


def treynor_ratio(nav: pd.Series, bench: pd.Series, rfr: float = RISK_FREE_RATE) -> Optional[float]:
    """(Portfolio return - Rfr) / Beta. Return per unit of systematic risk."""
    from engine.analytics import calculate_beta_alpha
    beta, _ = calculate_beta_alpha(nav, bench)
    if beta is None or abs(beta) < 0.01:
        return None
    ret = daily_return_series(nav)
    if ret.empty:
        return None
    ann_ret = (1 + ret.mean()) ** TRADING_DAYS - 1
    return round((ann_ret - rfr) / abs(beta), 4)


def information_ratio(nav: pd.Series, bench: pd.Series) -> Optional[float]:
    """Active return / Tracking Error. Efficiency of active management."""
    ret_f = daily_return_series(nav)
    ret_b = daily_return_series(bench)
    if ret_f.empty or ret_b.empty:
        return None
    aligned = pd.DataFrame({"f": ret_f, "b": ret_b}).dropna()
    if len(aligned) < 30:
        return None
    active_ret = aligned["f"] - aligned["b"]
    tracking_error = active_ret.std() * math.sqrt(TRADING_DAYS)
    if tracking_error < 1e-8:
        return None
    ann_active_ret = active_ret.mean() * TRADING_DAYS
    return round(ann_active_ret / tracking_error, 4)


def omega_ratio(nav: pd.Series, threshold: float = RISK_FREE_RATE) -> Optional[float]:
    """
    Omega = sum(gains above threshold) / sum(losses below threshold).
    Captures full return distribution, not just variance.
    """
    ret = daily_return_series(nav)
    if ret.empty:
        return None
    daily_threshold = threshold / TRADING_DAYS
    gains = (ret - daily_threshold).clip(lower=0).sum()
    losses = (daily_threshold - ret).clip(lower=0).sum()
    if losses < 1e-10:
        return None
    return round(gains / losses, 4)


def var_95(nav: pd.Series) -> Optional[float]:
    """
    Value at Risk at 95% confidence (1-day, historical simulation).
    Negative value: e.g. -0.018 means max daily loss is 1.8% with 95% confidence.
    """
    ret = daily_return_series(nav)
    if len(ret) < 30:
        return None
    return round(float(np.percentile(ret, 5)), 6)


def cvar_95(nav: pd.Series) -> Optional[float]:
    """
    Conditional VaR / Expected Shortfall — average loss beyond the 5th percentile.
    Shows what happens in tail events beyond VaR.
    """
    ret = daily_return_series(nav)
    if len(ret) < 30:
        return None
    cutoff = np.percentile(ret, 5)
    tail = ret[ret <= cutoff]
    return round(float(tail.mean()), 6) if len(tail) > 0 else None


def ulcer_index(nav: pd.Series) -> Optional[float]:
    """
    Ulcer Index = sqrt(mean(drawdown²)).
    Penalises both depth AND duration of drawdowns.
    More comprehensive than max drawdown.
    """
    if nav.empty:
        return None
    peak = nav.cummax()
    drawdown_pct = ((nav - peak) / peak * 100) ** 2
    return round(math.sqrt(float(drawdown_pct.mean())), 4)


def daily_return_series(nav: pd.Series) -> pd.Series:
    return nav.pct_change().dropna()


# ══════════════════════════════════════════════════════════════════════════════
# RETURN ATTRIBUTION
# ══════════════════════════════════════════════════════════════════════════════

def calendar_year_returns(fund_id: str) -> dict:
    """
    Year-by-year returns table for fund vs benchmark.
    Essential for seeing how fund behaved in 2020 crash, 2017 bull market etc.
    """
    nav   = load_nav(fund_id)
    info  = load_fund_info(fund_id)
    bench = load_benchmark(info.get("benchmark", "Nifty 50"))

    if nav.empty:
        return {"fund_id": fund_id, "error": "No NAV data"}

    def annual_return(series: pd.Series) -> dict:
        if series.empty:
            return {}
        years = sorted(series.index.year.unique())
        result = {}
        for yr in years:
            yr_data = series[series.index.year == yr]
            if len(yr_data) < 20:   # skip partial years
                continue
            ret = (yr_data.iloc[-1] / yr_data.iloc[0]) - 1
            result[str(yr)] = round(float(ret), 6)
        return result

    fund_annual  = annual_return(nav)
    bench_annual = annual_return(bench)

    years = sorted(set(fund_annual.keys()) & set(bench_annual.keys()))
    table = []
    for yr in years:
        f = fund_annual.get(yr)
        b = bench_annual.get(yr)
        table.append({
            "year":       int(yr),
            "fund":       f,
            "benchmark":  b,
            "excess":     round(f - b, 6) if f is not None and b is not None else None,
            "outperformed": f > b if f is not None and b is not None else None,
        })

    years_outperformed = sum(1 for r in table if r["outperformed"])
    return {
        "fund_id":          fund_id,
        "fund_name":        info.get("fund_name", fund_id),
        "benchmark":        info.get("benchmark", "Nifty 50"),
        "annual_returns":   table,
        "years_tracked":    len(table),
        "years_outperformed": years_outperformed,
        "consistency_pct":  round(years_outperformed / len(table) * 100, 1) if table else None,
    }


def up_down_capture(fund_id: str) -> dict:
    """
    Up Capture = fund return in up months / benchmark return in up months.
    Down Capture = fund return in down months / benchmark return in down months.

    Ideal: Up Capture > 100%, Down Capture < 100%.
    """
    nav   = load_nav(fund_id)
    info  = load_fund_info(fund_id)
    bench = load_benchmark(info.get("benchmark", "Nifty 50"))

    if nav.empty or bench.empty:
        return {"fund_id": fund_id, "error": "Insufficient data"}

    # Monthly returns
    nav_m   = nav.resample("ME").last().pct_change().dropna()
    bench_m = bench.resample("ME").last().pct_change().dropna()

    aligned = pd.DataFrame({"f": nav_m, "b": bench_m}).dropna()
    if len(aligned) < 12:
        return {"fund_id": fund_id, "error": "Need at least 12 months"}

    up_months   = aligned[aligned["b"] > 0]
    down_months = aligned[aligned["b"] < 0]

    def capture(sub):
        if len(sub) < 3:
            return None
        cum_f = (1 + sub["f"]).prod() ** (12 / len(sub)) - 1
        cum_b = (1 + sub["b"]).prod() ** (12 / len(sub)) - 1
        return round(cum_f / cum_b * 100, 2) if abs(cum_b) > 1e-8 else None

    up_cap   = capture(up_months)
    down_cap = capture(down_months)

    # Capture ratio (up/down) — higher is better
    capture_ratio = round(up_cap / down_cap, 2) if up_cap and down_cap and down_cap != 0 else None

    return {
        "fund_id":         fund_id,
        "fund_name":       info.get("fund_name", fund_id),
        "benchmark":       info.get("benchmark", "Nifty 50"),
        "up_capture_pct":  up_cap,
        "down_capture_pct":down_cap,
        "capture_ratio":   capture_ratio,
        "up_months":       len(up_months),
        "down_months":     len(down_months),
        "interpretation": (
            f"Captures {up_cap:.0f}% of benchmark upsides, "
            f"{down_cap:.0f}% of downsides. "
            + ("Excellent risk management." if down_cap and down_cap < 85 and up_cap and up_cap > 95
               else "Balanced participation." if down_cap and 85 <= down_cap <= 100
               else "Review downside protection.")
        ) if up_cap and down_cap else "Insufficient data",
    }


def win_rate_vs_benchmark(fund_id: str, window_months: int = 1) -> dict:
    """
    % of rolling N-month periods where fund beats benchmark.
    batting_average = win_rate for 12-month rolling windows.
    """
    nav   = load_nav(fund_id)
    info  = load_fund_info(fund_id)
    bench = load_benchmark(info.get("benchmark", "Nifty 50"))

    if nav.empty or bench.empty:
        return {"fund_id": fund_id, "error": "No data"}

    nav_m   = nav.resample("ME").last().pct_change().dropna()
    bench_m = bench.resample("ME").last().pct_change().dropna()

    results = {}
    for months in [1, 3, 6, 12, 36]:
        if months == 1:
            aligned = pd.DataFrame({"f": nav_m, "b": bench_m}).dropna()
            wins = (aligned["f"] > aligned["b"]).sum()
            total = len(aligned)
        else:
            # Rolling N-month
            nav_roll   = nav_m.rolling(months).apply(lambda x: (1+x).prod()-1)
            bench_roll = bench_m.rolling(months).apply(lambda x: (1+x).prod()-1)
            aligned    = pd.DataFrame({"f": nav_roll, "b": bench_roll}).dropna()
            wins  = (aligned["f"] > aligned["b"]).sum()
            total = len(aligned)

        results[f"{months}M"] = {
            "win_rate": round(wins / total * 100, 1) if total > 0 else None,
            "wins":     int(wins),
            "total":    total,
        }

    return {
        "fund_id":    fund_id,
        "fund_name":  info.get("fund_name", fund_id),
        "win_rates":  results,
        "batting_average_12M": results.get("12M", {}).get("win_rate"),
        "note": "Win rate = % of periods fund beat benchmark. Batting average = 12-month window.",
    }


def best_worst_periods(fund_id: str) -> dict:
    """Best and worst returns over 1M, 3M, 6M, 1Y, 3Y periods."""
    nav = load_nav(fund_id)
    if nav.empty:
        return {"fund_id": fund_id, "error": "No data"}

    info = load_fund_info(fund_id)
    result = {}

    for label, months in [("1M", 1), ("3M", 3), ("6M", 6), ("12M", 12), ("36M", 36)]:
        nav_m = nav.resample("ME").last()
        if len(nav_m) < months + 1:
            continue
        roll = nav_m.pct_change(months).dropna()
        if roll.empty:
            continue
        best_idx  = roll.idxmax()
        worst_idx = roll.idxmin()
        result[label] = {
            "best":       round(float(roll.max()), 4),
            "best_date":  str(best_idx.date()),
            "worst":      round(float(roll.min()), 4),
            "worst_date": str(worst_idx.date()),
            "median":     round(float(roll.median()), 4),
        }

    return {
        "fund_id":   fund_id,
        "fund_name": info.get("fund_name", fund_id),
        "periods":   result,
    }


# ══════════════════════════════════════════════════════════════════════════════
# PORTFOLIO CONSTRUCTION
# ══════════════════════════════════════════════════════════════════════════════

def correlation_matrix(fund_ids: List[str]) -> dict:
    """Pairwise return correlation for a list of funds."""
    returns = {}
    names   = {}
    for fid in fund_ids:
        nav = load_nav(fid)
        info = load_fund_info(fid)
        names[fid] = info.get("fund_name", fid)[:30]
        if not nav.empty:
            returns[fid] = nav.pct_change().dropna()

    if len(returns) < 2:
        return {"error": "Need at least 2 funds with data"}

    df   = pd.DataFrame(returns).dropna()
    corr = df.corr().round(4)

    matrix = {}
    for fid_a in fund_ids:
        matrix[fid_a] = {}
        for fid_b in fund_ids:
            val = corr.loc[fid_a, fid_b] if fid_a in corr.index and fid_b in corr.columns else None
            matrix[fid_a][fid_b] = float(val) if val is not None and not math.isnan(val) else None

    # High correlation warnings (>0.85 between different funds)
    warnings = []
    fids = list(returns.keys())
    for i, fa in enumerate(fids):
        for fb in fids[i+1:]:
            c = matrix.get(fa, {}).get(fb)
            if c is not None and c > 0.85:
                warnings.append({
                    "fund_a": fa, "fund_b": fb,
                    "fund_a_name": names.get(fa,""), "fund_b_name": names.get(fb,""),
                    "correlation": c,
                    "interpretation": f"Very high correlation ({c:.2f}) — these funds move together; owning both adds little diversification",
                })

    return {
        "fund_ids":   fund_ids,
        "fund_names": names,
        "matrix":     matrix,
        "high_correlation_warnings": warnings,
        "note": "Pearson correlation of daily returns. > 0.85 = redundant diversification.",
    }


def efficient_frontier(fund_ids: List[str], n_portfolios: int = 2000) -> dict:
    """
    Markowitz Mean-Variance Efficient Frontier.
    Returns frontier portfolios + maximum Sharpe + minimum variance portfolio.
    """
    returns_dict = {}
    for fid in fund_ids:
        nav = load_nav(fid)
        if not nav.empty:
            returns_dict[fid] = nav.pct_change().dropna()

    if len(returns_dict) < 2:
        return {"error": "Need at least 2 funds with NAV data"}

    df          = pd.DataFrame(returns_dict).dropna()
    mu          = df.mean() * TRADING_DAYS             # annualised expected returns
    sigma       = df.cov() * TRADING_DAYS              # annualised covariance
    n           = len(returns_dict)
    rng         = np.random.default_rng(42)

    portfolios = []
    for _ in range(n_portfolios):
        w = rng.dirichlet(np.ones(n))
        port_ret = float(np.dot(w, mu))
        port_vol = float(np.sqrt(np.dot(w, np.dot(sigma.values, w))))
        port_sr  = (port_ret - RISK_FREE_RATE) / port_vol if port_vol > 0 else 0
        portfolios.append({
            "weights":  dict(zip(returns_dict.keys(), [round(float(x), 4) for x in w])),
            "return":   round(port_ret, 4),
            "volatility": round(port_vol, 4),
            "sharpe":   round(port_sr, 4),
        })

    # Key portfolios
    max_sharpe_port = max(portfolios, key=lambda p: p["sharpe"])
    min_vol_port    = min(portfolios, key=lambda p: p["volatility"])

    # Frontier (Pareto-efficient) — thin out for API response
    portfolios.sort(key=lambda p: p["volatility"])
    frontier = portfolios[::max(1, n_portfolios // 100)]   # 1% sample

    names = {}
    for fid in fund_ids:
        info = load_fund_info(fid)
        names[fid] = info.get("fund_name", fid)[:30]

    return {
        "fund_ids":        list(returns_dict.keys()),
        "fund_names":      names,
        "max_sharpe":      max_sharpe_port,
        "min_volatility":  min_vol_port,
        "frontier_sample": frontier,
        "total_simulations": n_portfolios,
        "note": "Monte Carlo random portfolios. Max Sharpe = best risk-adjusted. Min Vol = lowest risk.",
    }


def swp_calculator(
    fund_id:          str,
    initial_corpus:   float,
    monthly_withdrawal: float,
    years:            int = 20,
    simulations:      int = 2000,
    seed:             int = 42,
) -> dict:
    """
    Systematic Withdrawal Plan (SWP) Monte Carlo — retirement drawdown sustainability.

    Uses bootstrap simulation of this fund's actual returns.
    Returns: probability corpus survives N years, median corpus at end.
    """
    nav = load_nav(fund_id)
    if len(nav) < 252:
        return {"error": "Insufficient NAV history"}

    daily_rets   = nav.pct_change().dropna().values
    rng          = np.random.default_rng(seed)
    total_months = years * 12
    block_size   = 21
    n_blocks     = math.ceil(years * TRADING_DAYS / block_size)
    n_hist       = len(daily_rets)

    survived    = 0
    final_corpora = []

    for _ in range(simulations):
        start_idxs = rng.integers(0, max(1, n_hist - block_size), size=n_blocks)
        path = np.concatenate([daily_rets[s: s + block_size] for s in start_idxs])
        path = path[:years * TRADING_DAYS]

        cum_prod  = np.cumprod(1 + path)
        month_end = [i * TRADING_DAYS // 12 for i in range(1, total_months + 1)]
        month_end = [min(d, len(cum_prod) - 1) for d in month_end]
        monthly_nav = cum_prod[month_end]

        corpus = initial_corpus
        depleted = False
        for m in range(total_months):
            # Grow corpus by monthly return
            if m > 0:
                growth = monthly_nav[m] / monthly_nav[m-1] - 1
                corpus *= (1 + growth)
            corpus -= monthly_withdrawal
            if corpus <= 0:
                depleted = True
                corpus = 0
                break

        if not depleted:
            survived += 1
        final_corpora.append(max(0, corpus))

    survival_rate    = survived / simulations * 100
    final_corpora_np = np.array(final_corpora)

    info = load_fund_info(fund_id)
    return {
        "fund_id":            fund_id,
        "fund_name":          info.get("fund_name", fund_id),
        "initial_corpus":     initial_corpus,
        "monthly_withdrawal": monthly_withdrawal,
        "years":              years,
        "simulations":        simulations,
        "survival_rate_pct":  round(survival_rate, 1),
        "final_corpus": {
            "p10": round(float(np.percentile(final_corpora_np, 10))),
            "p50": round(float(np.percentile(final_corpora_np, 50))),
            "p90": round(float(np.percentile(final_corpora_np, 90))),
        },
        "interpretation": (
            f"With ₹{monthly_withdrawal:,.0f}/month withdrawal from ₹{initial_corpus:,.0f} corpus, "
            f"the fund sustains withdrawals for {years} years in {survival_rate:.0f}% of historical scenarios. "
            + ("High confidence — very safe withdrawal rate." if survival_rate >= 85
               else "Moderate risk — consider reducing monthly withdrawal." if survival_rate >= 65
               else "High depletion risk — reduce withdrawal or grow corpus first.")
        ),
        "note": "Bootstrap Monte Carlo using historical returns. Past performance does not guarantee future results.",
    }


def lumpsum_vs_sip(
    fund_id:      str,
    total_amount: float,
    years:        int,
) -> dict:
    """
    Compare: investing total_amount as lumpsum on day 1 vs spreading as monthly SIP.
    Uses actual historical NAV data for simulation.
    """
    nav = load_nav(fund_id)
    if len(nav) < 252:
        return {"error": "Insufficient NAV history"}

    info = load_fund_info(fund_id)
    monthly_sip = total_amount / (years * 12)

    # Use latest N-year window
    cutoff = nav.index.max() - pd.DateOffset(years=years)
    period = nav[nav.index >= cutoff]
    if len(period) < 50:
        return {"error": "Insufficient data for this period"}

    start_nav = period.iloc[0]
    end_nav   = period.iloc[-1]

    # Lumpsum
    lumpsum_corpus = total_amount * (end_nav / start_nav)
    lumpsum_cagr   = (lumpsum_corpus / total_amount) ** (1 / years) - 1

    # SIP simulation using actual NAV
    nav_m    = period.resample("ME").last()
    sip_units = 0.0
    for m_nav in nav_m:
        if m_nav > 0:
            sip_units += monthly_sip / m_nav
    sip_corpus = sip_units * float(end_nav)
    sip_cagr   = (sip_corpus / total_amount) ** (1 / years) - 1

    winner = "lumpsum" if lumpsum_corpus > sip_corpus else "sip"
    diff   = abs(lumpsum_corpus - sip_corpus)

    return {
        "fund_id":          fund_id,
        "fund_name":        info.get("fund_name", fund_id),
        "total_amount":     total_amount,
        "years":            years,
        "monthly_sip":      round(monthly_sip),
        "lumpsum": {
            "corpus": round(lumpsum_corpus),
            "cagr":   round(lumpsum_cagr, 4),
        },
        "sip": {
            "corpus": round(sip_corpus),
            "cagr":   round(sip_cagr, 4),
        },
        "winner":      winner,
        "difference":  round(diff),
        "interpretation": (
            f"Over this {years}-year period with this fund, "
            f"{'lumpsum' if winner=='lumpsum' else 'SIP'} outperformed by ₹{diff:,.0f}. "
            "Note: lumpsum is better in strongly rising markets; SIP reduces timing risk."
        ),
    }


def sip_stepup_calculator(
    fund_id:       str,
    initial_sip:   float,
    stepup_pct:    float,   # annual increase, e.g. 0.10 = 10%
    years:         int,
    simulations:   int = 2000,
    seed:          int = 42,
) -> dict:
    """
    Step-Up SIP: SIP amount increases by stepup_pct every year.
    Compare corpus vs flat SIP using Monte Carlo.
    """
    from engine.advanced_analytics import sip_monte_carlo

    # Total invested — flat SIP
    total_flat = initial_sip * 12 * years

    # Total invested — step-up SIP
    total_stepup = 0
    monthly_amounts = []
    current = initial_sip
    for yr in range(years):
        for _ in range(12):
            monthly_amounts.append(current)
            total_stepup += current
        current *= (1 + stepup_pct)

    # Run MC for flat SIP
    flat_mc = sip_monte_carlo(fund_id, initial_sip, years, simulations, seed)
    if "error" in flat_mc:
        return flat_mc

    # Step-up simulation using same random paths as flat (for fair comparison)
    nav = load_nav(fund_id)
    daily_rets = nav.pct_change().dropna().values
    rng = np.random.default_rng(seed)
    block_size = 21
    n_blocks   = math.ceil(years * TRADING_DAYS / block_size)
    n_hist     = len(daily_rets)
    total_months = years * 12
    stepup_corpora = []

    for _ in range(simulations):
        start_idxs = rng.integers(0, max(1, n_hist - block_size), size=n_blocks)
        path = np.concatenate([daily_rets[s: s + block_size] for s in start_idxs])[:years * TRADING_DAYS]
        cum_prod  = np.cumprod(1 + path)
        month_end = [i * TRADING_DAYS // 12 for i in range(1, total_months + 1)]
        month_end = [min(d, len(cum_prod) - 1) for d in month_end]
        monthly_nav = cum_prod[month_end]
        corpus = sum(
            monthly_amounts[m] / monthly_nav[m] * monthly_nav[-1]
            for m in range(total_months) if monthly_nav[m] > 0
        )
        stepup_corpora.append(corpus)

    su_arr = np.array(stepup_corpora)
    info = load_fund_info(fund_id)

    return {
        "fund_id":       fund_id,
        "fund_name":     info.get("fund_name", fund_id),
        "initial_sip":   initial_sip,
        "stepup_pct":    stepup_pct,
        "years":         years,
        "flat_sip": {
            "total_invested": round(total_flat),
            "corpus_p50":    flat_mc["outcomes"]["p50"],
            "corpus_p10":    flat_mc["outcomes"]["p10"],
            "corpus_p90":    flat_mc["outcomes"]["p90"],
        },
        "stepup_sip": {
            "total_invested":round(total_stepup),
            "final_monthly": round(monthly_amounts[-1]),
            "corpus_p50":    round(float(np.percentile(su_arr, 50))),
            "corpus_p10":    round(float(np.percentile(su_arr, 10))),
            "corpus_p90":    round(float(np.percentile(su_arr, 90))),
        },
        "extra_corpus_p50": round(float(np.percentile(su_arr, 50)) - flat_mc["outcomes"]["p50"]),
        "note": f"Step-up SIP increases {stepup_pct:.0%} annually. Extra corpus at median outcome shown.",
    }


# ══════════════════════════════════════════════════════════════════════════════
# MARKET INTELLIGENCE
# ══════════════════════════════════════════════════════════════════════════════

def index_vs_active_scorecard() -> dict:
    """
    SPIVA India equivalent: % of active funds that beat their index over 1Y/3Y/5Y.
    Groups by category, compares vs category benchmark.
    """
    funds_df = load_all_funds()
    results  = {}

    equity_categories = [
        "Equity: Large Cap", "Equity: Mid Cap", "Equity: Small Cap",
        "Equity: Flexi Cap", "Equity: Multi Cap",
    ]

    for cat in equity_categories:
        cat_funds = funds_df[funds_df["category"] == cat]
        if cat_funds.empty:
            continue

        beats = {"1y": 0, "3y": 0, "5y": 0}
        total = {"1y": 0, "3y": 0, "5y": 0}
        active_returns = {"1y": [], "3y": [], "5y": []}

        for _, row in cat_funds.iterrows():
            an   = compute_fund_analytics(row["fund_id"])
            info = load_fund_info(row["fund_id"])
            if "error" in an:
                continue
            bench = load_benchmark(info.get("benchmark", "Nifty 50"))
            if bench.empty:
                continue

            for yr_key, yr in [("1y", 1), ("3y", 3), ("5y", 5)]:
                from engine.analytics import calculate_cagr
                f_ret = calculate_cagr(load_nav(row["fund_id"]), yr)
                b_ret = calculate_cagr(bench, yr)
                if f_ret is not None and b_ret is not None:
                    total[yr_key] += 1
                    if f_ret > b_ret:
                        beats[yr_key] += 1
                    active_returns[yr_key].append(f_ret)

        results[cat] = {
            "pct_beating_index": {
                yr: round(beats[yr] / total[yr] * 100, 1) if total[yr] > 0 else None
                for yr in ["1y", "3y", "5y"]
            },
            "total_funds":  total["5y"],
            "median_active_return_5y": round(float(np.median(active_returns["5y"])), 4) if active_returns["5y"] else None,
        }

    return {
        "scorecard":     results,
        "note": "% of active funds that beat their category benchmark. Data-driven SPIVA India equivalent.",
        "insight": "Globally, 80-90% of active funds underperform index over 10Y. India data shows similar trend.",
    }


def amc_scorecard() -> dict:
    """Rank AMCs by average quality score across all their funds."""
    from engine.advanced_analytics import compute_quality_score
    funds_df = load_all_funds()
    amc_data: Dict[str, List[float]] = {}

    for _, row in funds_df.iterrows():
        amc = row.get("amc", "Unknown")
        if not amc:
            continue
        qs = compute_quality_score(row["fund_id"])
        score = qs.get("quality_score")
        if score is not None:
            if amc not in amc_data:
                amc_data[amc] = []
            amc_data[amc].append(score)

    scoreboard = []
    for amc, scores in amc_data.items():
        scoreboard.append({
            "amc":          amc,
            "fund_count":   len(scores),
            "avg_quality":  round(float(np.mean(scores)), 1),
            "top_score":    round(float(np.max(scores)), 1),
            "consistency":  round(float(np.std(scores)), 1),   # lower = more consistent quality
        })

    scoreboard.sort(key=lambda x: -x["avg_quality"])
    return {
        "amc_rankings": scoreboard,
        "note": "AMCs ranked by average Fund Quality Score across all their funds. Consistency = std dev (lower = more uniform quality).",
    }


def direct_vs_regular_gap(fund_id_direct: str, fund_id_regular: str, years: int = 5) -> dict:
    """
    Quantify the real rupee cost of investing in Regular vs Direct plan.
    Most retail investors never see this number. It's damning.
    """
    nav_d  = load_nav(fund_id_direct)
    nav_r  = load_nav(fund_id_regular)
    info_d = load_fund_info(fund_id_direct)
    info_r = load_fund_info(fund_id_regular)

    if nav_d.empty or nav_r.empty:
        return {"error": "NAV data missing for one or both plans"}

    # Align on common dates
    cutoff    = max(nav_d.index.min(), nav_r.index.min())
    nav_d_cut = nav_d[nav_d.index >= cutoff]
    nav_r_cut = nav_r[nav_r.index >= cutoff]
    min_len   = min(len(nav_d_cut), len(nav_r_cut))

    if min_len < 50:
        return {"error": "Insufficient overlapping data"}

    nav_d_sub = nav_d_cut.iloc[-min(min_len, years * 252):]
    nav_r_sub = nav_r_cut.iloc[-min(min_len, years * 252):]

    from engine.analytics import calculate_cagr
    cagr_d = calculate_cagr(nav_d_sub, min(years, len(nav_d_sub) / 252 * 0.9))
    cagr_r = calculate_cagr(nav_r_sub, min(years, len(nav_r_sub) / 252 * 0.9))

    if cagr_d is None or cagr_r is None:
        return {"error": "Could not compute CAGR for comparison period"}

    annual_gap = cagr_d - cagr_r
    er_d = info_d.get("expense_ratio") or 0
    er_r = info_r.get("expense_ratio") or 0
    er_gap = (er_r or 0) - (er_d or 0)

    # Corpus impact on ₹1L lumpsum
    corpus_direct  = 100000 * ((1 + cagr_d) ** years)
    corpus_regular = 100000 * ((1 + cagr_r) ** years)
    rupee_gap      = corpus_direct - corpus_regular

    # SIP corpus impact (₹10K/month)
    monthly_r_d = cagr_d / 12
    monthly_r_r = cagr_r / 12
    n = years * 12
    sip_corpus_d = 10000 * ((1 + monthly_r_d) ** n - 1) / monthly_r_d if monthly_r_d > 0 else 0
    sip_corpus_r = 10000 * ((1 + monthly_r_r) ** n - 1) / monthly_r_r if monthly_r_r > 0 else 0
    sip_gap      = sip_corpus_d - sip_corpus_r

    return {
        "direct_fund":   info_d.get("fund_name", fund_id_direct),
        "regular_fund":  info_r.get("fund_name", fund_id_regular),
        "years":         years,
        "cagr_direct":   round(cagr_d, 4),
        "cagr_regular":  round(cagr_r, 4),
        "annual_gap_pct":round(annual_gap * 100, 2),
        "er_direct":     er_d,
        "er_regular":    er_r,
        "er_gap":        round(er_gap, 3),
        "lumpsum_impact": {
            "invested":          100000,
            "corpus_direct":     round(corpus_direct),
            "corpus_regular":    round(corpus_regular),
            "rupee_gap":         round(rupee_gap),
        },
        "sip_impact_10k_monthly": {
            "invested":          round(n * 10000),
            "corpus_direct":     round(sip_corpus_d),
            "corpus_regular":    round(sip_corpus_r),
            "rupee_gap":         round(sip_gap),
        },
        "note": "Direct plan bypasses distributor commission. Switch now if still in Regular plan.",
    }


# ══════════════════════════════════════════════════════════════════════════════
# STRESS TESTING
# ══════════════════════════════════════════════════════════════════════════════

def stress_test(fund_id: str) -> dict:
    """
    How did this fund perform during each major Indian market crisis?
    Compares fund vs benchmark in every stress period.
    """
    nav   = load_nav(fund_id)
    info  = load_fund_info(fund_id)
    bench = load_benchmark(info.get("benchmark", "Nifty 50"))

    if nav.empty:
        return {"fund_id": fund_id, "error": "No NAV data"}

    results = {}
    for scenario, (start_str, end_str, desc) in STRESS_SCENARIOS.items():
        start = pd.Timestamp(start_str)
        end   = pd.Timestamp(end_str)

        nav_period   = nav[(nav.index >= start) & (nav.index <= end)]
        bench_period = bench[(bench.index >= start) & (bench.index <= end)]

        if len(nav_period) < 5:
            results[scenario] = {"available": False, "description": desc}
            continue

        fund_ret  = float(nav_period.iloc[-1] / nav_period.iloc[0]) - 1
        bench_ret = float(bench_period.iloc[-1] / bench_period.iloc[0]) - 1 if len(bench_period) >= 5 else None

        # Max intra-period drawdown
        peak   = nav_period.cummax()
        max_dd = float(((nav_period - peak) / peak).min())

        results[scenario] = {
            "available":   True,
            "description": desc,
            "period":      f"{start_str} to {end_str}",
            "fund_return":  round(fund_ret, 4),
            "bench_return": round(bench_ret, 4) if bench_ret is not None else None,
            "excess_return":round(fund_ret - bench_ret, 4) if bench_ret is not None else None,
            "max_drawdown": round(max_dd, 4),
            "outperformed": (fund_ret > bench_ret) if bench_ret is not None else None,
        }

    crises_survived = sum(1 for v in results.values() if v.get("available") and v.get("outperformed"))
    crises_tracked  = sum(1 for v in results.values() if v.get("available") and v.get("outperformed") is not None)

    return {
        "fund_id":        fund_id,
        "fund_name":      info.get("fund_name", fund_id),
        "benchmark":      info.get("benchmark", "Nifty 50"),
        "stress_results": results,
        "crises_outperformed": crises_survived,
        "crises_tracked":      crises_tracked,
        "note": "Historical scenario analysis. Past performance in crises doesn't guarantee future resilience.",
    }


def drawdown_recovery_time(fund_id: str) -> dict:
    """
    For each major drawdown episode, how many trading days to full recovery?
    Critical for setting investor expectations.
    """
    nav = load_nav(fund_id)
    if nav.empty:
        return {"fund_id": fund_id, "error": "No NAV data"}

    info   = load_fund_info(fund_id)
    peak   = nav.cummax()
    dd     = (nav - peak) / peak

    # Find drawdown episodes (threshold: -10%)
    episodes = []
    in_dd    = False
    dd_start = None
    dd_peak_val = None
    dd_peak_date = None

    for dt, val in dd.items():
        if not in_dd and val < -0.10:
            in_dd        = True
            dd_start     = dt
            dd_peak_val  = float(peak[dt])
            dd_peak_date = dt
        elif in_dd:
            if float(val) >= -0.005:   # recovered within 0.5% of peak
                recovery_days = (dt - dd_start).days
                episodes.append({
                    "start":          str(dd_start.date()),
                    "recovery_date":  str(dt.date()),
                    "recovery_days":  recovery_days,
                    "recovery_months": round(recovery_days / 30.44, 1),
                    "max_drawdown":   round(float(dd[dd_start:dt].min()), 4),
                })
                in_dd = False

    # Current ongoing drawdown (if any)
    current_dd = float(dd.iloc[-1])
    ongoing = None
    if current_dd < -0.05:
        ongoing = {
            "in_drawdown":    True,
            "current_dd":     round(current_dd, 4),
            "days_in_dd":     (nav.index[-1] - dd.index[dd == dd.min()].min()).days,
            "still_recovering": True,
        }

    # Stats
    if episodes:
        avg_recovery = sum(e["recovery_days"] for e in episodes) / len(episodes)
        max_recovery = max(e["recovery_days"] for e in episodes)
    else:
        avg_recovery = max_recovery = None

    return {
        "fund_id":        fund_id,
        "fund_name":      info.get("fund_name", fund_id),
        "episodes":       episodes[-10:],   # last 10 episodes
        "total_episodes": len(episodes),
        "avg_recovery_days": round(avg_recovery) if avg_recovery else None,
        "max_recovery_days": max_recovery,
        "current_status":    ongoing,
        "note": "Drawdown episodes of >10% and days to full recovery. Helps investors know what to expect.",
    }


# ══════════════════════════════════════════════════════════════════════════════
# INVESTOR BEHAVIOUR TOOLS
# ══════════════════════════════════════════════════════════════════════════════

def delay_cost_calculator(
    fund_id:      str,
    monthly_sip:  float,
    total_years:  int,
    delay_years:  int,
) -> dict:
    """
    Quantify the rupee cost of delaying SIP start by N years.
    The most powerful investor education tool — shows procrastination cost.
    """
    nav = load_nav(fund_id)
    if len(nav) < 252:
        return {"error": "Insufficient data"}

    info    = load_fund_info(fund_id)
    ret_5y  = calculate_cagr(nav, min(5, len(nav)/252 * 0.9)) or 0.12
    monthly_r = ret_5y / 12

    def sip_corpus(years):
        n = years * 12
        if monthly_r <= 0 or n <= 0:
            return monthly_sip * n
        return monthly_sip * ((1 + monthly_r) ** n - 1) / monthly_r

    corpus_now     = sip_corpus(total_years)
    corpus_delayed = sip_corpus(total_years - delay_years)
    invested_now   = monthly_sip * total_years * 12
    invested_del   = monthly_sip * (total_years - delay_years) * 12

    return {
        "fund_id":           fund_id,
        "fund_name":         info.get("fund_name", fund_id),
        "monthly_sip":       monthly_sip,
        "assumed_cagr":      round(ret_5y, 4),
        "start_now": {
            "years":      total_years,
            "invested":   round(invested_now),
            "corpus":     round(corpus_now),
        },
        "start_after_delay": {
            "years":      total_years - delay_years,
            "invested":   round(invested_del),
            "corpus":     round(corpus_delayed),
        },
        "delay_cost_rupees": round(corpus_now - corpus_delayed),
        "delay_years":       delay_years,
        "note": f"Delaying SIP by {delay_years} years costs ₹{corpus_now - corpus_delayed:,.0f} in final corpus (using {ret_5y:.1%} CAGR from this fund).",
    }


# ══════════════════════════════════════════════════════════════════════════════
# FULL ANALYTICS BUNDLE (all metrics in one call)
# ══════════════════════════════════════════════════════════════════════════════

def full_pm_analytics(fund_id: str) -> dict:
    """
    Complete PM-grade analytics bundle for one fund.
    Combines all base + advanced + PM metrics.
    """
    nav   = load_nav(fund_id)
    info  = load_fund_info(fund_id)
    bench = load_benchmark(info.get("benchmark", "Nifty 50"))

    if nav.empty:
        return {"fund_id": fund_id, "error": "No NAV data"}

    from engine.analytics import (
        calculate_volatility, calculate_max_drawdown,
        calculate_sharpe_ratio, calculate_sortino_ratio, calculate_beta_alpha,
    )
    from engine.advanced_analytics import compute_quality_score

    base  = compute_fund_analytics(fund_id)
    qs    = compute_quality_score(fund_id)
    cal   = calendar_year_returns(fund_id)
    udc   = up_down_capture(fund_id)
    wr    = win_rate_vs_benchmark(fund_id)
    bw    = best_worst_periods(fund_id)
    st    = stress_test(fund_id)
    drt   = drawdown_recovery_time(fund_id)

    return {
        "fund_id":      fund_id,
        "fund_name":    info.get("fund_name", fund_id),
        "amc":          info.get("amc", ""),
        "category":     info.get("category", ""),
        "benchmark":    info.get("benchmark", ""),
        "expense_ratio":info.get("expense_ratio"),

        # Core returns
        "return_1y":    base.get("return_1y"),
        "return_3y":    base.get("return_3y"),
        "return_5y":    base.get("return_5y"),
        "return_10y":   base.get("return_10y"),

        # Standard risk
        "volatility":   base.get("volatility"),
        "max_drawdown": base.get("max_drawdown"),
        "sharpe_ratio": base.get("sharpe_ratio"),
        "sortino_ratio":base.get("sortino_ratio"),
        "beta":         base.get("beta"),
        "alpha":        base.get("alpha"),

        # PM-grade risk metrics
        "calmar_ratio":     calmar_ratio(nav),
        "treynor_ratio":    treynor_ratio(nav, bench),
        "information_ratio":information_ratio(nav, bench),
        "omega_ratio":      omega_ratio(nav),
        "var_95":           var_95(nav),
        "cvar_95":          cvar_95(nav),
        "ulcer_index":      ulcer_index(nav),

        # Attribution
        "calendar_returns": cal.get("annual_returns", []),
        "years_outperformed":cal.get("years_outperformed"),
        "years_tracked":    cal.get("years_tracked"),
        "consistency_pct":  cal.get("consistency_pct"),
        "up_capture":       udc.get("up_capture_pct"),
        "down_capture":     udc.get("down_capture_pct"),
        "capture_ratio":    udc.get("capture_ratio"),
        "batting_average":  wr.get("batting_average_12M"),
        "win_rates":        wr.get("win_rates"),
        "best_worst":       bw.get("periods"),

        # Quality
        "quality_score":    qs.get("quality_score"),
        "quality_grade":    qs.get("grade"),
        "quality_breakdown":qs.get("breakdown"),

        # Stress & recovery
        "stress_test":      st.get("stress_results"),
        "crises_outperformed": st.get("crises_outperformed"),
        "drawdown_recovery": drt.get("episodes"),
        "avg_recovery_days": drt.get("avg_recovery_days"),
    }
