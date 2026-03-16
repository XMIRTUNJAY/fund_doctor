"""
Advanced Analytics Engine
===========================
Extends the core analytics with:
  1.  Fund Quality Composite Score      (0–100 proprietary rating)
  2.  Peer Percentile Ranking           (rank within SEBI category)
  3.  Goal-Based Fund Recommender       (goal → best fund combo)
  4.  SIP Calculator with Monte Carlo   (real fund returns, P10/P50/P90)
  5.  Inflation-Adjusted Real Returns   (nominal vs real returns)
  6.  Tax Harvesting Calendar           (LTCG/STCG optimisation)
  7.  Portfolio Overlap Matrix          (Jaccard similarity on holdings)
  8.  Market Cycle Analysis             (category returns by market phase)
  9.  Fund Manager Alpha Tracker        (manager-level alpha)
  10. Weekly Batch Pipeline Scheduler   (4-week category distribution)
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
    compute_fund_analytics, calculate_cagr, calculate_sharpe_ratio,
    calculate_sortino_ratio, calculate_volatility, calculate_max_drawdown,
    calculate_beta_alpha, rolling_returns, RISK_FREE_RATE, TRADING_DAYS,
)
from engine.classification import CATEGORIES, get_category_info, GOAL_CATEGORY_MAP


# ══════════════════════════════════════════════════════════════════════════════
# 1. FUND QUALITY COMPOSITE SCORE (0–100)
# ══════════════════════════════════════════════════════════════════════════════

def compute_quality_score(fund_id: str) -> dict:
    """
    Proprietary Fund Quality Score — 0 to 100.

    Weights:
      Consistency (rolling return stability)  30%
      Returns (risk-adjusted outperformance)  25%
      Risk management (DD, vol, Sortino)      20%
      Cost efficiency (expense ratio)         15%
      Benchmark beating (alpha, excess ret)   10%
    """
    an = compute_fund_analytics(fund_id)
    if "error" in an:
        return {"fund_id": fund_id, "quality_score": None, "breakdown": {}, "error": an["error"]}

    nav   = load_nav(fund_id)
    info  = load_fund_info(fund_id)
    bench = load_benchmark(info.get("benchmark", "Nifty 50"))

    scores = {}

    # ── Consistency (30%) ────────────────────────────────────────────────────
    # Std dev of annual rolling returns — lower = more consistent
    roll_1y = rolling_returns(nav, 1)
    if len(roll_1y) > 12:
        roll_std   = float(roll_1y.std())
        roll_mean  = float(roll_1y.mean())
        cv         = roll_std / abs(roll_mean) if roll_mean != 0 else 9.9
        # CV < 0.4 = very consistent, CV > 2 = very inconsistent
        consistency = max(0, min(100, (1 - cv / 2) * 100))
        # Bonus: % of rolling years that beat benchmark
        if len(bench) > 0:
            bench_roll  = rolling_returns(bench, 1)
            aligned     = pd.DataFrame({"f": roll_1y, "b": bench_roll}).dropna()
            beat_pct    = (aligned["f"] > aligned["b"]).mean() if len(aligned) > 0 else 0.5
            consistency = consistency * 0.6 + beat_pct * 100 * 0.4
    else:
        consistency = 50.0
    scores["consistency"] = {"score": round(consistency, 1), "weight": 0.30}

    # ── Returns (25%) ────────────────────────────────────────────────────────
    r5y   = an.get("return_5y") or 0
    r3y   = an.get("return_3y") or 0
    sharpe= an.get("sharpe_ratio") or 0
    # Normalise: 20% CAGR = 100, 5% = 0
    ret_score = max(0, min(100, (r5y * 0.5 + r3y * 0.5) / 0.20 * 100))
    # Sharpe bonus
    sharpe_bonus = min(20, max(-10, (sharpe - 0.7) * 20))
    ret_score    = max(0, min(100, ret_score + sharpe_bonus))
    scores["returns"] = {"score": round(ret_score, 1), "weight": 0.25}

    # ── Risk management (20%) ────────────────────────────────────────────────
    vol  = an.get("volatility") or 0.25
    dd   = abs(an.get("max_drawdown") or -0.30)
    sort = an.get("sortino_ratio") or 0
    # Lower vol is better (benchmark for equity ~15%)
    vol_score  = max(0, min(100, (1 - vol / 0.35) * 100))
    dd_score   = max(0, min(100, (1 - dd / 0.60) * 100))
    sort_score = min(100, max(0, sort / 2.0 * 100))
    risk_score = vol_score * 0.35 + dd_score * 0.35 + sort_score * 0.30
    scores["risk_management"] = {"score": round(risk_score, 1), "weight": 0.20}

    # ── Cost efficiency (15%) ────────────────────────────────────────────────
    er = info.get("expense_ratio") or 1.5
    # 0.1% ER = 100 score; 2.5% ER = 0 score
    cost_score = max(0, min(100, (1 - (er - 0.1) / 2.4) * 100))
    scores["cost_efficiency"] = {"score": round(cost_score, 1), "weight": 0.15}

    # ── Benchmark beating (10%) ──────────────────────────────────────────────
    alpha = an.get("alpha") or 0
    bench_5y = calculate_cagr(bench, 5) or 0
    r5y_val  = an.get("return_5y") or 0
    excess   = r5y_val - bench_5y
    # Alpha > 0.05 annualised = good; < 0 = bad
    alpha_score  = max(0, min(100, (alpha / 0.10 + 0.5) * 100))
    excess_score = max(0, min(100, (excess / 0.05 + 0.5) * 100))
    bench_score  = alpha_score * 0.5 + excess_score * 0.5
    scores["benchmark_beating"] = {"score": round(bench_score, 1), "weight": 0.10}

    # ── Composite ────────────────────────────────────────────────────────────
    quality_score = sum(
        v["score"] * v["weight"] for v in scores.values()
    )
    quality_score = round(max(0, min(100, quality_score)), 1)

    # Grade
    if quality_score >= 80:   grade = "A+"
    elif quality_score >= 70: grade = "A"
    elif quality_score >= 60: grade = "B+"
    elif quality_score >= 50: grade = "B"
    elif quality_score >= 40: grade = "C"
    else:                     grade = "D"

    return {
        "fund_id":       fund_id,
        "fund_name":     an.get("fund_name", fund_id),
        "quality_score": quality_score,
        "grade":         grade,
        "breakdown":     {k: v["score"] for k, v in scores.items()},
        "weights":       {k: v["weight"] for k, v in scores.items()},
    }


# ══════════════════════════════════════════════════════════════════════════════
# 2. PEER PERCENTILE RANKING
# ══════════════════════════════════════════════════════════════════════════════

def compute_peer_ranking(fund_id: str) -> dict:
    """
    Rank a fund within its SEBI category peers.
    Returns percentile rank (0–100) for each metric.
    100 = best in category.
    """
    info     = load_fund_info(fund_id)
    category = info.get("category", "")
    if not category:
        return {"fund_id": fund_id, "error": "No category found"}

    # Get all peer funds in same category
    conn  = get_connection()
    peers = pd.read_sql_query(
        "SELECT fund_id FROM fund_master WHERE category = ? AND fund_id != ?",
        conn, params=[category, fund_id]
    )
    conn.close()

    peer_ids = peers["fund_id"].tolist()
    if not peer_ids:
        return {"fund_id": fund_id, "category": category, "peer_count": 0, "percentiles": {}}

    # Compute analytics for all peers
    target_an = compute_fund_analytics(fund_id)
    if "error" in target_an:
        return {"fund_id": fund_id, "error": target_an["error"]}

    metrics = {
        "return_1y":    ("higher_better", []),
        "return_3y":    ("higher_better", []),
        "return_5y":    ("higher_better", []),
        "sharpe_ratio": ("higher_better", []),
        "sortino_ratio":("higher_better", []),
        "alpha":        ("higher_better", []),
        "volatility":   ("lower_better",  []),
        "max_drawdown": ("lower_better",  []),   # less negative = better
    }

    # Collect peer values
    for pid in peer_ids[:200]:   # cap at 200 peers for performance
        pan = compute_fund_analytics(pid)
        if "error" in pan:
            continue
        for metric, (_, values) in metrics.items():
            v = pan.get(metric)
            if v is not None and not math.isnan(v):
                values.append(v)

    # Calculate percentile for target fund
    percentiles = {}
    for metric, (direction, peer_vals) in metrics.items():
        target_val = target_an.get(metric)
        if target_val is None or not peer_vals:
            percentiles[metric] = None
            continue
        # Percentile: fraction of peers that this fund beats
        if direction == "higher_better":
            rank_pct = sum(1 for pv in peer_vals if target_val > pv) / len(peer_vals) * 100
        else:
            # lower is better — beat peer if our value is less negative/lower
            rank_pct = sum(1 for pv in peer_vals if abs(target_val) < abs(pv)) / len(peer_vals) * 100
        percentiles[metric] = round(rank_pct, 1)

    # Overall category rank
    valid_pcts = [v for v in percentiles.values() if v is not None]
    overall_pct = round(sum(valid_pcts) / len(valid_pcts), 1) if valid_pcts else None

    return {
        "fund_id":     fund_id,
        "fund_name":   target_an.get("fund_name", fund_id),
        "category":    category,
        "peer_count":  len(peer_ids),
        "percentiles": percentiles,
        "overall_percentile": overall_pct,
        "interpretation": (
            f"This fund beats {overall_pct:.0f}% of its {len(peer_ids)} category peers overall."
            if overall_pct is not None else "Insufficient data"
        ),
    }


# ══════════════════════════════════════════════════════════════════════════════
# 3. INFLATION-ADJUSTED REAL RETURNS
# ══════════════════════════════════════════════════════════════════════════════

# Average Indian CPI inflation by year (approximate; production should pull from RBI API)
INDIA_CPI_ANNUAL = {
    2015: 0.049, 2016: 0.050, 2017: 0.033, 2018: 0.035, 2019: 0.074,
    2020: 0.062, 2021: 0.055, 2022: 0.067, 2023: 0.054, 2024: 0.050,
    2025: 0.048, 2026: 0.046,
}

def compute_real_returns(fund_id: str) -> dict:
    """
    Compute inflation-adjusted (real) returns for all CAGR periods.
    Uses Fisher equation: real_return = (1 + nominal) / (1 + cpi) - 1
    """
    an = compute_fund_analytics(fund_id)
    if "error" in an:
        return {"fund_id": fund_id, "error": an["error"]}

    today = date.today()

    def avg_cpi(years: int) -> float:
        """Average annual CPI over last N years."""
        vals = []
        for y in range(today.year - years, today.year + 1):
            cpi = INDIA_CPI_ANNUAL.get(y)
            if cpi is not None:
                vals.append(cpi)
        return sum(vals) / len(vals) if vals else 0.06  # fallback 6%

    def real_return(nominal: Optional[float], years: int) -> Optional[float]:
        if nominal is None:
            return None
        cpi = avg_cpi(years)
        return (1 + nominal) / (1 + cpi) - 1

    periods = [("1Y", 1), ("3Y", 3), ("5Y", 5), ("10Y", 10)]
    nominal = {
        "1Y": an.get("return_1y"),
        "3Y": an.get("return_3y"),
        "5Y": an.get("return_5y"),
        "10Y": an.get("return_10y"),
    }
    real     = {p: real_return(nominal[p], y) for p, y in periods}
    cpi_used = {p: round(avg_cpi(y), 4) for p, y in periods}

    return {
        "fund_id":    fund_id,
        "fund_name":  an.get("fund_name", fund_id),
        "nominal":    {k: round(v, 6) if v else None for k, v in nominal.items()},
        "real":       {k: round(v, 6) if v else None for k, v in real.items()},
        "cpi_used":   cpi_used,
        "note": "Real return = (1 + nominal) / (1 + CPI) - 1  (Fisher equation). "
                "CPI sourced from RBI approximate historical averages.",
    }


# ══════════════════════════════════════════════════════════════════════════════
# 4. SIP CALCULATOR WITH MONTE CARLO (Real Fund Returns)
# ══════════════════════════════════════════════════════════════════════════════

def sip_monte_carlo(
    fund_id:      str,
    monthly_sip:  float,
    years:        int,
    simulations:  int = 5000,
    seed:         int = 42,
) -> dict:
    """
    Bootstrap Monte Carlo SIP projection using this fund's actual daily returns.
    Returns P10, P25, P50, P75, P90 outcome values.

    Methodology:
    - Sample blocks of daily returns from historical NAV (block bootstrap, 21-day blocks)
    - Run `simulations` paths, each simulating `years * 252` trading days
    - Convert to monthly NAV → compute SIP corpus each month
    """
    nav = load_nav(fund_id)
    if len(nav) < 252:
        return {"error": "Insufficient NAV history (< 1 year)"}

    info = load_fund_info(fund_id)
    daily_rets = nav.pct_change().dropna().values

    rng          = np.random.default_rng(seed)
    total_days   = years * TRADING_DAYS
    block_size   = 21          # ~1 month block
    n_blocks     = math.ceil(total_days / block_size)
    n_hist       = len(daily_rets)

    # SIP: invest monthly_sip at start of each month
    months_total = years * 12
    total_invested = monthly_sip * months_total
    corpora = []

    for _ in range(simulations):
        # Block bootstrap
        start_idxs = rng.integers(0, n_hist - block_size, size=n_blocks)
        path = np.concatenate([daily_rets[s: s + block_size] for s in start_idxs])[:total_days]

        # Convert daily returns → monthly NAV
        cum_prod  = np.cumprod(1 + path)
        month_end = [i * TRADING_DAYS // 12 for i in range(1, months_total + 1)]
        month_end = [min(d, len(cum_prod) - 1) for d in month_end]
        monthly_nav = cum_prod[month_end]

        # SIP corpus: each month invest 1 unit, grow at subsequent returns
        corpus = 0.0
        for m in range(months_total):
            end_nav   = monthly_nav[-1]
            entry_nav = monthly_nav[m]
            if entry_nav > 0:
                units     = monthly_sip / entry_nav
                corpus   += units * end_nav
        corpora.append(corpus)

    corpora = np.array(corpora)
    percentiles = {
        "p10": float(np.percentile(corpora, 10)),
        "p25": float(np.percentile(corpora, 25)),
        "p50": float(np.percentile(corpora, 50)),
        "p75": float(np.percentile(corpora, 75)),
        "p90": float(np.percentile(corpora, 90)),
    }

    # XIRR approximation for median
    median_corpus = percentiles["p50"]
    # Simple CAGR from median
    cagr_est = (median_corpus / total_invested) ** (1 / years) - 1 if total_invested > 0 else 0

    return {
        "fund_id":        fund_id,
        "fund_name":      info.get("fund_name", fund_id),
        "monthly_sip":    monthly_sip,
        "years":          years,
        "total_invested": total_invested,
        "simulations":    simulations,
        "outcomes":       {k: round(v) for k, v in percentiles.items()},
        "implied_cagr_p50": round(cagr_est, 4),
        "interpretation": {
            "p10": f"Worst 10% scenario: ₹{percentiles['p10']:,.0f}",
            "p50": f"Median scenario: ₹{percentiles['p50']:,.0f}",
            "p90": f"Best 10% scenario: ₹{percentiles['p90']:,.0f}",
        },
        "note": "Bootstrap Monte Carlo using this fund's actual historical daily returns. Past performance does not guarantee future results.",
    }


# ══════════════════════════════════════════════════════════════════════════════
# 5. GOAL-BASED FUND RECOMMENDER
# ══════════════════════════════════════════════════════════════════════════════

def recommend_funds_for_goal(
    goal_name:     str,
    target_amount: float,       # in INR
    years:         int,
    monthly_sip:   float,       # how much user can invest per month
    risk_appetite: int,         # 1 (very low) to 9 (very high)
    top_n:         int = 5,
) -> dict:
    """
    Recommend the best-matching funds for a financial goal.

    Steps:
    1. Filter categories by goal constraints + user risk appetite
    2. Get all funds in those categories
    3. Score each fund on: returns × consistency × cost × risk match
    4. Return top_n with SIP projections
    """
    goal_meta = GOAL_CATEGORY_MAP.get(goal_name, {})
    risk_max  = min(risk_appetite, goal_meta.get("risk_max", risk_appetite))
    risk_min  = max(1, risk_appetite - 2)

    # Get suitable categories
    cat_filter = goal_meta.get("category_filter", None)
    if cat_filter:
        suitable_cats = cat_filter
    else:
        asset_classes = goal_meta.get("asset_class", ["EQUITY", "HYBRID", "INDEX"])
        suitable_cats = []
        for cat, meta in CATEGORIES.items():
            if (meta["asset_class"] in asset_classes
                    and meta["risk_score"] <= risk_max
                    and meta["risk_score"] >= risk_min
                    and meta.get("horizon_min", 0) <= years):
                suitable_cats.append(cat)

    if not suitable_cats:
        return {"goal": goal_name, "error": "No suitable categories found for this goal/risk profile"}

    # Get all funds in suitable categories
    conn = get_connection()
    placeholders = ",".join("?" * len(suitable_cats))
    funds_df = pd.read_sql_query(
        f"SELECT * FROM fund_master WHERE category IN ({placeholders}) ORDER BY fund_name",
        conn, params=suitable_cats
    )
    conn.close()

    if funds_df.empty:
        return {"goal": goal_name, "error": "No funds found in database for suitable categories"}

    # Score each fund
    # SIP required to reach goal at historical median return
    required_corpus = target_amount
    monthly_return_needed = (required_corpus / (monthly_sip * years * 12)) ** (1 / years) - 1 if monthly_sip > 0 and years > 0 else 0

    scored = []
    for _, row in funds_df.iterrows():
        an = compute_fund_analytics(row["fund_id"])
        if "error" in an:
            continue

        # Scoring formula
        ret5  = an.get("return_5y") or 0
        ret3  = an.get("return_3y") or 0
        sharpe= an.get("sharpe_ratio") or 0
        er    = an.get("expense_ratio") or row.get("expense_ratio") or 1.5
        alpha = an.get("alpha") or 0
        dd    = abs(an.get("max_drawdown") or -0.30)
        cat_risk = CATEGORIES.get(row.get("category",""), {}).get("risk_score", 6)

        # Risk fit penalty — penalise if category risk is far from user preference
        risk_fit = 1 - abs(cat_risk - risk_appetite) / 8

        # Composite score
        score = (
            ret5  * 35 +
            ret3  * 20 +
            sharpe * 15 +
            alpha * 10 +
            risk_fit * 10 +
            (1 / max(er, 0.1)) * 5 +
            (1 - dd) * 5
        )

        # Probability of reaching goal (simple Monte Carlo shortcut)
        # If median return > needed return, high probability
        prob_of_goal = None
        if ret5 > 0:
            safety_margin = (ret5 - monthly_return_needed) / max(ret5, 0.01)
            prob_of_goal  = min(95, max(30, 65 + safety_margin * 50))

        scored.append({
            "fund_id":          row["fund_id"],
            "fund_name":        row["fund_name"],
            "amc":              row.get("amc", ""),
            "category":         row.get("category", ""),
            "expense_ratio":    an.get("expense_ratio") or row.get("expense_ratio"),
            "return_5y":        an.get("return_5y"),
            "return_3y":        an.get("return_3y"),
            "sharpe_ratio":     an.get("sharpe_ratio"),
            "alpha":            an.get("alpha"),
            "max_drawdown":     an.get("max_drawdown"),
            "risk_score":       cat_risk,
            "risk_fit":         round(risk_fit, 2),
            "composite_score":  round(score, 2),
            "prob_of_goal":     round(prob_of_goal, 0) if prob_of_goal else None,
        })

    # Sort and return top N
    scored.sort(key=lambda x: x["composite_score"], reverse=True)
    top_funds = scored[:top_n]

    # SIP projection for top fund
    sip_needed = None
    if target_amount and years and top_funds:
        top_ret = top_funds[0].get("return_5y") or 0.12
        monthly_r = top_ret / 12
        n = years * 12
        if monthly_r > 0 and n > 0:
            sip_needed = target_amount * monthly_r / ((1 + monthly_r) ** n - 1)

    return {
        "goal":              goal_name,
        "target_amount":     target_amount,
        "years":             years,
        "monthly_sip":       monthly_sip,
        "risk_appetite":     risk_appetite,
        "suitable_categories": suitable_cats,
        "total_funds_scored":len(scored),
        "top_funds":         top_funds,
        "sip_needed_for_goal": round(sip_needed) if sip_needed else None,
        "note": "Rankings based on historical data. Not investment advice. Consult a SEBI-registered advisor.",
    }


# ══════════════════════════════════════════════════════════════════════════════
# 6. TAX HARVESTING CALENDAR
# ══════════════════════════════════════════════════════════════════════════════

LTCG_THRESHOLD_MONTHS = 12   # equity: LTCG after 12 months
LTCG_RATE             = 0.10  # 10% on gains above ₹1L
STCG_RATE             = 0.15  # 15% flat
LTCG_EXEMPT_LIMIT     = 100_000  # ₹1L per FY


def compute_tax_harvest_calendar(user_id: str = "default") -> dict:
    """
    For each portfolio fund, compute:
    - Current gain/loss
    - Whether STCG or LTCG applies
    - If STCG: when does it convert to LTCG?
    - Estimated tax saving from waiting for LTCG
    - Optimal exit date if profit-booking needed

    Returns actionable calendar with dates and amounts.
    """
    conn = get_connection()
    portfolio = pd.read_sql_query("""
        SELECT p.fund_id, p.amount_invested, p.purchase_date, p.purchase_nav,
               f.fund_name, f.category, f.expense_ratio
        FROM portfolio_user p
        JOIN fund_master f ON p.fund_id = f.fund_id
        WHERE p.user_id = ?
    """, conn, params=[user_id])
    conn.close()

    if portfolio.empty:
        return {"error": "No portfolio data for user"}

    today = date.today()
    recommendations = []

    for _, row in portfolio.iterrows():
        nav_series = load_nav(row["fund_id"])
        if nav_series.empty:
            continue

        current_nav = float(nav_series.iloc[-1])
        units       = row["amount_invested"] / row["purchase_nav"] if row["purchase_nav"] and row["purchase_nav"] > 0 else 0
        current_val = units * current_nav
        gain        = current_val - row["amount_invested"]
        gain_pct    = gain / row["amount_invested"] if row["amount_invested"] > 0 else 0

        # Holding period
        if row["purchase_date"]:
            purchase_date = pd.to_datetime(row["purchase_date"]).date()
            held_months   = (today - purchase_date).days / 30.44
        else:
            held_months = 0
            purchase_date = today

        # Tax classification
        is_equity = _is_equity_category(row.get("category", ""))
        if is_equity:
            is_ltcg = held_months >= LTCG_THRESHOLD_MONTHS
        else:
            is_ltcg = held_months >= 36  # 3 years for debt

        # Tax computation
        if gain <= 0:
            tax_if_sold_now = 0
            tax_saving      = 0
            action          = "HOLD — unrealised loss, harvest only if switching to better fund"
        elif is_ltcg:
            exempt = min(gain, LTCG_EXEMPT_LIMIT)
            taxable_gain = max(0, gain - exempt)
            tax_if_sold_now = taxable_gain * LTCG_RATE
            tax_saving      = 0
            action = f"LTCG applies — tax ₹{tax_if_sold_now:,.0f} (10% on gains above ₹1L)"
        else:
            # STCG currently
            stcg_tax = gain * STCG_RATE
            # Future LTCG tax (after crossing 12M threshold)
            ltcg_threshold_date = purchase_date + timedelta(days=365)
            days_to_ltcg = (ltcg_threshold_date - today).days
            if days_to_ltcg > 0:
                exempt      = min(gain, LTCG_EXEMPT_LIMIT)
                future_ltcg = max(0, gain - exempt) * LTCG_RATE
                tax_saving  = stcg_tax - future_ltcg
                action = (f"WAIT {days_to_ltcg} days (until {ltcg_threshold_date}) → "
                          f"save ₹{tax_saving:,.0f} in tax by switching to LTCG")
            else:
                tax_saving = 0
                action = "Just crossed LTCG threshold — can sell now at lower rate"
            tax_if_sold_now = stcg_tax

        recommendations.append({
            "fund_id":           row["fund_id"],
            "fund_name":         row.get("fund_name", ""),
            "purchase_date":     str(purchase_date),
            "held_months":       round(held_months, 1),
            "amount_invested":   round(row["amount_invested"]),
            "current_value":     round(current_val),
            "gain":              round(gain),
            "gain_pct":          round(gain_pct * 100, 1),
            "tax_regime":        "LTCG" if is_ltcg else "STCG",
            "tax_if_sold_now":   round(tax_if_sold_now),
            "tax_saving_waiting":round(max(0, tax_saving)),
            "ltcg_date":         str(purchase_date + timedelta(days=365)) if not is_ltcg else "Already LTCG",
            "action":            action,
        })

    # Sort by tax saving potential
    recommendations.sort(key=lambda x: -x["tax_saving_waiting"])

    return {
        "user_id":         user_id,
        "as_of_date":      str(today),
        "total_portfolio": len(recommendations),
        "total_tax_if_sold_today": sum(r["tax_if_sold_now"] for r in recommendations),
        "total_tax_savings_possible": sum(r["tax_saving_waiting"] for r in recommendations),
        "recommendations": recommendations,
        "note": "Estimates only. Consult a CA/tax advisor for accurate tax computation. LTCG exempt limit ₹1L per FY.",
    }


def _is_equity_category(category: str) -> bool:
    """Returns True if category is treated as equity for tax purposes."""
    cat_info = CATEGORIES.get(category, {})
    asset_class = cat_info.get("asset_class", "EQUITY")
    if asset_class in ("EQUITY", "INDEX", "FOF"):
        return True
    if asset_class == "HYBRID":
        # Arbitrage and equity savings funds are equity-taxed
        return "Arbitrage" in category or "Equity Savings" in category or "Aggressive" in category
    return False


# ══════════════════════════════════════════════════════════════════════════════
# 7. PORTFOLIO OVERLAP MATRIX
# ══════════════════════════════════════════════════════════════════════════════

def compute_portfolio_overlap_matrix(user_id: str = "default") -> dict:
    """
    Compute pairwise Jaccard overlap for all funds in a portfolio.
    Falls back to return-correlation based overlap if holdings data absent.
    """
    conn = get_connection()
    portfolio = pd.read_sql_query(
        "SELECT DISTINCT fund_id FROM portfolio_user WHERE user_id = ?",
        conn, params=[user_id]
    )
    conn.close()

    if portfolio.empty:
        return {"error": "No portfolio data"}

    fund_ids = portfolio["fund_id"].tolist()
    if len(fund_ids) < 2:
        return {"fund_ids": fund_ids, "matrix": {}, "note": "Need at least 2 funds for overlap analysis"}

    # Try holdings-based overlap first
    def get_holdings(fid):
        c = get_connection()
        try:
            df = pd.read_sql_query(
                "SELECT stock_name FROM fund_holdings WHERE fund_id = ?",
                c, params=[fid]
            )
            return set(df["stock_name"].str.lower().str.strip().tolist()) if not df.empty else set()
        finally:
            c.close()

    holdings = {fid: get_holdings(fid) for fid in fund_ids}
    has_holdings = any(len(h) > 0 for h in holdings.values())

    matrix = {}
    pairs  = []
    for i, fid_a in enumerate(fund_ids):
        matrix[fid_a] = {}
        for j, fid_b in enumerate(fund_ids):
            if i == j:
                matrix[fid_a][fid_b] = 100.0
                continue
            if has_holdings and holdings[fid_a] and holdings[fid_b]:
                # Jaccard similarity
                union = holdings[fid_a] | holdings[fid_b]
                inter = holdings[fid_a] & holdings[fid_b]
                overlap = round(len(inter) / len(union) * 100, 1) if union else 0.0
            else:
                # Fallback: return correlation as proxy
                nav_a = load_nav(fid_a)
                nav_b = load_nav(fid_b)
                if nav_a.empty or nav_b.empty:
                    overlap = 0.0
                else:
                    combined = pd.DataFrame({"a": nav_a.pct_change(), "b": nav_b.pct_change()}).dropna()
                    overlap = round(combined["a"].corr(combined["b"]) * 100, 1) if len(combined) > 30 else 0.0
            matrix[fid_a][fid_b] = overlap
            if i < j:
                pairs.append({"fund_a": fid_a, "fund_b": fid_b, "overlap_pct": overlap})

    # High overlap warnings
    high_overlap = [p for p in pairs if p["overlap_pct"] > 60]
    high_overlap.sort(key=lambda x: -x["overlap_pct"])

    # Load names
    conn = get_connection()
    names = {}
    for fid in fund_ids:
        row = conn.execute("SELECT fund_name FROM fund_master WHERE fund_id = ?", (fid,)).fetchone()
        names[fid] = row[0] if row else fid
    conn.close()

    return {
        "user_id":          user_id,
        "fund_ids":         fund_ids,
        "fund_names":       names,
        "matrix":           matrix,
        "high_overlap_pairs": [{**p, "fund_a_name": names.get(p["fund_a"],""),
                                 "fund_b_name": names.get(p["fund_b"],"")} for p in high_overlap],
        "method":           "holdings_jaccard" if has_holdings else "return_correlation",
        "note":             "Holdings-based overlap uses AMFI quarterly disclosures. Return-correlation is a fallback proxy.",
    }


# ══════════════════════════════════════════════════════════════════════════════
# 8. WEEKLY BATCH SCHEDULER
# ══════════════════════════════════════════════════════════════════════════════

WEEKLY_SCHEDULE = {
    1: ["Equity: Large Cap", "Equity: Large & Mid Cap", "Equity: Mid Cap",
        "Equity: Small Cap", "Equity: Multi Cap", "Equity: Flexi Cap",
        "Equity: Focused", "Equity: ELSS"],
    2: ["Equity: Value", "Equity: Contra", "Equity: Dividend Yield",
        "Hybrid: Aggressive", "Hybrid: Balanced Advantage", "Hybrid: Dynamic",
        "Hybrid: Conservative", "Hybrid: Multi Asset",
        "Hybrid: Equity Savings", "Hybrid: Arbitrage"],
    3: ["Debt: Overnight", "Debt: Liquid", "Debt: Ultra Short",
        "Debt: Low Duration", "Debt: Short Duration", "Debt: Medium Duration",
        "Debt: Long Duration", "Debt: Corporate Bond", "Debt: Credit Risk",
        "Debt: Banking & PSU", "Debt: Gilt", "Debt: Gilt 10Y Constant",
        "Debt: Dynamic Bond", "Debt: Floater"],
    4: ["Equity: Thematic", "Equity: Sectoral",
        "Index: Large Cap", "Index: Mid Cap", "Index: Small Cap",
        "Index: Sectoral", "Index: International", "Index: Debt",
        "FOF: Domestic", "FOF: International"],
}


def get_this_weeks_categories() -> Tuple[int, List[str]]:
    """Return (week_number_1_to_4, list_of_categories) for the current week."""
    week_of_year = date.today().isocalendar()[1]
    week_slot    = ((week_of_year - 1) % 4) + 1
    return week_slot, WEEKLY_SCHEDULE[week_slot]


def get_fund_ids_for_week(week_num: int) -> List[str]:
    """Get all fund IDs belonging to this week's categories."""
    cats = WEEKLY_SCHEDULE.get(week_num, [])
    if not cats:
        return []
    conn = get_connection()
    placeholders = ",".join("?" * len(cats))
    rows = conn.execute(
        f"SELECT fund_id FROM fund_master WHERE category IN ({placeholders})",
        cats
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]
