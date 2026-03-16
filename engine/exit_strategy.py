"""
Exit Strategy Engine
--------------------
Evaluates when to exit a fund and recommends replacements.
"""

import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.db import get_connection
from engine.analytics import (
    compute_fund_analytics,
    detect_underperformance,
    calculate_cagr,
    calculate_sharpe_ratio,
    load_nav,
    load_benchmark,
    load_all_funds,
    rolling_returns,
)


EXIT_LOAD_THRESHOLD = 0.01   # 1 % — avoid exiting if exit load is charged
LTCG_THRESHOLD_MONTHS = 12   # equity: 12 months for LTCG treatment


def assess_exit(fund_id: str, holding_months: int = None, invested_amount: float = None) -> dict:
    """
    Full exit assessment for a single fund.

    Returns:
        recommendation : 'HOLD' | 'WATCH' | 'SWITCH' | 'EXIT'
        reasons        : list of strings
        tax_notes      : tax implication summary
    """
    underperf = detect_underperformance(fund_id)
    analytics  = compute_fund_analytics(fund_id)
    info       = analytics  # same dict

    flag    = underperf.get("flag", "OK")
    reasons = []
    tax_notes = []
    trigger_details = []

    # ── Performance-based exit signal ─────────────────────────────────────
    if flag == "CRITICAL":
        reasons.append("Fund is CRITICALLY underperforming its benchmark.")
        recommendation = "EXIT"
        trigger_details.append({"rule": "critical_underperformance", "value": flag, "threshold": "CRITICAL"})
    elif flag == "SERIOUS":
        reasons.append("Fund shows SERIOUS underperformance vs benchmark and peers.")
        recommendation = "SWITCH"
        trigger_details.append({"rule": "serious_underperformance", "value": flag, "threshold": "SERIOUS"})
    elif flag == "WARNING":
        reasons.append("Fund is lagging its benchmark. Monitor closely.")
        recommendation = "WATCH"
        trigger_details.append({"rule": "warning_underperformance", "value": flag, "threshold": "WARNING"})
    else:
        recommendation = "HOLD"
        reasons.append("Fund performance is acceptable relative to benchmark.")

    # Enhanced trigger: 5Y underperformance + rolling 3Y underperformance > 50%
    benchmark_name = analytics.get("benchmark", "Nifty 50")
    nav = load_nav(fund_id)
    bench = load_benchmark(benchmark_name)
    rolling_underperf_pct = None
    if not nav.empty and not bench.empty:
        fund_5y = calculate_cagr(nav, 5)
        bench_5y = calculate_cagr(bench, 5)
        roll_f = rolling_returns(nav, 3)
        roll_b = rolling_returns(bench, 3)
        aligned = pd.DataFrame({"fund": roll_f, "bench": roll_b}).dropna()

        if len(aligned) > 0:
            rolling_underperf_pct = float((aligned["fund"] < aligned["bench"]).mean() * 100)

        if (
            fund_5y is not None
            and bench_5y is not None
            and fund_5y < bench_5y
            and rolling_underperf_pct is not None
            and rolling_underperf_pct > 50
        ):
            reasons.append(
                f"5Y return is below benchmark and rolling 3Y windows underperform {rolling_underperf_pct:.0f}% of the time."
            )
            recommendation = "EXIT" if recommendation in ("WATCH", "SWITCH") else recommendation
            trigger_details.append({"rule": "5y_and_rolling3y_underperformance", "value": rolling_underperf_pct, "threshold": ">50"})

    # ── Sharpe ratio check ────────────────────────────────────────────────
    sharpe = analytics.get("sharpe_ratio")
    if sharpe is not None and sharpe < 0.5 and recommendation in ("SWITCH", "EXIT"):
        reasons.append(f"Sharpe ratio ({sharpe:.2f}) is below acceptable threshold of 0.5.")
        trigger_details.append({"rule": "low_sharpe", "value": round(sharpe, 3), "threshold": "<0.5"})

    # ── Expense ratio check ───────────────────────────────────────────────
    er = analytics.get("expense_ratio")
    if er and er > 1.5:
        reasons.append(f"High expense ratio ({er:.2f}%) is a significant drag on returns.")
        if recommendation == "HOLD":
            recommendation = "WATCH"
        trigger_details.append({"rule": "high_expense_ratio", "value": round(er, 3), "threshold": ">1.5"})

    # ── Tax and exit load notes ───────────────────────────────────────────
    if holding_months is not None:
        if holding_months < LTCG_THRESHOLD_MONTHS:
            tax_notes.append(
                f"Held for {holding_months} months — STCG tax (15%) applies on equity gains."
            )
        else:
            tax_notes.append(
                f"Held for {holding_months} months — LTCG tax (10% above ₹1L) applies."
            )

    if invested_amount:
        nav_series = load_nav(fund_id)
        if not nav_series.empty and analytics.get("return_1y"):
            current_val = invested_amount * (1 + analytics.get("return_5y", 0))
            gain        = current_val - invested_amount
            if gain > 0:
                tax_notes.append(
                    f"Estimated gain ≈ ₹{gain:,.0f}. Consult a tax advisor before exiting."
                )

    return {
        "fund_id":        fund_id,
        "fund_name":      analytics.get("fund_name", fund_id),
        "flag":           flag,
        "recommendation": recommendation,
        "reasons":        reasons,
        "tax_notes":      tax_notes,
        "details":        underperf.get("details", ""),
        "rolling_3y_underperformance_pct": rolling_underperf_pct,
        "trigger_details": trigger_details,
    }


def find_replacement_funds(fund_id: str, top_n: int = 3) -> list:
    """
    Find better alternative funds in the same category.
    Ranks by: rolling 5Y CAGR, Sharpe ratio, expense ratio.
    """
    info      = compute_fund_analytics(fund_id)
    category  = info.get("category", "")
    fund_er   = info.get("expense_ratio") or 9999
    fund_5y   = info.get("return_5y")

    all_funds = load_all_funds()
    peers     = all_funds[
        (all_funds["category"] == category) &
        (all_funds["fund_id"] != fund_id)
    ]

    scored = []
    for _, peer in peers.iterrows():
        peer_analytics = compute_fund_analytics(peer["fund_id"])
        peer_5y    = peer_analytics.get("return_5y")
        peer_sharpe = peer_analytics.get("sharpe_ratio")
        peer_er     = peer_analytics.get("expense_ratio") or 999

        if peer_5y is None or peer_sharpe is None:
            continue

        # Score: favour higher return, higher sharpe, lower expense ratio
        score = (peer_5y * 40) + (peer_sharpe * 30) + ((1 / max(peer_er, 0.1)) * 30)

        scored.append({
            "fund_id":      peer["fund_id"],
            "fund_name":    peer["fund_name"],
            "return_5y":    peer_5y,
            "sharpe_ratio": peer_sharpe,
            "expense_ratio": peer_er,
            "score":        score,
        })

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored[:top_n]
