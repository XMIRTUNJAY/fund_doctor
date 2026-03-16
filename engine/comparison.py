"""Comparison and ranking utilities for fund analytics."""

from __future__ import annotations

import numpy as np
import pandas as pd

from engine.analytics import (
    compute_fund_analytics,
    load_nav,
    load_fund_info,
    load_all_funds,
    load_benchmark,
    calculate_cagr,
    calculate_volatility,
    calculate_max_drawdown,
    calculate_sharpe_ratio,
    consistency_score,
)


def _metric_snapshot(fund_id: str) -> dict:
    info = compute_fund_analytics(fund_id)
    if "error" in info:
        return {"fund_id": fund_id, "error": info["error"]}
    return {
        "fund_id": fund_id,
        "fund_name": info.get("fund_name", fund_id),
        "category": info.get("category", ""),
        "benchmark": info.get("benchmark", "Nifty 50"),
        "return_5y": info.get("return_5y"),
        "volatility": info.get("volatility"),
        "sharpe_ratio": info.get("sharpe_ratio"),
        "max_drawdown": info.get("max_drawdown"),
        "expense_ratio": info.get("expense_ratio"),
        "consistency_3y": info.get("consistency_3y"),
    }


def fund_vs_fund(fund_a: str, fund_b: str) -> dict:
    """Structured side-by-side comparison output for two funds."""
    return {
        "mode": "fund_vs_fund",
        "left": _metric_snapshot(fund_a),
        "right": _metric_snapshot(fund_b),
    }


def fund_vs_benchmark(fund_id: str) -> dict:
    """Compare a fund against its benchmark across key metrics."""
    info = load_fund_info(fund_id)
    bench_name = info.get("benchmark", "Nifty 50")

    fund_nav = load_nav(fund_id)
    bench_nav = load_benchmark(bench_name)

    if fund_nav.empty or bench_nav.empty:
        return {"mode": "fund_vs_benchmark", "fund_id": fund_id, "error": "Insufficient data"}

    aligned = pd.DataFrame({"fund": fund_nav, "benchmark": bench_nav}).dropna()
    if len(aligned) < 252:
        return {"mode": "fund_vs_benchmark", "fund_id": fund_id, "error": "Insufficient overlap"}

    f = aligned["fund"]
    b = aligned["benchmark"]
    return {
        "mode": "fund_vs_benchmark",
        "fund_id": fund_id,
        "benchmark": bench_name,
        "metrics": {
            "return_1y": {"fund": calculate_cagr(f, 1), "benchmark": calculate_cagr(b, 1)},
            "return_3y": {"fund": calculate_cagr(f, 3), "benchmark": calculate_cagr(b, 3)},
            "return_5y": {"fund": calculate_cagr(f, 5), "benchmark": calculate_cagr(b, 5)},
            "volatility": {"fund": calculate_volatility(f), "benchmark": calculate_volatility(b)},
            "max_drawdown": {"fund": calculate_max_drawdown(f), "benchmark": calculate_max_drawdown(b)},
            "sharpe_ratio": {"fund": calculate_sharpe_ratio(f), "benchmark": calculate_sharpe_ratio(b)},
            "consistency_1y": consistency_score(f, b, 1),
            "consistency_3y": consistency_score(f, b, 3),
            "consistency_5y": consistency_score(f, b, 5),
        },
    }


def fund_vs_category_average(fund_id: str) -> dict:
    """Compare a fund to category peer average analytics."""
    subject = compute_fund_analytics(fund_id)
    if "error" in subject:
        return {"mode": "fund_vs_category", "fund_id": fund_id, "error": subject["error"]}

    category = subject.get("category", "")
    funds = load_all_funds()
    peers = funds[(funds["category"] == category) & (funds["fund_id"] != fund_id)]

    rows = []
    for _, row in peers.iterrows():
        m = compute_fund_analytics(row["fund_id"])
        if "error" in m:
            continue
        rows.append(
            {
                "return_5y": m.get("return_5y"),
                "volatility": m.get("volatility"),
                "sharpe_ratio": m.get("sharpe_ratio"),
                "max_drawdown": m.get("max_drawdown"),
                "expense_ratio": m.get("expense_ratio"),
                "consistency_3y": m.get("consistency_3y"),
            }
        )

    if not rows:
        return {
            "mode": "fund_vs_category",
            "fund_id": fund_id,
            "category": category,
            "error": "No category peers with analytics",
        }

    df = pd.DataFrame(rows)
    peer_avg = {k: float(v) for k, v in df.mean(numeric_only=True).dropna().to_dict().items()}

    return {
        "mode": "fund_vs_category",
        "fund_id": fund_id,
        "fund_name": subject.get("fund_name", fund_id),
        "category": category,
        "fund_metrics": {
            "return_5y": subject.get("return_5y"),
            "volatility": subject.get("volatility"),
            "sharpe_ratio": subject.get("sharpe_ratio"),
            "max_drawdown": subject.get("max_drawdown"),
            "expense_ratio": subject.get("expense_ratio"),
            "consistency_3y": subject.get("consistency_3y"),
        },
        "category_average": peer_avg,
        "peer_count": len(df),
    }


def rank_funds_by_category(category: str, top_n: int = 10) -> dict:
    """Rank funds inside a category using weighted multi-factor score."""
    funds = load_all_funds()
    peers = funds[funds["category"] == category]
    if peers.empty:
        return {"category": category, "error": "No funds in category"}

    rows = []
    for _, row in peers.iterrows():
        m = compute_fund_analytics(row["fund_id"])
        if "error" in m:
            continue
        rows.append(
            {
                "fund_id": row["fund_id"],
                "fund_name": m.get("fund_name", row["fund_id"]),
                "return_5y": m.get("return_5y"),
                "consistency_3y": m.get("consistency_3y"),
                "sharpe_ratio": m.get("sharpe_ratio"),
                "max_drawdown": m.get("max_drawdown"),
                "expense_ratio": m.get("expense_ratio"),
            }
        )

    if not rows:
        return {"category": category, "error": "No analytics-ready funds in category"}

    df = pd.DataFrame(rows)
    df = df.replace([np.inf, -np.inf], np.nan)

    # normalised 0..1 scores (higher better)
    def norm_high(col):
        s = df[col].astype(float)
        span = s.max() - s.min()
        return (s - s.min()) / span if span > 0 else pd.Series([0.5] * len(s), index=s.index)

    def norm_low(col):
        s = df[col].astype(float)
        span = s.max() - s.min()
        return (s.max() - s) / span if span > 0 else pd.Series([0.5] * len(s), index=s.index)

    score = (
        0.35 * norm_high("return_5y").fillna(0.0)
        + 0.25 * (df["consistency_3y"].fillna(0.0) / 100.0)
        + 0.20 * norm_high("sharpe_ratio").fillna(0.0)
        + 0.10 * norm_low("max_drawdown").fillna(0.0)
        + 0.10 * norm_low("expense_ratio").fillna(0.0)
    )
    df["score"] = score.round(4)

    ranked = df.sort_values("score", ascending=False).reset_index(drop=True)
    n = len(ranked)
    q1 = max(1, int(np.ceil(n * 0.25)))

    return {
        "category": category,
        "count": n,
        "top_10": ranked.head(top_n).to_dict("records"),
        "top_quartile": ranked.head(q1).to_dict("records"),
        "bottom_quartile": ranked.tail(q1).to_dict("records"),
    }
