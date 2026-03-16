"""
Analytics Engine
----------------
Core financial calculations for mutual fund analysis.
All functions operate on pandas Series / DataFrames.
"""

import numpy as np
import pandas as pd
import sqlite3
import sys
from pathlib import Path
from typing import Optional, Tuple, Dict, List

sys.path.insert(0, str(Path(__file__).parent.parent))
import database.db as db

TRADING_DAYS = 252
RISK_FREE_RATE = 0.065  # 6.5% — approximate Indian 91-day T-bill rate


# ══════════════════════════════════════════════════════════════════════════════
# Data loaders
# ══════════════════════════════════════════════════════════════════════════════

def load_nav(fund_id: str, start: str = None, end: str = None) -> pd.Series:
    """Load NAV history as a date-indexed Series."""
    conn = db.get_connection()
    query = "SELECT date, nav FROM nav_history WHERE fund_id = ?"
    params = [fund_id]
    if start:
        query += " AND date >= ?"
        params.append(start)
    if end:
        query += " AND date <= ?"
        params.append(end)
    query += " ORDER BY date"

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()

    if df.empty:
        return pd.Series(dtype=float)

    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    return df["nav"].astype(float)


def load_benchmark(index_name: str, start: str = None, end: str = None) -> pd.Series:
    """Load benchmark index values as a date-indexed Series."""
    conn = db.get_connection()
    query = "SELECT date, index_value FROM benchmark_history WHERE index_name = ?"
    params = [index_name]
    if start:
        query += " AND date >= ?"
        params.append(start)
    if end:
        query += " AND date <= ?"
        params.append(end)
    query += " ORDER BY date"

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()

    if df.empty:
        return pd.Series(dtype=float)

    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)
    return df["index_value"].astype(float)


def load_fund_info(fund_id: str) -> dict:
    """Load fund metadata from fund_master."""
    conn = db.get_connection()
    row = conn.execute(
        "SELECT * FROM fund_master WHERE fund_id = ?", (fund_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else {}


def load_all_funds() -> pd.DataFrame:
    """Load all funds from fund_master."""
    conn = db.get_connection()
    df = pd.read_sql_query("SELECT * FROM fund_master ORDER BY fund_name", conn)
    conn.close()
    return df


# ══════════════════════════════════════════════════════════════════════════════
# Return calculations
# ══════════════════════════════════════════════════════════════════════════════

def calculate_cagr(nav: pd.Series, years: float) -> Optional[float]:
    """
    Compounded Annual Growth Rate over `years` from the most recent date.
    Returns None if insufficient data.
    """
    if nav.empty:
        return None
    end_date   = nav.index.max()
    start_date = end_date - pd.DateOffset(years=years)
    subset = nav[nav.index >= start_date]
    if len(subset) < 2:
        return None
    start_val, end_val = subset.iloc[0], subset.iloc[-1]
    if start_val <= 0:
        return None
    actual_years = (subset.index[-1] - subset.index[0]).days / 365.25
    if actual_years < 0.9 * years:   # need at least 90% of the period
        return None
    return (end_val / start_val) ** (1 / actual_years) - 1


def calculate_absolute_return(nav: pd.Series) -> Optional[float]:
    """Total return from first to last NAV point."""
    if len(nav) < 2:
        return None
    return (nav.iloc[-1] / nav.iloc[0]) - 1


def daily_returns(nav: pd.Series) -> pd.Series:
    """Compute daily percentage returns."""
    if nav is None or len(nav) < 2:
        return pd.Series(dtype=float)
    return nav.pct_change().dropna()


def calculate_cumulative_return(nav: pd.Series) -> Optional[float]:
    """Cumulative return between first and latest point."""
    if nav is None or len(nav) < 2:
        return None
    start_val = nav.iloc[0]
    if start_val <= 0:
        return None
    return float(nav.iloc[-1] / start_val - 1)


def calculate_annualized_return(nav: pd.Series) -> Optional[float]:
    """Annualised return computed from total return and elapsed calendar days."""
    if nav is None or len(nav) < 2:
        return None
    start_val = nav.iloc[0]
    if start_val <= 0:
        return None
    days = (nav.index[-1] - nav.index[0]).days
    if days <= 0:
        return None
    years = days / 365.25
    return float((nav.iloc[-1] / start_val) ** (1 / years) - 1)


# ══════════════════════════════════════════════════════════════════════════════
# Risk metrics
# ══════════════════════════════════════════════════════════════════════════════

def calculate_volatility(nav: pd.Series, annualise: bool = True) -> Optional[float]:
    """Annualised standard deviation of daily returns."""
    ret = daily_returns(nav)
    if ret.empty:
        return None
    vol = ret.std()
    return vol * np.sqrt(TRADING_DAYS) if annualise else vol


def calculate_max_drawdown(nav: pd.Series) -> Optional[float]:
    """Maximum peak-to-trough drawdown (as a negative fraction)."""
    if nav.empty:
        return None
    roll_max = nav.cummax()
    drawdown = (nav - roll_max) / roll_max
    return float(drawdown.min())


def calculate_sharpe_ratio(nav: pd.Series, rfr: float = RISK_FREE_RATE) -> Optional[float]:
    """
    Sharpe Ratio = (annualised return - risk-free rate) / annualised volatility
    """
    ret = daily_returns(nav)
    if ret.empty:
        return None
    ann_return = (1 + ret.mean()) ** TRADING_DAYS - 1
    ann_vol    = ret.std() * np.sqrt(TRADING_DAYS)
    if ann_vol == 0:
        return None
    return (ann_return - rfr) / ann_vol


def calculate_sortino_ratio(nav: pd.Series, rfr: float = RISK_FREE_RATE) -> Optional[float]:
    """Sortino Ratio — uses downside deviation only."""
    ret = daily_returns(nav)
    if ret.empty:
        return None
    ann_return   = (1 + ret.mean()) ** TRADING_DAYS - 1
    downside_ret = ret[ret < 0]
    if downside_ret.empty:
        return None
    downside_dev = downside_ret.std() * np.sqrt(TRADING_DAYS)
    if downside_dev == 0:
        return None
    return (ann_return - rfr) / downside_dev


def calculate_beta_alpha(
    nav: pd.Series,
    benchmark: pd.Series,
) -> Tuple[Optional[float], Optional[float]]:
    """
    Beta and Alpha vs benchmark.
    Aligns both series on common dates before computing.
    """
    if nav.empty or benchmark.empty:
        return None, None

    combined = pd.DataFrame({"fund": nav, "bench": benchmark}).dropna()
    if len(combined) < 30:
        return None, None

    fund_ret  = combined["fund"].pct_change().dropna()
    bench_ret = combined["bench"].pct_change().dropna()

    combined_ret = pd.DataFrame({"fund": fund_ret, "bench": bench_ret}).dropna()
    if len(combined_ret) < 30:
        return None, None

    cov_matrix = np.cov(combined_ret["fund"], combined_ret["bench"])
    beta  = cov_matrix[0, 1] / cov_matrix[1, 1]
    alpha = (combined_ret["fund"].mean() - beta * combined_ret["bench"].mean()) * TRADING_DAYS
    return float(beta), float(alpha)


def calculate_correlation(nav: pd.Series, benchmark: pd.Series) -> Optional[float]:
    """Daily return correlation between fund and benchmark."""
    if nav.empty or benchmark.empty:
        return None
    combined = pd.DataFrame({"fund": nav, "bench": benchmark}).dropna()
    if len(combined) < 30:
        return None
    ret = combined.pct_change().dropna()
    if ret.empty:
        return None
    return float(ret["fund"].corr(ret["bench"]))


def calculate_tracking_error(nav: pd.Series, benchmark: pd.Series) -> Optional[float]:
    """Annualised tracking error as std-dev of active return series."""
    if nav.empty or benchmark.empty:
        return None
    combined = pd.DataFrame({"fund": nav, "bench": benchmark}).dropna()
    if len(combined) < 30:
        return None
    ret = combined.pct_change().dropna()
    if ret.empty:
        return None
    active_ret = ret["fund"] - ret["bench"]
    if active_ret.empty:
        return None
    return float(active_ret.std() * np.sqrt(TRADING_DAYS))


# ══════════════════════════════════════════════════════════════════════════════
# Rolling returns
# ══════════════════════════════════════════════════════════════════════════════

def rolling_returns(nav: pd.Series, window_years: int = 1) -> pd.Series:
    """
    Compute rolling CAGR over a sliding window of `window_years`.
    Returns a Series indexed by end-date.
    """
    window_days = int(window_years * TRADING_DAYS)
    if len(nav) <= window_days:
        return pd.Series(dtype=float)

    roll = (nav / nav.shift(window_days)) ** (1 / window_years) - 1
    return roll.dropna()


def consistency_score(nav: pd.Series, benchmark: pd.Series, window_years: int = 1) -> Optional[float]:
    """
    Consistency score (%): percentage of rolling windows where fund beats benchmark.
    """
    fund_roll = rolling_returns(nav, window_years)
    bench_roll = rolling_returns(benchmark, window_years)
    if fund_roll.empty or bench_roll.empty:
        return None
    aligned = pd.DataFrame({"fund": fund_roll, "bench": bench_roll}).dropna()
    if aligned.empty:
        return None
    return float((aligned["fund"] > aligned["bench"]).mean() * 100)


def detect_index_like_behavior(
    nav: pd.Series,
    benchmark: pd.Series,
    tracking_error_threshold: float = 0.03,
    correlation_threshold: float = 0.95,
) -> dict:
    """Flag active funds that behave like benchmark despite active-fee profile."""
    te = calculate_tracking_error(nav, benchmark)
    corr = calculate_correlation(nav, benchmark)
    beta, _ = calculate_beta_alpha(nav, benchmark)
    is_index_like = (
        te is not None and corr is not None
        and te < tracking_error_threshold
        and corr > correlation_threshold
    )
    return {
        "tracking_error": te,
        "correlation": corr,
        "beta": beta,
        "index_like": bool(is_index_like),
        "label": "Index-like fund with active fees" if is_index_like else "Distinct active behavior",
    }


# ══════════════════════════════════════════════════════════════════════════════
# Full analytics bundle for a single fund
# ══════════════════════════════════════════════════════════════════════════════

def compute_fund_analytics(fund_id: str) -> dict:
    """
    Compute and return all analytics for a fund.
    """
    nav  = load_nav(fund_id)
    info = load_fund_info(fund_id)

    if nav.empty:
        return {"fund_id": fund_id, "error": "No NAV data"}

    benchmark_name = info.get("benchmark", "Nifty 50")
    bench = load_benchmark(benchmark_name)

    beta, alpha = calculate_beta_alpha(nav, bench)
    corr = calculate_correlation(nav, bench)
    te = calculate_tracking_error(nav, bench)
    consistency_1y = consistency_score(nav, bench, 1)
    consistency_3y = consistency_score(nav, bench, 3)
    consistency_5y = consistency_score(nav, bench, 5)
    index_like = detect_index_like_behavior(nav, bench)

    result = {
        "fund_id":          fund_id,
        "fund_name":        info.get("fund_name", fund_id),
        "category":         info.get("category", ""),
        "benchmark":        benchmark_name,
        "expense_ratio":    info.get("expense_ratio"),
        "amc":              info.get("amc", ""),
        "return_1y":        calculate_cagr(nav, 1),
        "return_3y":        calculate_cagr(nav, 3),
        "return_5y":        calculate_cagr(nav, 5),
        "return_10y":       calculate_cagr(nav, 10),
        "return_inception": calculate_absolute_return(nav),
        "cumulative_return": calculate_cumulative_return(nav),
        "annualized_return": calculate_annualized_return(nav),
        "volatility":       calculate_volatility(nav),
        "max_drawdown":     calculate_max_drawdown(nav),
        "sharpe_ratio":     calculate_sharpe_ratio(nav),
        "sortino_ratio":    calculate_sortino_ratio(nav),
        "beta":             beta,
        "alpha":            alpha,
        "correlation":      corr,
        "tracking_error":   te,
        "consistency_1y":   consistency_1y,
        "consistency_3y":   consistency_3y,
        "consistency_5y":   consistency_5y,
        "index_like_label": index_like["label"],
        "is_index_like":    index_like["index_like"],
        "nav_latest":       float(nav.iloc[-1]),
        "nav_start_date":   str(nav.index.min().date()),
        "nav_end_date":     str(nav.index.max().date()),
        "nav_count":        len(nav),
    }
    return result


def compute_benchmark_analytics(index_name: str) -> dict:
    """Analytics for a benchmark index."""
    bench = load_benchmark(index_name)
    if bench.empty:
        return {}
    return {
        "index_name":   index_name,
        "return_1y":    calculate_cagr(bench, 1),
        "return_3y":    calculate_cagr(bench, 3),
        "return_5y":    calculate_cagr(bench, 5),
        "volatility":   calculate_volatility(bench),
        "max_drawdown": calculate_max_drawdown(bench),
        "sharpe_ratio": calculate_sharpe_ratio(bench),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Underperformance detection
# ══════════════════════════════════════════════════════════════════════════════

def detect_underperformance(fund_id: str) -> dict:
    """
    Flags if a fund is underperforming its benchmark.

    Severity:
    - 'OK'       : no underperformance
    - 'WARNING'  : 5Y return < benchmark
    - 'SERIOUS'  : also lagging peer rolling returns > 50% of time
    - 'CRITICAL' : underperforming benchmark for > 3 consecutive years
    """
    analytics = compute_fund_analytics(fund_id)
    if "error" in analytics:
        return {"fund_id": fund_id, "flag": "NO_DATA", "details": analytics["error"]}

    benchmark_name = analytics["benchmark"]
    bench_analytics = compute_benchmark_analytics(benchmark_name)

    fund_5y  = analytics.get("return_5y")
    bench_5y = bench_analytics.get("return_5y")

    if fund_5y is None or bench_5y is None:
        return {"fund_id": fund_id, "flag": "INSUFFICIENT_DATA"}

    nav   = load_nav(fund_id)
    bench = load_benchmark(benchmark_name)

    # Rolling 1-year returns comparison
    fund_rolling  = rolling_returns(nav, 1)
    bench_rolling = rolling_returns(bench, 1)

    aligned = pd.DataFrame({"fund": fund_rolling, "bench": bench_rolling}).dropna()
    pct_underperforming = (
        (aligned["fund"] < aligned["bench"]).sum() / len(aligned)
        if len(aligned) > 0 else 0
    )

    flag    = "OK"
    details = []

    if fund_5y < bench_5y:
        flag = "WARNING"
        details.append(
            f"5Y return {fund_5y:.1%} < benchmark {bench_5y:.1%}"
        )

        if pct_underperforming > 0.5:
            flag = "SERIOUS"
            details.append(
                f"Rolling 1Y returns lag benchmark {pct_underperforming:.0%} of the time"
            )

    # Check 3 consecutive years of underperformance
    years_underperforming = 0
    for yr in range(1, 4):
        fund_ret  = calculate_cagr(nav, yr)
        bench_ret = calculate_cagr(bench, yr)
        if fund_ret is not None and bench_ret is not None and fund_ret < bench_ret:
            years_underperforming += 1

    if years_underperforming >= 3:
        flag = "CRITICAL"
        details.append("Underperforming benchmark for 3+ consecutive year windows")

    return {
        "fund_id":               fund_id,
        "fund_name":             analytics["fund_name"],
        "flag":                  flag,
        "fund_5y":               fund_5y,
        "bench_5y":              bench_5y,
        "pct_rolling_underperf": pct_underperforming,
        "details":               "; ".join(details),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Fund overlap
# ══════════════════════════════════════════════════════════════════════════════

def calculate_overlap(fund_id_a: str, fund_id_b: str) -> dict:
    """Compute holdings overlap between two funds."""
    conn = db.get_connection()

    def get_holdings(fid):
        df = pd.read_sql_query(
            "SELECT stock_name, weight FROM fund_holdings WHERE fund_id=? ORDER BY weight DESC",
            conn, params=[fid]
        )
        return set(df["stock_name"].str.lower().str.strip())

    holdings_a = get_holdings(fund_id_a)
    holdings_b = get_holdings(fund_id_b)
    conn.close()

    if not holdings_a or not holdings_b:
        return {"overlap_pct": None, "common_stocks": []}

    common = holdings_a & holdings_b
    overlap = len(common) / len(holdings_a | holdings_b)

    return {
        "overlap_pct":   round(overlap * 100, 1),
        "common_stocks": list(common),
        "count_a":       len(holdings_a),
        "count_b":       len(holdings_b),
        "count_common":  len(common),
    }


def fund_data_quality(fund_id: str, min_history_years: float = 3.0) -> dict:
    """Assess data completeness/freshness for a single fund."""
    nav = load_nav(fund_id)
    info = load_fund_info(fund_id)
    if nav.empty:
        return {
            "fund_id": fund_id,
            "fund_name": info.get("fund_name", fund_id),
            "quality_score": 0,
            "status": "LOW",
            "freshness_days": None,
            "history_years": 0.0,
            "nav_points": 0,
            "benchmark_aligned_ratio": 0.0,
            "issues": ["No NAV history"],
        }

    latest_date = nav.index.max()
    earliest_date = nav.index.min()
    freshness_days = int((pd.Timestamp.today().normalize() - latest_date.normalize()).days)
    history_years = max(0.0, (latest_date - earliest_date).days / 365.25)

    benchmark = load_benchmark(info.get("benchmark", "Nifty 50"))
    if benchmark.empty:
        aligned_ratio = 0.0
    else:
        aligned = pd.DataFrame({"fund": nav, "bench": benchmark}).dropna()
        aligned_ratio = float(len(aligned) / len(nav)) if len(nav) > 0 else 0.0

    issues = []
    if freshness_days > 7:
        issues.append(f"Stale NAV data ({freshness_days} days)")
    if history_years < min_history_years:
        issues.append(f"Short history ({history_years:.1f} years)")
    if aligned_ratio < 0.7:
        issues.append(f"Low benchmark overlap ({aligned_ratio:.0%})")

    # score components
    freshness_score = max(0.0, 100 - min(100, freshness_days * 4))
    history_score = min(100.0, (history_years / min_history_years) * 100)
    align_score = min(100.0, aligned_ratio * 100)
    quality_score = round(0.4 * freshness_score + 0.35 * history_score + 0.25 * align_score)

    status = "HIGH" if quality_score >= 75 else ("MEDIUM" if quality_score >= 50 else "LOW")

    return {
        "fund_id": fund_id,
        "fund_name": info.get("fund_name", fund_id),
        "quality_score": quality_score,
        "status": status,
        "freshness_days": freshness_days,
        "history_years": round(history_years, 2),
        "nav_points": int(len(nav)),
        "benchmark_aligned_ratio": round(aligned_ratio, 3),
        "issues": issues,
    }


def fund_decision_card(fund_id: str, holding_months: int = 24, invested_amount: float = 50000) -> dict:
    """Single-card decision summary combining performance, risk, exits and data quality."""
    analytics = compute_fund_analytics(fund_id)
    if "error" in analytics:
        return {"fund_id": fund_id, "error": analytics["error"]}

    up = detect_underperformance(fund_id)
    from engine.exit_strategy import assess_exit
    exit_assessment = assess_exit(fund_id, holding_months=holding_months, invested_amount=invested_amount)
    quality = fund_data_quality(fund_id)

    score = 50.0
    score += (analytics.get("return_5y") or 0) * 120
    score += (analytics.get("sharpe_ratio") or 0) * 10
    score += (analytics.get("consistency_3y") or 0) * 0.2
    score -= abs(analytics.get("max_drawdown") or 0) * 50
    score -= (analytics.get("expense_ratio") or 0.8) * 4

    penalties = {"OK": 0, "WARNING": 8, "SERIOUS": 15, "CRITICAL": 25}
    score -= penalties.get(up.get("flag", "OK"), 10)
    score += (quality.get("quality_score", 50) - 50) * 0.25

    final_score = max(0, min(100, round(score)))
    grade = "A" if final_score >= 80 else ("B" if final_score >= 65 else ("C" if final_score >= 50 else "D"))

    verdict_map = {
        "A": "STRONG_HOLD",
        "B": "HOLD",
        "C": "WATCH",
        "D": "REVIEW_EXIT",
    }

    return {
        "fund_id": fund_id,
        "fund_name": analytics.get("fund_name", fund_id),
        "grade": grade,
        "decision_score": final_score,
        "verdict": verdict_map[grade],
        "underperformance_flag": up.get("flag", "NO_DATA"),
        "exit_recommendation": exit_assessment.get("recommendation"),
        "data_quality": quality,
        "key_metrics": {
            "return_5y": analytics.get("return_5y"),
            "sharpe_ratio": analytics.get("sharpe_ratio"),
            "max_drawdown": analytics.get("max_drawdown"),
            "consistency_3y": analytics.get("consistency_3y"),
            "expense_ratio": analytics.get("expense_ratio"),
        },
        "why": [
            f"5Y return: {analytics.get('return_5y'):.2%}" if analytics.get('return_5y') is not None else "5Y return unavailable",
            f"Sharpe ratio: {analytics.get('sharpe_ratio'):.2f}" if analytics.get('sharpe_ratio') is not None else "Sharpe unavailable",
            f"Underperformance flag: {up.get('flag', 'NO_DATA')}",
            f"Data quality: {quality.get('status')} ({quality.get('quality_score')}/100)",
        ],
    }



# ══════════════════════════════════════════════════════════════════════════════
# Portfolio analytics
# ══════════════════════════════════════════════════════════════════════════════

def portfolio_analytics(user_id: str = "default") -> dict:
    """
    Full portfolio health analysis for a user.
    Returns a health score (0–100) and recommendations.
    """
    conn = db.get_connection()
    portfolio = pd.read_sql_query("""
        SELECT p.fund_id, p.amount_invested, p.purchase_date, p.purchase_nav,
               f.fund_name, f.category, f.expense_ratio, f.benchmark
        FROM portfolio_user p
        JOIN fund_master f ON p.fund_id = f.fund_id
        WHERE p.user_id = ?
    """, conn, params=[user_id])
    conn.close()

    if portfolio.empty:
        return {"error": "No portfolio data"}

    total_invested = portfolio["amount_invested"].sum()
    fund_ids       = portfolio["fund_id"].tolist()

    # Current values
    current_values = []
    for _, row in portfolio.iterrows():
        nav = load_nav(row["fund_id"])
        if nav.empty:
            current_val = row["amount_invested"]
        else:
            latest_nav = nav.iloc[-1]
            if row["purchase_nav"] and row["purchase_nav"] > 0:
                units = row["amount_invested"] / row["purchase_nav"]
                current_val = units * latest_nav
            else:
                current_val = row["amount_invested"]
        current_values.append(current_val)

    portfolio["current_value"] = current_values
    total_current = sum(current_values)

    # Portfolio-level return/risk using weighted daily returns
    weighted_returns = []
    for _, row in portfolio.iterrows():
        nav = load_nav(row["fund_id"])
        ret = daily_returns(nav)
        if ret.empty or total_current <= 0:
            continue
        weight = row["current_value"] / total_current
        weighted_returns.append(ret.rename(row["fund_id"]) * weight)

    if weighted_returns:
        returns_df = pd.concat(weighted_returns, axis=1).fillna(0.0)
        portfolio_daily = returns_df.sum(axis=1)
        portfolio_return_annual = float((1 + portfolio_daily.mean()) ** TRADING_DAYS - 1)
        portfolio_volatility = float(portfolio_daily.std() * np.sqrt(TRADING_DAYS))
    else:
        portfolio_return_annual = None
        portfolio_volatility = None

    # Category allocation
    cat_alloc = (
        portfolio.groupby("category")["current_value"]
        .sum()
        .div(total_current)
        .sort_values(ascending=False)
    )

    # Average expense ratio (weighted)
    avg_er = (
        portfolio["expense_ratio"].fillna(1.0) *
        portfolio["current_value"] / total_current
    ).sum()

    # Underperformance flags
    flags = {}
    for fid in fund_ids:
        result = detect_underperformance(fid)
        flags[fid] = result.get("flag", "OK")

    critical_count = sum(1 for f in flags.values() if f == "CRITICAL")
    serious_count  = sum(1 for f in flags.values() if f == "SERIOUS")
    warning_count  = sum(1 for f in flags.values() if f == "WARNING")

    # Diversification score (0–100)
    n_categories = len(cat_alloc)
    top_cat_pct  = float(cat_alloc.iloc[0]) if len(cat_alloc) > 0 else 1.0
    concentration_risk = top_cat_pct
    div_score    = min(100, n_categories * 15 + (1 - top_cat_pct) * 40)

    # Penalty for underperformers
    perf_penalty = critical_count * 20 + serious_count * 10 + warning_count * 5

    # Expense ratio drag penalty
    er_penalty = max(0, (avg_er - 0.5) * 10)

    health_score = max(0, min(100, div_score - perf_penalty - er_penalty))

    # Recommendations
    recommendations = []
    if top_cat_pct > 0.6:
        recommendations.append(
            f"Portfolio is concentrated in '{cat_alloc.index[0]}' ({top_cat_pct:.0%}). Consider diversifying."
        )
    if avg_er > 1.0:
        recommendations.append(
            f"Weighted avg expense ratio is {avg_er:.2f}%. Consider low-cost index alternatives."
        )
    if critical_count > 0:
        recommendations.append(
            f"{critical_count} fund(s) are CRITICALLY underperforming — review and consider exit."
        )
    if serious_count > 0:
        recommendations.append(
            f"{serious_count} fund(s) show SERIOUS underperformance — monitor closely."
        )
    if len(fund_ids) < 3:
        recommendations.append("Portfolio has fewer than 3 funds. Consider broadening allocation.")
    if len(fund_ids) > 8:
        recommendations.append("Too many funds — overlaps likely. Consolidate for cleaner exposure.")

    return {
        "user_id":        user_id,
        "total_invested": total_invested,
        "total_current":  total_current,
        "total_gain":     total_current - total_invested,
        "total_gain_pct": (total_current / total_invested - 1) if total_invested > 0 else 0,
        "health_score":   round(health_score),
        "portfolio_return_annual": portfolio_return_annual,
        "portfolio_volatility": portfolio_volatility,
        "concentration_risk": concentration_risk,
        "avg_er":         round(avg_er, 3),
        "n_funds":        len(fund_ids),
        "category_allocation": cat_alloc.to_dict(),
        "underperformance_flags": flags,
        "recommendations": recommendations,
        "prioritized_actions": sorted([
            {"action": r, "impact": (90 if "CRITICALLY" in r else 70 if "SERIOUS" in r else 60 if "concentrated" in r else 50)}
            for r in recommendations
        ], key=lambda x: x["impact"], reverse=True),
        "portfolio_df":   portfolio,
    }
