# Fund Doctor Technical Audit & Architecture Notes

## Module audit (current repository)

| Module | Purpose | Inputs | Outputs | Algorithms | Gaps identified |
|---|---|---|---|---|---|
| `engine/analytics.py` | Core performance/risk analytics + underperformance + portfolio doctor. | NAV series, benchmark series, fund IDs, user IDs. | Metric dicts, flags, portfolio score. | CAGR, volatility, drawdown, Sharpe, Sortino, beta/alpha, rolling returns. | Previously lacked explicit cumulative/annualized return, consistency scoring windows, and active-vs-passive behavior flag. |
| `engine/exit_strategy.py` | Recommendation engine for HOLD/WATCH/SWITCH/EXIT and replacements. | Fund ID, optional holding period + invested amount. | Recommendation dict + tax notes. | Rule-based decisions over underperformance and Sharpe/expense ratios. | Needed explicit 5Y-underperformance + rolling 3Y underperformance trigger. |
| `engine/advanced_analytics.py` | Higher-level quality scoring, goal-based recommendations, overlap matrix. | Fund IDs, goal profiles, user IDs. | Composite scoring/ranking outputs. | weighted scoring, overlap matrix, goal mapping. | Useful, but not wired as core comparison API. |
| `engine/pm_analytics.py` | Professional analytics toolkit. | Fund IDs, benchmark IDs, cashflow assumptions. | PM ratios, stress metrics, frontier calculations. | Calmar/Treynor/Information/Omega, VaR/CVaR, frontier simulation. | Rich capability; needs lightweight integration points for retail dashboard. |
| `dashboard/app.py` | Streamlit UI pages. | DB data + engine outputs. | Visual tables/charts and recommendations. | Plotly visuals, cached loading. | Comparison and ranking can be expanded with new modular API. |

## Implemented enhancements

1. Added missing/explicit core metrics in `analytics.py`
   - cumulative return
   - annualized return
   - benchmark correlation
   - tracking error
   - consistency score for 1Y/3Y/5Y windows
   - index-like active fund detection label

2. Added structured comparison/ranking engine in `engine/comparison.py`
   - fund vs fund
   - fund vs benchmark
   - fund vs category average
   - category ranking with top 10, top quartile, bottom quartile

3. Strengthened exit engine trigger
   - Escalates based on: 5Y return < benchmark AND rolling 3Y underperformance > 50%.

4. Enhanced portfolio doctor outputs
   - annualized portfolio return
   - portfolio volatility
   - concentration risk

5. Added tests for new financial metrics and comparison layer.

## Formula reference

- Daily returns: `r_t = NAV_t / NAV_{t-1} - 1`
- Cumulative return: `NAV_end / NAV_start - 1`
- Annualized return: `(NAV_end / NAV_start)^(1/years) - 1`
- Volatility: `std(daily_returns) * sqrt(252)`
- Sharpe: `(annualized_return - rf) / annualized_vol`
- Sortino: `(annualized_return - rf) / downside_dev`
- Beta: `cov(fund, benchmark) / var(benchmark)`
- Alpha: `(mean_fund - beta*mean_bench) * 252`
- Tracking error: `std(fund_ret - bench_ret) * sqrt(252)`
- Consistency score: `% of rolling windows with fund_return > benchmark_return`

## Local-scale architecture guidance (2000+ funds)

- Keep single-source core metrics in `engine/analytics.py`.
- Use `engine/comparison.py` for cross-sectional operations.
- Persist computed metrics in `analytics_cache` and refresh incrementally.
- Batch NAV loading for category/ranking jobs to reduce DB round-trips.
- Continue dashboard-level memoization (`st.cache_data`) for low-latency UX.
