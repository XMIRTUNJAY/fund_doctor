"""
Unit tests for the Fund Doctor analytics engine.
Run with: python -m pytest tests/test_analytics.py -v
"""

import sys
import numpy as np
import pandas as pd
import pytest
from pathlib import Path
from datetime import date, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from engine.analytics import (
    calculate_cagr,
    calculate_cumulative_return,
    calculate_annualized_return,
    calculate_volatility,
    calculate_max_drawdown,
    calculate_sharpe_ratio,
    calculate_sortino_ratio,
    calculate_beta_alpha,
    calculate_tracking_error,
    calculate_correlation,
    rolling_returns,
    consistency_score,
    detect_index_like_behavior,
    daily_returns,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def make_nav(annual_return: float = 0.12, years: int = 5, vol: float = 0.15) -> pd.Series:
    np.random.seed(0)
    n = int(years * 252)
    dr = annual_return / 252
    dv = vol / (252 ** 0.5)
    rets = np.random.normal(dr, dv, n)
    navs = 10.0 * np.cumprod(1 + rets)
    dates = pd.bdate_range(end=date.today(), periods=n)
    return pd.Series(navs, index=dates)


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestCAGR:
    def test_positive_return(self):
        nav = make_nav(0.12, 5)
        cagr = calculate_cagr(nav, 5)
        assert cagr is not None
        assert 0.05 < cagr < 0.25, f"Expected ~12%, got {cagr:.2%}"

    def test_insufficient_history(self):
        nav = make_nav(0.12, 1)
        cagr = calculate_cagr(nav, 5)  # asking for 5Y when only 1Y available
        assert cagr is None

    def test_empty_series(self):
        assert calculate_cagr(pd.Series(dtype=float), 5) is None


class TestVolatility:
    def test_reasonable_volatility(self):
        nav = make_nav(0.12, 5, vol=0.15)
        vol = calculate_volatility(nav)
        assert vol is not None
        assert 0.05 < vol < 0.35, f"Expected ~15% vol, got {vol:.2%}"

    def test_empty(self):
        assert calculate_volatility(pd.Series(dtype=float)) is None


class TestReturnMetrics:
    def test_cumulative_return(self):
        nav = pd.Series([100.0, 110.0], index=pd.bdate_range("2023-01-01", periods=2))
        assert calculate_cumulative_return(nav) == pytest.approx(0.10, abs=1e-6)

    def test_annualized_return(self):
        dates = pd.to_datetime(["2020-01-01", "2021-01-01"])
        nav = pd.Series([100.0, 121.0], index=dates)
        ann = calculate_annualized_return(nav)
        assert ann is not None
        assert ann == pytest.approx(0.21, abs=1e-2)


class TestMaxDrawdown:
    def test_negative_value(self):
        nav = make_nav(0.12, 5, 0.20)
        dd = calculate_max_drawdown(nav)
        assert dd is not None
        assert dd < 0, "Drawdown must be negative"
        assert dd > -1.0, "Drawdown can't be worse than -100%"

    def test_monotone_increasing(self):
        nav = pd.Series([10, 11, 12, 13, 14], index=pd.bdate_range("2020-01-01", periods=5))
        dd = calculate_max_drawdown(nav)
        assert dd == pytest.approx(0.0, abs=1e-6)


class TestSharpeRatio:
    def test_high_return_positive_sharpe(self):
        nav = make_nav(0.20, 5, 0.10)
        sharpe = calculate_sharpe_ratio(nav)
        assert sharpe is not None
        assert sharpe > 0

    def test_low_return_potentially_negative(self):
        nav = make_nav(0.03, 5, 0.20)
        sharpe = calculate_sharpe_ratio(nav)
        assert sharpe is not None  # result can be negative


class TestSortinoRatio:
    def test_sortino_computed(self):
        nav = make_nav(0.15, 5, 0.12)
        sortino = calculate_sortino_ratio(nav)
        assert sortino is not None


class TestBetaAlpha:
    def test_same_series_beta_is_one(self):
        nav = make_nav(0.12, 5, 0.15)
        beta, alpha = calculate_beta_alpha(nav, nav)
        assert beta is not None
        assert abs(beta - 1.0) < 0.05

    def test_empty_series(self):
        nav = make_nav(0.12, 5)
        beta, alpha = calculate_beta_alpha(nav, pd.Series(dtype=float))
        assert beta is None
        assert alpha is None

    def test_tracking_and_correlation(self):
        nav = make_nav(0.12, 5, 0.15)
        te = calculate_tracking_error(nav, nav)
        corr = calculate_correlation(nav, nav)
        assert te is not None
        assert te == pytest.approx(0.0, abs=1e-6)
        assert corr is not None
        assert corr == pytest.approx(1.0, abs=1e-6)


class TestRollingReturns:
    def test_returns_series(self):
        nav  = make_nav(0.12, 5, 0.15)
        roll = rolling_returns(nav, 1)
        assert isinstance(roll, pd.Series)
        assert len(roll) > 0

    def test_insufficient_data(self):
        nav  = make_nav(0.12, 1)
        roll = rolling_returns(nav, 3)
        assert roll.empty


class TestConsistencyAndIndexLike:
    def test_consistency_score_bounds(self):
        nav = make_nav(0.13, 5, 0.14)
        bench = make_nav(0.12, 5, 0.15)
        score = consistency_score(nav, bench, 1)
        assert score is not None
        assert 0 <= score <= 100

    def test_index_like_flag(self):
        nav = make_nav(0.12, 5, 0.15)
        result = detect_index_like_behavior(nav, nav, tracking_error_threshold=0.001, correlation_threshold=0.99)
        assert result["index_like"] is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
