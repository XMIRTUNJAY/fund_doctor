import sqlite3
from datetime import date

import numpy as np
import pandas as pd

import database.db as db
import engine.analytics as analytics
import engine.exit_strategy as exit_strategy
from engine.analytics import fund_data_quality, fund_decision_card, portfolio_analytics
from engine.comparison import rank_funds_by_category
from engine.exit_strategy import assess_exit


class _NC:
    def __init__(self, c):
        self._c = c

    def __getattr__(self, n):
        return getattr(self._c, n)

    def close(self):
        pass


def _seed_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    conn.execute("CREATE TABLE fund_master (fund_id TEXT PRIMARY KEY, fund_name TEXT, amc TEXT, category TEXT, benchmark TEXT, expense_ratio REAL)")
    conn.execute("CREATE TABLE nav_history (fund_id TEXT, date TEXT, nav REAL)")
    conn.execute("CREATE TABLE benchmark_history (index_name TEXT, date TEXT, index_value REAL)")
    conn.execute("CREATE TABLE portfolio_user (user_id TEXT, fund_id TEXT, amount_invested REAL, purchase_date TEXT, purchase_nav REAL)")

    conn.executemany(
        "INSERT INTO fund_master (fund_id,fund_name,amc,category,benchmark,expense_ratio) VALUES (?,?,?,?,?,?)",
        [
            ("F1", "Fund One", "AMC A", "Equity", "Nifty 50", 1.0),
            ("F2", "Fund Two", "AMC B", "Equity", "Nifty 50", 1.6),
            ("F3", "Fund Three", "AMC C", "Equity", "Nifty 50", 0.8),
        ],
    )

    rng = np.random.default_rng(0)
    n = 252 * 5
    dates = pd.bdate_range(end=date.today(), periods=n)

    for fid, ar, vol, start in [("F1", 0.14, 0.14, 100.0), ("F2", 0.07, 0.17, 80.0), ("F3", 0.16, 0.16, 95.0)]:
        nav = start * np.cumprod(1 + rng.normal(ar / 252, vol / (252**0.5), n))
        rows = [(fid, str(d.date()), float(v)) for d, v in zip(dates, nav)]
        conn.executemany("INSERT INTO nav_history (fund_id,date,nav) VALUES (?,?,?)", rows)

    bench = 12000 * np.cumprod(1 + rng.normal(0.11 / 252, 0.14 / (252**0.5), n))
    rows = [("Nifty 50", str(d.date()), float(v)) for d, v in zip(dates, bench)]
    conn.executemany("INSERT INTO benchmark_history (index_name,date,index_value) VALUES (?,?,?)", rows)

    conn.executemany(
        "INSERT INTO portfolio_user (user_id,fund_id,amount_invested,purchase_date,purchase_nav) VALUES (?,?,?,?,?)",
        [
            ("test", "F1", 50000, "2021-01-01", 95.0),
            ("test", "F2", 30000, "2021-01-01", 80.0),
            ("test", "F3", 20000, "2021-01-01", 90.0),
        ],
    )

    conn.commit()
    return _NC(conn)


def _patch(monkeypatch, conn):
    monkeypatch.setattr(db, "get_connection", lambda: conn)
    monkeypatch.setattr(analytics.db, "get_connection", lambda: conn)


def test_fund_data_quality(monkeypatch):
    conn = _seed_conn()
    _patch(monkeypatch, conn)

    q = fund_data_quality("F1")
    assert q["quality_score"] >= 0
    assert q["status"] in ("HIGH", "MEDIUM", "LOW")
    assert q["nav_points"] > 100


def test_fund_decision_card(monkeypatch):
    conn = _seed_conn()
    _patch(monkeypatch, conn)
    monkeypatch.setattr(exit_strategy, "load_nav", analytics.load_nav)
    monkeypatch.setattr(exit_strategy, "load_benchmark", analytics.load_benchmark)

    d = fund_decision_card("F1")
    assert d["grade"] in ("A", "B", "C", "D")
    assert d["verdict"] in ("STRONG_HOLD", "HOLD", "WATCH", "REVIEW_EXIT")
    assert "data_quality" in d


def test_exit_trigger_details(monkeypatch):
    conn = _seed_conn()
    _patch(monkeypatch, conn)
    monkeypatch.setattr(exit_strategy, "load_nav", analytics.load_nav)
    monkeypatch.setattr(exit_strategy, "load_benchmark", analytics.load_benchmark)

    result = assess_exit("F2", holding_months=24, invested_amount=50000)
    assert isinstance(result.get("trigger_details"), list)


def test_portfolio_prioritized_actions(monkeypatch):
    conn = _seed_conn()
    _patch(monkeypatch, conn)

    r = portfolio_analytics("test")
    assert "prioritized_actions" in r
    assert isinstance(r["prioritized_actions"], list)


def test_category_ranking(monkeypatch):
    conn = _seed_conn()
    _patch(monkeypatch, conn)

    rr = rank_funds_by_category("Equity", top_n=5)
    assert rr["count"] == 3
    assert len(rr["top_10"]) > 0
