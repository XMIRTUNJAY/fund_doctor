import sqlite3
from datetime import date

import numpy as np
import pandas as pd

import database.db as db
import engine.analytics as analytics
from engine.comparison import fund_vs_fund, fund_vs_benchmark, rank_funds_by_category


class _NC:
    def __init__(self, c): self._c = c
    def __getattr__(self, n): return getattr(self._c, n)
    def close(self):
        pass


def _make_seeded_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE fund_master (fund_id TEXT PRIMARY KEY, fund_name TEXT, amc TEXT, category TEXT, benchmark TEXT, expense_ratio REAL)"
    )
    conn.execute("CREATE TABLE nav_history (fund_id TEXT, date TEXT, nav REAL)")
    conn.execute("CREATE TABLE benchmark_history (index_name TEXT, date TEXT, index_value REAL)")

    rng = np.random.default_rng(42)
    n = 252 * 5
    dates = pd.bdate_range(end=date.today(), periods=n)

    conn.executemany(
        "INSERT INTO fund_master (fund_id,fund_name,amc,category,benchmark,expense_ratio) VALUES (?,?,?,?,?,?)",
        [
            ("F1", "Fund 1", "A", "Equity", "Nifty 50", 1.0),
            ("F2", "Fund 2", "B", "Equity", "Nifty 50", 0.9),
            ("F3", "Fund 3", "C", "Equity", "Nifty 50", 0.6),
        ],
    )

    for fid, ar, vol, start in [("F1", 0.12, 0.14, 100.0), ("F2", 0.10, 0.18, 95.0), ("F3", 0.15, 0.16, 102.0)]:
        nav = start * np.cumprod(1 + rng.normal(ar / 252, vol / (252**0.5), n))
        rows = [(fid, str(d.date()), float(v)) for d, v in zip(dates, nav)]
        conn.executemany("INSERT INTO nav_history (fund_id,date,nav) VALUES (?,?,?)", rows)

    bench = 12000 * np.cumprod(1 + rng.normal(0.11 / 252, 0.15 / (252**0.5), n))
    rows = [("Nifty 50", str(d.date()), float(v)) for d, v in zip(dates, bench)]
    conn.executemany("INSERT INTO benchmark_history (index_name,date,index_value) VALUES (?,?,?)", rows)
    conn.commit()
    return _NC(conn)


def test_fund_vs_fund_shape(monkeypatch):
    conn = _make_seeded_conn()
    monkeypatch.setattr(db, "get_connection", lambda: conn)
    monkeypatch.setattr(analytics.db, "get_connection", lambda: conn)

    r = fund_vs_fund("F1", "F2")
    assert r["mode"] == "fund_vs_fund"
    assert "left" in r and "right" in r


def test_fund_vs_benchmark_metrics(monkeypatch):
    conn = _make_seeded_conn()
    monkeypatch.setattr(db, "get_connection", lambda: conn)
    monkeypatch.setattr(analytics.db, "get_connection", lambda: conn)

    r = fund_vs_benchmark("F1")
    assert r["mode"] == "fund_vs_benchmark"
    assert "metrics" in r
    assert "consistency_3y" in r["metrics"]


def test_rank_by_category(monkeypatch):
    conn = _make_seeded_conn()
    monkeypatch.setattr(db, "get_connection", lambda: conn)
    monkeypatch.setattr(analytics.db, "get_connection", lambda: conn)

    r = rank_funds_by_category("Equity")
    assert r["count"] >= 3
    assert len(r["top_10"]) > 0
