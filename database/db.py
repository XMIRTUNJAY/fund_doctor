"""
Database initialization and schema management.
Uses SQLite for local MVP deployment.
"""

import sqlite3
import os
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "fund_doctor.db"


def get_connection():
    """Return a SQLite connection to the fund_doctor database."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def initialize_database():
    """Create all tables if they don't exist."""
    conn = get_connection()
    cursor = conn.cursor()

    # ── fund_master ───────────────────────────────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fund_master (
            fund_id       TEXT PRIMARY KEY,
            fund_name     TEXT NOT NULL,
            amc           TEXT,
            category      TEXT,
            sub_category  TEXT,
            benchmark     TEXT,
            launch_date   TEXT,
            expense_ratio REAL,
            aum           REAL,
            fund_manager  TEXT,
            fund_type     TEXT,
            risk_level    TEXT,
            updated_at    TEXT DEFAULT (datetime('now'))
        )
    """)

    # ── nav_history ───────────────────────────────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS nav_history (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_id  TEXT NOT NULL REFERENCES fund_master(fund_id),
            date     TEXT NOT NULL,
            nav      REAL NOT NULL,
            UNIQUE(fund_id, date)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_nav_fund_date ON nav_history(fund_id, date)")

    # ── benchmark_history ─────────────────────────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS benchmark_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            index_name  TEXT NOT NULL,
            date        TEXT NOT NULL,
            index_value REAL NOT NULL,
            UNIQUE(index_name, date)
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_bench_name_date ON benchmark_history(index_name, date)")

    # ── fund_holdings ─────────────────────────────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fund_holdings (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_id    TEXT NOT NULL REFERENCES fund_master(fund_id),
            stock_name TEXT NOT NULL,
            isin       TEXT,
            sector     TEXT,
            weight     REAL,
            as_of_date TEXT,
            UNIQUE(fund_id, stock_name, as_of_date)
        )
    """)

    # ── portfolio_user ────────────────────────────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS portfolio_user (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id         TEXT NOT NULL DEFAULT 'default',
            fund_id         TEXT NOT NULL REFERENCES fund_master(fund_id),
            amount_invested REAL NOT NULL,
            units           REAL,
            purchase_date   TEXT,
            purchase_nav    REAL,
            created_at      TEXT DEFAULT (datetime('now'))
        )
    """)

    # ── analytics_cache ───────────────────────────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS analytics_cache (
            fund_id        TEXT PRIMARY KEY REFERENCES fund_master(fund_id),
            return_1y      REAL,
            return_3y      REAL,
            return_5y      REAL,
            return_10y     REAL,
            return_inception REAL,
            volatility     REAL,
            max_drawdown   REAL,
            sharpe_ratio   REAL,
            sortino_ratio  REAL,
            beta           REAL,
            alpha          REAL,
            underperform_flag TEXT,
            updated_at     TEXT DEFAULT (datetime('now'))
        )
    """)

    conn.commit()
    conn.close()
    print(f"✅ Database initialized at: {DB_PATH}")


if __name__ == "__main__":
    initialize_database()
