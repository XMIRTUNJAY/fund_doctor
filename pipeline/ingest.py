"""
Data Ingestion Pipeline
-----------------------
Fetches mutual fund data from AMFI India (free, no API key needed).
Populates fund_master and nav_history tables.
"""

import requests
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.db import get_connection, initialize_database

# ── AMFI endpoints ─────────────────────────────────────────────────────────
AMFI_NAV_ALL      = "https://www.amfiindia.com/spages/NAVAll.txt"
AMFI_NAV_HISTORY  = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"

# ── Benchmark tickers (via Yahoo Finance) ──────────────────────────────────
BENCHMARKS = {
    "Nifty 50":          "^NSEI",
    "Nifty Next 50":     "^NSMIDCP",
    "Nifty Midcap 150":  "NIFTYMIDCAP150.NS",
    "Nifty Smallcap 250":"NIFTYSMALLCAP250.NS",
    "Nifty 500":         "^CRSLDX",
}


# ══════════════════════════════════════════════════════════════════════════════
# 1. Parse AMFI NAVAll.txt → fund_master
# ══════════════════════════════════════════════════════════════════════════════

def fetch_all_funds_from_amfi() -> pd.DataFrame:
    """
    Parse the AMFI NAVAll.txt file.
    Returns a DataFrame with columns:
        scheme_code, scheme_name, net_asset_value, date, amc, category
    """
    print("⬇  Downloading NAVAll.txt from AMFI …")
    resp = requests.get(AMFI_NAV_ALL, timeout=60)
    resp.raise_for_status()
    lines = resp.text.splitlines()

    records = []
    current_amc      = ""
    current_category = ""

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # AMC header line (no semicolons)
        if ";" not in line:
            # Could be AMC name or category header
            if line.startswith("Open Ended") or line.startswith("Close Ended") or \
               line.startswith("Interval") or line.startswith("Exchange"):
                current_category = line
            else:
                current_amc = line
            continue

        parts = line.split(";")
        if len(parts) < 5:
            continue

        try:
            scheme_code = parts[0].strip()
            isin_div    = parts[1].strip()
            isin_growth = parts[2].strip()
            scheme_name = parts[3].strip()
            nav_str     = parts[4].strip()
            date_str    = parts[5].strip() if len(parts) > 5 else ""

            nav = float(nav_str) if nav_str not in ("", "N.A.", "-") else None
            if nav is None:
                continue

            records.append({
                "scheme_code": scheme_code,
                "scheme_name": scheme_name,
                "nav":         nav,
                "date":        date_str,
                "amc":         current_amc,
                "category":    current_category,
            })
        except (ValueError, IndexError):
            continue

    df = pd.DataFrame(records)
    print(f"   Found {len(df):,} fund records from AMFI.")
    return df


def seed_fund_master(df: pd.DataFrame, limit: int = None):
    """
    Insert / update fund_master from the parsed AMFI data.
    `limit` caps the number of funds for quick testing.
    """
    conn = get_connection()
    cur  = conn.cursor()

    if limit:
        df = df.head(limit)

    inserted = 0
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO fund_master (fund_id, fund_name, amc, category, updated_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(fund_id) DO UPDATE SET
                fund_name  = excluded.fund_name,
                amc        = excluded.amc,
                category   = excluded.category,
                updated_at = datetime('now')
        """, (row["scheme_code"], row["scheme_name"], row["amc"], row["category"]))
        inserted += 1

    conn.commit()
    conn.close()
    print(f"✅ fund_master: {inserted:,} records upserted.")


def seed_latest_nav(df: pd.DataFrame, limit: int = None):
    """Insert the latest NAV (from NAVAll.txt) into nav_history."""
    conn = get_connection()
    cur  = conn.cursor()

    if limit:
        df = df.head(limit)

    inserted = 0
    for _, row in df.iterrows():
        if not row["date"]:
            continue
        try:
            cur.execute("""
                INSERT OR IGNORE INTO nav_history (fund_id, date, nav)
                VALUES (?, ?, ?)
            """, (row["scheme_code"], row["date"], row["nav"]))
            inserted += 1
        except Exception:
            pass

    conn.commit()
    conn.close()
    print(f"✅ nav_history: {inserted:,} latest NAV records inserted.")


# ══════════════════════════════════════════════════════════════════════════════
# 2. Fetch historical NAV for a single fund from AMFI portal
# ══════════════════════════════════════════════════════════════════════════════

def fetch_nav_history_for_fund(fund_id: str, from_date: str = None, to_date: str = None) -> pd.DataFrame:
    """
    Download historical NAV for one fund from AMFI portal.
    from_date / to_date: 'DD-MMM-YYYY' format (e.g. '01-Jan-2019')
    """
    if not from_date:
        from_date = (datetime.now() - timedelta(days=365 * 10)).strftime("%d-%b-%Y")
    if not to_date:
        to_date = datetime.now().strftime("%d-%b-%Y")

    params = {
        "mf":     0,
        "tp":     1,
        "sc":     fund_id,
        "From":   from_date,
        "To":     to_date,
    }
    try:
        resp = requests.get(AMFI_NAV_HISTORY, params=params, timeout=30)
        resp.raise_for_status()
        text = resp.text.strip()

        if not text or "No Data" in text:
            return pd.DataFrame()

        df = pd.read_csv(StringIO(text), sep=";", header=0)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        # Normalise column names across AMFI response variants
        rename_map = {}
        for col in df.columns:
            if "nav" in col and "net" not in col:
                rename_map[col] = "nav"
            elif "date" in col:
                rename_map[col] = "date"
        df.rename(columns=rename_map, inplace=True)

        if "nav" not in df.columns or "date" not in df.columns:
            return pd.DataFrame()

        df["nav"]  = pd.to_numeric(df["nav"],  errors="coerce")
        df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce")
        df.dropna(subset=["nav", "date"], inplace=True)
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        df["fund_id"] = fund_id
        return df[["fund_id", "date", "nav"]]

    except Exception as e:
        print(f"   ⚠  Could not fetch history for {fund_id}: {e}")
        return pd.DataFrame()


def bulk_load_nav_history(fund_ids: list, delay: float = 0.3):
    """Fetch and store 10-year NAV history for a list of fund IDs."""
    conn = get_connection()
    cur  = conn.cursor()
    total = 0

    for i, fid in enumerate(fund_ids):
        df = fetch_nav_history_for_fund(fid)
        if df.empty:
            continue

        rows = list(df[["fund_id", "date", "nav"]].itertuples(index=False, name=None))
        cur.executemany("""
            INSERT OR IGNORE INTO nav_history (fund_id, date, nav)
            VALUES (?, ?, ?)
        """, rows)
        total += len(rows)

        if (i + 1) % 20 == 0:
            conn.commit()
            print(f"   … {i+1}/{len(fund_ids)} funds processed, {total:,} rows so far")

        time.sleep(delay)

    conn.commit()
    conn.close()
    print(f"✅ nav_history bulk load complete: {total:,} rows inserted.")


# ══════════════════════════════════════════════════════════════════════════════
# 3. Benchmark data via Yahoo Finance
# ══════════════════════════════════════════════════════════════════════════════

def fetch_benchmark_data(years: int = 10):
    """Download benchmark index data via yfinance and store in benchmark_history."""
    try:
        import yfinance as yf
    except ImportError:
        print("⚠  yfinance not installed. Skipping benchmark download.")
        return

    conn = get_connection()
    cur  = conn.cursor()
    start = (datetime.now() - timedelta(days=365 * years)).strftime("%Y-%m-%d")

    for name, ticker in BENCHMARKS.items():
        print(f"   ⬇  Fetching {name} ({ticker}) …")
        try:
            data = yf.download(ticker, start=start, auto_adjust=True, progress=False)
            if data.empty:
                print(f"      No data for {ticker}")
                continue

            rows = [
                (name, str(date.date()), float(close))
                for date, close in zip(data.index, data["Close"])
                if not pd.isna(close)
            ]
            cur.executemany("""
                INSERT OR IGNORE INTO benchmark_history (index_name, date, index_value)
                VALUES (?, ?, ?)
            """, rows)
            print(f"      {len(rows):,} rows stored.")
        except Exception as e:
            print(f"      Error: {e}")

    conn.commit()
    conn.close()
    print("✅ Benchmark data loaded.")


# ══════════════════════════════════════════════════════════════════════════════
# 4. Demo / seed data (for offline / testing use)
# ══════════════════════════════════════════════════════════════════════════════

def seed_demo_data():
    """
    Seed a small set of well-known funds with synthetic NAV history
    so the dashboard works without internet access.
    """
    import numpy as np

    DEMO_FUNDS = [
        ("119551", "Mirae Asset Large Cap Fund - Growth", "Mirae Asset", "Equity: Large Cap",   "Nifty 50",       1.1),
        ("120503", "Axis Bluechip Fund - Growth",         "Axis AMC",    "Equity: Large Cap",   "Nifty 50",       0.87),
        ("100033", "SBI Small Cap Fund - Growth",         "SBI MF",      "Equity: Small Cap",   "Nifty Smallcap 250", 1.6),
        ("118825", "HDFC Mid-Cap Opportunities - Growth", "HDFC AMC",    "Equity: Mid Cap",     "Nifty Midcap 150",   1.42),
        ("120716", "Parag Parikh Flexi Cap - Growth",     "PPFAS",       "Equity: Flexi Cap",   "Nifty 500",      0.77),
        ("102885", "ICICI Pru Balanced Advantage - Growth","ICICI Pru",  "Hybrid: Dynamic",     "Nifty 50",       1.05),
        ("119598", "Nippon India Index Nifty 50 - Growth","Nippon India","Index: Large Cap",    "Nifty 50",       0.20),
        ("125354", "Kotak Emerging Equity - Growth",      "Kotak AMC",   "Equity: Mid Cap",     "Nifty Midcap 150",   0.98),
    ]

    conn = get_connection()
    cur  = conn.cursor()

    # fund_master
    for fid, name, amc, cat, bench, er in DEMO_FUNDS:
        cur.execute("""
            INSERT INTO fund_master (fund_id, fund_name, amc, category, benchmark, expense_ratio, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(fund_id) DO UPDATE SET
                fund_name=excluded.fund_name, amc=excluded.amc,
                category=excluded.category, benchmark=excluded.benchmark,
                expense_ratio=excluded.expense_ratio, updated_at=datetime('now')
        """, (fid, name, amc, cat, bench, er))

    conn.commit()

    # nav_history — 5 years of daily NAV with realistic drift
    np.random.seed(42)
    dates = pd.date_range(end=datetime.today(), periods=365 * 5, freq="B")

    annual_returns = [0.14, 0.12, 0.18, 0.16, 0.20, 0.10, 0.13, 0.17]
    volatilities   = [0.15, 0.13, 0.22, 0.19, 0.14, 0.11, 0.10, 0.20]

    for (fid, *_), ann_ret, vol in zip(DEMO_FUNDS, annual_returns, volatilities):
        daily_ret  = ann_ret / 252
        daily_vol  = vol / (252 ** 0.5)
        returns    = np.random.normal(daily_ret, daily_vol, len(dates))
        nav_series = 10.0 * np.cumprod(1 + returns)

        rows = [(fid, str(d.date()), round(float(n), 4)) for d, n in zip(dates, nav_series)]
        cur.executemany("""
            INSERT OR IGNORE INTO nav_history (fund_id, date, nav) VALUES (?, ?, ?)
        """, rows)

    # benchmark_history
    bench_params = {
        "Nifty 50":           (0.13, 0.15, 12000),
        "Nifty Midcap 150":   (0.16, 0.20,  8000),
        "Nifty Smallcap 250": (0.18, 0.25,  5000),
        "Nifty 500":          (0.14, 0.16, 10000),
    }
    for bname, (ann_ret, vol, start_val) in bench_params.items():
        daily_ret  = ann_ret / 252
        daily_vol  = vol / (252 ** 0.5)
        returns    = np.random.normal(daily_ret, daily_vol, len(dates))
        vals       = start_val * np.cumprod(1 + returns)
        rows = [(bname, str(d.date()), round(float(v), 2)) for d, v in zip(dates, vals)]
        cur.executemany("""
            INSERT OR IGNORE INTO benchmark_history (index_name, date, index_value) VALUES (?, ?, ?)
        """, rows)

    # Sample portfolio
    portfolio = [
        ("119551", 50000, "2021-01-15"),
        ("120503", 30000, "2021-03-10"),
        ("100033", 20000, "2022-06-01"),
        ("118825", 25000, "2021-09-15"),
        ("120716", 40000, "2020-12-01"),
    ]
    for fid, amount, pdate in portfolio:
        cur.execute("""
            INSERT INTO portfolio_user (user_id, fund_id, amount_invested, purchase_date)
            VALUES ('default', ?, ?, ?)
        """, (fid, amount, pdate))

    conn.commit()
    conn.close()
    print("✅ Demo data seeded successfully.")


# ══════════════════════════════════════════════════════════════════════════════
# CLI entry point
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fund Doctor Data Pipeline")
    parser.add_argument("--init",        action="store_true", help="Initialise database")
    parser.add_argument("--demo",        action="store_true", help="Seed demo / offline data")
    parser.add_argument("--amfi",        action="store_true", help="Fetch all funds from AMFI")
    parser.add_argument("--benchmarks",  action="store_true", help="Fetch benchmark data")
    parser.add_argument("--limit",       type=int, default=100, help="Fund limit for AMFI seed")
    args = parser.parse_args()

    if args.init or True:          # always initialise
        initialize_database()

    if args.demo:
        seed_demo_data()

    if args.amfi:
        df = fetch_all_funds_from_amfi()
        seed_fund_master(df, limit=args.limit)
        seed_latest_nav(df, limit=args.limit)

    if args.benchmarks:
        fetch_benchmark_data()
