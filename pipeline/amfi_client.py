"""
AMFI Client — Hardened Data Fetcher
=====================================
Handles:
  - Rate limiting  (token-bucket, max 30 req/min to AMFI portal)
  - Exponential back-off retries (up to 5 attempts)
  - Batch processing with per-batch commits & progress tracking
  - Robust CSV parsing (handles all AMFI response variants)
  - Full NAV date normalisation (DD-MMM-YYYY, DD/MM/YYYY, YYYY-MM-DD)
  - Resume support  (skips funds already having fresh data)
  - Structured logging with per-fund error capture
  - Edge-case guards: zero NAV, future dates, non-numeric, encoding errors
"""

import csv
import logging
import re
import sys
import time
import threading
from collections import deque
from datetime import datetime, timedelta, date
from io import StringIO
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.db import get_connection, initialize_database

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("amfi")

# ── AMFI endpoints ─────────────────────────────────────────────────────────────
AMFI_NAV_ALL     = "https://www.amfiindia.com/spages/NAVAll.txt"
AMFI_NAV_HISTORY = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"

# ── Rate-limit config ──────────────────────────────────────────────────────────
MAX_REQUESTS_PER_MINUTE = 25     # stay safely under AMFI's limit
BATCH_SIZE              = 50     # funds per DB commit batch
MAX_RETRIES             = 5
BASE_BACKOFF            = 2.0    # seconds, doubles each retry
REQUEST_TIMEOUT         = 45     # seconds
INTER_REQUEST_DELAY     = 60 / MAX_REQUESTS_PER_MINUTE   # ~2.4 s

# ── Date formats accepted by AMFI ─────────────────────────────────────────────
_DATE_FMTS = ["%d-%b-%Y", "%d/%b/%Y", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"]


# ══════════════════════════════════════════════════════════════════════════════
# Token-bucket rate limiter (thread-safe)
# ══════════════════════════════════════════════════════════════════════════════

class _TokenBucket:
    """Sliding-window token bucket. Thread-safe."""

    def __init__(self, rate: int, window: float = 60.0):
        self._rate   = rate        # max requests per window
        self._window = window      # window in seconds
        self._times: deque = deque()
        self._lock   = threading.Lock()

    def acquire(self):
        """Block until a request slot is available."""
        while True:
            with self._lock:
                now = time.monotonic()
                # purge timestamps outside the window
                while self._times and self._times[0] < now - self._window:
                    self._times.popleft()
                if len(self._times) < self._rate:
                    self._times.append(now)
                    return
                # need to wait
                wait_until = self._times[0] + self._window
            sleep_for = wait_until - time.monotonic()
            if sleep_for > 0:
                log.debug("Rate-limit: sleeping %.1fs", sleep_for)
                time.sleep(sleep_for)


_bucket = _TokenBucket(MAX_REQUESTS_PER_MINUTE)


# ══════════════════════════════════════════════════════════════════════════════
# HTTP helper with retry + back-off
# ══════════════════════════════════════════════════════════════════════════════

_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 FundDoctorBot/1.0",
    "Accept-Encoding": "gzip, deflate",
})


def _get(url: str, params: dict = None, stream: bool = False) -> Optional[requests.Response]:
    """
    Rate-limited GET with exponential back-off.
    Returns Response on success, None after all retries exhausted.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        _bucket.acquire()
        try:
            resp = _SESSION.get(url, params=params, timeout=REQUEST_TIMEOUT, stream=stream)
            if resp.status_code == 429:
                wait = BASE_BACKOFF * (2 ** attempt)
                log.warning("HTTP 429 — back-off %.0fs (attempt %d/%d)", wait, attempt, MAX_RETRIES)
                time.sleep(wait)
                continue
            if resp.status_code in (502, 503, 504):
                wait = BASE_BACKOFF * (2 ** attempt)
                log.warning("HTTP %d — back-off %.0fs (attempt %d/%d)", resp.status_code, wait, attempt, MAX_RETRIES)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except requests.exceptions.Timeout:
            log.warning("Timeout on attempt %d/%d for %s", attempt, MAX_RETRIES, url)
        except requests.exceptions.ConnectionError as exc:
            log.warning("Connection error attempt %d/%d: %s", attempt, MAX_RETRIES, exc)
        except requests.exceptions.HTTPError as exc:
            log.warning("HTTP error attempt %d/%d: %s", attempt, MAX_RETRIES, exc)
            break   # 4xx — don't retry
        if attempt < MAX_RETRIES:
            sleep_t = BASE_BACKOFF * (2 ** (attempt - 1))
            log.debug("Sleeping %.1fs before retry", sleep_t)
            time.sleep(sleep_t)
    log.error("All %d retries exhausted for %s", MAX_RETRIES, url)
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Date parsing
# ══════════════════════════════════════════════════════════════════════════════

def _parse_date(raw: str) -> Optional[str]:
    """
    Parse any AMFI date variant → 'YYYY-MM-DD' or None.
    Also rejects dates in the future.
    """
    if not raw or not raw.strip():
        return None
    raw = raw.strip()
    for fmt in _DATE_FMTS:
        try:
            d = datetime.strptime(raw, fmt).date()
            if d > date.today():
                return None      # future date — invalid
            if d < date(1990, 1, 1):
                return None      # unreasonably old
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


# ══════════════════════════════════════════════════════════════════════════════
# NAV validation
# ══════════════════════════════════════════════════════════════════════════════

def _parse_nav(raw: str) -> Optional[float]:
    """Parse NAV string → float or None. Rejects zero, negative, non-numeric."""
    if not raw or raw.strip() in ("", "N.A.", "N/A", "-", "#"):
        return None
    try:
        val = float(raw.strip().replace(",", ""))
        if val <= 0 or val > 1_000_000:   # sanity bounds
            return None
        return round(val, 6)
    except (ValueError, TypeError):
        return None


# ══════════════════════════════════════════════════════════════════════════════
# 1.  NAVAll.txt — full fund universe
# ══════════════════════════════════════════════════════════════════════════════

def fetch_all_funds() -> pd.DataFrame:
    """
    Download and parse AMFI NAVAll.txt.
    Returns DataFrame: scheme_code, scheme_name, nav, date, amc, category, sub_category.
    Handles all known AMFI format variants robustly.
    """
    log.info("Downloading NAVAll.txt …")
    resp = _get(AMFI_NAV_ALL)
    if resp is None:
        log.error("Could not download NAVAll.txt")
        return pd.DataFrame()

    # AMFI serves Windows-1252; fall back to latin-1
    try:
        text = resp.content.decode("utf-8")
    except UnicodeDecodeError:
        text = resp.content.decode("latin-1")

    records        = []
    current_amc    = ""
    current_cat    = ""
    current_subcat = ""

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        if ";" not in line:
            # header / category line
            l = line.lower()
            if any(l.startswith(p) for p in ("open ended", "close ended", "interval", "exchange")):
                current_cat    = line
                current_subcat = ""
            elif current_cat and not any(c.isdigit() for c in line):
                # sub-category under a known category
                current_subcat = line
            else:
                current_amc = line
            continue

        parts = [p.strip() for p in line.split(";")]
        if len(parts) < 5:
            continue

        scheme_code = parts[0]
        scheme_name = parts[3] if len(parts) > 3 else ""
        nav_raw     = parts[4] if len(parts) > 4 else ""
        date_raw    = parts[5] if len(parts) > 5 else ""

        # validate scheme_code is numeric
        if not scheme_code.isdigit():
            continue

        nav_val  = _parse_nav(nav_raw)
        date_val = _parse_date(date_raw)

        if nav_val is None:
            continue     # skip schemes with no valid NAV

        records.append({
            "scheme_code":  scheme_code,
            "scheme_name":  scheme_name,
            "nav":          nav_val,
            "date":         date_val or "",
            "amc":          current_amc,
            "category":     current_cat,
            "sub_category": current_subcat,
        })

    df = pd.DataFrame(records)
    if df.empty:
        log.warning("NAVAll.txt parsed but produced no records")
        return df

    # Deduplicate on scheme_code (keep latest date)
    df.sort_values("date", ascending=False, inplace=True)
    df.drop_duplicates(subset="scheme_code", keep="first", inplace=True)
    df.reset_index(drop=True, inplace=True)

    log.info("NAVAll.txt: %d unique funds parsed", len(df))
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 2.  Persist fund_master
# ══════════════════════════════════════════════════════════════════════════════

def upsert_fund_master(df: pd.DataFrame, limit: Optional[int] = None) -> int:
    """
    Upsert fund_master from parsed NAVAll DataFrame.
    Returns number of rows upserted.
    """
    if df.empty:
        log.warning("upsert_fund_master: empty DataFrame, nothing to insert")
        return 0

    if limit:
        df = df.head(limit)

    conn = get_connection()
    cur  = conn.cursor()
    upserted = 0

    for _, row in df.iterrows():
        if not row["scheme_code"] or not row["scheme_name"]:
            continue
        cur.execute("""
            INSERT INTO fund_master (fund_id, fund_name, amc, category, sub_category, updated_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(fund_id) DO UPDATE SET
                fund_name    = excluded.fund_name,
                amc          = excluded.amc,
                category     = excluded.category,
                sub_category = excluded.sub_category,
                updated_at   = datetime('now')
        """, (
            row["scheme_code"], row["scheme_name"],
            row["amc"],         row["category"],
            row.get("sub_category", ""),
        ))
        upserted += 1

    conn.commit()
    conn.close()
    log.info("fund_master: %d records upserted", upserted)
    return upserted


def upsert_latest_nav(df: pd.DataFrame, limit: Optional[int] = None) -> int:
    """Insert today's NAV from NAVAll into nav_history. Returns rows inserted."""
    if df.empty:
        return 0
    if limit:
        df = df.head(limit)

    conn = get_connection()
    cur  = conn.cursor()
    inserted = 0

    for _, row in df.iterrows():
        if not row["date"] or not row["scheme_code"]:
            continue
        try:
            cur.execute("""
                INSERT OR IGNORE INTO nav_history (fund_id, date, nav)
                VALUES (?, ?, ?)
            """, (row["scheme_code"], row["date"], row["nav"]))
            inserted += 1
        except Exception as exc:
            log.debug("nav_history insert error for %s: %s", row["scheme_code"], exc)

    conn.commit()
    conn.close()
    log.info("nav_history latest NAV: %d rows inserted", inserted)
    return inserted


# ══════════════════════════════════════════════════════════════════════════════
# 3.  Historical NAV — single fund
# ══════════════════════════════════════════════════════════════════════════════

def _detect_nav_column(columns: List[str]) -> Optional[str]:
    """
    Find the NAV column from AMFI's varying column name formats.
    Known variants: 'Net Asset Value', 'NAV', 'Repurchase Price', 'nav'
    """
    priority = ["net_asset_value", "nav", "repurchase_price", "sale_price"]
    for p in priority:
        if p in columns:
            return p
    # fuzzy
    for col in columns:
        if "nav" in col or "asset" in col:
            return col
    return None


def _detect_date_column(columns: List[str]) -> Optional[str]:
    for col in columns:
        if "date" in col:
            return col
    return None


def fetch_nav_history(
    fund_id:   str,
    from_date: Optional[str] = None,
    to_date:   Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch historical NAV for one fund from AMFI portal.
    from_date / to_date: 'DD-MMM-YYYY' format.
    Returns DataFrame with columns [fund_id, date, nav] or empty DataFrame.
    """
    if not from_date:
        from_date = (datetime.now() - timedelta(days=365 * 10)).strftime("%d-%b-%Y")
    if not to_date:
        to_date = datetime.now().strftime("%d-%b-%Y")

    params = {"mf": 0, "tp": 1, "sc": fund_id, "From": from_date, "To": to_date}
    resp   = _get(AMFI_NAV_HISTORY, params=params)

    if resp is None:
        return pd.DataFrame()

    # Decode robustly
    try:
        text = resp.content.decode("utf-8").strip()
    except UnicodeDecodeError:
        text = resp.content.decode("latin-1").strip()

    # AMFI returns HTML error pages on bad scheme codes
    if not text or text.startswith("<") or "No Data" in text or "no records" in text.lower():
        log.debug("No data for fund %s", fund_id)
        return pd.DataFrame()

    # Parse CSV with semicolon separator
    try:
        df = pd.read_csv(
            StringIO(text),
            sep=";",
            header=0,
            dtype=str,
            on_bad_lines="skip",
            encoding_errors="replace",
        )
    except Exception as exc:
        log.warning("CSV parse error for %s: %s", fund_id, exc)
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    # Normalise column names
    df.columns = [
        re.sub(r"[^a-z0-9_]", "_", c.strip().lower().replace(" ", "_"))
        for c in df.columns
    ]

    nav_col  = _detect_nav_column(df.columns.tolist())
    date_col = _detect_date_column(df.columns.tolist())

    if not nav_col or not date_col:
        log.warning("Cannot identify NAV/date columns for fund %s. Columns: %s", fund_id, list(df.columns))
        return pd.DataFrame()

    df = df[[date_col, nav_col]].copy()
    df.rename(columns={date_col: "date_raw", nav_col: "nav_raw"}, inplace=True)

    # Parse and validate each row
    rows = []
    for _, r in df.iterrows():
        d = _parse_date(str(r["date_raw"]))
        n = _parse_nav(str(r["nav_raw"]))
        if d and n:
            rows.append((fund_id, d, n))

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows, columns=["fund_id", "date", "nav"])
    result.drop_duplicates(subset=["fund_id", "date"], keep="last", inplace=True)
    result.sort_values("date", inplace=True)
    return result


# ══════════════════════════════════════════════════════════════════════════════
# 4.  Bulk historical loader  (batch + rate-limit + resume)
# ══════════════════════════════════════════════════════════════════════════════

def _funds_needing_history(fund_ids: List[str], max_age_days: int = 7) -> List[str]:
    """
    Return fund IDs that either have no history or whose latest NAV
    is older than max_age_days (staleness check).
    """
    conn     = get_connection()
    cur      = conn.cursor()
    cutoff   = (datetime.now() - timedelta(days=max_age_days)).strftime("%Y-%m-%d")
    needs    = []

    for fid in fund_ids:
        row = cur.execute(
            "SELECT MAX(date) FROM nav_history WHERE fund_id = ?", (fid,)
        ).fetchone()
        latest = row[0] if row else None
        if not latest or latest < cutoff:
            needs.append(fid)

    conn.close()
    return needs


def bulk_load_nav_history(
    fund_ids:     List[str],
    years:        int   = 10,
    batch_size:   int   = BATCH_SIZE,
    max_age_days: int   = 7,
    skip_fresh:   bool  = True,
    errors_out:   Optional[List[dict]] = None,
) -> dict:
    """
    Fetch and store historical NAV for a list of funds.

    Args:
        fund_ids:     list of AMFI scheme codes
        years:        how many years of history to fetch
        batch_size:   commit to DB every N funds
        max_age_days: skip fund if its latest NAV is fresher than this
        skip_fresh:   if True, skip funds already having fresh data
        errors_out:   if a list is passed, error dicts are appended to it

    Returns:
        dict with keys: processed, skipped, rows_inserted, errors
    """
    initialize_database()

    to_fetch = _funds_needing_history(fund_ids, max_age_days) if skip_fresh else fund_ids
    skipped  = len(fund_ids) - len(to_fetch)
    log.info(
        "Bulk history: %d funds total, %d to fetch, %d skipped (fresh)",
        len(fund_ids), len(to_fetch), skipped,
    )

    if not to_fetch:
        return {"processed": 0, "skipped": skipped, "rows_inserted": 0, "errors": []}

    from_date = (datetime.now() - timedelta(days=365 * years)).strftime("%d-%b-%Y")
    to_date   = datetime.now().strftime("%d-%b-%Y")

    conn         = get_connection()
    cur          = conn.cursor()
    rows_total   = 0
    error_list   = errors_out if errors_out is not None else []
    processed    = 0

    for i, fid in enumerate(to_fetch):
        try:
            df = fetch_nav_history(fid, from_date=from_date, to_date=to_date)

            if df.empty:
                log.debug("No history returned for fund %s", fid)
                error_list.append({"fund_id": fid, "reason": "empty_response"})
            else:
                rows = list(df.itertuples(index=False, name=None))
                cur.executemany(
                    "INSERT OR IGNORE INTO nav_history (fund_id, date, nav) VALUES (?, ?, ?)",
                    rows,
                )
                rows_total += len(rows)
                processed  += 1

        except Exception as exc:
            log.error("Unhandled error for fund %s: %s", fid, exc)
            error_list.append({"fund_id": fid, "reason": str(exc)})

        # Commit every batch
        if (i + 1) % batch_size == 0:
            conn.commit()
            pct = (i + 1) / len(to_fetch) * 100
            log.info(
                "Progress: %d/%d (%.0f%%) | rows so far: %d | errors: %d",
                i + 1, len(to_fetch), pct, rows_total, len(error_list),
            )

        # Inter-request delay — already enforced by token bucket,
        # but add a small fixed floor to avoid burst on fast responses
        time.sleep(max(0, INTER_REQUEST_DELAY - 0.5))

    conn.commit()
    conn.close()

    summary = {
        "processed":    processed,
        "skipped":      skipped,
        "rows_inserted": rows_total,
        "errors":       len(error_list),
    }
    log.info("Bulk load complete: %s", summary)
    return summary


# ══════════════════════════════════════════════════════════════════════════════
# 5.  Benchmark data (Yahoo Finance / fallback NSE CSV)
# ══════════════════════════════════════════════════════════════════════════════

BENCHMARKS = {
    "Nifty 50":           "^NSEI",
    "Nifty Next 50":      "^NSMIDCP",
    "Nifty Midcap 150":   "NIFTYMIDCAP150.NS",
    "Nifty Smallcap 250": "NIFTYSMALLCAP250.NS",
    "Nifty 500":          "^CRSLDX",
}


def fetch_benchmarks(years: int = 10) -> dict:
    """
    Fetch benchmark index history via yfinance.
    Returns {index_name: rows_inserted} dict.
    """
    try:
        import yfinance as yf
    except ImportError:
        log.error("yfinance not installed — run: pip install yfinance")
        return {}

    conn    = get_connection()
    cur     = conn.cursor()
    start   = (datetime.now() - timedelta(days=365 * years)).strftime("%Y-%m-%d")
    results = {}

    for name, ticker in BENCHMARKS.items():
        log.info("Fetching benchmark %s (%s) …", name, ticker)
        try:
            data = yf.download(ticker, start=start, auto_adjust=True, progress=False)
            if data.empty:
                log.warning("No data returned for %s", ticker)
                results[name] = 0
                continue

            # Handle MultiIndex columns (yfinance ≥ 0.2.x)
            if hasattr(data.columns, "levels"):
                data.columns = data.columns.get_level_values(0)

            close_col = "Close" if "Close" in data.columns else data.columns[0]
            rows = []
            for ts, val in zip(data.index, data[close_col]):
                if pd.isna(val) or val <= 0:
                    continue
                rows.append((name, str(ts.date()), round(float(val), 4)))

            cur.executemany("""
                INSERT OR IGNORE INTO benchmark_history (index_name, date, index_value)
                VALUES (?, ?, ?)
            """, rows)
            results[name] = len(rows)
            log.info("  %s: %d rows stored", name, len(rows))

        except Exception as exc:
            log.error("  Error fetching %s: %s", name, exc)
            results[name] = 0

    conn.commit()
    conn.close()
    return results


# ══════════════════════════════════════════════════════════════════════════════
# 6.  Incremental daily update  (designed for cron)
# ══════════════════════════════════════════════════════════════════════════════

def daily_update(limit: Optional[int] = None) -> dict:
    """
    Lightweight daily refresh:
      1. Re-download NAVAll.txt for today's NAV
      2. Upsert fund_master
      3. Insert latest NAVs
      4. Refresh benchmarks (last 5 days to fill any gaps)
    Returns summary dict.
    """
    log.info("=== Daily update started ===")
    df = fetch_all_funds()

    if df.empty:
        log.error("Daily update aborted: could not fetch NAVAll.txt")
        return {"status": "error", "reason": "fetch_failed"}

    fund_rows = upsert_fund_master(df, limit=limit)
    nav_rows  = upsert_latest_nav(df, limit=limit)
    bench     = fetch_benchmarks(years=1)   # only 1 year to be fast

    summary = {
        "status":        "ok",
        "funds_upserted": fund_rows,
        "nav_rows":       nav_rows,
        "benchmarks":     bench,
        "timestamp":      datetime.now().isoformat(),
    }
    log.info("Daily update complete: %s", summary)
    return summary


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="AMFI Client CLI")
    sub    = parser.add_subparsers(dest="cmd")

    p_daily  = sub.add_parser("daily",   help="Run daily update")
    p_full   = sub.add_parser("full",    help="Full history load for N funds")
    p_full.add_argument("--limit", type=int, default=100)
    p_full.add_argument("--years", type=int, default=10)
    p_bench  = sub.add_parser("benchmarks", help="Fetch benchmark data")
    p_bench.add_argument("--years", type=int, default=10)

    args = parser.parse_args()
    initialize_database()

    if args.cmd == "daily":
        daily_update()
    elif args.cmd == "full":
        df = fetch_all_funds()
        upsert_fund_master(df, limit=args.limit)
        fund_ids = df["scheme_code"].head(args.limit).tolist()
        errors   = []
        bulk_load_nav_history(fund_ids, years=args.years, errors_out=errors)
        if errors:
            log.warning("%d funds had errors: %s", len(errors), [e["fund_id"] for e in errors[:10]])
    elif args.cmd == "benchmarks":
        fetch_benchmarks(years=args.years)
    else:
        parser.print_help()
