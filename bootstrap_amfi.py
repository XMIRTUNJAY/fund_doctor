"""
Fund Doctor — Full AMFI Bootstrap
====================================
One-time script to populate the database with ALL Indian mutual funds.

What it does:
  1. Downloads NAVAll.txt from AMFI — gets all ~1,500+ active fund schemes
  2. Upserts fund_master with scheme name, AMC, category
  3. Tags Direct/Regular/Growth/IDCW from scheme name
  4. Fetches 10-year NAV history for every fund (rate-limited, resumable)
  5. Fetches benchmark index data via yfinance
  6. Attempts to fetch top-10 stock holdings from AMFI (quarterly disclosure)

Usage:
  python bootstrap_amfi.py                 # full run (all funds, 10Y history)
  python bootstrap_amfi.py --limit 200     # test run with first 200 funds
  python bootstrap_amfi.py --resume        # skip funds with fresh data
  python bootstrap_amfi.py --funds-only    # only update fund_master, no history
  python bootstrap_amfi.py --benchmarks    # only refresh benchmark data
  python bootstrap_amfi.py --status        # show current DB status and exit

IMPORTANT:
  Network connection required. The full run takes 60-90 minutes due to
  AMFI's rate limits (25 requests/min). The script is fully resumable —
  if interrupted, run again with --resume to continue from where it stopped.

Estimated time:
  Full (1,500 funds × 10Y history):  ~90 minutes
  Partial test (200 funds):          ~12 minutes
  Daily update (today's NAV only):   ~3 minutes
"""

import argparse
import logging
import sys
import time
from datetime import datetime, date
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(ROOT / "bootstrap.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("bootstrap")


# ─── Tag Direct/Regular/Growth/IDCW from scheme name ────────────────────────

def _tag_scheme(name: str) -> dict:
    """Extract plan type and option from AMFI scheme name."""
    n = name.lower()
    plan   = "direct"  if "direct" in n else "regular"
    option = "idcw"    if ("idcw" in n or "dividend" in n) else "growth"
    return {"plan": plan, "option": option}


def _map_category(amfi_category: str) -> str:
    """Map AMFI's raw category string to our SEBI classification."""
    c = amfi_category.lower().strip()
    # Equity
    if "large and mid"  in c or "large & mid" in c: return "Equity: Large & Mid Cap"
    if "large cap"      in c:                        return "Equity: Large Cap"
    if "mid cap"        in c:                        return "Equity: Mid Cap"
    if "small cap"      in c:                        return "Equity: Small Cap"
    if "multi cap"      in c:                        return "Equity: Multi Cap"
    if "flexi cap"      in c or "flexi-cap" in c:    return "Equity: Flexi Cap"
    if "focused"        in c:                        return "Equity: Focused"
    if "elss"           in c or "tax sav" in c:      return "Equity: ELSS"
    if "dividend yield" in c:                        return "Equity: Dividend Yield"
    if "value"          in c and "equity" in c:      return "Equity: Value"
    if "contra"         in c:                        return "Equity: Contra"
    if "sectoral"       in c or "sector" in c:       return "Equity: Sectoral"
    if "thematic"       in c:                        return "Equity: Thematic"
    # Hybrid
    if "aggressive"     in c and "hybrid" in c:      return "Hybrid: Aggressive"
    if "balanced advantage" in c:                    return "Hybrid: Balanced Advantage"
    if "dynamic asset"  in c:                        return "Hybrid: Dynamic"
    if "multi asset"    in c:                        return "Hybrid: Multi Asset"
    if "conservative"   in c and "hybrid" in c:      return "Hybrid: Conservative"
    if "equity savings" in c:                        return "Hybrid: Equity Savings"
    if "arbitrage"      in c:                        return "Hybrid: Arbitrage"
    if "hybrid"         in c:                        return "Hybrid: Dynamic"
    # Debt
    if "overnight"      in c:                        return "Debt: Overnight"
    if "liquid"         in c:                        return "Debt: Liquid"
    if "ultra short"    in c or "ultra-short" in c:  return "Debt: Ultra Short"
    if "low duration"   in c:                        return "Debt: Low Duration"
    if "short duration" in c:                        return "Debt: Short Duration"
    if "medium duration" in c:                       return "Debt: Medium Duration"
    if "long duration"  in c:                        return "Debt: Long Duration"
    if "credit risk"    in c:                        return "Debt: Credit Risk"
    if "banking"        in c and "psu" in c:         return "Debt: Banking & PSU"
    if "corporate bond" in c or "corp bond" in c:    return "Debt: Corporate Bond"
    if "gilt"           in c and "10"   in c:        return "Debt: Gilt 10Y Constant"
    if "gilt"           in c:                        return "Debt: Gilt"
    if "dynamic bond"   in c:                        return "Debt: Dynamic Bond"
    if "floater"        in c or "floating" in c:     return "Debt: Floater"
    # Index / ETF
    if "index" in c or "etf" in c:
        if "small"   in c: return "Index: Small Cap"
        if "mid"     in c: return "Index: Mid Cap"
        if "debt"    in c or "bond" in c or "gilt" in c: return "Index: Debt"
        if "inter"   in c or "global" in c or "us" in c: return "Index: International"
        if "sector"  in c or "sectoral" in c:  return "Index: Sectoral"
        return "Index: Large Cap"
    # FOF
    if "fund of fund"   in c or "fof" in c:
        if "inter" in c or "global" in c or "us" in c or "overseas" in c:
            return "FOF: International"
        return "FOF: Domestic"
    # Fallback
    if "equity"   in c: return "Equity: Flexi Cap"
    if "debt"     in c: return "Debt: Short Duration"
    return "Equity: Flexi Cap"


# ─── Stock holdings scraper (AMFI quarterly) ─────────────────────────────────

def fetch_holdings_for_fund(fund_id: str) -> list:
    """
    Attempt to fetch top-10 stock holdings from AMFI monthly portfolio disclosure.
    Returns list of {fund_id, stock_name, sector, weight, as_of_date} dicts.
    NOTE: AMFI's holding disclosure endpoint is inconsistent; this is best-effort.
    """
    try:
        import requests
        # AMFI portfolio disclosure
        url = f"https://www.amfiindia.com/modules/PortfolioReports?subcat=&mfid={fund_id}&ftcode=PURGE&date="
        resp = requests.get(url, timeout=15)
        if not resp.ok or not resp.text.strip():
            return []
        # Parse table — very basic HTML scraping
        import re
        rows = re.findall(r'<td[^>]*>([^<]{2,60})</td>', resp.text)
        # Not reliable enough — return empty for now
        return []
    except Exception:
        return []


# ─── Main bootstrap logic ─────────────────────────────────────────────────────

def run_bootstrap(
    limit:       int  = None,
    resume:      bool = True,
    years:       int  = 10,
    funds_only:  bool = False,
    benchmarks_only: bool = False,
):
    from database.db import initialize_database, get_connection
    from pipeline.amfi_client import (
        fetch_all_funds, upsert_fund_master, upsert_latest_nav,
        bulk_load_nav_history, fetch_benchmarks,
    )

    log.info("=" * 60)
    log.info("FUND DOCTOR — AMFI FULL BOOTSTRAP")
    log.info("=" * 60)

    initialize_database()

    # ── Step 0: Status check ────────────────────────────────────────────────
    conn = get_connection()
    existing_funds  = conn.execute("SELECT COUNT(*) FROM fund_master").fetchone()[0]
    existing_nav    = conn.execute("SELECT COUNT(*) FROM nav_history").fetchone()[0]
    conn.close()
    log.info("Current DB: %d funds, %d NAV rows", existing_funds, existing_nav)

    # ── Step 1: Benchmarks only mode ────────────────────────────────────────
    if benchmarks_only:
        log.info("=== Fetching benchmark data ===")
        result = fetch_benchmarks(years=years)
        for name, rows in result.items():
            log.info("  %s: %d rows", name, rows)
        return

    # ── Step 2: Download NAVAll — all active schemes ────────────────────────
    log.info("=== Step 1/4: Downloading NAVAll.txt from AMFI ===")
    log.info("This fetches the master list of all active MF schemes …")
    df = fetch_all_funds()

    if df.empty:
        log.error("FAILED to fetch NAVAll.txt — check internet connection")
        log.error("If behind a proxy, set HTTPS_PROXY environment variable")
        sys.exit(1)

    log.info("Downloaded: %d total schemes from AMFI", len(df))

    # Enhance with Direct/Regular/Growth/IDCW tagging
    df["plan"]     = df["scheme_name"].apply(lambda n: _tag_scheme(n)["plan"])
    df["option"]   = df["scheme_name"].apply(lambda n: _tag_scheme(n)["option"])
    df["category"] = df["category"].apply(_map_category)

    # Filter to apply limit
    if limit:
        df = df.head(limit)
        log.info("Limited to first %d schemes for this run", limit)

    # ── Step 3: Upsert fund_master ──────────────────────────────────────────
    log.info("=== Step 2/4: Upserting fund_master (%d funds) ===", len(df))
    conn = get_connection()
    cur  = conn.cursor()
    upserted = 0
    for _, row in df.iterrows():
        if not row.get("scheme_code") or not row.get("scheme_name"):
            continue
        cur.execute("""
            INSERT INTO fund_master (fund_id, fund_name, amc, category, sub_category,
                                     plan, option, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(fund_id) DO UPDATE SET
                fund_name    = excluded.fund_name,
                amc          = excluded.amc,
                category     = excluded.category,
                sub_category = excluded.sub_category,
                plan         = excluded.plan,
                option       = excluded.option,
                updated_at   = datetime('now')
        """, (
            str(row["scheme_code"]),
            row["scheme_name"],
            row.get("amc", ""),
            row.get("category", "Equity: Flexi Cap"),
            row.get("sub_category", ""),
            row.get("plan", "direct"),
            row.get("option", "growth"),
        ))
        upserted += 1
        if upserted % 200 == 0:
            conn.commit()
    conn.commit()
    conn.close()
    log.info("fund_master: %d records upserted", upserted)

    # Also insert today's NAV from NAVAll
    upsert_latest_nav(df)

    if funds_only:
        log.info("--funds-only mode: skipping history fetch")
        _print_summary()
        return

    # ── Step 4: Fetch 10-year NAV history ───────────────────────────────────
    fund_ids = df["scheme_code"].astype(str).tolist()
    log.info("=== Step 3/4: Fetching %dY NAV history for %d funds ===", years, len(fund_ids))
    log.info("Rate limit: 25 req/min → estimated time: %d minutes", max(1, len(fund_ids) * 2.5 // 60))
    log.info("Resume mode: %s — press Ctrl+C to pause, re-run with --resume to continue", resume)

    errors = []
    summary = bulk_load_nav_history(
        fund_ids,
        years=years,
        skip_fresh=resume,
        max_age_days=3,
        batch_size=50,
        errors_out=errors,
    )
    log.info("History load: processed=%d skipped=%d rows=%d errors=%d",
             summary["processed"], summary["skipped"],
             summary["rows_inserted"], summary["errors"])

    if errors:
        log.warning("Funds with errors (%d): %s",
                    len(errors), [e["fund_id"] for e in errors[:20]])

    # ── Step 5: Benchmark data ───────────────────────────────────────────────
    log.info("=== Step 4/4: Fetching benchmark data ===")
    try:
        bench_result = fetch_benchmarks(years=years)
        for name, rows in bench_result.items():
            log.info("  %s: %d rows", name, rows)
    except Exception as e:
        log.warning("Benchmark fetch failed (non-critical): %s", e)
        log.warning("Install yfinance: pip install yfinance")

    _print_summary()


def _print_summary():
    from database.db import get_connection
    conn = get_connection()
    funds   = conn.execute("SELECT COUNT(*) FROM fund_master").fetchone()[0]
    nav_r   = conn.execute("SELECT COUNT(*) FROM nav_history").fetchone()[0]
    cats    = conn.execute("SELECT COUNT(DISTINCT category) FROM fund_master").fetchone()[0]
    amcs    = conn.execute("SELECT COUNT(DISTINCT amc) FROM fund_master").fetchone()[0]
    last    = conn.execute("SELECT MAX(date) FROM nav_history").fetchone()[0]
    benches = conn.execute("SELECT COUNT(DISTINCT index_name) FROM benchmark_history").fetchone()[0]
    conn.close()

    log.info("=" * 60)
    log.info("BOOTSTRAP COMPLETE")
    log.info("  Funds in DB:       %d", funds)
    log.info("  NAV rows:          %d (~%.0f MB)", nav_r, nav_r * 30 / 1e6)
    log.info("  Categories:        %d SEBI categories", cats)
    log.info("  AMCs:              %d fund houses", amcs)
    log.info("  Benchmarks:        %d indices", benches)
    log.info("  Latest NAV date:   %s", last)
    log.info("=" * 60)
    log.info("Next steps:")
    log.info("  1. Run: python start.py")
    log.info("  2. Visit: http://localhost:3000")
    log.info("  3. Set up weekly cron: 0 7 * * 1 python bootstrap_amfi.py --resume")
    log.info("=" * 60)


def show_status():
    from database.db import initialize_database, get_connection
    initialize_database()
    conn = get_connection()
    funds   = conn.execute("SELECT COUNT(*) FROM fund_master").fetchone()[0]
    nav_r   = conn.execute("SELECT COUNT(*) FROM nav_history").fetchone()[0]
    last    = conn.execute("SELECT MAX(date) FROM nav_history").fetchone()[0]
    benches = conn.execute("SELECT COUNT(DISTINCT index_name) FROM benchmark_history").fetchone()[0]
    cats    = conn.execute("SELECT category, COUNT(*) as n FROM fund_master GROUP BY category ORDER BY n DESC LIMIT 10").fetchall()
    amcs    = conn.execute("SELECT COUNT(DISTINCT amc) FROM fund_master").fetchone()[0]

    print("\n" + "=" * 55)
    print("  FUND DOCTOR — DATABASE STATUS")
    print("=" * 55)
    print(f"  Funds:          {funds:,}")
    print(f"  NAV rows:       {nav_r:,}  (~{nav_r*30/1e6:.1f} MB)")
    print(f"  AMCs:           {amcs}")
    print(f"  Benchmarks:     {benches} indices")
    print(f"  Last NAV date:  {last}")
    print(f"\n  Top categories:")
    for cat, n in cats:
        print(f"    {cat:<35} {n:>4} funds")
    print("=" * 55)

    if funds < 100:
        print("\n  ⚠  Only demo data loaded.")
        print("  Run: python bootstrap_amfi.py")
        print("  Or:  python bootstrap_amfi.py --limit 200  (quick test)")
    elif nav_r < funds * 50:
        print("\n  ⚠  NAV history incomplete.")
        print("  Run: python bootstrap_amfi.py --resume")
    else:
        print("\n  ✅ Database looks healthy!")
    print()
    conn.close()


# ─── Schema migration — add plan/option columns if missing ──────────────────

def migrate_schema():
    from database.db import initialize_database, get_connection
    initialize_database()
    conn = get_connection()
    try:
        conn.execute("ALTER TABLE fund_master ADD COLUMN plan TEXT DEFAULT 'direct'")
        conn.commit()
        log.info("Added 'plan' column to fund_master")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE fund_master ADD COLUMN option TEXT DEFAULT 'growth'")
        conn.commit()
        log.info("Added 'option' column to fund_master")
    except Exception:
        pass
    conn.close()


# ─── CLI ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fund Doctor — Full AMFI Bootstrap",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python bootstrap_amfi.py                  # full run (all funds, 10Y)
  python bootstrap_amfi.py --limit 200      # quick test (200 funds)
  python bootstrap_amfi.py --resume         # continue interrupted run
  python bootstrap_amfi.py --funds-only     # update fund list, skip history
  python bootstrap_amfi.py --benchmarks     # refresh benchmark data only
  python bootstrap_amfi.py --status         # show current DB state
  python bootstrap_amfi.py --years 5        # only 5Y history (faster)
        """,
    )
    parser.add_argument("--limit",      type=int,  default=None, help="Limit to first N funds (default: all)")
    parser.add_argument("--resume",     action="store_true", default=True, help="Skip funds with recent data (default: True)")
    parser.add_argument("--no-resume",  action="store_true", help="Re-fetch all funds even if fresh")
    parser.add_argument("--years",      type=int,  default=10,   help="Years of history to fetch (default: 10)")
    parser.add_argument("--funds-only", action="store_true",     help="Only update fund_master, no NAV history")
    parser.add_argument("--benchmarks", action="store_true",     help="Only refresh benchmark data")
    parser.add_argument("--status",     action="store_true",     help="Show DB status and exit")
    args = parser.parse_args()

    migrate_schema()

    if args.status:
        show_status()
    else:
        run_bootstrap(
            limit=args.limit,
            resume=not args.no_resume,
            years=args.years,
            funds_only=args.funds_only,
            benchmarks_only=args.benchmarks,
        )
