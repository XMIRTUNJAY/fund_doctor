"""
Fund Doctor — Complete Test Suite
===================================
Coverage:
  1.  Date parsing          — all AMFI formats, edge cases
  2.  NAV parsing           — zero/negative/non-numeric/overflow guards
  3.  Token-bucket limiter  — rate enforcement, thread safety
  4.  Column detection      — all known AMFI header variants
  5.  CAGR                  — normal, edge, boundary
  6.  Volatility            — annualised, constant, single-point
  7.  Max Drawdown          — step-drop, monotone, all-equal
  8.  Sharpe / Sortino      — high/low return, zero-vol, no-downside
  9.  Beta / Alpha          — same series, high-beta construction, empty inputs
  10. Rolling returns       — window sizes, insufficient data, zero-return
  11. Full analytics bundle — valid fund, missing fund, empty fund, NaN check
  12. Underperformance      — all flag paths, missing/empty fund safety
  13. Portfolio analytics   — health score, allocation sum, empty portfolio
  14. Exit strategy         — all rec paths, STCG/LTCG notes, replacements
  15. Database integrity    — table existence, upserts, uniqueness constraint
  16. fetch_nav_history     — mocked CSV, network fail, zero/future NAV removal
  17. fetch_all_funds       — mocked NAVAll, AMC propagation, dedup
  18. Bulk load             — insert, empty response, exception capture, skip-fresh
  19. HTTP retry            — timeout retry, 404 no-retry, all-retries-exhausted

Run:  python tests/test_full.py
"""

import math
import sqlite3
import sys
import threading
import time
import unittest
import warnings
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")

# ══════════════════════════════════════════════════════════════════════════════
# Shared in-memory DB — conn.close() is a no-op so production code
# cannot destroy the single test connection.
# ══════════════════════════════════════════════════════════════════════════════

class _NC:
    """Non-Closing sqlite3 wrapper."""
    def __init__(self, c):    object.__setattr__(self, "_c", c)
    def __getattr__(self, n): return getattr(object.__getattribute__(self, "_c"), n)
    def close(self): pass
    def cursor(self):               return object.__getattribute__(self,"_c").cursor()
    def execute(self,*a,**k):       return object.__getattribute__(self,"_c").execute(*a,**k)
    def executemany(self,*a,**k):   return object.__getattribute__(self,"_c").executemany(*a,**k)
    def commit(self):               return object.__getattribute__(self,"_c").commit()
    def rollback(self):             return object.__getattribute__(self,"_c").rollback()
    def __enter__(self):            return object.__getattribute__(self,"_c").__enter__()
    def __exit__(self,*a):          return object.__getattribute__(self,"_c").__exit__(*a)


_RAW = sqlite3.connect(":memory:", check_same_thread=False)
_RAW.row_factory = sqlite3.Row
_RAW.execute("PRAGMA foreign_keys=ON")
_CONN = _NC(_RAW)

# Patch DB module BEFORE importing anything that uses it
import database.db as _db
_db.get_connection = lambda: _CONN


def _create_tables():
    stmts = [
        "CREATE TABLE IF NOT EXISTS fund_master ("
        "fund_id TEXT PRIMARY KEY, fund_name TEXT NOT NULL, amc TEXT,"
        "category TEXT, sub_category TEXT, benchmark TEXT, launch_date TEXT,"
        "expense_ratio REAL, aum REAL, fund_manager TEXT, fund_type TEXT,"
        "risk_level TEXT, updated_at TEXT)",

        "CREATE TABLE IF NOT EXISTS nav_history ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, fund_id TEXT NOT NULL,"
        "date TEXT NOT NULL, nav REAL NOT NULL, UNIQUE(fund_id,date))",

        "CREATE TABLE IF NOT EXISTS benchmark_history ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, index_name TEXT NOT NULL,"
        "date TEXT NOT NULL, index_value REAL NOT NULL, UNIQUE(index_name,date))",

        "CREATE TABLE IF NOT EXISTS fund_holdings ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, fund_id TEXT NOT NULL,"
        "stock_name TEXT NOT NULL, isin TEXT, sector TEXT, weight REAL,"
        "as_of_date TEXT, UNIQUE(fund_id,stock_name,as_of_date))",

        "CREATE TABLE IF NOT EXISTS portfolio_user ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "user_id TEXT NOT NULL DEFAULT 'default',"
        "fund_id TEXT NOT NULL, amount_invested REAL NOT NULL, units REAL,"
        "purchase_date TEXT, purchase_nav REAL, created_at TEXT)",

        "CREATE TABLE IF NOT EXISTS analytics_cache ("
        "fund_id TEXT PRIMARY KEY, return_1y REAL, return_3y REAL,"
        "return_5y REAL, return_10y REAL, return_inception REAL,"
        "volatility REAL, max_drawdown REAL, sharpe_ratio REAL,"
        "sortino_ratio REAL, beta REAL, alpha REAL,"
        "underperform_flag TEXT, updated_at TEXT)",
    ]
    for s in stmts:
        _RAW.execute(s)
    _RAW.commit()


_create_tables()

# Patch initialize_database everywhere so it never touches the real file DB
def _noop_init(): _create_tables()
_db.initialize_database = _noop_init
import pipeline.amfi_client as _ami
_ami.initialize_database = _noop_init

# Now safe to import all application code
from engine.analytics import (
    calculate_cagr, calculate_volatility, calculate_max_drawdown,
    calculate_sharpe_ratio, calculate_sortino_ratio, calculate_beta_alpha,
    rolling_returns, compute_fund_analytics, detect_underperformance,
    portfolio_analytics,
)
from engine.exit_strategy import assess_exit, find_replacement_funds
from pipeline.amfi_client import (
    _parse_date, _parse_nav, _TokenBucket,
    _detect_nav_column, _detect_date_column,
    fetch_nav_history, fetch_all_funds as amfi_fetch_all,
    bulk_load_nav_history, upsert_fund_master, upsert_latest_nav,
    MAX_RETRIES,
)

# ── Test data ─────────────────────────────────────────────────────────────────

def _make_nav(ret=0.12, years=5, vol=0.15, start=100.0, seed=42):
    rng   = np.random.default_rng(seed)
    n     = int(years * 252)
    rets  = rng.normal(ret/252, vol/(252**0.5), n)
    navs  = start * np.cumprod(1+rets)
    dates = pd.bdate_range(end=date.today(), periods=n)
    return pd.Series(navs, index=dates, dtype=float)


_SEEDED = False

def _seed():
    global _SEEDED
    if _SEEDED: return
    _create_tables()
    rng = np.random.default_rng(0)
    n   = 252*5
    _RAW.executemany(
        "INSERT OR REPLACE INTO fund_master "
        "(fund_id,fund_name,amc,category,benchmark,expense_ratio,updated_at) "
        "VALUES (?,?,?,?,?,?,datetime('now'))",
        [
            ("F001","Alpha Large Cap","AlphaAMC","Equity: Large Cap","Nifty 50",1.0),
            ("F002","Beta Mid Cap",   "BetaAMC", "Equity: Mid Cap",  "Nifty Midcap 150",1.2),
            ("F003","Gamma Flexi Cap","GammaAMC","Equity: Flexi Cap","Nifty 500",0.8),
            ("F004","Delta Index",    "DeltaAMC","Index: Large Cap", "Nifty 50",0.2),
            ("F005","Empty Fund",     "EmptyAMC","Equity: Large Cap","Nifty 50",1.0),
        ],
    )
    dates = pd.bdate_range(end=date.today(), periods=n)
    for fid,ret,vol in [("F001",0.14,0.15),("F002",0.10,0.20),
                        ("F003",0.18,0.12),("F004",0.13,0.15)]:
        nav  = 100.0 * np.cumprod(1+rng.normal(ret/252,vol/(252**0.5),n))
        rows = [(fid,str(d.date()),round(float(v),4)) for d,v in zip(dates,nav)]
        _RAW.executemany("INSERT OR IGNORE INTO nav_history (fund_id,date,nav) VALUES (?,?,?)", rows)
    for bn,ret,vol,sv in [("Nifty 50",0.13,0.15,12000),
                           ("Nifty Midcap 150",0.16,0.20,8000),
                           ("Nifty 500",0.14,0.16,10000)]:
        b    = sv * np.cumprod(1+rng.normal(ret/252,vol/(252**0.5),n))
        rows = [(bn,str(d.date()),round(float(v),2)) for d,v in zip(dates,b)]
        _RAW.executemany(
            "INSERT OR IGNORE INTO benchmark_history (index_name,date,index_value) VALUES (?,?,?)", rows
        )
    _RAW.execute("DELETE FROM portfolio_user WHERE user_id='test'")
    _RAW.executemany(
        "INSERT INTO portfolio_user (user_id,fund_id,amount_invested,purchase_date,purchase_nav) "
        "VALUES (?,?,?,?,?)",
        [("test","F001",50000,"2021-01-01",95.0),
         ("test","F002",30000,"2021-06-01",42.0),
         ("test","F003",20000,"2022-01-01",80.0)],
    )
    _RAW.commit()
    _SEEDED = True


# ══════════════════════════════════════════════════════════════════════════════

class TestDateParsing(unittest.TestCase):
    def test_dd_mmm_yyyy(self):         self.assertEqual(_parse_date("01-Jan-2022"), "2022-01-01")
    def test_dd_slash_mmm_yyyy(self):   self.assertEqual(_parse_date("15/Mar/2021"), "2021-03-15")
    def test_dd_mm_yyyy(self):          self.assertEqual(_parse_date("20-05-2020"),  "2020-05-20")
    def test_yyyy_mm_dd(self):          self.assertEqual(_parse_date("2023-07-04"),  "2023-07-04")
    def test_whitespace(self):          self.assertEqual(_parse_date("  01-Jan-2022  "), "2022-01-01")
    def test_empty(self):               self.assertIsNone(_parse_date(""))
    def test_none(self):                self.assertIsNone(_parse_date(None))
    def test_garbage(self):             self.assertIsNone(_parse_date("not-a-date"))
    def test_very_old(self):            self.assertIsNone(_parse_date("01-Jan-1889"))
    def test_today(self):               self.assertIsNotNone(_parse_date(date.today().strftime("%d-%b-%Y")))
    def test_future(self):
        self.assertIsNone(_parse_date((datetime.now()+timedelta(days=10)).strftime("%d-%b-%Y")))
    def test_no_crash_weird(self):
        for v in ("","  ","99-99-9999","Jan","01-01","2020/01"):
            self.assertIsInstance(_parse_date(v), (str, type(None)))


class TestNAVParsing(unittest.TestCase):
    def test_normal(self):      self.assertAlmostEqual(_parse_nav("123.4567"), 123.4567, places=4)
    def test_comma(self):       self.assertAlmostEqual(_parse_nav("1,234.56"), 1234.56, places=2)
    def test_integer(self):     self.assertAlmostEqual(_parse_nav("100"), 100.0, places=1)
    def test_whitespace(self):  self.assertAlmostEqual(_parse_nav("  50.25  "), 50.25, places=2)
    def test_zero(self):        self.assertIsNone(_parse_nav("0.00"))
    def test_negative(self):    self.assertIsNone(_parse_nav("-5.00"))
    def test_overflow(self):    self.assertIsNone(_parse_nav("9999999999"))
    def test_non_numeric(self): self.assertIsNone(_parse_nav("abc"))
    def test_none(self):        self.assertIsNone(_parse_nav(None))
    def test_na_variants(self):
        for s in ("N.A.","N/A","-","#",""):
            self.assertIsNone(_parse_nav(s))


class TestTokenBucket(unittest.TestCase):
    def test_allows_up_to_limit(self):
        b = _TokenBucket(rate=5, window=60.0)
        t0 = time.monotonic()
        for _ in range(5): b.acquire()
        self.assertLess(time.monotonic()-t0, 0.5)

    def test_blocks_over_limit(self):
        b = _TokenBucket(rate=5, window=1.0)
        for _ in range(5): b.acquire()
        t0 = time.monotonic()
        b.acquire()
        self.assertGreater(time.monotonic()-t0, 0.5)

    def test_thread_safety(self):
        b = _TokenBucket(rate=20, window=60.0)
        done = {"n":0}; lock = threading.Lock()
        def w():
            b.acquire()
            with lock: done["n"]+=1
        ts = [threading.Thread(target=w) for _ in range(10)]
        [t.start() for t in ts]; [t.join() for t in ts]
        self.assertEqual(done["n"], 10)


class TestColumnDetection(unittest.TestCase):
    def test_net_asset_value(self):  self.assertEqual(_detect_nav_column(["date","net_asset_value"]), "net_asset_value")
    def test_nav(self):              self.assertEqual(_detect_nav_column(["date","nav"]), "nav")
    def test_repurchase(self):       self.assertEqual(_detect_nav_column(["date","repurchase_price"]), "repurchase_price")
    def test_nav_missing(self):      self.assertIsNone(_detect_nav_column(["date","other"]))
    def test_date(self):             self.assertEqual(_detect_date_column(["date","nav"]), "date")
    def test_nav_date(self):         self.assertEqual(_detect_date_column(["nav_date","value"]), "nav_date")
    def test_date_missing(self):     self.assertIsNone(_detect_date_column(["code","value"]))


class TestCAGR(unittest.TestCase):
    def test_in_range(self):
        c = calculate_cagr(_make_nav(0.12,5),5)
        self.assertIsNotNone(c); self.assertGreater(c,-0.05); self.assertLess(c,0.40)
    def test_1y(self):              self.assertIsNotNone(calculate_cagr(_make_nav(0.15,3),1))
    def test_insufficient(self):    self.assertIsNone(calculate_cagr(_make_nav(0.12,1),5))
    def test_empty(self):           self.assertIsNone(calculate_cagr(pd.Series(dtype=float),3))
    def test_single_point(self):
        self.assertIsNone(calculate_cagr(pd.Series([100.0],index=pd.DatetimeIndex([date.today()])),1))
    def test_negative_start(self):
        dates = pd.bdate_range(end=date.today(),periods=252)
        self.assertIsNone(calculate_cagr(pd.Series([-1.0]+[100.0]*251,index=dates),1))
    def test_all_same(self):
        dates = pd.bdate_range(end=date.today(),periods=252*3)
        self.assertAlmostEqual(calculate_cagr(pd.Series([100.0]*len(dates),index=dates),3), 0.0, places=4)
    def test_perfect_doubling(self):
        n=252*3; dates=pd.bdate_range(end=date.today(),periods=n)
        nav=pd.Series([100.0*2**(i/252) for i in range(n)],index=dates)
        self.assertAlmostEqual(calculate_cagr(nav,3), 1.0, delta=0.05)


class TestVolatility(unittest.TestCase):
    def test_range(self):
        self.assertAlmostEqual(calculate_volatility(_make_nav(0.12,5,0.15)), 0.15, delta=0.03)
    def test_constant_zero(self):
        dates=pd.bdate_range(end=date.today(),periods=252)
        self.assertAlmostEqual(calculate_volatility(pd.Series([100.0]*252,index=dates)), 0.0, places=6)
    def test_empty(self):           self.assertIsNone(calculate_volatility(pd.Series(dtype=float)))
    def test_single(self):
        self.assertIsNone(calculate_volatility(pd.Series([100.0],index=pd.DatetimeIndex([date.today()]))))
    def test_annualise_relation(self):
        nav=_make_nav(); va=calculate_volatility(nav,True); vd=calculate_volatility(nav,False)
        self.assertAlmostEqual(va, vd*(252**0.5), places=4)


class TestMaxDrawdown(unittest.TestCase):
    def test_negative(self):
        dd=calculate_max_drawdown(_make_nav(0.12,5,0.20)); self.assertLess(dd,0)
    def test_gt_minus_one(self):    self.assertGreater(calculate_max_drawdown(_make_nav(0.12,5,0.30)),-1.0)
    def test_monotone_zero(self):
        dates=pd.bdate_range(end=date.today(),periods=100)
        self.assertAlmostEqual(calculate_max_drawdown(pd.Series(range(1,101),index=dates,dtype=float)),0.0,places=6)
    def test_step_50pct(self):
        dates=pd.bdate_range(end=date.today(),periods=6)
        self.assertAlmostEqual(calculate_max_drawdown(pd.Series([100,100,50,50,100,100],index=dates,dtype=float)),-0.50,places=2)
    def test_all_equal(self):
        dates=pd.bdate_range(end=date.today(),periods=50)
        self.assertAlmostEqual(calculate_max_drawdown(pd.Series([100.0]*50,index=dates)),0.0,places=6)
    def test_empty(self):           self.assertIsNone(calculate_max_drawdown(pd.Series(dtype=float)))


class TestSharpeAndSortino(unittest.TestCase):
    def test_positive_sharpe(self):
        s=calculate_sharpe_ratio(_make_nav(0.20,5,0.10)); self.assertIsNotNone(s); self.assertGreater(s,0)
    def test_low_return_not_none(self): self.assertIsNotNone(calculate_sharpe_ratio(_make_nav(0.03,5,0.20)))
    def test_zero_vol_none(self):
        dates=pd.bdate_range(end=date.today(),periods=252)
        self.assertIsNone(calculate_sharpe_ratio(pd.Series([100.0]*252,index=dates)))
    def test_sortino_computed(self):    self.assertIsNotNone(calculate_sortino_ratio(_make_nav(0.15,5,0.12)))
    def test_sortino_no_downside(self):
        dates=pd.bdate_range(end=date.today(),periods=252)
        self.assertIsNone(calculate_sortino_ratio(pd.Series(range(1,253),index=dates,dtype=float)))
    def test_empty(self):
        self.assertIsNone(calculate_sharpe_ratio(pd.Series(dtype=float)))
        self.assertIsNone(calculate_sortino_ratio(pd.Series(dtype=float)))


class TestBetaAlpha(unittest.TestCase):
    def test_same_series(self):
        nav=_make_nav(); beta,_=calculate_beta_alpha(nav,nav)
        self.assertAlmostEqual(beta,1.0,places=1)
    def test_empty_fund(self):
        b,a=calculate_beta_alpha(pd.Series(dtype=float),_make_nav()); self.assertIsNone(b); self.assertIsNone(a)
    def test_empty_bench(self):
        b,a=calculate_beta_alpha(_make_nav(),pd.Series(dtype=float)); self.assertIsNone(b); self.assertIsNone(a)
    def test_insufficient_overlap(self):
        nav=_make_nav(); bench=pd.Series([100.0]*20,index=pd.bdate_range(end=date.today(),periods=20))
        self.assertIsNone(calculate_beta_alpha(nav,bench)[0])
    def test_high_beta(self):
        bench=_make_nav(0.13,5,0.15); br=bench.pct_change().dropna(); fr=br*2.0
        fund=pd.Series(100.0*(1+fr).cumprod().values,index=br.index)
        beta,_=calculate_beta_alpha(fund,bench)
        self.assertIsNotNone(beta); self.assertAlmostEqual(beta,2.0,delta=0.15)
    def test_no_nan_inf(self):
        beta,alpha=calculate_beta_alpha(_make_nav(),_make_nav(0.10,5,seed=99))
        if beta:  self.assertFalse(math.isnan(beta)  or math.isinf(beta))
        if alpha: self.assertFalse(math.isnan(alpha) or math.isinf(alpha))


class TestRollingReturns(unittest.TestCase):
    def test_non_empty(self):           self.assertGreater(len(rolling_returns(_make_nav(),1)),0)
    def test_plausible_values(self):
        r=rolling_returns(_make_nav(),1); self.assertTrue((r>-1).all()); self.assertTrue((r<5).all())
    def test_insufficient(self):        self.assertTrue(rolling_returns(_make_nav(years=1),3).empty)
    def test_3y(self):                  self.assertGreater(len(rolling_returns(_make_nav(years=6),3)),0)
    def test_constant_zero(self):
        dates=pd.bdate_range(end=date.today(),periods=252*3)
        self.assertAlmostEqual(rolling_returns(pd.Series([100.0]*len(dates),index=dates),1).mean(),0.0,places=4)


class TestComputeFundAnalytics(unittest.TestCase):
    @classmethod
    def setUpClass(cls): _seed()
    def test_valid(self):
        r=compute_fund_analytics("F001"); self.assertNotIn("error",r); self.assertIn("return_5y",r)
    def test_missing(self):             self.assertIn("error",compute_fund_analytics("NONE"))
    def test_empty_nav(self):           self.assertIn("error",compute_fund_analytics("F005"))
    def test_no_nan(self):
        r=compute_fund_analytics("F001")
        for k in ("return_1y","return_3y","return_5y","volatility","sharpe_ratio","max_drawdown"):
            v=r.get(k)
            if v is not None: self.assertFalse(math.isnan(v) or math.isinf(v), k)
    def test_benchmark_alpha_beta(self):
        r=compute_fund_analytics("F003")
        self.assertNotIn("error",r); self.assertIn("benchmark",r)
        self.assertIn("alpha",r); self.assertIn("beta",r)


class TestUnderperformanceDetection(unittest.TestCase):
    @classmethod
    def setUpClass(cls): _seed()
    def test_valid_flag(self):
        r=detect_underperformance("F001")
        self.assertIn(r["flag"],("OK","WARNING","SERIOUS","CRITICAL","NO_DATA","INSUFFICIENT_DATA"))
    def test_missing_no_crash(self):    self.assertEqual(detect_underperformance("XXXXXX")["flag"],"NO_DATA")
    def test_empty_no_crash(self):
        self.assertIn(detect_underperformance("F005")["flag"],("NO_DATA","INSUFFICIENT_DATA"))
    def test_fund_name_present(self):
        r=detect_underperformance("F001")
        if r["flag"] not in ("NO_DATA","INSUFFICIENT_DATA"): self.assertIn("fund_name",r)
    def test_all_no_crash(self):
        for fid in ("F001","F002","F003","F004","F005"):
            self.assertIn("flag",detect_underperformance(fid))


class TestPortfolioAnalytics(unittest.TestCase):
    @classmethod
    def setUpClass(cls): _seed()
    def test_valid(self):
        r=portfolio_analytics("test"); self.assertNotIn("error",r); self.assertIn("health_score",r)
    def test_score_range(self):
        r=portfolio_analytics("test"); self.assertGreaterEqual(r["health_score"],0); self.assertLessEqual(r["health_score"],100)
    def test_totals_nonneg(self):
        r=portfolio_analytics("test"); self.assertGreaterEqual(r["total_invested"],0); self.assertGreaterEqual(r["total_current"],0)
    def test_recommendations_list(self):    self.assertIsInstance(portfolio_analytics("test")["recommendations"],list)
    def test_empty_user(self):              self.assertIn("error",portfolio_analytics("no_such_user_xyz"))
    def test_alloc_sums_one(self):
        alloc=portfolio_analytics("test").get("category_allocation",{})
        if alloc: self.assertAlmostEqual(sum(alloc.values()),1.0,places=3)


class TestExitStrategy(unittest.TestCase):
    @classmethod
    def setUpClass(cls): _seed()
    def test_valid_rec(self):
        r=assess_exit("F001",holding_months=24,invested_amount=50000)
        self.assertIn(r["recommendation"],("HOLD","WATCH","SWITCH","EXIT"))
    def test_missing_no_crash(self):    r=assess_exit("XXXXXX"); self.assertIn("recommendation",r)
    def test_stcg(self):
        notes=assess_exit("F001",holding_months=6).get("tax_notes",[])
        self.assertTrue(any("STCG" in n for n in notes),f"got:{notes}")
    def test_ltcg(self):
        notes=assess_exit("F001",holding_months=18).get("tax_notes",[])
        self.assertTrue(any("LTCG" in n for n in notes),f"got:{notes}")
    def test_reasons_list(self):        self.assertIsInstance(assess_exit("F001").get("reasons",[]),list)
    def test_replacements_list(self):   self.assertIsInstance(find_replacement_funds("F001"),list)
    def test_replacements_missing(self):self.assertIsInstance(find_replacement_funds("NONEXISTENT"),list)


class TestDatabaseIntegrity(unittest.TestCase):
    @classmethod
    def setUpClass(cls): _seed()
    def test_tables_exist(self):
        t={r[0] for r in _RAW.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        for n in ("fund_master","nav_history","benchmark_history","portfolio_user"):
            self.assertIn(n,t)
    def test_upsert_idempotent(self):
        df=pd.DataFrame([{"scheme_code":"T99","scheme_name":"TF","nav":100.0,
                           "date":date.today().strftime("%Y-%m-%d"),"amc":"TA",
                           "category":"Equity","sub_category":""}])
        self.assertEqual(upsert_fund_master(df),1)
        self.assertEqual(upsert_fund_master(df),1)
    def test_empty_df(self):            self.assertEqual(upsert_fund_master(pd.DataFrame()),0)
    def test_uniqueness(self):
        _RAW.execute("INSERT OR IGNORE INTO nav_history (fund_id,date,nav) VALUES ('F001','2010-01-01',99.9)")
        _RAW.execute("INSERT OR IGNORE INTO nav_history (fund_id,date,nav) VALUES ('F001','2010-01-01',88.8)")
        _RAW.commit()
        c=_RAW.execute("SELECT COUNT(*) FROM nav_history WHERE fund_id='F001' AND date='2010-01-01'").fetchone()[0]
        self.assertEqual(c,1)
    def test_upsert_latest_nav(self):
        df=pd.DataFrame([{"scheme_code":"T99","scheme_name":"TF","nav":105.5,
                           "date":date.today().strftime("%Y-%m-%d"),"amc":"TA",
                           "category":"Equity","sub_category":""}])
        upsert_fund_master(df); ins=upsert_latest_nav(df); self.assertGreaterEqual(ins,0)


# ── Mock HTTP helpers ─────────────────────────────────────────────────────────
_CSV = (
    "Scheme Code;ISIN Div;ISIN Growth;Scheme Name;Net Asset Value;Date\n"
    "120503;A;B;Axis Bluechip Fund;52.3456;01-Mar-2024\n"
    "120503;A;B;Axis Bluechip Fund;52.1234;29-Feb-2024\n"
    "120503;A;B;Axis Bluechip Fund;51.9876;28-Feb-2024\n"
)
_NAVALL = (
    "Axis Mutual Fund\n"
    "Open Ended Schemes(Equity Scheme - Large Cap Fund)\n"
    "120503;A;B;Axis Bluechip Fund - Growth;52.3456;01-Mar-2024\n"
    "120716;C;D;Parag Parikh Flexi Cap - Growth;68.1234;01-Mar-2024\n"
    "\nSBI Mutual Fund\n"
    "Open Ended Schemes(Equity Scheme - Small Cap Fund)\n"
    "100033;E;F;SBI Small Cap Fund - Growth;112.5678;01-Mar-2024\n"
)
def _mr(text, status=200):
    r=MagicMock(); r.status_code=status; r.content=text.encode(); r.raise_for_status=MagicMock(); return r


class TestFetchNavHistory(unittest.TestCase):
    @patch("pipeline.amfi_client._get")
    def test_parses_csv(self,m):
        m.return_value=_mr(_CSV); df=fetch_nav_history("120503")
        self.assertFalse(df.empty); self.assertEqual(len(df),3)
        self.assertEqual(set(df.columns),{"fund_id","date","nav"})
    @patch("pipeline.amfi_client._get")
    def test_dates_normalised(self,m):
        m.return_value=_mr(_CSV); df=fetch_nav_history("120503")
        for d in df["date"]: self.assertRegex(d,r"^\d{4}-\d{2}-\d{2}$")
    @patch("pipeline.amfi_client._get")
    def test_network_fail(self,m):
        m.return_value=None; self.assertTrue(fetch_nav_history("0").empty)
    @patch("pipeline.amfi_client._get")
    def test_no_data(self,m):
        m.return_value=_mr("No Data Available"); self.assertTrue(fetch_nav_history("0").empty)
    @patch("pipeline.amfi_client._get")
    def test_html_error(self,m):
        m.return_value=_mr("<html>Error</html>"); self.assertTrue(fetch_nav_history("0").empty)
    @patch("pipeline.amfi_client._get")
    def test_zero_nav_excluded(self,m):
        m.return_value=_mr(_CSV+"120503;A;B;X;0.00;02-Mar-2024\n")
        df=fetch_nav_history("120503"); self.assertTrue((df["nav"]>0).all())
    @patch("pipeline.amfi_client._get")
    def test_future_excluded(self,m):
        fut=(datetime.now()+timedelta(days=5)).strftime("%d-%b-%Y")
        m.return_value=_mr(_CSV+f"120503;A;B;X;55.00;{fut}\n")
        df=fetch_nav_history("120503"); today=date.today().strftime("%Y-%m-%d")
        self.assertTrue((df["date"]<=today).all())
    @patch("pipeline.amfi_client._get")
    def test_no_duplicates(self,m):
        m.return_value=_mr(_CSV+"120503;A;B;X;51.99;28-Feb-2024\n")
        df=fetch_nav_history("120503"); self.assertEqual(df.duplicated(["fund_id","date"]).sum(),0)
    @patch("pipeline.amfi_client._get")
    def test_all_positive(self,m):
        m.return_value=_mr(_CSV); df=fetch_nav_history("120503"); self.assertTrue((df["nav"]>0).all())


class TestFetchAllFunds(unittest.TestCase):
    @patch("pipeline.amfi_client._get")
    def test_parses(self,m):
        m.return_value=_mr(_NAVALL); df=amfi_fetch_all()
        self.assertFalse(df.empty)
        for c in ("scheme_code","scheme_name","amc"): self.assertIn(c,df.columns)
    @patch("pipeline.amfi_client._get")
    def test_amc_propagated(self,m):
        m.return_value=_mr(_NAVALL); df=amfi_fetch_all()
        row=df[df["scheme_code"]=="120503"]; self.assertIn("Axis",row.iloc[0]["amc"])
    @patch("pipeline.amfi_client._get")
    def test_network_fail(self,m):
        m.return_value=None; self.assertTrue(amfi_fetch_all().empty)
    @patch("pipeline.amfi_client._get")
    def test_no_duplicates(self,m):
        m.return_value=_mr(_NAVALL); df=amfi_fetch_all()
        self.assertEqual(df.duplicated("scheme_code").sum(),0)
    @patch("pipeline.amfi_client._get")
    def test_numeric_codes(self,m):
        m.return_value=_mr(_NAVALL); df=amfi_fetch_all()
        for c in df["scheme_code"]: self.assertTrue(str(c).isdigit(),c)


class TestBulkLoad(unittest.TestCase):
    @classmethod
    def setUpClass(cls): _seed()

    def _df(self, fid, n=100):
        dates=pd.bdate_range(end=date.today(),periods=n)
        return pd.DataFrame({"fund_id":[fid]*n,"date":[str(d.date()) for d in dates],"nav":[float(i+50) for i in range(n)]})

    @patch("pipeline.amfi_client.fetch_nav_history")
    def test_inserts_rows(self,m):
        fid="BLK001"; m.return_value=self._df(fid)
        _RAW.execute("INSERT OR IGNORE INTO fund_master (fund_id,fund_name,amc) VALUES (?,'BLK','BAMC')",(fid,)); _RAW.commit()
        errors=[]; r=bulk_load_nav_history([fid],years=1,skip_fresh=False,errors_out=errors)
        self.assertGreater(r["rows_inserted"],0); self.assertEqual(len(errors),0)

    @patch("pipeline.amfi_client.fetch_nav_history")
    def test_empty_response_error(self,m):
        m.return_value=pd.DataFrame(); errors=[]
        bulk_load_nav_history(["GHOST1"],years=1,skip_fresh=False,errors_out=errors)
        self.assertTrue(any(e["fund_id"]=="GHOST1" for e in errors))

    @patch("pipeline.amfi_client.fetch_nav_history")
    def test_exception_captured(self,m):
        m.side_effect=RuntimeError("boom"); errors=[]
        bulk_load_nav_history(["BOOM1"],years=1,skip_fresh=False,errors_out=errors)
        self.assertTrue(any(e["fund_id"]=="BOOM1" for e in errors))

    @patch("pipeline.amfi_client.fetch_nav_history")
    def test_skip_fresh(self,m):
        fid="FRESH1"
        _RAW.execute("INSERT OR IGNORE INTO fund_master (fund_id,fund_name,amc) VALUES (?,'F','A')",(fid,))
        _RAW.execute("INSERT OR IGNORE INTO nav_history (fund_id,date,nav) VALUES (?,?,?)",(fid,date.today().strftime("%Y-%m-%d"),100.0))
        _RAW.commit()
        errors=[]; r=bulk_load_nav_history([fid],years=1,skip_fresh=True,max_age_days=1,errors_out=errors)
        self.assertEqual(r["skipped"],1); m.assert_not_called()

    @patch("pipeline.amfi_client.fetch_nav_history")
    def test_multi_batch(self,m):
        funds=[f"BATCH{i}" for i in range(5)]
        for fid in funds:
            _RAW.execute("INSERT OR IGNORE INTO fund_master (fund_id,fund_name,amc) VALUES (?,'BF','BA')",(fid,))
        _RAW.commit()
        m.side_effect=lambda fid,**kw: self._df(fid,50)
        errors=[]; r=bulk_load_nav_history(funds,years=1,skip_fresh=False,batch_size=3,errors_out=errors)
        self.assertGreater(r["rows_inserted"],0); self.assertEqual(len(errors),0)


class TestHTTPRetry(unittest.TestCase):

    @patch("pipeline.amfi_client._SESSION")
    @patch("pipeline.amfi_client.time.sleep")
    def test_retries_timeout(self,slp,ses):
        from requests.exceptions import Timeout
        ok=MagicMock(status_code=200,content=b"",raise_for_status=MagicMock())
        ses.get.side_effect=[Timeout(),Timeout(),ok]
        from pipeline.amfi_client import _get,_bucket
        with patch.object(_bucket,"acquire"): r=_get("http://x")
        self.assertIsNotNone(r); self.assertEqual(ses.get.call_count,3)

    @patch("pipeline.amfi_client._SESSION")
    @patch("pipeline.amfi_client.time.sleep")
    def test_all_exhausted(self,slp,ses):
        from requests.exceptions import Timeout
        ses.get.side_effect=Timeout()
        from pipeline.amfi_client import _get,_bucket
        with patch.object(_bucket,"acquire"): r=_get("http://x")
        self.assertIsNone(r); self.assertEqual(ses.get.call_count,MAX_RETRIES)

    @patch("pipeline.amfi_client._SESSION")
    @patch("pipeline.amfi_client.time.sleep")
    def test_no_retry_404(self,slp,ses):
        from requests.exceptions import HTTPError
        ses.get.return_value=MagicMock(status_code=404,
            raise_for_status=MagicMock(side_effect=HTTPError(response=MagicMock(status_code=404))))
        from pipeline.amfi_client import _get,_bucket
        with patch.object(_bucket,"acquire"): _get("http://x")
        self.assertEqual(ses.get.call_count,1)

    @patch("pipeline.amfi_client._SESSION")
    @patch("pipeline.amfi_client.time.sleep")
    def test_backoff_429(self,slp,ses):
        ok=MagicMock(status_code=200,content=b"",raise_for_status=MagicMock())
        ses.get.side_effect=[MagicMock(status_code=429,raise_for_status=MagicMock()),ok]
        from pipeline.amfi_client import _get,_bucket
        with patch.object(_bucket,"acquire"): _get("http://x")
        self.assertTrue(slp.called)

    @patch("pipeline.amfi_client._SESSION")
    @patch("pipeline.amfi_client.time.sleep")
    def test_backoff_503(self,slp,ses):
        ok=MagicMock(status_code=200,content=b"",raise_for_status=MagicMock())
        ses.get.side_effect=[MagicMock(status_code=503,raise_for_status=MagicMock()),ok]
        from pipeline.amfi_client import _get,_bucket
        with patch.object(_bucket,"acquire"): _get("http://x")
        self.assertTrue(slp.called)


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite  = loader.loadTestsFromModule(__import__(__name__))
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(suite)
    passed = result.testsRun - len(result.failures) - len(result.errors)
    print(f"\n{'='*65}")
    print(f"  {passed}/{result.testsRun} PASSED  |  "
          f"{len(result.failures)} FAILURES  |  {len(result.errors)} ERRORS")
    print(f"{'='*65}")
    sys.exit(0 if result.wasSuccessful() else 1)
