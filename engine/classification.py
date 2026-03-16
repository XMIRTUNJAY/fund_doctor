"""
Fund Classification Registry
==============================
Complete SEBI / AMFI category taxonomy for all ~1,579 Indian MF schemes.
Used by analytics engine, goal planner, and peer ranking.
"""

# ── SEBI Category Registry ───────────────────────────────────────────────────
# risk_score: 1 (lowest) → 9 (highest)
# horizon_min: minimum recommended holding period in years
# asset_class: EQUITY | DEBT | HYBRID | INDEX | FOF

CATEGORIES = {
    # ── EQUITY ──────────────────────────────────────────────────────────────
    "Equity: Large Cap": {
        "asset_class": "EQUITY", "risk_score": 6, "horizon_min": 5,
        "benchmark": "Nifty 50",
        "description": "Top 100 companies by market cap",
        "sebi_definition": "Min 80% in large cap stocks",
    },
    "Equity: Large & Mid Cap": {
        "asset_class": "EQUITY", "risk_score": 6, "horizon_min": 5,
        "benchmark": "Nifty LargeMidcap 250",
        "description": "Blend of top 100 + next 150 companies",
        "sebi_definition": "Min 35% each in large cap and mid cap",
    },
    "Equity: Mid Cap": {
        "asset_class": "EQUITY", "risk_score": 7, "horizon_min": 7,
        "benchmark": "Nifty Midcap 150",
        "description": "101st to 250th companies by market cap",
        "sebi_definition": "Min 65% in mid cap stocks",
    },
    "Equity: Small Cap": {
        "asset_class": "EQUITY", "risk_score": 8, "horizon_min": 10,
        "benchmark": "Nifty Smallcap 250",
        "description": "251st and beyond companies",
        "sebi_definition": "Min 65% in small cap stocks",
    },
    "Equity: Multi Cap": {
        "asset_class": "EQUITY", "risk_score": 6, "horizon_min": 5,
        "benchmark": "Nifty 500 Multicap 50:25:25",
        "description": "Spread across large, mid, small cap",
        "sebi_definition": "Min 25% each in large, mid, small cap",
    },
    "Equity: Flexi Cap": {
        "asset_class": "EQUITY", "risk_score": 6, "horizon_min": 5,
        "benchmark": "Nifty 500",
        "description": "Flexible allocation across all market caps",
        "sebi_definition": "Min 65% equity; no cap restriction",
    },
    "Equity: Focused": {
        "asset_class": "EQUITY", "risk_score": 7, "horizon_min": 5,
        "benchmark": "Nifty 500",
        "description": "Concentrated portfolio of max 30 stocks",
        "sebi_definition": "Max 30 stocks, min 65% equity",
    },
    "Equity: ELSS": {
        "asset_class": "EQUITY", "risk_score": 6, "horizon_min": 3,
        "benchmark": "Nifty 500",
        "description": "Tax-saving fund with 3-year lock-in",
        "sebi_definition": "Min 80% equity; 3yr lock-in; 80C benefit",
    },
    "Equity: Dividend Yield": {
        "asset_class": "EQUITY", "risk_score": 5, "horizon_min": 5,
        "benchmark": "Nifty Dividend Opportunities 50",
        "description": "High dividend yielding stocks",
        "sebi_definition": "Min 65% in dividend-yielding stocks",
    },
    "Equity: Value": {
        "asset_class": "EQUITY", "risk_score": 6, "horizon_min": 7,
        "benchmark": "Nifty 500",
        "description": "Undervalued stocks using value investing",
        "sebi_definition": "Min 65% equity following value strategy",
    },
    "Equity: Contra": {
        "asset_class": "EQUITY", "risk_score": 6, "horizon_min": 7,
        "benchmark": "Nifty 500",
        "description": "Contrarian strategy — buy out-of-favour stocks",
        "sebi_definition": "Min 65% equity following contrarian strategy",
    },
    "Equity: Thematic": {
        "asset_class": "EQUITY", "risk_score": 8, "horizon_min": 7,
        "benchmark": "Nifty 500",
        "description": "Theme-based: ESG, Consumption, Manufacturing etc.",
        "sebi_definition": "Min 80% in theme-specific stocks",
    },
    "Equity: Sectoral": {
        "asset_class": "EQUITY", "risk_score": 9, "horizon_min": 7,
        "benchmark": "Respective sector index",
        "description": "Single sector: Banking, IT, Pharma, FMCG etc.",
        "sebi_definition": "Min 80% in single sector",
    },
    # ── HYBRID ───────────────────────────────────────────────────────────────
    "Hybrid: Aggressive": {
        "asset_class": "HYBRID", "risk_score": 5, "horizon_min": 3,
        "benchmark": "Nifty 50",
        "description": "Mostly equity with some debt allocation",
        "sebi_definition": "65–80% equity, 20–35% debt",
    },
    "Hybrid: Balanced Advantage": {
        "asset_class": "HYBRID", "risk_score": 4, "horizon_min": 3,
        "benchmark": "Nifty 50",
        "description": "Dynamic allocation between equity and debt",
        "sebi_definition": "Dynamic equity-debt allocation based on valuation",
    },
    "Hybrid: Dynamic": {
        "asset_class": "HYBRID", "risk_score": 4, "horizon_min": 3,
        "benchmark": "Nifty 50",
        "description": "Active equity-debt rebalancing",
        "sebi_definition": "Active dynamic asset allocation",
    },
    "Hybrid: Multi Asset": {
        "asset_class": "HYBRID", "risk_score": 4, "horizon_min": 3,
        "benchmark": "Nifty 50",
        "description": "Equity + debt + gold or commodities",
        "sebi_definition": "Min 10% each in at least 3 asset classes",
    },
    "Hybrid: Conservative": {
        "asset_class": "HYBRID", "risk_score": 3, "horizon_min": 2,
        "benchmark": "CRISIL Hybrid Conservative",
        "description": "Mostly debt with small equity component",
        "sebi_definition": "10–25% equity, 75–90% debt",
    },
    "Hybrid: Equity Savings": {
        "asset_class": "HYBRID", "risk_score": 3, "horizon_min": 2,
        "benchmark": "Nifty 50",
        "description": "Equity + arbitrage + debt for tax efficiency",
        "sebi_definition": "Min 65% equity (including arbitrage), min 10% debt",
    },
    "Hybrid: Arbitrage": {
        "asset_class": "HYBRID", "risk_score": 2, "horizon_min": 1,
        "benchmark": "Nifty 50",
        "description": "Risk-free arbitrage between cash and futures",
        "sebi_definition": "Min 65% arbitrage; taxed as equity",
    },
    # ── DEBT ─────────────────────────────────────────────────────────────────
    "Debt: Overnight": {
        "asset_class": "DEBT", "risk_score": 1, "horizon_min": 0,
        "benchmark": "CRISIL Overnight Index",
        "description": "1-day maturity securities only",
        "sebi_definition": "Overnight securities only",
    },
    "Debt: Liquid": {
        "asset_class": "DEBT", "risk_score": 1, "horizon_min": 0,
        "benchmark": "CRISIL Liquid Fund Index",
        "description": "Up to 91-day instruments",
        "sebi_definition": "Up to 91 days maturity",
    },
    "Debt: Ultra Short": {
        "asset_class": "DEBT", "risk_score": 2, "horizon_min": 0,
        "benchmark": "CRISIL Ultra Short Duration",
        "description": "3–6 month duration instruments",
        "sebi_definition": "Macaulay duration 3–6 months",
    },
    "Debt: Low Duration": {
        "asset_class": "DEBT", "risk_score": 2, "horizon_min": 1,
        "benchmark": "CRISIL Low Duration Index",
        "description": "6–12 month duration",
        "sebi_definition": "Macaulay duration 6–12 months",
    },
    "Debt: Short Duration": {
        "asset_class": "DEBT", "risk_score": 3, "horizon_min": 1,
        "benchmark": "CRISIL Short Duration Index",
        "description": "1–3 year duration",
        "sebi_definition": "Macaulay duration 1–3 years",
    },
    "Debt: Medium Duration": {
        "asset_class": "DEBT", "risk_score": 3, "horizon_min": 2,
        "benchmark": "CRISIL Composite Bond Index",
        "description": "3–4 year duration",
        "sebi_definition": "Macaulay duration 3–4 years",
    },
    "Debt: Long Duration": {
        "asset_class": "DEBT", "risk_score": 4, "horizon_min": 3,
        "benchmark": "CRISIL Long Duration Index",
        "description": "7+ year duration bonds",
        "sebi_definition": "Macaulay duration > 7 years",
    },
    "Debt: Corporate Bond": {
        "asset_class": "DEBT", "risk_score": 3, "horizon_min": 2,
        "benchmark": "CRISIL AA Short Term Bond",
        "description": "Min AA-rated corporate bonds",
        "sebi_definition": "Min 80% in AA+ or higher rated bonds",
    },
    "Debt: Credit Risk": {
        "asset_class": "DEBT", "risk_score": 5, "horizon_min": 2,
        "benchmark": "CRISIL AA Short Term Bond",
        "description": "Below AA-rated bonds for higher yield",
        "sebi_definition": "Min 65% in below AA-rated bonds",
    },
    "Debt: Banking & PSU": {
        "asset_class": "DEBT", "risk_score": 3, "horizon_min": 1,
        "benchmark": "CRISIL Short Duration Index",
        "description": "Banks and public sector bonds",
        "sebi_definition": "Min 80% in banks/PSU bonds",
    },
    "Debt: Gilt": {
        "asset_class": "DEBT", "risk_score": 4, "horizon_min": 3,
        "benchmark": "CRISIL Gilt Index",
        "description": "Government securities only — no credit risk",
        "sebi_definition": "Min 80% in G-secs across maturities",
    },
    "Debt: Gilt 10Y Constant": {
        "asset_class": "DEBT", "risk_score": 4, "horizon_min": 3,
        "benchmark": "CRISIL 10 Year Gilt Index",
        "description": "10-year constant maturity G-sec",
        "sebi_definition": "Min 80% in 10Y G-sec",
    },
    "Debt: Dynamic Bond": {
        "asset_class": "DEBT", "risk_score": 3, "horizon_min": 3,
        "benchmark": "CRISIL Composite Bond Index",
        "description": "Active duration management across all maturities",
        "sebi_definition": "Active duration management",
    },
    "Debt: Floater": {
        "asset_class": "DEBT", "risk_score": 2, "horizon_min": 1,
        "benchmark": "CRISIL Liquid Fund Index",
        "description": "Floating rate bonds — low interest rate risk",
        "sebi_definition": "Min 65% in floating rate instruments",
    },
    # ── INDEX / ETF ──────────────────────────────────────────────────────────
    "Index: Large Cap": {
        "asset_class": "INDEX", "risk_score": 5, "horizon_min": 5,
        "benchmark": "Nifty 50",
        "description": "Passive tracking of large cap indices",
        "sebi_definition": "Index/ETF tracking large cap index",
    },
    "Index: Mid Cap": {
        "asset_class": "INDEX", "risk_score": 6, "horizon_min": 7,
        "benchmark": "Nifty Midcap 150",
        "description": "Passive tracking of mid cap indices",
        "sebi_definition": "Index/ETF tracking mid cap index",
    },
    "Index: Small Cap": {
        "asset_class": "INDEX", "risk_score": 7, "horizon_min": 7,
        "benchmark": "Nifty Smallcap 250",
        "description": "Passive tracking of small cap indices",
        "sebi_definition": "Index/ETF tracking small cap index",
    },
    "Index: Sectoral": {
        "asset_class": "INDEX", "risk_score": 8, "horizon_min": 5,
        "benchmark": "Respective sector index",
        "description": "Passive tracking of sector indices",
        "sebi_definition": "ETF/Index fund on sector/thematic index",
    },
    "Index: International": {
        "asset_class": "INDEX", "risk_score": 7, "horizon_min": 5,
        "benchmark": "Respective international index",
        "description": "US, global, emerging market index funds",
        "sebi_definition": "ETF/Index fund on international index",
    },
    "Index: Debt": {
        "asset_class": "INDEX", "risk_score": 3, "horizon_min": 2,
        "benchmark": "Respective debt index",
        "description": "Passive debt index tracking",
        "sebi_definition": "ETF/Index fund on debt index",
    },
    # ── FOF ──────────────────────────────────────────────────────────────────
    "FOF: Domestic": {
        "asset_class": "FOF", "risk_score": 6, "horizon_min": 5,
        "benchmark": "Nifty 50",
        "description": "Fund of domestic mutual funds",
        "sebi_definition": "Min 95% in domestic MF schemes",
    },
    "FOF: International": {
        "asset_class": "FOF", "risk_score": 7, "horizon_min": 5,
        "benchmark": "Respective international index",
        "description": "Fund of overseas funds",
        "sebi_definition": "Min 95% in international MF/ETF",
    },
}


def get_category_info(category: str) -> dict:
    """Return metadata for a category, with safe fallback."""
    return CATEGORIES.get(category, {
        "asset_class": "EQUITY",
        "risk_score": 6,
        "horizon_min": 5,
        "benchmark": "Nifty 500",
        "description": category,
        "sebi_definition": "",
    })


def get_categories_by_risk(min_risk: int, max_risk: int) -> list:
    """Return list of category names within a risk band."""
    return [cat for cat, meta in CATEGORIES.items()
            if min_risk <= meta["risk_score"] <= max_risk]


def get_categories_by_asset_class(asset_class: str) -> list:
    """Return categories for a given asset class."""
    return [cat for cat, meta in CATEGORIES.items()
            if meta["asset_class"] == asset_class]


def get_benchmark_for_category(category: str) -> str:
    return get_category_info(category).get("benchmark", "Nifty 50")


# ── Risk grouping for UI display ─────────────────────────────────────────────
RISK_GROUPS = {
    "Capital Preservation": {
        "risk_range": (1, 2), "color": "#22c55e",
        "description": "Priority: protect capital. Returns beat FD.",
        "categories": get_categories_by_risk(1, 2),
    },
    "Conservative": {
        "risk_range": (3, 3), "color": "#84cc16",
        "description": "Stable returns with low equity exposure.",
        "categories": get_categories_by_risk(3, 3),
    },
    "Moderate": {
        "risk_range": (4, 4), "color": "#f0b429",
        "description": "Balanced equity-debt for steady growth.",
        "categories": get_categories_by_risk(4, 4),
    },
    "Moderate-Aggressive": {
        "risk_range": (5, 5), "color": "#f97316",
        "description": "Mostly equity, some downside buffer.",
        "categories": get_categories_by_risk(5, 5),
    },
    "Aggressive": {
        "risk_range": (6, 6), "color": "#ef4444",
        "description": "Full equity, diversified across market caps.",
        "categories": get_categories_by_risk(6, 6),
    },
    "High Risk": {
        "risk_range": (7, 7), "color": "#dc2626",
        "description": "Mid/small cap, higher volatility, higher potential.",
        "categories": get_categories_by_risk(7, 7),
    },
    "Very High Risk": {
        "risk_range": (8, 8), "color": "#b91c1c",
        "description": "Thematic, sectoral, small cap. High conviction needed.",
        "categories": get_categories_by_risk(8, 8),
    },
    "Speculative": {
        "risk_range": (9, 9), "color": "#7f1d1d",
        "description": "Single sector concentration. Expert investors only.",
        "categories": get_categories_by_risk(9, 9),
    },
}

# ── Goal → suitable category mapping ─────────────────────────────────────────
GOAL_CATEGORY_MAP = {
    "Emergency Fund":     {"risk_max": 2, "horizon_max": 1,   "asset_class": ["DEBT"]},
    "Short Term Savings": {"risk_max": 3, "horizon_max": 2,   "asset_class": ["DEBT", "HYBRID"]},
    "Wealth Building":    {"risk_max": 7, "horizon_min": 5,   "asset_class": ["EQUITY", "INDEX"]},
    "Retirement":         {"risk_max": 6, "horizon_min": 10,  "asset_class": ["EQUITY", "HYBRID", "INDEX"]},
    "Child Education":    {"risk_max": 7, "horizon_min": 7,   "asset_class": ["EQUITY", "INDEX"]},
    "Tax Saving":         {"risk_max": 6, "horizon_min": 3,   "asset_class": ["EQUITY"], "category_filter": ["Equity: ELSS"]},
    "Home Purchase":      {"risk_max": 5, "horizon_min": 3,   "asset_class": ["HYBRID", "EQUITY"]},
    "Monthly Income":     {"risk_max": 4, "horizon_min": 2,   "asset_class": ["HYBRID", "DEBT"]},
    "Inflation Beating":  {"risk_max": 5, "horizon_min": 3,   "asset_class": ["EQUITY", "HYBRID", "INDEX"]},
}
