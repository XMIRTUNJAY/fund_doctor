# 💊 Fund Doctor — Mutual Fund Analyzer & Comparison Platform

> **India Mutual Fund Intelligence Platform** — runs entirely on your local machine.

---

## 🗂 Project Structure

```
fund_doctor/
├── run.py                    ← Bootstrap + launch script
├── requirements.txt
├── .env.example
│
├── database/
│   └── db.py                 ← SQLite schema, connection helpers
│
├── pipeline/
│   └── ingest.py             ← AMFI data fetcher, demo seeder, benchmark loader
│
├── engine/
│   ├── analytics.py          ← Core financial calculations (CAGR, Sharpe, etc.)
│   └── exit_strategy.py      ← Exit assessment & replacement recommendations
│
├── dashboard/
│   └── app.py                ← Streamlit multi-page dashboard
│
├── data/
│   └── fund_doctor.db        ← Auto-created SQLite database
│
└── tests/
    └── test_analytics.py     ← Pytest unit tests
```

---

## ⚡ Quick Start

### 1. Install dependencies

```bash
cd fund_doctor
pip install -r requirements.txt
```

### 2. Launch with demo data (offline — no internet needed)

```bash
python run.py
```

This will:
- Initialise the SQLite database
- Seed 8 representative funds with 5 years of realistic NAV history
- Seed benchmark indices (Nifty 50, Midcap 150, Smallcap 250, Nifty 500)
- Seed a sample portfolio
- Launch the Streamlit dashboard at **http://localhost:8501**

### 3. Fetch live data from AMFI (requires internet)

```bash
python run.py --fetch-amfi
```

Fetches the full fund universe from AMFI India (free, no API key needed).

---

## 🖥 Dashboard Pages

| Page | Description |
|------|-------------|
| 🏠 **Overview** | Database stats, fund universe table |
| 📈 **Fund Analysis** | NAV chart, returns, risk metrics, rolling returns, drawdown |
| ⚖️ **Fund Comparison** | Side-by-side metrics, radar chart, normalised NAV, overlap |
| 🚨 **Underperformance Radar** | Flags funds lagging benchmarks (OK / WARNING / SERIOUS / CRITICAL) |
| 🚪 **Exit Strategy** | Hold / Watch / Switch / Exit recommendation + tax notes |
| 🩺 **Portfolio Doctor** | Health score, sector allocation, fund flags, recommendations |

---

## 📐 Analytics Engine

All metrics are computed in `engine/analytics.py`:

| Function | Description |
|----------|-------------|
| `calculate_cagr(nav, years)` | Compounded Annual Growth Rate |
| `calculate_volatility(nav)` | Annualised std dev of daily returns |
| `calculate_max_drawdown(nav)` | Largest peak-to-trough decline |
| `calculate_sharpe_ratio(nav)` | Risk-adjusted return (annualised) |
| `calculate_sortino_ratio(nav)` | Downside-risk-adjusted return |
| `calculate_beta_alpha(nav, bench)` | Beta & Alpha vs benchmark |
| `rolling_returns(nav, years)` | Sliding-window CAGR series |
| `detect_underperformance(fund_id)` | Multi-rule underperformance flag |
| `portfolio_analytics(user_id)` | Full portfolio health analysis |

---

## 🗄 Database Schema

| Table | Key Columns |
|-------|-------------|
| `fund_master` | fund_id, fund_name, amc, category, benchmark, expense_ratio, aum |
| `nav_history` | fund_id, date, nav |
| `benchmark_history` | index_name, date, index_value |
| `fund_holdings` | fund_id, stock_name, sector, weight |
| `portfolio_user` | user_id, fund_id, amount_invested, purchase_date |
| `analytics_cache` | fund_id, return_1y/3y/5y, sharpe, drawdown, … |

---

## 🧪 Running Tests

```bash
pip install pytest
python -m pytest tests/ -v
```

---

## 📡 Data Sources

| Source | Data | Method |
|--------|------|--------|
| [AMFI India](https://www.amfiindia.com/spages/NAVAll.txt) | NAV (all funds) | HTTP GET (free) |
| [AMFI Portal](https://portal.amfiindia.com) | Historical NAV | HTTP GET (free) |
| Yahoo Finance (`yfinance`) | Benchmark indices | Python library |

---

## 🛣 Roadmap (per Design Document)

- [x] Month 1 — NAV ingestion, database schema
- [x] Month 2 — Performance analytics engine (CAGR, rolling returns)
- [x] Month 3 — Risk analytics (Sharpe, Sortino, Drawdown, Beta, Alpha)
- [x] Month 4 — Fund comparison tool + overlap detection
- [x] Month 5 — Portfolio Doctor (health score, sector allocation, recommendations)
- [x] Month 6 — Exit Strategy Engine + full dashboard UI

---

## 🔮 Future Expansion

- [ ] Web application (FastAPI + React frontend)
- [ ] AI-generated fund research reports (Claude API)
- [ ] Portfolio rebalancing engine
- [ ] Email / Telegram investment alerts
- [ ] Mobile app

---

## ⚖️ Disclaimer

This tool is for **educational and research purposes only**. It does not constitute financial advice. Always consult a SEBI-registered investment advisor before making investment decisions.
