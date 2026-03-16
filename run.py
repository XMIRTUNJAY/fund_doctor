#!/usr/bin/env python3
"""
Fund Doctor — Project Bootstrap & Runner
----------------------------------------
Run this script to initialise the database, seed demo data,
and launch the Streamlit dashboard.

Usage:
    python run.py               # Full setup + launch dashboard
    python run.py --setup-only  # Only initialise DB + seed data
    python run.py --fetch-amfi  # Fetch real data from AMFI (requires internet)
"""

import subprocess
import sys
import argparse
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))


def run_setup(fetch_amfi: bool = False):
    print("=" * 60)
    print("  💊 FUND DOCTOR — Setup")
    print("=" * 60)

    from database.db import initialize_database
    initialize_database()

    from pipeline.ingest import seed_demo_data
    print("\n🌱 Seeding demo data …")
    seed_demo_data()

    if fetch_amfi:
        print("\n📡 Fetching live AMFI data (this may take a while) …")
        from pipeline.ingest import (
            fetch_all_funds_from_amfi,
            seed_fund_master,
            seed_latest_nav,
            fetch_benchmark_data,
        )
        df = fetch_all_funds_from_amfi()
        seed_fund_master(df, limit=200)
        seed_latest_nav(df, limit=200)
        fetch_benchmark_data(years=5)

    print("\n✅ Setup complete!\n")


def launch_dashboard():
    dashboard = ROOT / "dashboard" / "app.py"
    print("🚀 Launching Streamlit dashboard …")
    print("   Navigate to: http://localhost:8501\n")
    subprocess.run([
        sys.executable, "-m", "streamlit", "run",
        str(dashboard),
        "--server.headless=true",
        "--server.port=8501",
    ])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fund Doctor Runner")
    parser.add_argument("--setup-only", action="store_true", help="Only setup DB, don't launch")
    parser.add_argument("--fetch-amfi", action="store_true", help="Fetch live AMFI data")
    args = parser.parse_args()

    run_setup(fetch_amfi=args.fetch_amfi)

    if not args.setup_only:
        launch_dashboard()
