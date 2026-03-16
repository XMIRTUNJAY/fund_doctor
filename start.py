#!/usr/bin/env python3
"""
Fund Doctor — Full Stack Launcher  (Windows + Mac + Linux)

Usage:
    python start.py              # backend + frontend
    python start.py --api-only   # backend only  (no Node.js needed)
    python start.py --test       # integration tests (API must be running first)
"""

import argparse
import os
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT       = Path(__file__).parent
IS_WINDOWS = platform.system() == "Windows"


# ── Locate npm / node on Windows ─────────────────────────────────────────────

def _find_npm() -> str:
    """Return the correct npm command string for this OS."""
    if not IS_WINDOWS:
        return "npm"

    # Priority order — covers official installer, nvm-windows, fnm, Chocolatey
    search = [
        shutil.which("npm.cmd"),
        shutil.which("npm"),
        str(Path(os.environ.get("ProgramFiles",  "C:/Program Files")) / "nodejs/npm.cmd"),
        str(Path(os.environ.get("APPDATA",       "")) / "npm/npm.cmd"),
        str(Path(os.environ.get("LOCALAPPDATA",  "")) / "Programs/nodejs/npm.cmd"),
        str(Path(os.environ.get("NVM_HOME",      "")) / "npm.cmd"),
    ]
    for candidate in search:
        if candidate and Path(candidate).exists():
            return candidate

    return "npm.cmd"   # last-resort; Popen will raise a clear error


def _find_node() -> str:
    if not IS_WINDOWS:
        return "node"
    return shutil.which("node.exe") or shutil.which("node") or "node.exe"


NPM  = _find_npm()
NODE = _find_node()


# ── Checks ────────────────────────────────────────────────────────────────────

def check_python_deps() -> bool:
    missing = []
    for pkg in ["fastapi", "uvicorn", "pandas", "numpy"]:
        try:
            __import__(pkg.replace("[standard]", ""))
        except ImportError:
            missing.append(pkg)
    if missing:
        print(f"\n⚠  Missing Python packages: {', '.join(missing)}")
        print("   Fix:  pip install -r requirements.txt\n")
        return False
    return True


def check_node_npm() -> tuple:
    """Returns (node_ok, npm_ok, node_ver, npm_ver)."""
    def _ver(cmd):
        try:
            r = subprocess.run([cmd, "--version"], capture_output=True, text=True, timeout=5)
            return r.returncode == 0, r.stdout.strip()
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False, ""

    node_ok, node_ver = _ver(NODE)
    npm_ok,  npm_ver  = _ver(NPM)
    return node_ok, npm_ok, node_ver, npm_ver


# ── Subprocess launchers ──────────────────────────────────────────────────────

def start_backend() -> subprocess.Popen:
    print("🐍 Starting FastAPI backend on http://localhost:8000 …")
    return subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "api:app",
         "--host", "0.0.0.0", "--port", "8000", "--reload"],
        cwd=ROOT,
    )


def start_frontend() -> subprocess.Popen:
    fe = ROOT / "frontend"

    if not (fe / "node_modules").exists():
        print("📦 Installing npm dependencies (first run only) …")
        r = subprocess.run(
            [NPM, "install"],
            cwd=fe,
            shell=IS_WINDOWS,   # shell=True ensures .cmd scripts resolve on Windows
        )
        if r.returncode != 0:
            raise RuntimeError("npm install failed — see output above")
        print("   ✅ npm install done")

    print("⚛️  Starting React frontend on http://localhost:3000 …")
    return subprocess.Popen(
        [NPM, "run", "dev"],
        cwd=fe,
        shell=IS_WINDOWS,
    )


# ── API readiness check ───────────────────────────────────────────────────────

def wait_for_api(timeout=40) -> bool:
    import urllib.request
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen("http://localhost:8000/api/health", timeout=2) as r:
                if r.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(1)
    return False


# ── Integration test ──────────────────────────────────────────────────────────

def run_integration_test() -> bool:
    import urllib.request, json as _json

    BASE = "http://localhost:8000/api"
    passed = failed = 0

    def get(path):
        try:
            with urllib.request.urlopen(f"{BASE}{path}", timeout=15) as r:
                return _json.loads(r.read()), r.status
        except urllib.request.HTTPError as e:
            return None, e.code
        except Exception as ex:
            return None, str(ex)

    def chk(label, cond, detail=""):
        nonlocal passed, failed
        if cond: print(f"  ✅ {label}"); passed += 1
        else:    print(f"  ❌ {label}  {detail}"); failed += 1

    print("\n" + "="*56)
    print("  FUND DOCTOR — INTEGRATION TESTS")
    print("="*56)

    d,s = get("/health")
    chk("GET /health → 200",           s == 200, s)

    d,s = get("/overview")
    chk("GET /overview → 200",         s == 200, s)
    chk("  funds loaded",              d and len(d.get("funds",[])) > 0)
    chk("  flag_counts present",       d and "flag_counts" in d)
    chk("  return_1y in each fund",    d and "return_1y" in (d.get("funds") or [{}])[0])

    d,s = get("/funds")
    chk("GET /funds → 200",            s == 200)
    fid = (d.get("funds") or [{"fund_id":"120716"}])[0]["fund_id"] if d else "120716"

    d,s = get(f"/funds/{fid}/analytics")
    chk("GET analytics → 200",         s == 200, s)
    for k in ["return_1y","return_5y","sharpe_ratio","max_drawdown","volatility","alpha","beta"]:
        chk(f"  analytics.{k}",        d and k in d)

    for period in ["1Y","5Y"]:
        d,s = get(f"/funds/{fid}/nav?period={period}&thin=5")
        chk(f"GET nav period={period}", s == 200)
        chk(f"  nav_norm[0] ≈ 100",    d and abs((d.get("nav_norm") or [{"nav":0}])[0]["nav"]-100) < 0.1)

    d,s = get(f"/funds/{fid}/drawdown?period=5Y")
    chk("GET drawdown → 200",          s == 200)
    chk("  all dd ≤ 0",                d and all(x["dd"]<=0 for x in d.get("drawdown",[{"dd":-1}])))

    d,s = get(f"/funds/{fid}/rolling?window_years=3")
    chk("GET rolling 3Y → 200",        s == 200)
    chk("  fund_rolling non-empty",    d and len(d.get("fund_rolling",[])) > 0)

    d,s = get(f"/funds/{fid}/underperformance")
    chk("GET underperformance → 200",  s == 200)
    chk("  flag valid",                d and d.get("flag") in ["OK","WARNING","SERIOUS","CRITICAL","NO_DATA","INSUFFICIENT_DATA"])

    d,s = get(f"/comparison?fund_a=120716&fund_b=120503&period=5Y")
    chk("GET comparison → 200",        s == 200, s)
    chk("  comparison_table present",  d and len(d.get("comparison_table",[])) > 0)
    chk("  nav_chart non-empty",       d and len(d.get("nav_chart",[])) > 0)
    chk("  winner field present",      d and "winner" in (d.get("comparison_table") or [{}])[0])

    d,s = get("/radar")
    chk("GET radar → 200",             s == 200)
    chk("  excess_return_pct present", d and "excess_return_pct" in (d.get("funds") or [{}])[0])

    d,s = get("/exit/120503?holding_months=24&invested_amount=30000")
    chk("GET exit (24mo) → 200",       s == 200, s)
    chk("  recommendation valid",      d and d.get("assessment",{}).get("recommendation") in ["HOLD","WATCH","SWITCH","EXIT"])
    chk("  LTCG note present",         d and any("LTCG" in t for t in d.get("assessment",{}).get("tax_notes",[])))
    chk("  replacements list",         d and isinstance(d.get("replacements"),list))

    d,s = get("/exit/120503?holding_months=6&invested_amount=30000")
    chk("  STCG note (6mo)",           d and any("STCG" in t for t in d.get("assessment",{}).get("tax_notes",[])))

    d,s = get("/portfolio")
    chk("GET portfolio → 200",         s == 200, s)
    for k in ["health_score","total_invested","total_current","total_gain","avg_er",
              "category_allocation","recommendations","holdings"]:
        chk(f"  portfolio.{k}",        d and k in d)
    chk("  health_score 0-100",        d and 0 <= d.get("health_score",-1) <= 100)

    d,s = get("/pipeline/status")
    chk("GET pipeline/status → 200",   s == 200)
    chk("  fund_count > 0",            d and d.get("fund_count",0) > 0)

    d,s = get("/funds/NONEXISTENT_XYZ")
    chk("GET nonexistent → 404",       s == 404)

    total = passed + failed
    print(f"\n{'='*56}")
    print(f"  {passed}/{total} PASSED  |  {failed} FAILED")
    print(f"{'='*56}\n")
    return failed == 0


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fund Doctor Launcher")
    parser.add_argument("--api-only", action="store_true")
    parser.add_argument("--test",     action="store_true")
    args = parser.parse_args()

    if args.test:
        print("🔍 Checking API is up …")
        if not wait_for_api(10):
            print("❌ API not reachable. Run:  python start.py --api-only")
            sys.exit(1)
        sys.exit(0 if run_integration_test() else 1)

    print("\n" + "="*56)
    print("  💊 FUND DOCTOR — Full Stack Launcher")
    print(f"  Platform: {platform.system()} {platform.machine()}")
    print("="*56)

    if not check_python_deps():
        sys.exit(1)

    procs = []
    try:
        procs.append(start_backend())

        if args.api_only:
            print("\n✅ Backend → http://localhost:8000")
            print("   Swagger → http://localhost:8000/docs")
            print("   Ctrl+C to stop.\n")
            procs[0].wait()
            sys.exit(0)

        # Check Node.js
        print("\n🔍 Checking Node.js / npm …")
        node_ok, npm_ok, node_ver, npm_ver = check_node_npm()
        if node_ok: print(f"   Node.js : {node_ver}")
        if npm_ok:  print(f"   npm     : {npm_ver}")

        if not node_ok or not npm_ok:
            print("\n⚠  Node.js / npm not found.")
            print("─────────────────────────────────────────────────────")
            print("  Install Node.js LTS from: https://nodejs.org")
            print("  After install, RESTART your terminal, then re-run:")
            print("    python start.py")
            print()
            print("  Or run the frontend manually:")
            print("    cd frontend")
            print("    npm install")
            print("    npm run dev")
            print("─────────────────────────────────────────────────────")
            print("\n✅ Backend IS running → http://localhost:8000")
            print("   Swagger Docs       → http://localhost:8000/docs")
            print("   Ctrl+C to stop.\n")
            procs[0].wait()
        else:
            print("\n⏳ Giving backend 4s to initialise …")
            time.sleep(4)
            procs.append(start_frontend())

            print("\n" + "="*56)
            print("  ✅ Fund Doctor is LIVE!")
            print()
            print("  🌐 React UI   →  http://localhost:3000")
            print("  🔌 Backend    →  http://localhost:8000")
            print("  📖 Swagger    →  http://localhost:8000/docs")
            print()
            print("  Click 'AMFI Live' in the sidebar to pull real data.")
            print("  Press Ctrl+C to stop both servers.")
            print("="*56 + "\n")

            for p in procs:
                p.wait()

    except KeyboardInterrupt:
        print("\n🛑 Shutting down …")
        for p in procs:
            try: p.terminate()
            except: pass
        print("   Goodbye!")
