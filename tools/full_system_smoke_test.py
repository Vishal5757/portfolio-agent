import base64
import io
import json
import re
import shutil
import sys
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

import openpyxl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import app


def build_tradebook_bytes():
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Equity"
    ws.append(["Symbol", "Trade Date", "Trade Type", "Quantity", "Price", "Trade ID", "Exchange", "Segment"])
    ws.append(["KITEX", "2025-01-10", "BUY", 10, 100, "TID-1001", "NSE", "EQ"])
    ws.append(["KITEX", "2025-01-11", "SELL", 5, 120, "TID-1002", "NSE", "EQ"])
    ws.append(["HDFCBANK", "2025-01-12", "BUY", 8, 1500, "TID-2001", "NSE", "EQ"])
    b = io.BytesIO()
    wb.save(b)
    return b.getvalue()


def build_cashflow_bytes():
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Funds Summary"
    ws.append(["Date", "Type", "Amount", "Remarks", "Txn ID"])
    ws.append(["2025-01-01", "deposit", 500000, "Funds added", "CF-1"])
    ws.append(["2025-01-15", "withdrawal", -25000, "instant payout to bank", "CF-2"])
    ws.append(
        [
            "2025-01-16",
            "investment",
            -50000,
            "Net settlement for Equity with settlement number: 2025107",
            "CF-3",
        ]
    )
    ws.append(["2025-01-17", "charge", -50, "DP Charges for Sale of HDFCBANK on 09/06/2025", "CF-4"])
    b = io.BytesIO()
    wb.save(b)
    return b.getvalue()


class FakeClient(app.MarketDataClient):
    def __init__(self):
        pass

    def fetch_multi_source_quote(self, exchange, symbol, feed_code, source_order=None, max_deviation_pct=8.0, asset_class=None):
        sym = str(symbol).upper()
        ltp = 100.0 + (sum(ord(c) for c in sym) % 50)
        candidates = [
            {"source": "nse_api", "ltp": ltp, "change_abs": 1.2, "latency_ms": 60},
            {"source": "yahoo_finance", "ltp": ltp * 1.001, "change_abs": 1.1, "latency_ms": 120},
            {"source": "screener_scrape", "ltp": ltp * 0.998, "change_abs": 0.0, "latency_ms": 180},
        ]
        attempts = [
            {"source": "nse_api", "success": True, "ltp": candidates[0]["ltp"], "latency_ms": 60},
            {"source": "yahoo_finance", "success": True, "ltp": candidates[1]["ltp"], "latency_ms": 120},
            {"source": "screener_scrape", "success": True, "ltp": candidates[2]["ltp"], "latency_ms": 180},
        ]
        return {
            "selected": candidates[2],
            "consensus_ltp": candidates[2]["ltp"],
            "candidates": candidates,
            "attempts": attempts,
            "filtered_candidates": candidates,
            "source_order": source_order or [],
        }

    def fetch_nse_all_indices(self):
        return {
            "NIFTY 50": {"last": 25000, "change_abs": 120, "change_pct": 0.5},
            "NIFTY BANK": {"last": 52000, "change_abs": 180, "change_pct": 0.35},
            "NIFTY MIDCAP 100": {"last": 60000, "change_abs": 200, "change_pct": 0.42},
            "NIFTY SMALLCAP 100": {"last": 18000, "change_abs": 45, "change_pct": 0.25},
        }


def run():
    root = Path(__file__).resolve().parents[1]
    base = root / "data" / "_tmp_full_audit_run"
    if base.exists():
        shutil.rmtree(base, ignore_errors=True)
    base.mkdir(parents=True, exist_ok=True)

    app.DB_PATH = base / "portfolio.db"
    app.MARKET_HISTORY_DB_PATH = base / "market_history.db"
    app.DATA_DIR = base
    app.UPLOAD_DIR = base / "uploads"
    app.BACKUP_DIR = base / "backups"

    app.MarketDataClient = FakeClient
    app.init_db()

    tradebook_b64 = base64.b64encode(build_tradebook_bytes()).decode("ascii")
    cashflow_b64 = base64.b64encode(build_cashflow_bytes()).decode("ascii")

    server = ThreadingHTTPServer(("127.0.0.1", 0), app.AppHandler)
    port = server.server_address[1]
    th = threading.Thread(target=server.serve_forever, daemon=True)
    th.start()

    def req(method, path, body=None, expected=None):
        headers = {}
        data = None
        if method.upper() in {"POST", "PUT", "DELETE"}:
            headers["X-Portfolio-Agent-Local"] = "1"
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        r = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(r, timeout=20) as resp:
                code = resp.getcode()
                out = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            code = e.code
            out = json.loads(e.read().decode("utf-8"))
        if expected is not None and code != expected:
            raise AssertionError(f"{method} {path} expected {expected}, got {code}, body={out}")
        return code, out

    def req_text(path, expected=None):
        r = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method="GET")
        try:
            with urllib.request.urlopen(r, timeout=20) as resp:
                code = resp.getcode()
                out = resp.read().decode("utf-8", errors="ignore")
        except urllib.error.HTTPError as e:
            code = e.code
            out = e.read().decode("utf-8", errors="ignore")
        if expected is not None and code != expected:
            raise AssertionError(f"GET {path} expected {expected}, got {code}")
        return code, out

    checks = []

    def check(name, fn):
        try:
            fn()
            checks.append((name, "PASS", ""))
        except Exception as ex:
            checks.append((name, "FAIL", str(ex)))

    def expect(cond, msg):
        if not cond:
            raise AssertionError(msg)

    def upsert_symbol(conn, symbol, qty, avg_cost, ltp, exchange="NSE", asset_class="EQUITY"):
        sym = str(symbol or "").strip().upper()
        invested = round(float(qty) * float(avg_cost), 2)
        market_value = round(float(qty) * float(ltp), 2)
        unrealized = round(market_value - invested, 2)
        total_return = round((unrealized / invested) * 100.0, 2) if invested > 0 else 0.0
        stamp = app.now_iso()
        conn.execute(
            """
            INSERT INTO instruments(symbol, exchange, asset_class, active)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(symbol) DO UPDATE SET exchange=excluded.exchange, asset_class=excluded.asset_class, active=1
            """,
            (sym, exchange, asset_class),
        )
        conn.execute(
            """
            INSERT INTO holdings(symbol, qty, avg_cost, invested, market_value, realized_pnl, unrealized_pnl, total_return_pct, updated_at)
            VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
              qty=excluded.qty,
              avg_cost=excluded.avg_cost,
              invested=excluded.invested,
              market_value=excluded.market_value,
              realized_pnl=excluded.realized_pnl,
              unrealized_pnl=excluded.unrealized_pnl,
              total_return_pct=excluded.total_return_pct,
              updated_at=excluded.updated_at
            """,
            (sym, qty, avg_cost, invested, market_value, unrealized, total_return, stamp),
        )
        conn.execute(
            """
            INSERT INTO latest_prices(symbol, ltp, change_abs, updated_at)
            VALUES (?, ?, 0, ?)
            ON CONFLICT(symbol) DO UPDATE SET ltp=excluded.ltp, change_abs=excluded.change_abs, updated_at=excluded.updated_at
            """,
            (sym, ltp, stamp),
        )

    def clear_symbol(conn, symbol):
        sym = str(symbol or "").strip().upper()
        conn.execute("DELETE FROM trades WHERE UPPER(symbol) = ?", (sym,))
        conn.execute("DELETE FROM holdings WHERE UPPER(symbol) = ?", (sym,))
        conn.execute("DELETE FROM latest_prices WHERE UPPER(symbol) = ?", (sym,))
        conn.execute("DELETE FROM instruments WHERE UPPER(symbol) = ?", (sym,))

    def insert_trade(conn, symbol, side, trade_date, quantity, price, notes="smoke"):
        sym = str(symbol or "").strip().upper()
        side_u = str(side or "").strip().upper()
        qty = float(quantity)
        px = float(price)
        conn.execute(
            """
            INSERT INTO trades(symbol, side, trade_date, quantity, price, amount, source, notes)
            VALUES (?, ?, ?, ?, ?, ?, 'smoke', ?)
            """,
            (sym, side_u, str(trade_date), qty, px, round(qty * px, 2), str(notes or "")),
        )

    check("health", lambda: req("GET", "/api/v1/health", expected=200))
    def ui_index_structure():
        _, html = req_text("/", expected=200)
        required = [
            'id="holdingsTable"',
            'id="holdingsZeroTable"',
            'data-tab="dashboard"',
            'data-tab="scrip"',
            'data-tab="trades"',
            'data-tab="peak"',
            'data-tab="strategy"',
            'data-tab="dailytarget"',
            'data-tab="harvest"',
            'data-tab="losslots"',
            'data-tab="cashflow"',
            'data-tab="agents"',
            'data-tab="assistant"',
        ]
        for k in required:
            expect(k in html, f"index missing {k}")

    def ui_sticky_regression_contract():
        css = (root / "web" / "styles.css").read_text(encoding="utf-8")
        js = (root / "web" / "app.js").read_text(encoding="utf-8")
        css_required = [
            "#holdingsTable th:first-child",
            "#holdingsZeroTable th:first-child",
            "position: sticky;",
            "left: 0;",
            "thead th",
            "holding-symbol-fresh",
            "holding-symbol-stale",
        ]
        for k in css_required:
            expect(k in css, f"styles missing sticky contract: {k}")
        expect(
            "#holdingsTable thead th[data-sort-key], #holdingsZeroTable thead th[data-sort-key]" in js,
            "app.js missing dual-holdings sort binding",
        )
        expect("function holdingSymbolFreshnessClass(r)" in js, "holding symbol freshness function missing")
        expect("price_updated_at" in js, "price_updated_at not consumed in holdings UI")
        expect("state.latestPriceUpdatedAt = st.updated_at || \"\"" in js, "global price timestamp fallback missing")
        expect("getHoldingPriceUpdatedMs" in js, "holding timestamp fallback helper missing")
        expect("const multiplier = 20;" in js, "holding symbol freshness multiplier is not 20x")

    check("ui_index_structure", ui_index_structure)
    check("ui_sticky_regression_contract", ui_sticky_regression_contract)
    def ui_button_binding_contract():
        html = (root / "web" / "index.html").read_text(encoding="utf-8")
        js = (root / "web" / "app.js").read_text(encoding="utf-8")
        html_ids = {
            str(m.group(1) or "").strip()
            for m in re.finditer(r"<button\b[^>]*\bid\s*=\s*['\"]([^'\"]+)['\"][^>]*>", html, flags=re.IGNORECASE)
            if str(m.group(1) or "").strip()
        }
        js_ids = {
            str(m.group(1) or "").strip()
            for m in re.finditer(r"registerButton\(\s*['\"]([^'\"]+)['\"]", js)
            if str(m.group(1) or "").strip()
        }
        missing = sorted(x for x in html_ids if x not in js_ids)
        stale = sorted(x for x in js_ids if x not in html_ids)
        expect(not missing, f"button ids not bound via registerButton: {missing}")
        expect(not stale, f"registerButton ids missing in html: {stale}")
        expect("function verifyButtonBindings()" in js, "verifyButtonBindings function missing")
        expect("async function refreshSoftwarePerfNow()" in js, "refreshSoftwarePerfNow function missing")
        expect("btn.textContent = \"Refreshing...\"" in js, "refreshSoftwarePerfNow does not set refreshing button state")
        expect("Software logs refreshing:" in js, "refreshSoftwarePerfNow does not update refreshing stamp text")
        expect(
            "registerButton(\n    \"refreshSoftwarePerfBtn\",\n    refreshSoftwarePerfNow," in js,
            "refreshSoftwarePerfBtn is not bound to refreshSoftwarePerfNow",
        )

    check("ui_button_binding_contract", ui_button_binding_contract)
    def app_exit_contract():
        py = (root / "app.py").read_text(encoding="utf-8")
        js = (root / "web" / "app.js").read_text(encoding="utf-8")
        html = (root / "web" / "index.html").read_text(encoding="utf-8")
        css = (root / "web" / "styles.css").read_text(encoding="utf-8")
        expect('id="exitAppBtn"' in html, "exit button missing in header")
        expect("async function exitApp()" in js, "exit app handler missing")
        expect('registerButton("exitAppBtn", exitApp' in js, "exit button is not registered")
        expect('"/api/v1/system/shutdown"' in js, "exit button does not call shutdown endpoint")
        expect('if parsed.path == "/api/v1/system/shutdown":' in py, "shutdown endpoint missing")
        expect("self.server.shutdown" in py, "shutdown endpoint does not stop server")
        expect("server.server_close()" in py, "server socket close missing after shutdown")
        expect(".btn.danger" in css, "danger button style missing")

    check("app_exit_contract", app_exit_contract)
    def sqlite_lock_resilience_contract():
        py = (root / "app.py").read_text(encoding="utf-8")
        expect("SQLITE_TIMEOUT_SEC = 30" in py, "SQLite timeout constant missing")
        expect("SQLITE_BUSY_TIMEOUT_MS = SQLITE_TIMEOUT_SEC * 1000" in py, "SQLite busy timeout constant missing")
        expect("def _configure_sqlite_connection(conn):" in py, "SQLite connection configurator missing")
        expect("PRAGMA busy_timeout" in py, "SQLite busy_timeout pragma missing")
        expect("def _enable_sqlite_wal(conn):" in py, "SQLite WAL helper missing")
        expect("PRAGMA journal_mode=WAL" in py, "SQLite WAL mode not enabled")
        expect('sqlite3.connect(p["db_path"], timeout=SQLITE_TIMEOUT_SEC)' in py, "main DB connect timeout missing")
        expect('sqlite3.connect(p["market_db_path"], timeout=SQLITE_TIMEOUT_SEC)' in py, "market DB connect timeout missing")

    check("sqlite_lock_resilience_contract", sqlite_lock_resilience_contract)
    def modular_extraction_contract():
        py = (root / "app.py").read_text(encoding="utf-8")
        init_py = (root / "portfolio_agent" / "__init__.py").read_text(encoding="utf-8")
        utils_py = (root / "portfolio_agent" / "utils.py").read_text(encoding="utf-8")
        quote_py = (root / "portfolio_agent" / "quote_manager.py").read_text(encoding="utf-8")
        expect("from portfolio_agent.utils import" in py, "app.py missing utils module imports")
        expect("from portfolio_agent.quote_manager import" in py, "app.py missing quote_manager module imports")
        for left, right in (
            ("clamp", "_mod_clamp"),
            ("parse_float", "_mod_parse_float"),
            ("now_iso", "_mod_now_iso"),
            ("get_ranked_quote_sources", "_mod_get_ranked_quote_sources"),
            ("quote_source_ranking", "_mod_quote_source_ranking"),
            ("_quote_corroboration_count", "_mod_quote_corroboration_count"),
        ):
            expect(re.search(rf"^{re.escape(left)}\s*=\s*{re.escape(right)}\s*$", py, re.MULTILINE), f"app.py module wiring missing {left} = {right}")
        for token in (
            "def parse_float(",
            "def parse_excel_date(",
            "def parse_token_list(",
            "def is_zero_qty_eod_window(",
        ):
            expect(token in utils_py, f"utils module missing {token}")
        for token in (
            "def discovered_quote_sources(",
            "def ensure_quote_source_registry(",
            "def get_ranked_quote_sources(",
            "def quote_source_ranking(",
            "def quote_corroboration_count(",
        ):
            expect(token in quote_py, f"quote_manager module missing {token}")
        expect('"utils"' in init_py and '"quote_manager"' in init_py, "portfolio_agent exports missing extracted modules")

    check("modular_extraction_contract", modular_extraction_contract)
    check("config_get", lambda: req("GET", "/api/v1/config/live", expected=200))
    check(
        "config_put",
        lambda: req(
            "PUT",
            "/api/v1/config/live",
            {
                "enabled": True,
                "interval_seconds": 8,
                "quote_sources": ["nse_api", "yahoo_finance", "screener_scrape"],
                "quote_max_deviation_pct": 6.5,
                "quote_top_k": 3,
                "quote_explore_ratio": 0.15,
            },
            expected=200,
        ),
    )
    def agents_status_contract():
        _, out = req("GET", "/api/v1/agents/status", expected=200)
        items = out.get("items") or []
        agents = {str(i.get("agent") or "") for i in items}
        expect("chart_intel" in agents, "chart_intel agent missing in status")

    check("agents_status", agents_status_contract)
    def gold_daily_snapshot_contract():
        py = (root / "app.py").read_text(encoding="utf-8")
        expect("def insert_market_daily_prices_if_missing(rows):" in py, "gold daily snapshot insert helper missing")
        expect(
            "insert_market_daily_prices_if_missing(gold_hist_payload)" in py,
            "gold daily first-hit history logging not wired in refresh flow",
        )
        expect(
            "change_abs = ltp - prev_close" in py and "GOLD day-change should be derived from previously logged daily close." in py,
            "gold day-change not derived from previous logged close",
        )

    check("gold_daily_snapshot_contract", gold_daily_snapshot_contract)
    def rebalance_locked_qty_edit_contract():
        py = (root / "app.py").read_text(encoding="utf-8")
        js = (root / "web" / "app.js").read_text(encoding="utf-8")
        expect("def set_rebalance_lot_item_planned_qty(conn, item_id, planned_qty):" in py, "rebalance qty-edit backend handler missing")
        expect("state_or_completed_or_planned_qty_required" in py, "rebalance lot item endpoint not accepting qty-only updates")
        expect("rebalance-locked-qty-input" in js, "rebalance planner locked qty is not editable in UI")
        expect("payload.planned_qty = Number(plannedQty);" in js, "rebalance planner update payload missing planned_qty")

    check("rebalance_locked_qty_edit_contract", rebalance_locked_qty_edit_contract)
    def daily_target_planner_contract():
        py = (root / "app.py").read_text(encoding="utf-8")
        js = (root / "web" / "app.js").read_text(encoding="utf-8")
        html = (root / "web" / "index.html").read_text(encoding="utf-8")
        schema = (root / "schema.sql").read_text(encoding="utf-8")
        expect("CREATE TABLE IF NOT EXISTS daily_target_plans" in py, "daily target plan table missing")
        expect("CREATE TABLE IF NOT EXISTS daily_target_plan_pairs" in py, "daily target pair table missing")
        expect("llm_verdict TEXT" in py and "llm_score_adjustment REAL" in schema, "daily target llm judgement columns missing")
        expect("CREATE TABLE IF NOT EXISTS daily_target_pair_snapshots" in py, "daily target snapshot table missing")
        expect("def build_daily_target_suggestions(" in py, "daily target suggestion builder missing")
        expect("def get_or_create_daily_target_plan(" in py, "daily target plan function missing")
        expect("def update_daily_target_pair(" in py, "daily target pair update function missing")
        expect("/api/v1/daily-target/plan" in py, "daily target plan endpoint missing")
        expect("/api/v1/daily-target/history" in py, "daily target history endpoint missing")
        expect("/api/v1/daily-target/reset" in py, "daily target reset endpoint missing")
        expect("/api/v1/daily-target/pairs/" in py, "daily target pair endpoint missing")
        expect('data-tab="dailytarget"' in html, "daily target tab missing in ui")
        for token in (
            'id="dailyTargetSeedCapital"',
            'id="dailyTargetProfitPct"',
            'id="dailyTargetTopN"',
            'id="dailyTargetUseHostedLlm"',
            'id="dailyTargetLlmReview"',
            'id="dailyTargetTable"',
            'id="dailyTargetCompletedTable"',
            'id="dailyTargetFullCycleTable"',
            'id="dailyTargetSnapshotsTable"',
            'id="dailyTargetHistoryFrom"',
            'id="dailyTargetHistoryTo"',
            'id="dailyTargetHistoryState"',
            'id="dailyTargetHistoryTable"',
            'id="dailyTargetHistorySummary"',
            'id="dailyTargetPerformance"',
        ):
            expect(token in html, f"daily target ui missing {token}")
        expect("def sync_daily_target_positions(" in py, "daily target position sync helper missing")
        expect("def list_daily_target_full_cycles(" in py, "daily target full-cycle listing helper missing")
        expect("_append_daily_target_full_cycle_note" in py, "daily target full-cycle auto-comment helper missing")
        expect("CREATE TABLE IF NOT EXISTS daily_target_positions" in py, "daily target positions table missing")
        expect("def compute_daily_target_performance(" in py, "daily target performance helper missing")
        expect("used_symbols = set()" in py, "daily target disjoint buy/sell symbol guard missing")
        expect("function renderDailyTargetPlan(" in js, "daily target renderer missing")
        expect("function loadDailyTargetPlan(options = {})" in js, "daily target loader missing")
        expect("function loadDailyTargetHistory(options = {})" in js, "daily target history loader missing")
        expect("function isClosedDailyTargetState(" in js, "daily target closed-state helper missing")
        expect("dailyTargetRefreshBtn" in js and "dailyTargetResetBtn" in js, "daily target buttons not wired")
        expect("dailyTargetDrafts" in js, "daily target draft state missing")
        expect("delete state.dailyTargetDrafts[itemId]" in js, "daily target draft cleanup missing after save")
        expect("date_from" in py and "state_filter" in py, "daily target history backend filters missing")
        expect("live_mtm_basis_value" in py, "daily target net live mtm basis metric missing")
        expect('Net Live P/L' in js, "daily target net live pnl label missing")
        expect("Full Cycle Complete - Latest 10" in html, "daily target full-cycle table title missing")
        expect("effective_state = state_norm" in py, "daily target effective state auto-upgrade missing")
        expect("def _daily_target_live_pair_metrics(" in py, "daily target live pair metrics helper missing")
        expect("def attach_daily_target_llm_review(" in py, "daily target hosted llm review helper missing")
        expect("def _daily_target_llm_judgement_map(" in py, "daily target llm judgement parser missing")
        expect("def _apply_daily_target_llm_judgements(" in py, "daily target llm judgement applier missing")
        expect("purpose=\"daily_target\"" in py, "daily target hosted llm purpose not tracked")
        expect("def _daily_target_build_buy_leg(" in py, "daily target buy-leg builder missing")
        expect("def _daily_target_zerodha_delivery_costs(" in py, "daily target Zerodha delivery cost helper missing")
        expect("def _daily_target_charge_drag(" in py, "daily target charge-drag helper missing")
        expect("def _daily_target_economic_trade_size(" in py, "daily target economic trade-size helper missing")
        expect("def _daily_target_trade_size_summary(" in py, "daily target trade-size summary helper missing")
        expect("def _daily_target_estimate_sell_tax_profile(" in py, "daily target sell tax profile helper missing")
        expect("def _daily_target_required_exit_price_for_net_goal(" in py, "daily target net-goal target exit helper missing")
        expect("def compute_realized_equity_tax_summary(" in py, "daily target FY-aware realized equity tax summary helper missing")
        expect("ltcg_remaining_exemption" in py, "daily target LTCG exemption tracking missing")
        expect("tax_mode" in py and "equity_stcg_tax_pct" in py and "equity_ltcg_tax_pct" in py, "daily target tax summary fields missing")
        expect("Tax Mode:" in js and "Broker Cost Model:" in js, "daily target tax assumptions not surfaced in ui summary")
        expect("Economical Trade Value:" in js and "Charge Drag @ Seed:" in js and "Size Advice:" in js, "daily target charge-aware sizing not surfaced in ui summary")
        expect("use_hosted_llm" in js and "Agent Note" in html and "LLM pair judgment" in js, "daily target hosted llm judgement not wired in ui")
        expect("pipeline_buy_switch_after_update" in py, "daily target pipeline switch recalibration missing")

    check("daily_target_planner_contract", daily_target_planner_contract)
    def attention_console_contract():
        py = (root / "app.py").read_text(encoding="utf-8")
        js = (root / "web" / "app.js").read_text(encoding="utf-8")
        html = (root / "web" / "index.html").read_text(encoding="utf-8")
        schema = (root / "schema.sql").read_text(encoding="utf-8")
        expect("CREATE TABLE IF NOT EXISTS tax_rate_sync_runs" in py and "CREATE TABLE IF NOT EXISTS tax_rate_sync_runs" in schema, "tax rate sync run table missing")
        expect("CREATE TABLE IF NOT EXISTS attention_alerts" in py and "CREATE TABLE IF NOT EXISTS attention_alerts" in schema, "attention alerts table missing")
        expect("def run_tax_rate_monitor_once(" in py, "tax rate monitor runner missing")
        expect("def build_attention_console_payload(" in py, "attention console payload builder missing")
        expect('data-tab="attention"' in html, "attention console tab missing")
        for token in (
            'id="attentionRefreshBtn"',
            'id="attentionRunTaxMonitorBtn"',
            'id="attentionSummary"',
            'id="attentionTaxProfile"',
            'id="attentionOpenTable"',
            'id="attentionTaxRunsTable"',
            'id="attentionResolvedTable"',
        ):
            expect(token in html, f"attention console ui missing {token}")
        expect('"/api/v1/attention"' in py, "attention console endpoint missing")
        expect('"agent": "tax_monitor"' in py, "tax monitor agent missing from status")
        expect("function loadAttentionConsole(options = {})" in js, "attention console loader missing")
        expect("function runAttentionTaxMonitor()" in js, "attention tax monitor runner missing")
        expect("attentionRefreshBtn" in js and "attentionRunTaxMonitorBtn" in js, "attention console buttons not wired")

    check("attention_console_contract", attention_console_contract)
    def strategy_audit_contract():
        py = (root / "app.py").read_text(encoding="utf-8")
        js = (root / "web" / "app.js").read_text(encoding="utf-8")
        html = (root / "web" / "index.html").read_text(encoding="utf-8")
        schema = (root / "schema.sql").read_text(encoding="utf-8")
        schema = (root / "schema.sql").read_text(encoding="utf-8")
        expect("CREATE TABLE IF NOT EXISTS strategy_audit_runs" in py, "strategy audit run table missing")
        expect("CREATE TABLE IF NOT EXISTS strategy_audit_findings" in py, "strategy audit findings table missing")
        expect("CREATE TABLE IF NOT EXISTS hosted_llm_runs" in py and "CREATE TABLE IF NOT EXISTS hosted_llm_runs" in schema, "hosted llm metrics table missing")
        expect("def run_strategy_audit(conn, refresh_strategy=False" in py, "strategy audit runner missing")
        expect("def list_strategy_audit_runs(conn, limit=25):" in py, "strategy audit history helper missing")
        expect("/api/v1/strategy/audits" in py, "strategy audit list endpoint missing")
        expect("/api/v1/strategy/audits/run" in py, "strategy audit run endpoint missing")
        expect('data-tab="strategyaudit"' in html, "strategy audit tab missing in ui")
        for token in (
            'id="strategyAuditRunBtn"',
            'id="strategyAuditRefreshFirst"',
            'id="strategyAuditRefreshBtn"',
            'id="strategyAuditSummary"',
            'id="strategyAuditFindingsTable"',
            'id="strategyAuditHistoryTable"',
        ):
            expect(token in html, f"strategy audit ui missing {token}")
        expect("function renderStrategyAudit(" in js, "strategy audit renderer missing")
        expect("function loadStrategyAudit(options = {})" in js, "strategy audit loader missing")
        expect("function runStrategyAudit()" in js, "strategy audit runner missing in ui")
        expect("strategyAuditRunBtn" in js and "strategyAuditRefreshBtn" in js, "strategy audit buttons not wired")

    check("strategy_audit_contract", strategy_audit_contract)
    def harvest_planner_contract():
        py = (root / "app.py").read_text(encoding="utf-8")
        js = (root / "web" / "app.js").read_text(encoding="utf-8")
        html = (root / "web" / "index.html").read_text(encoding="utf-8")
        expect("def build_tax_harvest_plan(conn, target_loss=0.0, run_analysis=False):" in py, "harvest planner backend missing")
        expect("def open_lot_tax_bucket_summary(" in py, "harvest planner tax-bucket lot helper missing")
        expect("def open_lot_tax_bucket_rows(" in py, "harvest planner lot-level helper missing")
        expect("def build_loss_lot_analysis(conn):" in py, "loss lot analysis backend missing")
        expect("/api/v1/loss-lots" in py, "loss lot analysis endpoint missing")
        expect("total_loss_available_stcg" in py and "suggested_offset_profit_ltcg" in py, "harvest planner summary lacks tax-bucket totals")
        expect("/api/v1/harvest/plan" in py, "harvest planner endpoint missing")
        expect("run_tax_harvest_dynamic_analysis" in py, "harvest planner local dynamic-analysis hook missing")
        expect('data-tab="harvest"' in html, "harvest tab missing in ui")
        expect('data-tab="losslots"' in html, "loss lots tab missing in ui")
        expect("<th>Tax</th>" in html and "<th>Held Days</th>" in html and "<th>Buy Date</th>" in html, "harvest ui missing lot-level columns")
        expect(
            'id="lossLotsStcgTable"' in html and 'id="lossLotsLtcgTable"' in html and
            'id="lossLotsStcgProfitTable"' in html and 'id="lossLotsLtcgProfitTable"' in html,
            "loss lots tables missing",
        )
        expect("function loadHarvestPlan(options = {})" in js, "harvest planner load function missing")
        expect("function loadLossLots(options = {})" in js, "loss lots loader missing")
        expect("function renderLossLots(payload)" in js, "loss lots renderer missing")
        expect("function renderProfitLotsTable(" in js, "loss lots profit table renderer missing")
        expect("function harvestHeldDaysLabel(row)" in js, "harvest ui missing held-days helper")
        expect("function renderHarvestLossBucketTable(" in js, "harvest ui missing top bucket table renderer")
        expect(
            "Loss Lots" in html and "Top 10 STCG Loss Lots" in html and "Top 10 LTCG Loss Lots" in html and
            "Top 10 STCG Profit Lots" in html and "Top 10 LTCG Profit Lots" in html,
            "loss lots section titles missing",
        )
        expect("STCG Loss Available" in js and "Suggested LTCG Offset" in js, "harvest ui summary missing tax-bucket metrics")
        expect("harvestRunAnalysisBtn" in js, "harvest dynamic analysis button not wired")

    check("harvest_planner_contract", harvest_planner_contract)
    def hosted_free_llm_contract():
        py = (root / "app.py").read_text(encoding="utf-8")
        js = (root / "web" / "app.js").read_text(encoding="utf-8")
        html = (root / "web" / "index.html").read_text(encoding="utf-8")
        schema = (root / "schema.sql").read_text(encoding="utf-8")
        perf = (root / "portfolio_agent" / "software_performance.py").read_text(encoding="utf-8")
        expect("/api/v1/hosted-llm/config" in py, "hosted free-tier llm config endpoint missing")
        expect("/api/v1/hosted-llm/test" in py, "hosted free-tier llm test endpoint missing")
        expect("/api/v1/hosted-llm/metrics" in py, "hosted llm metrics endpoint missing")
        expect("HOSTED_LLM_PROVIDERS" in py and "openrouter" in py and "groq" in py and "huggingface" in py, "hosted llm provider mix missing")
        expect("rate_limited_429" in py and "auth_or_access_" in py, "hosted llm actionable http diagnostics missing")
        expect("function loadHostedLlmConfig" in js and "function saveHostedLlmConfig" in js, "hosted llm ui config functions missing")
        expect("function renderHostedLlmMetrics" in js and "function loadHostedLlmMetrics" in js, "hosted llm metrics ui missing")
        expect(
            "function hostedLlmErrorHint" in js
            and "Groq free-tier quota" in js
            and (("Hugging Face token/model access" in js) or ("HF token invalid or missing READ scope" in js)),
            "hosted llm actionable error hints missing",
        )
        expect("hostedLlmSaveBtn" in js and "hostedLlmTestBtn" in js, "hosted llm buttons not wired")
        expect('id="strategyAuditUseHostedLlm"' in html, "strategy audit hosted llm toggle missing")
        expect('id="hostedLlmMetricsTable"' in html and 'id="hostedLlmMetricsSummary"' in html, "hosted llm operational dashboard missing")
        expect('id="hostedLlmOpenrouterKey"' in html and 'id="hostedLlmGroqKey"' in html and 'id="hostedLlmHuggingfaceKey"' in html, "hosted llm provider key inputs missing")
        expect("_software_perf_generate_local_proposal" in perf, "software perf local proposal generator missing")
        expect("purpose=\"software_performance\"" in perf and "hosted_llm_support" in perf, "software performance agent hosted llm support missing")

    check("hosted_free_llm_contract", hosted_free_llm_contract)
    def equity_gold_source_guard_contract():
        py = (root / "app.py").read_text(encoding="utf-8")
        expect('if source == "gold_rate_scrape" and ac != ASSET_CLASS_GOLD:' in py, "equity gold-source plausibility guard missing")
        expect('normalized_sources = [s for s in normalized_sources if str(s or "").strip().lower() != "gold_rate_scrape"]' in py, "equity quote collection still allows gold source")

    check("equity_gold_source_guard_contract", equity_gold_source_guard_contract)
    def exchange_day_change_contract():
        py = (root / "app.py").read_text(encoding="utf-8")
        expect("def resolve_preferred_equity_day_change_abs(" in py, "exchange day-change resolver missing")
        expect("change_abs = resolve_preferred_equity_day_change_abs(" in py, "live refresh not using exchange day-change resolver")
        expect("def is_plausible_day_reference_price(" in py, "day reference plausibility guard missing")
        expect("def sanitize_latest_price_day_change_outliers(" in py, "latest day-change sanitizer missing")
        expect("sanitize_latest_price_day_change_outliers(conn, fetched_at)" in py, "live refresh not sanitizing implausible day-change values")
        expect("def repair_all_tenants_market_data_once(" in py, "cross-tenant market-data repair missing")
        expect("repair_results = repair_all_tenants_market_data_once()" in py, "startup does not run cross-tenant market-data repair")
        expect("def export_repo_data_snapshots(" in py, "repo data snapshot exporter missing")
        expect("def sync_repo_data_snapshots_to_git(" in py, "repo data git sync helper missing")
        expect("def repo_data_sync_worker(stop_event):" in py, "repo data sync worker missing")
        expect("export_repo_data_snapshots()" in py, "startup does not export repo data snapshots")

    check("exchange_day_change_contract", exchange_day_change_contract)
    check("intel_autopilot_get", lambda: req("GET", "/api/v1/intel/autopilot", expected=200))
    check("intel_charts_get", lambda: req("GET", "/api/v1/intel/charts?limit=40", expected=200))
    check("ranking_get", lambda: req("GET", "/api/v1/prices/source-ranking", expected=200))
    check(
        "ranking_toggle",
        lambda: req("PUT", "/api/v1/prices/source-ranking", {"source": "yahoo_finance", "enabled": False}, expected=200),
    )
    check(
        "ranking_toggle_invalid_bool",
        lambda: req("PUT", "/api/v1/prices/source-ranking", {"source": "yahoo_finance", "enabled": "maybe"}, expected=400),
    )
    check("add_scrip_kitex", lambda: req("POST", "/api/v1/scrips", {"symbol": "KITEX", "exchange": "NSE", "ltp": 95}, expected=200))
    check("add_scrip_hdfc", lambda: req("POST", "/api/v1/scrips", {"symbol": "HDFCBANK", "exchange": "NSE"}, expected=200))
    check(
        "manual_trade_buy",
        lambda: req(
            "POST",
            "/api/v1/scrips/KITEX/trades",
            {"side": "BUY", "trade_date": "2025-02-01", "quantity": 10, "price": 100, "notes": "manual"},
            expected=200,
        ),
    )
    check(
        "manual_trade_duplicate",
        lambda: req(
            "POST",
            "/api/v1/scrips/KITEX/trades",
            {"side": "BUY", "trade_date": "2025-02-01", "quantity": 10, "price": 100, "notes": "manual"},
            expected=409,
        ),
    )
    check(
        "manual_trade_sell",
        lambda: req(
            "POST",
            "/api/v1/scrips/KITEX/trades",
            {"side": "SELL", "trade_date": "2025-02-10", "quantity": 2, "price": 115, "notes": "exit"},
            expected=200,
        ),
    )
    check(
        "trade_override",
        lambda: req(
            "POST",
            "/api/v1/trades/override",
            {
                "symbol": "KITEX",
                "side": "BUY",
                "trade_date": "2025-03-01",
                "quantity": 1,
                "price": 1,
                "notes": "override",
                "external_trade_id": "OV-1",
            },
            expected=200,
        ),
    )
    check(
        "tradebook_upload_1",
        lambda: req(
            "POST",
            "/api/v1/upload/tradebook",
            {"filename": "tb1.xlsx", "content_base64": tradebook_b64, "include_skipped": True},
            expected=200,
        ),
    )
    check(
        "tradebook_upload_2_dedupe",
        lambda: req(
            "POST",
            "/api/v1/upload/tradebook",
            {"filename": "tb1.xlsx", "content_base64": tradebook_b64, "include_skipped": True},
            expected=200,
        ),
    )
    check(
        "cashflow_upload_1",
        lambda: req(
            "POST",
            "/api/v1/upload/cashflow",
            {"filename": "cf1.xlsx", "content_base64": cashflow_b64, "replace_all": False},
            expected=200,
        ),
    )
    check(
        "cashflow_upload_2_dedupe",
        lambda: req(
            "POST",
            "/api/v1/upload/cashflow",
            {"filename": "cf1.xlsx", "content_base64": cashflow_b64, "replace_all": False},
            expected=200,
        ),
    )
    check("cashflows_get", lambda: req("GET", "/api/v1/cashflows", expected=200))
    check("prices_refresh", lambda: req("POST", "/api/v1/prices/refresh", {}, expected=200))
    def kitex_day_change_prefers_exchange():
        _, out = req("GET", "/api/v1/scrips/KITEX", expected=200)
        expect(abs(float(out.get("day_pnl") or 0.0)) > 0.01, "KITEX day_pnl stayed near zero; exchange day change not applied")

    check("kitex_day_change_prefers_exchange", kitex_day_change_prefers_exchange)
    check("prices_status", lambda: req("GET", "/api/v1/prices/status", expected=200))
    check("prices_sources", lambda: req("GET", "/api/v1/prices/sources?symbol=KITEX&limit=50", expected=200))
    def daily_target_plan_runtime():
        _, out = req("GET", "/api/v1/daily-target/plan?seed_capital=10000&target_profit_pct=1&top_n=3&recalibrate=1", expected=200)
        summary = out.get("summary") or {}
        perf = out.get("performance") or {}
        for key in ("seed_capital", "target_profit_pct", "target_profit_value", "pending_pairs"):
            expect(key in summary, f"daily target summary missing {key}")
        for key in ("tax_mode", "equity_stcg_tax_pct", "equity_ltcg_tax_pct", "zerodha_cost_model", "remaining_ltcg_exemption", "equity_ltcg_exemption_limit"):
            expect(key in summary, f"daily target summary missing {key}")
        for key in ("effective_trade_capital", "economic_min_trade_value", "charge_drag_pct_at_seed", "charge_drag_pct_at_effective", "trade_size_advice"):
            expect(key in summary, f"daily target charge-aware sizing summary missing {key}")
        for key in ("starting_capital", "current_compounded_capital", "realized_compounded_capital", "realized_profit_value", "suggested_next_seed_capital"):
            expect(key in perf, f"daily target performance missing {key}")
        pairs = out.get("pairs") or []
        if pairs:
            sample = pairs[0]
            for key in ("pair_id", "sell_symbol", "buy_symbol", "buy_target_exit_price", "expected_profit_value", "rotation_score", "llm_verdict", "llm_note"):
                expect(key in sample, f"daily target pair missing {key}")
            pair_id = int(sample.get("pair_id") or 0)
            expect(pair_id > 0, "daily target pair id invalid")
            _, updated = req(
                "PUT",
                f"/api/v1/daily-target/pairs/{pair_id}",
                {
                    "state": "sell_done",
                    "executed_sell_price": sample.get("sell_ref_price"),
                    "executed_sell_at": "2026-04-21",
                    "executed_buy_price": sample.get("buy_ref_price"),
                    "executed_buy_at": "2026-04-21",
                    "note": "smoke",
                },
                expected=200,
            )
            updated_pairs = updated.get("pairs") or []
            match = next((x for x in updated_pairs if int(x.get("pair_id") or 0) == pair_id), None)
            expect(match is not None and str(match.get("state") or "").lower() == "executed", "daily target pair update failed")
            perf_after = updated.get("performance") or {}
            expect("live_mtm_basis_value" in perf_after, "daily target performance missing net live mtm basis")
            plan_id = int((updated.get("plan") or {}).get("id") or 0)
            reverse_pair = {
                "priority_rank": 99,
                "sell_symbol": sample.get("buy_symbol"),
                "sell_qty": sample.get("buy_qty"),
                "sell_ref_price": float(sample.get("buy_ref_price") or 0) + 10,
                "sell_trade_value": float(sample.get("buy_qty") or 0) * (float(sample.get("buy_ref_price") or 0) + 10),
                "sell_target_price": float(sample.get("buy_ref_price") or 0) + 10,
                "sell_score": 1,
                "sell_reason": "smoke full cycle exit",
                "buy_symbol": sample.get("sell_symbol"),
                "buy_qty": sample.get("sell_qty"),
                "buy_ref_price": sample.get("sell_ref_price"),
                "buy_trade_value": sample.get("sell_trade_value"),
                "buy_target_exit_price": float(sample.get("sell_ref_price") or 0) + 10,
                "buy_score": 1,
                "buy_reason": "smoke reverse leg",
                "expected_profit_value": 10,
                "rotation_score": 1,
            }
            with app.db_connect() as conn:
                reverse_pair_id = app._insert_daily_target_pair(conn, plan_id, reverse_pair, state="pending")
                conn.commit()
            _, cycle_payload = req(
                "PUT",
                f"/api/v1/daily-target/pairs/{reverse_pair_id}",
                {
                    "state": "sell_done",
                    "executed_sell_price": reverse_pair["sell_ref_price"],
                    "executed_sell_at": "2026-04-22",
                    "executed_buy_price": reverse_pair["buy_ref_price"],
                    "executed_buy_at": "2026-04-22",
                    "note": "reverse smoke",
                },
                expected=200,
            )
            cycles = cycle_payload.get("full_cycles") or []
            expect(len(cycles) <= 10, "daily target full cycle list not capped to latest 10")
            closed_cycle = next((c for c in cycles if c.get("symbol") == sample.get("buy_symbol")), None)
            expect(closed_cycle is not None, "daily target full cycle row missing closed scrip")
            expect("full cycle complete" in str(closed_cycle.get("comment") or "").lower(), "daily target full cycle comment missing")
        _, hist = req("GET", "/api/v1/daily-target/history?limit=50", expected=200)
        expect("items" in hist and "summary" in hist, "daily target history payload incomplete")
        if hist.get("items"):
            expect("current_buy_value" in hist["items"][0], "daily target history missing current buy value")
        _, hist_filtered = req("GET", "/api/v1/daily-target/history?limit=20&state=closed&date_from=2026-01-01&date_to=2026-12-31", expected=200)
        hist_summary = hist_filtered.get("summary") or {}
        expect(hist_summary.get("state_filter") == "closed", "daily target history state filter not echoed")
        expect(hist_summary.get("date_from") == "2026-01-01", "daily target history from-date filter not echoed")
        expect(hist_summary.get("date_to") == "2026-12-31", "daily target history to-date filter not echoed")
        _, llm_out = req("GET", "/api/v1/daily-target/plan?seed_capital=10000&target_profit_pct=1&top_n=3&recalibrate=0&use_hosted_llm=1", expected=200)
        review = llm_out.get("llm_review") or {}
        expect(review.get("status") == "disabled", "daily target hosted llm disabled review should be reported")

    check("daily_target_plan", daily_target_plan_runtime)
    def daily_target_mixed_bucket_tax_runtime():
        with app.db_connect() as conn:
            sym = "SMKMIXED"
            clear_symbol(conn, sym)
            upsert_symbol(conn, sym, qty=20, avg_cost=130, ltp=130)
            insert_trade(conn, sym, "BUY", "2024-01-01", 10, 100, notes="ltcg-gain-lot")
            insert_trade(conn, sym, "BUY", "2026-03-01", 10, 160, notes="stcg-loss-lot")
            conn.commit()
            tax = app._daily_target_estimate_sell_tax_profile(
                conn,
                sym,
                20,
                130,
                as_of_date="2026-04-23",
                realized_tax_summary={
                    "fy_label": "FY26",
                    "stcg_net_gain": 0.0,
                    "ltcg_net_gain": 0.0,
                    "ltcg_remaining_exemption": 0.0,
                },
            )
        expect(abs(float(tax.get("tax_payable") or 0.0) - 37.5) < 0.01, f"mixed bucket payable mismatch: {tax}")
        expect(abs(float(tax.get("tax_relief") or 0.0) - 37.5) < 0.01, f"mixed bucket relief mismatch: {tax}")
        expect(abs(float(tax.get("tax_drag") or 0.0)) < 0.01, f"mixed bucket drag should net to zero: {tax}")
        expect(str(tax.get("tax_bucket_mix") or "") == "MIXED", f"mixed bucket label mismatch: {tax}")

    check("daily_target_mixed_bucket_tax", daily_target_mixed_bucket_tax_runtime)
    def daily_target_batch_gain_pool_runtime():
        sell_syms = ("SMKLSA", "SMKLSB")
        buy_syms = ("SMKBYA", "SMKBYB")
        orig_realized = app.compute_realized_equity_tax_summary
        orig_strategy = app.latest_strategy_recommendation_map
        try:
            with app.db_connect() as conn:
                for sym in sell_syms + buy_syms:
                    clear_symbol(conn, sym)
                for sym in sell_syms:
                    upsert_symbol(conn, sym, qty=10, avg_cost=100, ltp=90)
                    insert_trade(conn, sym, "BUY", "2026-03-01", 10, 100, notes="stcg-loss-lot")
                for sym in buy_syms:
                    upsert_symbol(conn, sym, qty=10, avg_cost=100, ltp=100)
                    insert_trade(conn, sym, "BUY", "2025-12-01", 10, 100, notes="buy-candidate")
                conn.commit()

                def fake_realized(_conn, as_of_date=None):
                    return {
                        "fy_label": "FY26",
                        "fy_start_date": "2025-04-01",
                        "fy_end_date": "2026-03-31",
                        "stcg_net_gain": 100.0,
                        "ltcg_net_gain": 0.0,
                        "ltcg_remaining_exemption": 0.0,
                    }

                def fake_strategy(_conn):
                    return {
                        "SMKLSA": {"action": "TRIM"},
                        "SMKLSB": {"action": "TRIM"},
                        "SMKBYA": {"action": "ADD"},
                        "SMKBYB": {"action": "ADD"},
                    }

                app.compute_realized_equity_tax_summary = fake_realized
                app.latest_strategy_recommendation_map = fake_strategy
                out = app.build_daily_target_suggestions(conn, seed_capital=1000, target_profit_pct=1.0, top_n=2)
        finally:
            app.compute_realized_equity_tax_summary = orig_realized
            app.latest_strategy_recommendation_map = orig_strategy

        pairs = out.get("pairs") or []
        expect(len(pairs) >= 2, f"expected at least 2 pairs for gain-pool depletion test, got {pairs}")
        selected = [x for x in pairs if str(x.get("sell_symbol") or "") in sell_syms]
        expect(len(selected) == 2, f"expected both sell symbols to be selected, got {pairs}")
        relief_by_symbol = {str(x.get("sell_symbol")): round(float(x.get("sell_tax_relief") or 0.0), 2) for x in selected}
        total_relief = round(sum(relief_by_symbol.values()), 2)
        expect(total_relief <= 20.01, f"batch tax relief reused FY gain pool: {relief_by_symbol}")
        expect(any(v >= 19.99 for v in relief_by_symbol.values()), f"first pair did not use FY STCG pool: {relief_by_symbol}")
        expect(any(abs(v) <= 0.01 for v in relief_by_symbol.values()), f"second pair should have near-zero relief after pool depletion: {relief_by_symbol}")

    check("daily_target_batch_gain_pool", daily_target_batch_gain_pool_runtime)
    def attention_console_runtime():
        _, out = req("GET", "/api/v1/attention", expected=200)
        summary = out.get("summary") or {}
        tax = out.get("tax_profile") or {}
        for key in ("open_count", "latest_tax_sync_status"):
            expect(key in summary, f"attention summary missing {key}")
        for key in ("stcg_rate_pct", "ltcg_rate_pct", "ltcg_exemption_limit", "remaining_ltcg_exemption"):
            expect(key in tax, f"attention tax profile missing {key}")
        expect("open_alerts" in out and "tax_sync_runs" in out, "attention payload missing tables")
        _, agents = req("GET", "/api/v1/agents/status", expected=200)
        expect(any(str(x.get("agent") or "") == "tax_monitor" for x in (agents.get("items") or [])), "tax monitor agent missing from agent status")

    check("attention_console", attention_console_runtime)
    def harvest_plan_runtime():
        _, out = req("GET", "/api/v1/harvest/plan?target_loss=250", expected=200)
        summary = out.get("summary") or {}
        for key in (
            "total_loss_available_stcg",
            "total_loss_available_ltcg",
            "total_profit_available_stcg",
            "total_profit_available_ltcg",
            "suggested_harvest_loss_stcg",
            "suggested_harvest_loss_ltcg",
            "suggested_offset_profit_stcg",
            "suggested_offset_profit_ltcg",
        ):
            expect(key in summary, f"harvest plan summary missing {key}")
        sample = None
        for row in (out.get("harvest_candidates") or []) + (out.get("profit_offset_candidates") or []):
            sample = row
            break
        if sample is not None:
            expect("tax_bucket" in sample, "harvest row missing tax_bucket")
            expect("held_days_min" in sample, "harvest row missing held_days_min")
            expect("buy_date" in sample, "harvest row missing buy_date")

    check("harvest_plan", harvest_plan_runtime)
    def loss_lots_runtime():
        _, out = req("GET", "/api/v1/loss-lots", expected=200)
        summary = out.get("summary") or {}
        for key in (
            "total_loss_lots",
            "total_loss_qty",
            "total_loss_available",
            "stcg_loss_lots",
            "ltcg_loss_lots",
            "total_profit_lots",
            "total_profit_qty",
            "total_profit_available",
            "stcg_profit_lots",
            "ltcg_profit_lots",
        ):
            expect(key in summary, f"loss lots summary missing {key}")
        for bucket in ("stcg_items", "ltcg_items", "stcg_profit_items", "ltcg_profit_items"):
            rows = out.get(bucket) or []
            expect(len(rows) <= 10, f"{bucket} exceeds top-10 limit")
            if rows:
                expect("buy_date" in rows[0], f"{bucket} row missing buy_date")
                if "profit" in bucket:
                    expect("profit_available" in rows[0], f"{bucket} row missing profit_available")
                else:
                    expect("loss_available" in rows[0], f"{bucket} row missing loss_available")

    check("loss_lots", loss_lots_runtime)
    check("harvest_plan_dynamic_analysis", lambda: req("GET", "/api/v1/harvest/plan?target_loss=250&run_analysis=1", expected=200))
    check("summary", lambda: req("GET", "/api/v1/portfolio/summary", expected=200))
    check("performance", lambda: req("GET", "/api/v1/portfolio/performance?basis=1y", expected=200))
    check("timeseries", lambda: req("GET", "/api/v1/portfolio/timeseries", expected=200))
    check("scrips_list", lambda: req("GET", "/api/v1/scrips", expected=200))
    def scrips_contract_shape():
        _, out = req("GET", "/api/v1/scrips", expected=200)
        items = out.get("items") or []
        expect(len(items) > 0, "scrips list is empty")
        row = items[0]
        for key in ("symbol", "qty", "ltp", "day_pnl", "day_change_pct", "trade_count", "strategy_action"):
            expect(key in row, f"scrip row missing key: {key}")

    check("scrips_contract_shape", scrips_contract_shape)
    check("scrip_detail", lambda: req("GET", "/api/v1/scrips/KITEX", expected=200))
    check("scrip_trades", lambda: req("GET", "/api/v1/scrips/KITEX/trades", expected=200))
    check("scrip_perf", lambda: req("GET", "/api/v1/scrips/KITEX/performance?basis=1y", expected=200))
    check("sell_sim", lambda: req("POST", "/api/v1/scrips/KITEX/sell-simulate", {"quantity": 3}, expected=200))
    check("peak_diff", lambda: req("GET", "/api/v1/analytics/peak-diff", expected=200))
    check("strategy_sets", lambda: req("GET", "/api/v1/strategy/sets", expected=200))
    check("strategy_set_active_invalid_id", lambda: req("PUT", "/api/v1/strategy/sets/active", {"id": "x"}, expected=400))
    check("strategy_set_active_not_found", lambda: req("PUT", "/api/v1/strategy/sets/active", {"id": 9999}, expected=404))
    check("strategy_refresh", lambda: req("POST", "/api/v1/strategy/refresh", {}, expected=200))
    check("strategy_insights", lambda: req("GET", "/api/v1/strategy/insights", expected=200))
    def strategy_audit_runtime():
        _, out = req("POST", "/api/v1/strategy/audits/run", {"refresh_strategy": False}, expected=200)
        expect("overall_status" in out and "overall_score" in out, "strategy audit run missing status/score")
        expect("findings" in out and "stats" in out, "strategy audit run missing findings/stats")
        _, hist = req("GET", "/api/v1/strategy/audits?limit=10", expected=200)
        expect("items" in hist and "latest" in hist, "strategy audit list payload incomplete")
        latest = hist.get("latest") or {}
        if latest:
            expect("findings" in latest, "strategy audit latest payload missing findings")
            expect(str(latest.get("audit_mode") or "") == "heuristic", "strategy audit mode mismatch")

    check("strategy_audit", strategy_audit_runtime)
    def hosted_free_llm_runtime():
        _, cfg = req("GET", "/api/v1/hosted-llm/config", expected=200)
        expect("providers" in cfg and "enabled" in cfg, "hosted llm config payload incomplete")
        for provider in cfg.get("providers") or []:
            key_val = str(provider.get("api_key") or "")
            expect((not key_val) or ("****" in key_val) or ("..." in key_val), "hosted llm config exposed raw key")
        _, saved = req(
            "POST",
            "/api/v1/hosted-llm/config",
            {
                "enabled": False,
                "provider_order": "openrouter,groq,huggingface",
                "timeout_sec": 15,
                "providers": [
                    {"provider": "openrouter", "model": "openrouter/free"},
                    {"provider": "groq", "model": "llama-3.1-8b-instant"},
                    {"provider": "huggingface", "model": "Qwen/Qwen2.5-7B-Instruct"},
                ],
            },
            expected=200,
        )
        expect(saved.get("enabled") is False, "hosted llm disabled config did not persist")
        _, test = req("POST", "/api/v1/hosted-llm/test", {}, expected=200)
        expect(test.get("ok") is False and test.get("status") == "disabled", "disabled hosted llm test should fail gracefully")
        _, metrics = req("GET", "/api/v1/hosted-llm/metrics?limit=20", expected=200)
        expect("summary" in metrics and "providers" in metrics and "items" in metrics, "hosted llm metrics payload incomplete")
        summary = metrics.get("summary") or {}
        for key in ("total_attempts", "ok_attempts", "error_attempts", "skipped_attempts", "success_rate_pct", "avg_ok_latency_ms"):
            expect(key in summary, f"hosted llm metrics summary missing {key}")

    check("hosted_free_llm", hosted_free_llm_runtime)

    check(
        "intel_doc_analyze",
        lambda: req(
            "POST",
            "/api/v1/intel/docs",
            {
                "doc_type": "policy",
                "source": "budget_note",
                "source_ref": "union_budget",
                "doc_date": "2025-07-23",
                "title": "Budget capex push",
                "content": "Budget announces strong capex growth and infra support. KITEX and HDFCBANK outlook looks resilient.",
                "run_strategy": True,
            },
            expected=200,
        ),
    )
    check("intel_summary", lambda: req("GET", "/api/v1/intel/summary?limit=50", expected=200))
    check(
        "intel_financial_add",
        lambda: req(
            "POST",
            "/api/v1/intel/financials",
            {
                "symbol": "KITEX",
                "fiscal_period": "FY25-Q4",
                "report_date": "2025-03-31",
                "revenue": 1200,
                "pat": 110,
                "operating_cash_flow": 140,
                "investing_cash_flow": -80,
                "financing_cash_flow": 30,
                "debt": 220,
                "fii_holding_pct": 3.2,
                "dii_holding_pct": 6.1,
                "promoter_holding_pct": 54.0,
                "source": "smoke",
                "run_strategy": True,
            },
            expected=200,
        ),
    )
    check(
        "strategy_params_set_missing",
        lambda: req(
            "PUT",
            "/api/v1/strategy/sets/99999/parameters",
            {"parameters": [{"key": "buy_l1_discount", "value": 0.1}]},
            expected=404,
        ),
    )
    check(
        "split_add",
        lambda: req(
            "POST",
            "/api/v1/corporate-actions/splits",
            {"symbol": "KITEX", "effective_date": "2025-01-01", "factor": 2.0, "note": "2:1 split"},
            expected=200,
        ),
    )

    def split_flow():
        _, out = req("GET", "/api/v1/corporate-actions/splits?symbol=KITEX", expected=200)
        sid = out["items"][0]["id"]
        req("PUT", f"/api/v1/analytics/peak-splits/{sid}/review", {"decision": "apply"}, expected=200)
        req("DELETE", f"/api/v1/corporate-actions/splits/{sid}", expected=200)

    check("split_review_delete", split_flow)
    check("split_delete_invalid", lambda: req("DELETE", "/api/v1/corporate-actions/splits/notanumber", expected=400))
    check("split_delete_missing", lambda: req("DELETE", "/api/v1/corporate-actions/splits/99999", expected=404))
    check(
        "history_backfill_bad_runtime",
        lambda: req("POST", "/api/v1/prices/history/backfill", {"full": False, "max_runtime_sec": "abc"}, expected=400),
    )
    check("assistant_help", lambda: req("POST", "/api/v1/assistant/chat", {"message": "help"}, expected=200))
    check(
        "assistant_preview",
        lambda: req("POST", "/api/v1/assistant/chat", {"message": 'preview notes like \"upload:tb1.xlsx\"'}, expected=200),
    )
    def assistant_gold_query_permutations():
        variants = [
            "refresh gold price",
            "gold ltp",
            "update 24 carat gold rate",
            "refresh 24k",
            "gold price not supported in assistant",
        ]
        for msg in variants:
            _, out = req("POST", "/api/v1/assistant/chat", {"message": msg}, expected=200)
            expect(out.get("intent") == "gold_price_status", f"gold query not routed: {msg}")

    check("assistant_gold_query_permutations", assistant_gold_query_permutations)

    def approval_flow():
        _, out = req("POST", "/api/v1/assistant/chat", {"message": 'erase trades notes like \"upload:tb1.xlsx\"'}, expected=200)
        aid = out["approval"]["id"]
        req("GET", "/api/v1/assistant/approvals?status=pending&limit=20", expected=200)
        req("POST", f"/api/v1/assistant/approvals/{aid}/decision", {"decision": "approve"}, expected=200)

    check("assistant_approval_flow", approval_flow)
    check(
        "agent_backtest_run",
        lambda: req(
            "POST",
            "/api/v1/agents/backtest/run",
            {
                "from_date": "2025-01-01",
                "to_date": "2025-12-31",
                "horizon_days": 20,
                "apply_tuning": True,
                "fix_data_pipes": False,
                "min_samples": 5,
            },
            expected=200,
        ),
    )
    check("agent_backtest_history", lambda: req("GET", "/api/v1/agents/backtest/history?limit=10", expected=200))
    check(
        "self_learning_control",
        lambda: req(
            "PUT",
            "/api/v1/agents/self_learning/control",
            {"enabled": True, "interval_seconds": 86400, "min_samples": 10},
            expected=200,
        ),
    )
    check(
        "intel_autopilot_control",
        lambda: req(
            "PUT",
            "/api/v1/agents/intel_autopilot/control",
            {"enabled": True, "interval_seconds": 43200, "max_docs": 8, "symbols_limit": 10},
            expected=200,
        ),
    )
    check(
        "chart_agent_control",
        lambda: req(
            "PUT",
            "/api/v1/agents/chart_intel/control",
            {"enabled": True, "interval_seconds": 21600, "sources": ["market_history", "tradingview_scan"]},
            expected=200,
        ),
    )
    def bulk_delete_flow():
        req("POST", "/api/v1/scrips/bulk-delete", {"symbols": ["HDFCBANK"]}, expected=200)
        _, out = req("GET", "/api/v1/scrips", expected=200)
        symbols = {str(i.get("symbol", "")).upper() for i in (out.get("items") or [])}
        expect("HDFCBANK" not in symbols, "HDFCBANK still visible after bulk delete")

    def single_delete_flow():
        req("DELETE", "/api/v1/scrips/KITEX", expected=200)
        _, out = req("GET", "/api/v1/scrips", expected=200)
        symbols = {str(i.get("symbol", "")).upper() for i in (out.get("items") or [])}
        expect("KITEX" not in symbols, "KITEX still visible after single delete")

    check("bulk_delete", bulk_delete_flow)
    check("single_delete", single_delete_flow)

    server.shutdown()
    th.join(timeout=2)

    fails = [c for c in checks if c[1] == "FAIL"]
    print(f"TOTAL={len(checks)} FAIL={len(fails)}")
    for name, status, msg in checks:
        if status == "PASS":
            print(f"PASS: {name}")
        else:
            print(f"FAIL: {name} -> {msg}")
    if fails:
        raise SystemExit(1)


if __name__ == "__main__":
    run()
