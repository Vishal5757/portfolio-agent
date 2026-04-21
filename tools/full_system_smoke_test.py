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
    def harvest_planner_contract():
        py = (root / "app.py").read_text(encoding="utf-8")
        js = (root / "web" / "app.js").read_text(encoding="utf-8")
        html = (root / "web" / "index.html").read_text(encoding="utf-8")
        expect("def build_tax_harvest_plan(conn, target_loss=0.0, run_llm=False):" in py, "harvest planner backend missing")
        expect("def open_lot_tax_bucket_summary(" in py, "harvest planner tax-bucket lot helper missing")
        expect("def open_lot_tax_bucket_rows(" in py, "harvest planner lot-level helper missing")
        expect("def build_loss_lot_analysis(conn):" in py, "loss lot analysis backend missing")
        expect("/api/v1/loss-lots" in py, "loss lot analysis endpoint missing")
        expect("total_loss_available_stcg" in py and "suggested_offset_profit_ltcg" in py, "harvest planner summary lacks tax-bucket totals")
        expect("/api/v1/harvest/plan" in py, "harvest planner endpoint missing")
        expect("run_tax_harvest_llm_analysis" in py, "harvest planner llm hook missing")
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
    def llm_runtime_contract():
        py = (root / "app.py").read_text(encoding="utf-8")
        js = (root / "web" / "app.js").read_text(encoding="utf-8")
        html = (root / "web" / "index.html").read_text(encoding="utf-8")
        perf = (root / "portfolio_agent" / "software_performance.py").read_text(encoding="utf-8")
        expect("def get_llm_runtime_config(" in py, "llm runtime config helper missing")
        expect("/api/v1/llm/config" in py, "llm config endpoint missing")
        expect("/api/v1/llm/test" in py, "llm test endpoint missing")
        expect("function loadLlmConfig(options = {})" in js, "llm config ui loader missing")
        expect("saveLlmConfigBtn" in js and "testLlmConfigBtn" in js, "llm buttons not wired")
        expect('id="llmApiKeyInput"' in html, "llm api key input missing")
        expect("def _software_perf_generate_llm_proposal(" in perf, "software perf llm proposal generator missing")

    check("llm_runtime_contract", llm_runtime_contract)
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
    check("llm_config_get", lambda: req("GET", "/api/v1/llm/config", expected=200))
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
    check("harvest_plan_dynamic_analysis", lambda: req("GET", "/api/v1/harvest/plan?target_loss=250&run_llm=1", expected=200))
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
