import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INDEX_HTML = ROOT / "web" / "index.html"
APP_JS = ROOT / "web" / "app.js"


def _extract_button_ids_from_html(text):
    ids = set()
    for m in re.finditer(r"<button\b[^>]*\bid\s*=\s*['\"]([^'\"]+)['\"][^>]*>", text, flags=re.IGNORECASE):
        bid = str(m.group(1) or "").strip()
        if bid:
            ids.add(bid)
    return ids


def _extract_registered_button_ids(js_text):
    ids = set()
    for m in re.finditer(r"registerButton\(\s*['\"]([^'\"]+)['\"]", js_text):
        bid = str(m.group(1) or "").strip()
        if bid:
            ids.add(bid)
    return ids


def run():
    html = INDEX_HTML.read_text(encoding="utf-8")
    js = APP_JS.read_text(encoding="utf-8")

    html_ids = _extract_button_ids_from_html(html)
    registered_ids = _extract_registered_button_ids(js)

    missing = sorted(x for x in html_ids if x not in registered_ids)
    stale = sorted(x for x in registered_ids if x not in html_ids)

    if missing:
        print("FAIL: unbound button IDs found in index.html")
        for x in missing:
            print(f"  - {x}")
    else:
        print(f"PASS: all {len(html_ids)} button IDs are bound via registerButton()")

    if stale:
        print("FAIL: registerButton IDs not found in index.html")
        for x in stale:
            print(f"  - {x}")
    else:
        print("PASS: no stale registerButton IDs")

    if "function verifyButtonBindings()" not in js:
        print("FAIL: verifyButtonBindings() is missing in app.js")
        raise SystemExit(1)
    print("PASS: verifyButtonBindings() exists")

    perf_contract_checks = [
        "async function refreshSoftwarePerfNow()",
        "btn.textContent = \"Refreshing...\"",
        "Software logs refreshing:",
        "registerButton(\n    \"refreshSoftwarePerfBtn\",\n    refreshSoftwarePerfNow,",
    ]
    missing_perf = [x for x in perf_contract_checks if x not in js]
    if missing_perf:
        print("FAIL: refresh software performance button UX contract missing")
        for x in missing_perf:
            print(f"  - {x}")
        raise SystemExit(1)
    print("PASS: refresh software performance button UX contract exists")

    rebalance_contract_checks = [
        "async function lockRebalanceLot()",
        "async function resetRebalanceLot()",
        "async function loadRebalanceClosedHistory(options = {})",
        "registerButton(\"rebalanceLockLotBtn\", lockRebalanceLot",
        "registerButton(\"rebalanceResetLotBtn\", resetRebalanceLot",
        "registerButton(\"rebalanceHistoryRefreshBtn\", () => loadRebalanceClosedHistory({ throwOnError: true })",
        "rebalance-complete-save-btn",
        "/api/v1/rebalance/lot/items/",
        "/api/v1/rebalance/closed-history",
        "/api/v1/rebalance/closed-history/items/",
        "rebalance-history-buyback-complete-btn",
    ]
    missing_rebalance = [x for x in rebalance_contract_checks if x not in js]
    if missing_rebalance:
        print("FAIL: rebalance lot lock/reset UX contract missing")
        for x in missing_rebalance:
            print(f"  - {x}")
        raise SystemExit(1)
    print("PASS: rebalance lot lock/reset UX contract exists")

    if missing or stale:
        raise SystemExit(1)


if __name__ == "__main__":
    run()
