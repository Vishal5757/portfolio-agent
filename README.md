# Portfolio Agent

Full-stack local portfolio analytics app:

- Stores normalized data in SQLite
- Stores historical daily market prices in a separate SQLite DB (`data/market_history.db`) for chart valuation
- Serves REST APIs
- Hosts a web UI for dashboard, per-scrip history, and performance views
- Live refreshes current prices from NSE/BSE APIs on configurable interval (default 10s)
- Auto-syncs historical daily prices (including symbols with current qty=0 but past trades) via dedicated history worker
- Supports stock split adjustments and recalculates analytics
- Peak-buy split adjustments are confirmation-gated: applicable split-years are detected from trade dates and must be manually marked `Apply`/`Ignore` from UI
- Shows `% diff from highest buy price` per scrip
- Allows adding new scrips and deleting full scrip history/data
- Supports UI upload of broker tradebook Excel (`Symbol/Trade Date/Trade Type/Quantity/Price`) with duplicate skipping
- Trade upload returns skipped-row details and supports per-row override add from UI with confirmation
- Supports dedicated UI upload for payin/payout cashflow Excel (append with dedupe, or replace-all mode); cash balance is computed from this ledger
- Cashflow classifier maps ledger text: withdrawal requests/instant payouts -> `withdrawal`, net settlements -> `investment`/`trade_credit`, DP/AMC -> `charge`
- Supports raw Kite tradebook exports (extra metadata columns such as Exchange, Segment, Trade ID, Order ID)
- Duplicate suppression for uploads: `Trade ID` (or `Order ID`) when present; fallback to value/date/side matching when missing
- Cashflow duplicate suppression: `Entry ID` when present; fallback to date/type/amount matching across sources
- Live quote agent collects from multiple sources: `NSE`, `BSE`, `Yahoo Finance`, `Google`, `Screener`, `Trendlyne`, `CNBC`; applies outlier filtering and selects a validated LTP for dashboard/DB updates
- Symbols with `qty = 0` are excluded from intraday live refresh; they are refreshed at most once per day after EOD window (IST)
- Optional quote adapters supported: `nsetools_api` (Python `nsetools`) and `stock_nse_india_api` (REST service compatible with `hi-imcodeman/stock-nse-india`)
- Source ranking is dynamic: each source is scored continuously by reliability (success rate), speed (latency), and accuracy (deviation from consensus); top-ranked sources are used with controlled exploration for new/less-used sources
- Per-source quote samples are persisted in `quote_samples`; latest selected quote is written to `latest_prices` and `price_ticks`
- UI includes symbol suggestions (autocomplete), manual trade add/delete per scrip, and table filters for large datasets
- Delete scrip requires exact symbol selection from suggestions in UI
- Bulk delete scrips (multi-select) with full history purge
- Snapshot includes `Today's P/L` and `Today's Change %`
- Holdings include per-scrip `Day P/L`, `Day %`, `Abs P/L`, and `UPL %`
- Scrip Detail includes a sell-profit simulator: enter quantity (and optional sell price) to view matched buy lots and projected profit
- Snapshot chart supports interactive hover, mouse-wheel zoom, drag-pan, and quick range buttons (1M/3M/6M/1Y/All)
- Snapshot market-value timeseries uses separate historical-price store (daily close carry-forward with live end-day override)
- Strategy tab includes a rotation engine (`TRIM / ADD / HOLD / REVIEW`) with price points, rationale, and multi-year portfolio projection
- Strategy recommendations include macro thoughts/regime (risk-on/risk-off/neutral) derived from NSE index breadth
- Strategy engine focuses existing holdings and limits fresh-stock ideas to at most 1-2 symbols
- Intelligence autopilot can auto-collect financial context online from reliable sources (`Screener` snapshots, `NSE` corporate announcements, and company-site investor-relations links) when manual uploads are unavailable
- Software Performance Agent tracks runtime/data-pipe health, applies bounded self-healing, auto-tunes live quote knobs, and writes guarded improvement draft code to `data/agent_improvements/` without touching core objective paths
- Risk Analysis Agent computes portfolio risk snapshots (volatility, downside volatility, max drawdown, historical VaR/CVaR, cross-symbol correlation, and concentration/HHI) using robust outlier-capped returns
- Assistant tab supports natural-language actions and queries:
  - bulk trade erase by notes pattern (example: `erase trades notes like "upload:tradebook-OWY330.xlsx"`)
  - `portfolio summary`, `price status`, `upload summary`, `show duplicates`, `top gainers/losers`
  - `strategy summary`, `strategy projection`, `refresh strategy`
  - explanation queries like `how is cash balance calculated`, `explain unrealized pnl`, `formula for total return`
- Automatic SQLite backup is created on startup and then every 2 days in `data/backups/`
- Manual history backfill API: `POST /api/v1/prices/history/backfill` and status API: `GET /api/v1/prices/history/status`
- Per-source quote feed inspection API: `GET /api/v1/prices/sources?symbol=RELIANCE&limit=200`
- Source ranking API: `GET /api/v1/prices/source-ranking`
- Software-performance telemetry API: `GET /api/v1/agents/software-performance`
- Risk-analysis telemetry API: `GET /api/v1/agents/risk-analysis`
- Enable/disable any source dynamically: `PUT /api/v1/prices/source-ranking` with `{ "source": "yahoo_finance", "enabled": true }`
- Live-config API (`PUT /api/v1/config/live`) also supports ranking policy knobs: `quote_sources`, `quote_top_k`, `quote_explore_ratio`, `quote_max_deviation_pct`

## Quick Start

1. Run:

```powershell
python app.py
```

2. Open:

`http://127.0.0.1:8080`

## Code Structure

- `app.py`: compatibility entrypoint and API/server orchestration.
- `portfolio_agent/`: modular package for extracted domains.
  - `software_performance.py`: software-performance agent config, telemetry, self-heal loop, and improvement-draft logic.
  - `risk_analysis.py`: risk-analysis agent config, scheduled snapshot engine, and portfolio risk scoring.
- `web/`: frontend assets.
- `tools/`: smoke/integration checks.

Refactor direction: continue moving domain logic from `app.py` into `portfolio_agent/` modules (`market`, `strategy`, `intel`, `api`, `storage`) while keeping `app.py` as a thin compatibility shell.

## Import Model

- DB is the source of truth at runtime.

## Repository Data Policy

- `main` is kept focused on application code and lightweight assets.
- Local SQL snapshot exports live under `repo_data/`, but the heavy tenant dump files are intentionally not tracked on `main`.
- If you want a Git-backed archive of snapshot exports, use the dedicated `data-snapshots` branch rather than pushing large SQL dumps to `main`.
- Startup does not auto-read any Excel file.
- Use UI tradebook upload (`Upload Tradebook`) for all future trade updates.
- Optional one-time historical bootstrap from workbook:
- `--seed-xlsx` and `--xlsx-path` accept relative paths (resolved from project root).

```powershell
python app.py --seed-xlsx data/Portfolio.xlsx
```

## Notes

- Works with Python standard library + `openpyxl`.
- Optional: `pip install nsetools` to enable `nsetools_api`.
- Optional: set `STOCK_NSE_INDIA_API_BASE` (default `http://127.0.0.1:3000`) to enable `stock_nse_india_api`.
- DB file: `data/portfolio.db`.
- `/api/v1/sync/excel` is a replace import endpoint and requires `confirm_replace=true`.
- Exhaustive local smoke test (isolated temp DB + fake market client):
  `python tools/full_system_smoke_test.py`
- UI button binding contract test (guards all current/future `button[id]` controls):
  `python tools/button_contract_test.py`
