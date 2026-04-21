PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS instruments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  exchange TEXT NOT NULL DEFAULT 'NSE',
  symbol TEXT NOT NULL UNIQUE,
  name TEXT,
  isin TEXT,
  feed_code TEXT,
  price_source TEXT NOT NULL DEFAULT 'exchange_api',
  asset_class TEXT NOT NULL DEFAULT 'EQUITY',
  active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL,
  side TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
  trade_date TEXT NOT NULL,
  quantity REAL NOT NULL,
  price REAL NOT NULL,
  amount REAL NOT NULL,
  external_trade_id TEXT,
  source TEXT NOT NULL DEFAULT 'excel',
  notes TEXT
);
CREATE INDEX IF NOT EXISTS ix_trades_symbol_date ON trades(symbol, trade_date);

CREATE TABLE IF NOT EXISTS latest_prices (
  symbol TEXT PRIMARY KEY,
  ltp REAL NOT NULL,
  change_abs REAL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS holdings (
  symbol TEXT PRIMARY KEY,
  qty REAL NOT NULL,
  avg_cost REAL NOT NULL,
  invested REAL NOT NULL,
  market_value REAL NOT NULL,
  realized_pnl REAL NOT NULL,
  unrealized_pnl REAL NOT NULL,
  total_return_pct REAL NOT NULL,
  updated_at TEXT NOT NULL
);


CREATE TABLE IF NOT EXISTS scrip_position_guards (
  symbol TEXT PRIMARY KEY,
  min_value REAL NOT NULL DEFAULT 0,
  max_value REAL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_scrip_position_guards_updated ON scrip_position_guards(updated_at DESC);


CREATE TABLE IF NOT EXISTS rebalance_lots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  side TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
  percent REAL NOT NULL,
  allocation_basis TEXT NOT NULL DEFAULT 'portfolio_market_value',
  target_trade_value REAL NOT NULL DEFAULT 0,
  total_current_market_value REAL NOT NULL DEFAULT 0,
  status TEXT NOT NULL CHECK (status IN ('active','completed','reset')) DEFAULT 'active',
  created_at TEXT NOT NULL,
  completed_at TEXT,
  reset_at TEXT
);
CREATE INDEX IF NOT EXISTS ix_rebalance_lots_status_created ON rebalance_lots(status, created_at DESC);

CREATE TABLE IF NOT EXISTS rebalance_lot_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  lot_id INTEGER NOT NULL REFERENCES rebalance_lots(id) ON DELETE CASCADE,
  symbol TEXT NOT NULL,
  planned_qty REAL NOT NULL DEFAULT 0,
  planned_trade_value REAL NOT NULL DEFAULT 0,
  ltp_at_lock REAL NOT NULL DEFAULT 0,
  market_value_at_lock REAL NOT NULL DEFAULT 0,
  min_value_at_lock REAL NOT NULL DEFAULT 0,
  max_value_at_lock REAL,
  note TEXT,
  completed INTEGER NOT NULL DEFAULT 0,
  completed_at TEXT,
  completion_note TEXT,
  execution_state TEXT NOT NULL DEFAULT 'pending',
  executed_price REAL,
  executed_at TEXT,
  buyback_completed INTEGER NOT NULL DEFAULT 0,
  buyback_completed_at TEXT,
  buyback_price REAL,
  buyback_note TEXT
);
CREATE INDEX IF NOT EXISTS ix_rebalance_lot_items_lot ON rebalance_lot_items(lot_id, id);
CREATE INDEX IF NOT EXISTS ix_rebalance_lot_items_symbol ON rebalance_lot_items(symbol);

CREATE TABLE IF NOT EXISTS lot_closures (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL,
  close_date TEXT NOT NULL,
  qty_closed REAL NOT NULL,
  buy_price REAL NOT NULL,
  sell_price REAL NOT NULL,
  realized_pnl REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_lot_closures_symbol_date ON lot_closures(symbol, close_date);

CREATE TABLE IF NOT EXISTS cash_ledger (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entry_date TEXT NOT NULL,
  entry_type TEXT NOT NULL,
  amount REAL NOT NULL,
  reference_text TEXT,
  external_entry_id TEXT,
  source TEXT NOT NULL DEFAULT 'cashflow_upload'
);
CREATE INDEX IF NOT EXISTS ix_cash_ledger_entry_date ON cash_ledger(entry_date);

CREATE TABLE IF NOT EXISTS dividends (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL,
  entry_date TEXT NOT NULL,
  amount REAL NOT NULL,
  reference_text TEXT,
  external_entry_id TEXT,
  source TEXT NOT NULL DEFAULT 'dividend_upload',
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_dividends_symbol_date ON dividends(symbol, entry_date DESC);
CREATE INDEX IF NOT EXISTS ix_dividends_date ON dividends(entry_date DESC);
CREATE INDEX IF NOT EXISTS ix_dividends_external_entry_id ON dividends(external_entry_id);

CREATE TABLE IF NOT EXISTS strategy_sets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  is_active INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS strategy_parameters (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  set_id INTEGER NOT NULL REFERENCES strategy_sets(id) ON DELETE CASCADE,
  key TEXT NOT NULL,
  value REAL NOT NULL,
  UNIQUE(set_id, key)
);

CREATE TABLE IF NOT EXISTS signals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL,
  signal_date TEXT NOT NULL,
  buy_signal TEXT,
  sell_signal TEXT,
  score REAL NOT NULL DEFAULT 0,
  reason TEXT
);

CREATE TABLE IF NOT EXISTS corporate_actions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL,
  action_type TEXT NOT NULL CHECK (action_type IN ('SPLIT')),
  effective_date TEXT NOT NULL,
  factor REAL NOT NULL,
  note TEXT,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_corp_actions_symbol_date ON corporate_actions(symbol, effective_date);

CREATE TABLE IF NOT EXISTS peak_split_reviews (
  corporate_action_id INTEGER PRIMARY KEY,
  decision TEXT NOT NULL CHECK (decision IN ('apply','ignore')),
  decided_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_peak_split_reviews_decision ON peak_split_reviews(decision);

CREATE TABLE IF NOT EXISTS app_config (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_approvals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending','approved','rejected','executed','expired')),
  action_type TEXT NOT NULL,
  query_text TEXT,
  payload_json TEXT NOT NULL,
  summary TEXT,
  decided_at TEXT,
  executed_at TEXT,
  decision_note TEXT
);
CREATE INDEX IF NOT EXISTS ix_agent_approvals_status_created ON agent_approvals(status, created_at DESC);

CREATE TABLE IF NOT EXISTS price_ticks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL,
  ltp REAL NOT NULL,
  change_abs REAL,
  fetched_at TEXT NOT NULL,
  source TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_price_ticks_symbol_time ON price_ticks(symbol, fetched_at DESC);

CREATE TABLE IF NOT EXISTS quote_samples (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL,
  source TEXT NOT NULL,
  ltp REAL NOT NULL,
  change_abs REAL,
  latency_ms REAL,
  accuracy_error_pct REAL,
  fetched_at TEXT NOT NULL,
  selected INTEGER NOT NULL DEFAULT 0,
  consensus_ltp REAL
);
CREATE INDEX IF NOT EXISTS ix_quote_samples_symbol_time ON quote_samples(symbol, fetched_at DESC);
CREATE INDEX IF NOT EXISTS ix_quote_samples_source_time ON quote_samples(source, fetched_at DESC);

CREATE TABLE IF NOT EXISTS quote_source_registry (
  source TEXT PRIMARY KEY,
  adapter TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS quote_source_stats (
  source TEXT PRIMARY KEY,
  attempts INTEGER NOT NULL DEFAULT 0,
  successes INTEGER NOT NULL DEFAULT 0,
  failures INTEGER NOT NULL DEFAULT 0,
  total_latency_ms REAL NOT NULL DEFAULT 0,
  total_accuracy_error_pct REAL NOT NULL DEFAULT 0,
  accuracy_samples INTEGER NOT NULL DEFAULT 0,
  score REAL NOT NULL DEFAULT 0,
  last_success_at TEXT,
  last_error_at TEXT,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_quote_source_stats_score ON quote_source_stats(score DESC);

CREATE TABLE IF NOT EXISTS strategy_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_date TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL,
  market_value REAL NOT NULL,
  invested_value REAL NOT NULL,
  cash_balance REAL NOT NULL,
  projected_start_value REAL NOT NULL,
  macro_regime TEXT NOT NULL DEFAULT 'neutral',
  macro_score REAL NOT NULL DEFAULT 0,
  macro_confidence REAL NOT NULL DEFAULT 0,
  macro_thought TEXT,
  intel_score REAL NOT NULL DEFAULT 0,
  intel_confidence REAL NOT NULL DEFAULT 0,
  intel_thought TEXT,
  add_count INTEGER NOT NULL DEFAULT 0,
  trim_count INTEGER NOT NULL DEFAULT 0,
  hold_count INTEGER NOT NULL DEFAULT 0,
  review_count INTEGER NOT NULL DEFAULT 0,
  watch_add_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS strategy_recommendations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_date TEXT NOT NULL,
  symbol TEXT NOT NULL,
  action TEXT NOT NULL,
  priority INTEGER NOT NULL DEFAULT 0,
  weight_current REAL NOT NULL DEFAULT 0,
  weight_target REAL NOT NULL DEFAULT 0,
  delta_weight REAL NOT NULL DEFAULT 0,
  confidence REAL NOT NULL DEFAULT 0,
  price_now REAL NOT NULL DEFAULT 0,
  buy_price_1 REAL,
  buy_price_2 REAL,
  sell_price_1 REAL,
  sell_price_2 REAL,
  expected_annual_return REAL NOT NULL DEFAULT 0,
  intel_score REAL NOT NULL DEFAULT 0,
  intel_confidence REAL NOT NULL DEFAULT 0,
  intel_summary TEXT,
  reason TEXT,
  source TEXT NOT NULL DEFAULT 'rotation_engine'
);
CREATE INDEX IF NOT EXISTS ix_strategy_reco_run ON strategy_recommendations(run_date, priority DESC);
CREATE INDEX IF NOT EXISTS ix_strategy_reco_symbol ON strategy_recommendations(symbol);

CREATE TABLE IF NOT EXISTS strategy_projection_points (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_date TEXT NOT NULL,
  scenario TEXT NOT NULL,
  year_offset INTEGER NOT NULL,
  annual_return REAL NOT NULL,
  projected_value REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_strategy_projection_run ON strategy_projection_points(run_date, scenario, year_offset);

CREATE TABLE IF NOT EXISTS agent_backtest_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL,
  from_date TEXT NOT NULL,
  to_date TEXT NOT NULL,
  horizon_days INTEGER NOT NULL,
  sample_count INTEGER NOT NULL DEFAULT 0,
  hit_rate REAL NOT NULL DEFAULT 0,
  avg_future_return REAL NOT NULL DEFAULT 0,
  momentum_hit_rate REAL NOT NULL DEFAULT 0,
  intel_hit_rate REAL NOT NULL DEFAULT 0,
  applied_tuning INTEGER NOT NULL DEFAULT 0,
  params_before_json TEXT,
  params_after_json TEXT,
  suggestions_json TEXT,
  diagnostics_json TEXT,
  errors_json TEXT
);
CREATE INDEX IF NOT EXISTS ix_agent_backtest_runs_created ON agent_backtest_runs(created_at DESC);

CREATE TABLE IF NOT EXISTS intelligence_documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_type TEXT NOT NULL,
  source TEXT,
  source_ref TEXT,
  doc_date TEXT NOT NULL,
  title TEXT,
  content TEXT NOT NULL,
  sentiment_score REAL NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_intel_docs_date ON intelligence_documents(doc_date DESC, id DESC);
CREATE INDEX IF NOT EXISTS ix_intel_docs_type ON intelligence_documents(doc_type, doc_date DESC);

CREATE TABLE IF NOT EXISTS intelligence_impacts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id INTEGER NOT NULL REFERENCES intelligence_documents(id) ON DELETE CASCADE,
  symbol TEXT NOT NULL,
  impact_score REAL NOT NULL,
  confidence REAL NOT NULL DEFAULT 0.5,
  reason TEXT,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_intel_impacts_symbol_time ON intelligence_impacts(symbol, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_intel_impacts_doc ON intelligence_impacts(doc_id);

CREATE TABLE IF NOT EXISTS chart_analysis_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL,
  as_of_date TEXT NOT NULL,
  score REAL NOT NULL DEFAULT 0,
  confidence REAL NOT NULL DEFAULT 0,
  signal TEXT NOT NULL DEFAULT 'NEUTRAL',
  trend_score REAL NOT NULL DEFAULT 0,
  momentum_score REAL NOT NULL DEFAULT 0,
  mean_reversion_score REAL NOT NULL DEFAULT 0,
  breakout_score REAL NOT NULL DEFAULT 0,
  relative_strength_score REAL NOT NULL DEFAULT 0,
  index_correlation REAL NOT NULL DEFAULT 0,
  source_summary TEXT,
  pattern_flags_json TEXT,
  details_json TEXT,
  created_at TEXT NOT NULL,
  UNIQUE(symbol, as_of_date)
);
CREATE INDEX IF NOT EXISTS ix_chart_snapshots_symbol_time ON chart_analysis_snapshots(symbol, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_chart_snapshots_date ON chart_analysis_snapshots(as_of_date DESC, created_at DESC);

CREATE TABLE IF NOT EXISTS company_financials (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL,
  fiscal_period TEXT NOT NULL,
  report_date TEXT NOT NULL,
  revenue REAL,
  pat REAL,
  operating_cash_flow REAL,
  investing_cash_flow REAL,
  financing_cash_flow REAL,
  debt REAL,
  fii_holding_pct REAL,
  dii_holding_pct REAL,
  promoter_holding_pct REAL,
  source TEXT NOT NULL DEFAULT 'manual',
  notes TEXT,
  created_at TEXT NOT NULL,
  UNIQUE(symbol, fiscal_period, source)
);
CREATE INDEX IF NOT EXISTS ix_company_fin_symbol_date ON company_financials(symbol, report_date DESC, id DESC);
