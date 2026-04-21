import datetime as dt
import math
import statistics
import time


TRADING_DAYS_PER_YEAR = 252


def _core():
    import app  # local import keeps module decoupled from import order during startup

    return app


def _fallback_default_interval():
    core = _core()
    return int(getattr(core, "RISK_AGENT_INTERVAL_DEFAULT_SEC", 6 * 60 * 60))


def _fallback_min_interval():
    core = _core()
    return int(getattr(core, "RISK_AGENT_MIN_INTERVAL_SEC", 15 * 60))


def get_risk_agent_config(conn):
    core = _core()
    rows = conn.execute(
        """
        SELECT key, value
        FROM app_config
        WHERE key IN (
          'risk_agent_enabled',
          'risk_agent_interval_sec',
          'risk_agent_last_run_at',
          'risk_agent_lookback_days',
          'risk_agent_winsorize_pct'
        )
        """
    ).fetchall()
    cfg = {r["key"]: r["value"] for r in rows}
    enabled = str(cfg.get("risk_agent_enabled", "1")) == "1"
    try:
        interval_sec = int(float(cfg.get("risk_agent_interval_sec", str(_fallback_default_interval()))))
    except Exception:
        interval_sec = _fallback_default_interval()
    try:
        lookback_days = int(float(cfg.get("risk_agent_lookback_days", "252")))
    except Exception:
        lookback_days = 252
    lookback_days = max(60, min(5 * TRADING_DAYS_PER_YEAR, lookback_days))
    try:
        winsorize_pct = float(cfg.get("risk_agent_winsorize_pct", "0.05"))
    except Exception:
        winsorize_pct = 0.05
    winsorize_pct = core.clamp(winsorize_pct, 0.0, 0.2)
    return {
        "enabled": enabled,
        "interval_seconds": max(_fallback_min_interval(), min(7 * 24 * 60 * 60, interval_sec)),
        "last_run_at": str(cfg.get("risk_agent_last_run_at", "") or ""),
        "lookback_days": int(lookback_days),
        "winsorize_pct": round(winsorize_pct, 4),
    }


def set_risk_agent_config(
    enabled=None,
    interval_seconds=None,
    lookback_days=None,
    winsorize_pct=None,
    last_run_at=None,
):
    core = _core()
    with core.db_connect() as conn:
        if enabled is not None:
            conn.execute(
                """
                INSERT INTO app_config(key, value, updated_at) VALUES ('risk_agent_enabled', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                ("1" if enabled else "0", core.now_iso()),
            )
        if interval_seconds is not None:
            v = max(_fallback_min_interval(), min(7 * 24 * 60 * 60, int(interval_seconds)))
            conn.execute(
                """
                INSERT INTO app_config(key, value, updated_at) VALUES ('risk_agent_interval_sec', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (str(v), core.now_iso()),
            )
        if lookback_days is not None:
            d = max(60, min(5 * TRADING_DAYS_PER_YEAR, int(lookback_days)))
            conn.execute(
                """
                INSERT INTO app_config(key, value, updated_at) VALUES ('risk_agent_lookback_days', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (str(d), core.now_iso()),
            )
        if winsorize_pct is not None:
            p = core.clamp(float(winsorize_pct), 0.0, 0.2)
            conn.execute(
                """
                INSERT INTO app_config(key, value, updated_at) VALUES ('risk_agent_winsorize_pct', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (str(round(p, 6)), core.now_iso()),
            )
        if last_run_at is not None:
            conn.execute(
                """
                INSERT INTO app_config(key, value, updated_at) VALUES ('risk_agent_last_run_at', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (str(last_run_at or ""), core.now_iso()),
            )
        conn.commit()


def _iso_age_seconds(ts_text):
    raw = str(ts_text or "").strip()
    if not raw:
        return None
    try:
        dt_ts = dt.datetime.fromisoformat(raw)
        return max(0.0, (dt.datetime.now() - dt_ts).total_seconds())
    except Exception:
        return None


def _to_float(value):
    try:
        v = float(value)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return None


def _mean(values):
    vals = [float(v) for v in values if _to_float(v) is not None]
    if not vals:
        return 0.0
    return float(sum(vals) / len(vals))


def _stdev(values):
    vals = [float(v) for v in values if _to_float(v) is not None]
    if len(vals) < 2:
        return 0.0
    try:
        return float(statistics.stdev(vals))
    except Exception:
        return 0.0


def _quantile(sorted_values, q):
    if not sorted_values:
        return 0.0
    qn = max(0.0, min(1.0, float(q)))
    pos = (len(sorted_values) - 1) * qn
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(sorted_values[lo])
    frac = pos - lo
    return float(sorted_values[lo] * (1.0 - frac) + sorted_values[hi] * frac)


def _winsorize(values, pct):
    vals = [float(v) for v in values if _to_float(v) is not None]
    if not vals:
        return []
    p = max(0.0, min(0.2, float(pct)))
    if p <= 0.0 or len(vals) < 6:
        return vals
    sv = sorted(vals)
    lo = _quantile(sv, p)
    hi = _quantile(sv, 1.0 - p)
    out = []
    for v in vals:
        if v < lo:
            out.append(lo)
        elif v > hi:
            out.append(hi)
        else:
            out.append(v)
    return out


def _max_drawdown(prices):
    vals = [float(x) for x in prices if _to_float(x) is not None and float(x) > 0]
    if len(vals) < 2:
        return 0.0
    peak = vals[0]
    mdd = 0.0
    for p in vals[1:]:
        if p > peak:
            peak = p
        if peak <= 0:
            continue
        dd = (p / peak) - 1.0
        if dd < mdd:
            mdd = dd
    return float(mdd)


def _historical_var_cvar(returns, alpha=0.05):
    vals = [float(v) for v in returns if _to_float(v) is not None]
    if not vals:
        return 0.0, 0.0
    sv = sorted(vals)
    q = _quantile(sv, alpha)
    tail = [v for v in vals if v <= q]
    var95 = max(0.0, -float(q))
    cvar95 = max(0.0, -_mean(tail)) if tail else var95
    return var95, cvar95


def _downside_deviation(returns):
    vals = [float(v) for v in returns if _to_float(v) is not None]
    if not vals:
        return 0.0
    downside_sq = []
    for v in vals:
        m = min(0.0, v)
        downside_sq.append(m * m)
    return math.sqrt(sum(downside_sq) / len(downside_sq))


def _pearson_corr(xs, ys):
    n = min(len(xs), len(ys))
    if n < 2:
        return 0.0
    xv = [float(x) for x in xs[:n] if _to_float(x) is not None]
    yv = [float(y) for y in ys[:n] if _to_float(y) is not None]
    n2 = min(len(xv), len(yv))
    if n2 < 2:
        return 0.0
    xv = xv[:n2]
    yv = yv[:n2]
    mx = _mean(xv)
    my = _mean(yv)
    num = 0.0
    x_ss = 0.0
    y_ss = 0.0
    for i in range(n2):
        dx = xv[i] - mx
        dy = yv[i] - my
        num += dx * dy
        x_ss += dx * dx
        y_ss += dy * dy
    den = math.sqrt(max(1e-12, x_ss * y_ss))
    if den <= 0:
        return 0.0
    return max(-1.0, min(1.0, num / den))


def _risk_level(score):
    s = float(score)
    if s >= 65.0:
        return "high"
    if s >= 35.0:
        return "medium"
    return "low"


def _close_series_to_return_map(close_series, winsorize_pct):
    if len(close_series) < 2:
        return {}
    raw_returns = []
    dates = []
    for i in range(1, len(close_series)):
        prev_px = _to_float(close_series[i - 1][1])
        cur_px = _to_float(close_series[i][1])
        if prev_px is None or cur_px is None or prev_px <= 0 or cur_px <= 0:
            continue
        raw_returns.append((cur_px / prev_px) - 1.0)
        dates.append(str(close_series[i][0] or ""))
    if not raw_returns:
        return {}
    treated = _winsorize(raw_returns, winsorize_pct)
    out = {}
    for i, d in enumerate(dates):
        if not d:
            continue
        out[d] = float(treated[i])
    return out


def _symbol_risk_metrics(symbol, close_series, return_map):
    core = _core()
    ordered_dates = sorted(return_map.keys())
    returns = [float(return_map[d]) for d in ordered_dates]
    ann_vol = _stdev(returns) * math.sqrt(TRADING_DAYS_PER_YEAR)
    downside_vol = _downside_deviation(returns) * math.sqrt(TRADING_DAYS_PER_YEAR)
    mdd = _max_drawdown([x[1] for x in close_series])
    var95, cvar95 = _historical_var_cvar(returns, alpha=0.05)
    avg_daily = _mean(returns)
    sd_daily = _stdev(returns)
    sharpe_like = (avg_daily / sd_daily) * math.sqrt(TRADING_DAYS_PER_YEAR) if sd_daily > 0 else 0.0
    vol_norm = core.clamp(ann_vol / 0.55, 0.0, 1.0)
    mdd_norm = core.clamp(abs(mdd) / 0.60, 0.0, 1.0)
    var_norm = core.clamp(var95 / 0.06, 0.0, 1.0)
    risk_score = 100.0 * (0.45 * vol_norm + 0.35 * mdd_norm + 0.20 * var_norm)
    return {
        "symbol": str(symbol or ""),
        "observation_count": len(returns),
        "annualized_volatility": round(ann_vol, 6),
        "downside_volatility": round(downside_vol, 6),
        "max_drawdown": round(mdd, 6),
        "var_95": round(var95, 6),
        "cvar_95": round(cvar95, 6),
        "mean_daily_return": round(avg_daily, 8),
        "sharpe_like": round(sharpe_like, 6),
        "risk_score": round(risk_score, 3),
        "risk_level": _risk_level(risk_score),
    }


def _portfolio_return_series(weights, return_maps):
    all_dates = set()
    for m in return_maps.values():
        all_dates.update(m.keys())
    if not all_dates:
        return []
    out = []
    for d in sorted(all_dates):
        weighted_sum = 0.0
        weight_seen = 0.0
        for symbol, w in weights.items():
            r = (return_maps.get(symbol) or {}).get(d)
            if r is None:
                continue
            weighted_sum += float(w) * float(r)
            weight_seen += float(w)
        if weight_seen >= 0.35:
            out.append(weighted_sum / weight_seen)
    return out


def _average_pair_correlation(return_maps, min_overlap=30):
    syms = sorted(return_maps.keys())
    if len(syms) < 2:
        return 0.0, 0
    corrs = []
    for i in range(len(syms)):
        for j in range(i + 1, len(syms)):
            a = return_maps.get(syms[i]) or {}
            b = return_maps.get(syms[j]) or {}
            common = sorted(set(a.keys()) & set(b.keys()))
            if len(common) < min_overlap:
                continue
            xa = [float(a[d]) for d in common]
            xb = [float(b[d]) for d in common]
            corrs.append(_pearson_corr(xa, xb))
    if not corrs:
        return 0.0, 0
    return _mean(corrs), len(corrs)


def _load_symbol_close_series(mconn, symbol, min_date, lookback_days):
    rows = mconn.execute(
        """
        SELECT price_date, close
        FROM daily_prices
        WHERE UPPER(symbol) = ? AND price_date >= ?
        ORDER BY price_date
        """,
        (str(symbol or "").upper(), str(min_date or "")),
    ).fetchall()
    out = []
    for r in rows:
        d = str(r["price_date"] or "").strip()
        c = _to_float(r["close"])
        if not d or c is None or c <= 0:
            continue
        out.append((d, float(c)))
    if len(out) > (lookback_days + 1):
        out = out[-(lookback_days + 1) :]
    return out


def _collect_risk_snapshot(conn, lookback_days=252, winsorize_pct=0.05, persist=True):
    core = _core()
    holdings = conn.execute(
        """
        SELECT symbol, qty, market_value
        FROM holdings
        WHERE qty > 0 AND market_value > 0
        ORDER BY market_value DESC
        """
    ).fetchall()
    created_at = core.now_iso()
    lookback_n = max(60, min(5 * TRADING_DAYS_PER_YEAR, int(lookback_days)))
    winsor_n = core.clamp(float(winsorize_pct), 0.0, 0.2)
    if not holdings:
        snap = {
            "created_at": created_at,
            "lookback_days": lookback_n,
            "winsorize_pct": round(winsor_n, 4),
            "symbols_in_portfolio": 0,
            "symbols_with_history": 0,
            "symbols_analyzed": 0,
            "observation_count": 0,
            "portfolio_volatility": 0.0,
            "downside_volatility": 0.0,
            "max_drawdown": 0.0,
            "var_95": 0.0,
            "cvar_95": 0.0,
            "avg_pair_correlation": 0.0,
            "pair_count": 0,
            "concentration_hhi": 0.0,
            "risk_score": 0.0,
            "risk_level": "low",
            "notes": {"warnings": ["No active holdings found."]},
        }
        if persist:
            _persist_snapshot(conn, snap)
        return snap

    raw_weights = {}
    total_mv = 0.0
    for r in holdings:
        s = str(r["symbol"] or "").upper()
        mv = max(0.0, core.parse_float(r["market_value"], 0.0))
        if not s or mv <= 0:
            continue
        raw_weights[s] = mv
        total_mv += mv
    if total_mv <= 0:
        total_mv = 1.0
    weights = {s: (mv / total_mv) for s, mv in raw_weights.items()}

    min_date = (dt.date.today() - dt.timedelta(days=max(lookback_n * 3, lookback_n + 60))).isoformat()
    close_map = {}
    return_map = {}
    symbol_metrics = []
    with core.market_db_connect() as mconn:
        for symbol in weights.keys():
            close_series = _load_symbol_close_series(mconn, symbol, min_date, lookback_n)
            if len(close_series) < 60:
                continue
            rmap = _close_series_to_return_map(close_series, winsor_n)
            if len(rmap) < 45:
                continue
            close_map[symbol] = close_series
            return_map[symbol] = rmap
            symbol_metrics.append(_symbol_risk_metrics(symbol, close_series, rmap))

    valid_symbols = set(return_map.keys())
    valid_weight_total = sum(weights.get(s, 0.0) for s in valid_symbols)
    if valid_weight_total <= 0:
        valid_weight_total = 1.0
    valid_weights = {s: (weights.get(s, 0.0) / valid_weight_total) for s in valid_symbols}
    portfolio_returns = _portfolio_return_series(valid_weights, return_map)
    ann_vol = _stdev(portfolio_returns) * math.sqrt(TRADING_DAYS_PER_YEAR)
    downside_vol = _downside_deviation(portfolio_returns) * math.sqrt(TRADING_DAYS_PER_YEAR)
    var95, cvar95 = _historical_var_cvar(portfolio_returns, alpha=0.05)

    equity_curve = [1.0]
    eq = 1.0
    for r in portfolio_returns:
        eq *= (1.0 + float(r))
        equity_curve.append(eq)
    mdd = _max_drawdown(equity_curve)

    avg_corr, pair_count = _average_pair_correlation(return_map, min_overlap=30)
    hhi = sum((weights.get(s, 0.0) ** 2) for s in weights.keys())

    vol_norm = core.clamp(ann_vol / 0.35, 0.0, 1.0)
    mdd_norm = core.clamp(abs(mdd) / 0.45, 0.0, 1.0)
    conc_norm = core.clamp(hhi / 0.25, 0.0, 1.0)
    corr_norm = core.clamp((avg_corr - 0.15) / 0.70, 0.0, 1.0)
    var_norm = core.clamp(var95 / 0.04, 0.0, 1.0)
    risk_score = 100.0 * (0.30 * vol_norm + 0.25 * mdd_norm + 0.20 * conc_norm + 0.15 * corr_norm + 0.10 * var_norm)
    risk_level = _risk_level(risk_score)

    warnings = []
    if ann_vol >= 0.30:
        warnings.append("Portfolio volatility is elevated.")
    if abs(mdd) >= 0.20:
        warnings.append("Portfolio drawdown risk is elevated.")
    if hhi >= 0.18:
        warnings.append("Portfolio concentration is elevated.")
    if avg_corr >= 0.60 and pair_count > 0:
        warnings.append("Cross-holding correlation is elevated.")
    if len(portfolio_returns) < 45:
        warnings.append("Limited return observations for robust risk scoring.")

    top_risky = sorted(symbol_metrics, key=lambda x: x.get("risk_score", 0.0), reverse=True)[:5]
    missing_symbols = [s for s in weights.keys() if s not in valid_symbols]

    snap = {
        "created_at": created_at,
        "lookback_days": lookback_n,
        "winsorize_pct": round(winsor_n, 4),
        "symbols_in_portfolio": len(weights),
        "symbols_with_history": len(close_map),
        "symbols_analyzed": len(valid_symbols),
        "observation_count": len(portfolio_returns),
        "portfolio_volatility": round(ann_vol, 6),
        "downside_volatility": round(downside_vol, 6),
        "max_drawdown": round(mdd, 6),
        "var_95": round(var95, 6),
        "cvar_95": round(cvar95, 6),
        "avg_pair_correlation": round(avg_corr, 6),
        "pair_count": int(pair_count),
        "concentration_hhi": round(hhi, 6),
        "risk_score": round(risk_score, 3),
        "risk_level": risk_level,
        "notes": {
            "warnings": warnings,
            "top_risky_symbols": top_risky,
            "missing_history_symbols": missing_symbols,
            "method": {
                "returns": "daily pct_change",
                "outlier_treatment": f"winsorize {int(winsor_n * 100)}% tails",
                "core_metrics": ["volatility", "max_drawdown", "VaR_95", "CVaR_95", "correlation", "HHI"],
            },
        },
    }
    if persist:
        _persist_snapshot(conn, snap)
    return snap


def _persist_snapshot(conn, snap):
    core = _core()
    conn.execute(
        """
        INSERT INTO risk_analysis_snapshots(
          created_at,
          lookback_days,
          winsorize_pct,
          symbols_in_portfolio,
          symbols_with_history,
          symbols_analyzed,
          observation_count,
          portfolio_volatility,
          downside_volatility,
          max_drawdown,
          var_95,
          cvar_95,
          avg_pair_correlation,
          pair_count,
          concentration_hhi,
          risk_score,
          risk_level,
          notes_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(snap.get("created_at") or core.now_iso()),
            int(core.parse_float(snap.get("lookback_days"), 252)),
            float(core.parse_float(snap.get("winsorize_pct"), 0.05)),
            int(core.parse_float(snap.get("symbols_in_portfolio"), 0.0)),
            int(core.parse_float(snap.get("symbols_with_history"), 0.0)),
            int(core.parse_float(snap.get("symbols_analyzed"), 0.0)),
            int(core.parse_float(snap.get("observation_count"), 0.0)),
            float(core.parse_float(snap.get("portfolio_volatility"), 0.0)),
            float(core.parse_float(snap.get("downside_volatility"), 0.0)),
            float(core.parse_float(snap.get("max_drawdown"), 0.0)),
            float(core.parse_float(snap.get("var_95"), 0.0)),
            float(core.parse_float(snap.get("cvar_95"), 0.0)),
            float(core.parse_float(snap.get("avg_pair_correlation"), 0.0)),
            int(core.parse_float(snap.get("pair_count"), 0.0)),
            float(core.parse_float(snap.get("concentration_hhi"), 0.0)),
            float(core.parse_float(snap.get("risk_score"), 0.0)),
            str(snap.get("risk_level") or "low"),
            core._safe_json_dumps(snap.get("notes") or {}),
        ),
    )


def list_risk_analysis_snapshots(conn, limit=40):
    core = _core()
    lim = max(1, min(500, int(limit)))
    rows = conn.execute(
        """
        SELECT
          id, created_at, lookback_days, winsorize_pct, symbols_in_portfolio, symbols_with_history,
          symbols_analyzed, observation_count, portfolio_volatility, downside_volatility, max_drawdown,
          var_95, cvar_95, avg_pair_correlation, pair_count, concentration_hhi, risk_score, risk_level, notes_json
        FROM risk_analysis_snapshots
        ORDER BY id DESC
        LIMIT ?
        """,
        (lim,),
    ).fetchall()
    out = []
    for r in rows:
        item = dict(r)
        item["notes"] = core._safe_json_loads(item.get("notes_json"), {})
        out.append(item)
    return out


def run_risk_analysis_agent_once(max_runtime_sec=45, force=False):
    core = _core()
    with core.db_connect() as conn:
        cfg = get_risk_agent_config(conn)
    if not cfg.get("enabled") and not force:
        return {"ok": True, "skipped": "disabled", "config": cfg}

    last = str(cfg.get("last_run_at") or "").strip()
    if last and not force:
        age = _iso_age_seconds(last)
        if age is not None and age < int(cfg.get("interval_seconds", _fallback_default_interval())):
            return {"ok": True, "skipped": "interval_not_elapsed", "seconds_since_last_run": round(age, 3)}

    t0 = time.time()
    runtime_cap = max(10, int(core.parse_float(max_runtime_sec, 45)))
    with core.db_connect() as conn:
        snap = _collect_risk_snapshot(
            conn,
            lookback_days=cfg.get("lookback_days", 252),
            winsorize_pct=cfg.get("winsorize_pct", 0.05),
            persist=True,
        )
        conn.commit()

    set_risk_agent_config(last_run_at=core.now_iso())
    return {
        "ok": True,
        "agent": "risk_analysis",
        "runtime_sec": round(time.time() - t0, 3),
        "runtime_cap_sec": runtime_cap,
        "snapshot": snap,
    }


def maybe_run_risk_analysis_agent_once():
    core = _core()
    with core.db_connect() as conn:
        cfg = get_risk_agent_config(conn)
    if not cfg.get("enabled"):
        return None
    last = str(cfg.get("last_run_at") or "").strip()
    if last:
        age = _iso_age_seconds(last)
        if age is not None and age < int(cfg.get("interval_seconds", _fallback_default_interval())):
            return None
    return run_risk_analysis_agent_once(max_runtime_sec=45, force=False)


def risk_analysis_worker(stop_event):
    core = _core()
    while not stop_event.is_set():
        sleep_for = _fallback_default_interval()
        try:
            with core.db_connect() as conn:
                cfg = get_risk_agent_config(conn)
            sleep_for = int(cfg.get("interval_seconds", _fallback_default_interval()))
            if cfg.get("enabled"):
                maybe_run_risk_analysis_agent_once()
        except Exception:
            pass
        stop_event.wait(timeout=max(_fallback_min_interval(), int(sleep_for)))
