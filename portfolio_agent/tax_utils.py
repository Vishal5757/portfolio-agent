"""
portfolio_agent.tax_utils
~~~~~~~~~~~~~~~~~~~~~~~~~
India equity tax helpers extracted from app.py.

Pure helpers (FY bounds, bucket classification) have no external deps.
Functions that touch the database use the ``_core()`` lazy import pattern
to access app-level helpers (parse_history_date, load_split_map, etc.).
"""

import datetime as dt
from collections import defaultdict, deque

from portfolio_agent.utils import clamp, ist_now, parse_float


# ---------------------------------------------------------------------------
# Lazy back-reference to app module
# ---------------------------------------------------------------------------

def _core():
    import app  # noqa: PLC0415 -- intentional late import
    return app


# ---------------------------------------------------------------------------
# India financial-year helpers
# ---------------------------------------------------------------------------

def india_fy_bounds(as_of_date=None) -> dict:
    """Return the start/end dates for the Indian FY containing *as_of_date*.

    The Indian FY runs from 1 April to 31 March.
    Returns a dict with keys ``fy_label``, ``start_date``, ``end_date``
    (ISO strings).
    """
    core = _core()
    d = core.parse_history_date(as_of_date) or ist_now().date()
    start_year = d.year if d.month >= 4 else (d.year - 1)
    start = dt.date(start_year, 4, 1)
    end = dt.date(start_year + 1, 3, 31)
    return {
        "fy_label": "FY{}-{}".format(start_year, str(start_year + 1)[-2:]),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
    }


# keep the private alias used by existing app.py call-sites
_india_fy_bounds = india_fy_bounds


# ---------------------------------------------------------------------------
# Tax bucket helpers (pure)
# ---------------------------------------------------------------------------

def harvest_tax_bucket(buy_date, as_of_date=None):
    """Return ``("LTCG"|"STCG", held_days)`` for a lot bought on *buy_date*.

    LTCG applies when the holding period is >= 365 days.
    """
    core = _core()
    as_of = core.parse_history_date(as_of_date) or ist_now().date()
    buy_d = core.parse_history_date(buy_date)
    if not buy_d:
        return "STCG", 0
    held_days = max(0, (as_of - buy_d).days)
    return ("LTCG" if held_days >= 365 else "STCG"), held_days


def harvest_tax_bucket_rank(bucket) -> int:
    """Return a sort rank for *bucket* (STCG=0, LTCG=1)."""
    return 0 if str(bucket or "").upper() == "STCG" else 1


def harvest_tax_bucket_bias(bucket, side: str = "loss") -> float:
    """Return a priority bias score for harvesting *bucket* on the given *side*.

    Higher score means the bucket is more useful to harvest.
    STCG loss > LTCG loss; gains are de-prioritised.
    """
    b = str(bucket or "").upper()
    side_s = str(side or "loss").strip().lower()
    if b == "STCG":
        return 6.0 if side_s == "loss" else 3.0
    if b == "LTCG":
        return 2.0 if side_s == "loss" else 0.5
    return 0.0


# Private aliases for existing app.py call-sites
_harvest_tax_bucket = harvest_tax_bucket
_harvest_tax_bucket_rank = harvest_tax_bucket_rank
_harvest_tax_bucket_bias = harvest_tax_bucket_bias


# ---------------------------------------------------------------------------
# Harvest signal / action bias helpers (pure)
# ---------------------------------------------------------------------------

def harvest_action_bias(action) -> float:
    """Map a strategy action string to a numeric buy/sell bias score."""
    act = str(action or "").strip().upper()
    if act == "TRIM":
        return -8.0
    if act == "REVIEW":
        return -5.0
    if act == "HOLD":
        return 0.0
    if act == "WATCH_ADD":
        return 4.0
    if act == "ADD":
        return 7.0
    return 0.0


def harvest_signal_bias(buy_signal, sell_signal) -> float:
    """Map chart buy/sell signal strings to a combined bias score."""
    score = 0.0
    b = str(buy_signal or "").upper()
    s = str(sell_signal or "").upper()
    if b in ("BUY", "B1"):
        score += 5.0
    elif b == "B2":
        score += 3.0
    if s == "S1":
        score -= 4.0
    elif s in ("S2", "SELL"):
        score -= 6.0
    return score


def harvest_expected_move_score(intel_score, chart_score, fin_score, action_bias, signal_bias) -> float:
    """Combine individual bias scores into a single expected-move score."""
    return (
        clamp(parse_float(intel_score, 0.0), -10.0, 10.0) * 0.25
        + clamp(parse_float(chart_score, 0.0), -10.0, 10.0) * 0.25
        + clamp(parse_float(fin_score, 0.0), -10.0, 10.0) * 0.15
        + clamp(parse_float(action_bias, 0.0), -10.0, 10.0) * 0.20
        + clamp(parse_float(signal_bias, 0.0), -10.0, 10.0) * 0.15
    )


def harvest_direction_label(expected_move_score) -> str:
    """Return a human-readable direction label from the expected-move score."""
    s = parse_float(expected_move_score, 0.0)
    if s >= 4.0:
        return "strong_buy"
    if s >= 1.5:
        return "buy"
    if s <= -4.0:
        return "strong_sell"
    if s <= -1.5:
        return "sell"
    return "neutral"


def harvest_priority_reason(direction, strategy_action, chart_signal, intel_summary, fin_summary) -> str:
    """Build a short human-readable reason string for a harvest candidate."""
    bits = [str(direction or "neutral")]
    if strategy_action:
        bits.append("strategy {}".format(strategy_action))
    if chart_signal:
        bits.append("chart {}".format(chart_signal))
    if intel_summary and intel_summary.lower() != "no strong intelligence bias.":
        bits.append(intel_summary)
    elif fin_summary:
        bits.append(fin_summary)
    return ", ".join(bits[:4]) if bits else "Signal mix neutral."


# Private aliases for existing app.py call-sites
_harvest_action_bias = harvest_action_bias
_harvest_signal_bias = harvest_signal_bias
_harvest_expected_move_score = harvest_expected_move_score
_harvest_direction_label = harvest_direction_label
_harvest_priority_reason = harvest_priority_reason


# ---------------------------------------------------------------------------
# Realised equity tax summary
# ---------------------------------------------------------------------------

def compute_realized_equity_tax_summary(conn, as_of_date=None) -> dict:
    """Compute STCG/LTCG realised gains and losses for the current Indian FY.

    Processes all trades up to the FY end date using FIFO lot matching.
    Gold instruments are excluded.  Returns a dict with gain/loss subtotals
    and the remaining LTCG exemption (Rs 1.25L default limit).
    """
    core = _core()
    fy = india_fy_bounds(as_of_date=as_of_date)
    split_map = core.load_split_map(conn)
    tax_cfg = core.get_tax_profile_config(conn)
    start_d = dt.date.fromisoformat(fy["start_date"])
    end_d = dt.date.fromisoformat(fy["end_date"])

    rows = conn.execute(
        """
        SELECT t.id, UPPER(t.symbol) AS symbol, UPPER(t.side) AS side,
               t.trade_date, t.quantity, t.price,
               UPPER(COALESCE(i.asset_class, 'EQUITY')) AS asset_class
        FROM trades t
        LEFT JOIN instruments i ON UPPER(i.symbol) = UPPER(t.symbol)
        WHERE t.trade_date <= ?
        ORDER BY t.trade_date, t.id
        """,
        (fy["end_date"],),
    ).fetchall()

    lots_by_symbol = defaultdict(deque)
    stcg_gain = stcg_loss = ltcg_gain = ltcg_loss = 0.0

    for r in rows:
        if str(r["asset_class"] or "EQUITY").upper() == "GOLD":
            continue
        symbol = core.symbol_upper(r["symbol"])
        side = str(r["side"] or "").upper()
        trade_date = str(r["trade_date"] or "")[:10]
        q, p = core.adjusted_trade_values(symbol, trade_date, float(r["quantity"]), float(r["price"]), split_map)
        if q <= 0 or p <= 0:
            continue
        if side == "BUY":
            lots_by_symbol[symbol].append({"qty": q, "buy_price": p, "buy_date": trade_date})
            continue
        if side != "SELL":
            continue
        sell_d = core.parse_history_date(trade_date)
        remaining = q
        while remaining > 1e-9 and lots_by_symbol[symbol]:
            lot = lots_by_symbol[symbol][0]
            matched = min(remaining, parse_float(lot.get("qty"), 0.0))
            if matched <= 0:
                lots_by_symbol[symbol].popleft()
                continue
            if sell_d and start_d <= sell_d <= end_d:
                pnl = (p - parse_float(lot.get("buy_price"), 0.0)) * matched
                bucket, _ = harvest_tax_bucket(lot.get("buy_date"), as_of_date=trade_date)
                if pnl >= 0:
                    if bucket == "LTCG":
                        ltcg_gain += pnl
                    else:
                        stcg_gain += pnl
                else:
                    if bucket == "LTCG":
                        ltcg_loss += abs(pnl)
                    else:
                        stcg_loss += abs(pnl)
            lot["qty"] = max(0.0, parse_float(lot.get("qty"), 0.0) - matched)
            if parse_float(lot.get("qty"), 0.0) <= 1e-9:
                lots_by_symbol[symbol].popleft()
            remaining -= matched

    ltcg_net_gain = max(0.0, ltcg_gain - ltcg_loss)
    stcg_net_gain = max(0.0, stcg_gain - stcg_loss)
    exemption_limit = parse_float(tax_cfg.get("ltcg_exemption_limit"), 125000.0)
    ltcg_remaining_exemption = max(0.0, exemption_limit - ltcg_net_gain)
    return {
        "fy_label": fy["fy_label"],
        "fy_start_date": fy["start_date"],
        "fy_end_date": fy["end_date"],
        "stcg_gain": round(stcg_gain, 2),
        "stcg_loss": round(stcg_loss, 2),
        "ltcg_gain": round(ltcg_gain, 2),
        "ltcg_loss": round(ltcg_loss, 2),
        "stcg_net_gain": round(stcg_net_gain, 2),
        "ltcg_net_gain": round(ltcg_net_gain, 2),
        "ltcg_exemption_limit": round(exemption_limit, 2),
        "ltcg_remaining_exemption": round(ltcg_remaining_exemption, 2),
    }
