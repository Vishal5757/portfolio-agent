"""
portfolio_agent.utils
~~~~~~~~~~~~~~~~~~~~~
Pure utility helpers extracted from app.py.  No dependency on the app
module -- safe to import at any point without circular-import risk.
"""

import datetime as dt
import re

# ---------------------------------------------------------------------------
# Time / timezone helpers
# ---------------------------------------------------------------------------

IST_TZ = dt.timezone(dt.timedelta(hours=5, minutes=30))
_EOD_IST_HOUR = 15
_EOD_IST_MINUTE = 35


def now_iso() -> str:
    """Return the current local time as an ISO-8601 string (no microseconds)."""
    return dt.datetime.now().replace(microsecond=0).isoformat()


def ist_now() -> dt.datetime:
    """Return the current time as a timezone-aware IST datetime."""
    return dt.datetime.now(IST_TZ)


def is_zero_qty_eod_window(now_ist=None) -> bool:
    """True after 15:35 IST -- the window where zero-qty quotes are valid."""
    now_ist = now_ist or ist_now()
    h = int(now_ist.hour)
    m = int(now_ist.minute)
    return (h > _EOD_IST_HOUR) or (h == _EOD_IST_HOUR and m >= _EOD_IST_MINUTE)


# ---------------------------------------------------------------------------
# Numeric helpers
# ---------------------------------------------------------------------------

def clamp(value, low, high):
    """Clamp *value* to the closed interval [low, high]."""
    return max(low, min(high, value))


def parse_float(value, default: float = 0.0) -> float:
    """Coerce *value* to float, stripping common currency / suffix decorations.

    Handles INR symbols, compact suffixes (K/L/Cr/M/B), sign characters and
    parenthesised negatives.  Returns *default* on any parse failure.
    """
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return default
        cleaned = (
            raw.replace(",", "")
            .replace("\u20b9", "")   # INR rupee sign (U+20B9)
            .replace("\u20a8", "")   # legacy rupee sign (U+20A8)
            .replace("$", "")
            .replace("rs.", "")
            .replace("rs", "")
            .replace("inr", "")
            .replace("%", "")
            .replace("+", "")
            .replace("(", "-")
            .replace(")", "")
            .replace("\u2212", "-")  # Unicode minus sign (U+2212)
            .strip()
        )
        if not cleaned:
            return default
        try:
            return float(cleaned)
        except ValueError:
            low = cleaned.lower()
            multipliers = {
                "k": 1e3, "thousand": 1e3,
                "l": 1e5, "lac": 1e5, "lakh": 1e5, "lakhs": 1e5,
                "cr": 1e7, "crore": 1e7, "crores": 1e7,
                "m": 1e6, "mn": 1e6, "million": 1e6,
                "b": 1e9, "bn": 1e9, "billion": 1e9,
            }
            match = re.search(r"([+\-]?\d+(?:\.\d+)?)\s*([a-zA-Z]+)?", low)
            if not match:
                return default
            try:
                base = float(match.group(1))
            except Exception:
                return default
            unit = str(match.group(2) or "").strip().lower()
            return base * multipliers.get(unit, 1.0)
    return default


def money(value) -> str:
    """Format *value* as an INR currency string, e.g. Rs 1,23,456.78."""
    return "\u20b9{:,.2f}".format(parse_float(value, 0.0))


def median_value(values) -> float:
    """Return the median of *values*, ignoring non-positive entries."""
    vals = sorted(float(v) for v in values if parse_float(v, 0.0) > 0)
    if not vals:
        return 0.0
    n = len(vals)
    m = n // 2
    if n % 2 == 1:
        return float(vals[m])
    return float((vals[m - 1] + vals[m]) / 2.0)


# ---------------------------------------------------------------------------
# Boolean / list parsers
# ---------------------------------------------------------------------------

def parse_bool(value, default=None):
    """Parse a truthy/falsy value from a wide variety of representations.

    Raises ValueError for unrecognised strings when *default* is None.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(int(value))
    s = str(value).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    if default is not None:
        return default
    raise ValueError("invalid_boolean")


def parse_token_list(value, default=None) -> list:
    """Split a comma-separated string into a de-duplicated list of tokens.

    Each token must match ``[a-z0-9_:-]{2,64}``.  Returns an empty list when
    nothing valid is found (or *default* as a fallback source string).
    """
    raw = str(value or "").strip()
    if not raw:
        raw = str(default or "")
    parts = [p.strip().lower() for p in raw.split(",") if str(p).strip()]
    out = []
    for p in parts:
        if not re.match(r"^[a-z0-9_:-]{2,64}$", p):
            continue
        if p not in out:
            out.append(p)
    return out


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Market calendar helpers
# ---------------------------------------------------------------------------

# NSE equity market holidays -- weekday-only dates that the exchange is closed.
# Weekends are excluded automatically; only add dates that fall Mon-Fri.
_NSE_HOLIDAYS = frozenset({
    # 2024
    "2024-01-26", "2024-03-25", "2024-04-14", "2024-04-17",
    "2024-04-21", "2024-05-23", "2024-06-17", "2024-07-17",
    "2024-08-15", "2024-10-02", "2024-10-14", "2024-11-15",
    "2024-11-20",
    # 2025
    "2025-02-26", "2025-03-14", "2025-03-31", "2025-04-10",
    "2025-04-14", "2025-04-18", "2025-05-01", "2025-08-15",
    "2025-08-27", "2025-10-02", "2025-10-22", "2025-10-28",
    "2025-11-05", "2025-12-25",
    # 2026
    "2026-01-26", "2026-03-20", "2026-04-02", "2026-04-03",
    "2026-04-14", "2026-04-20", "2026-05-01", "2026-07-06",
    "2026-08-15", "2026-09-16", "2026-10-02", "2026-10-19",
    "2026-11-06", "2026-11-25", "2026-12-25",
})


def count_market_working_days(from_date, to_date) -> int:
    """Count NSE equity market trading days between *from_date* and *to_date* (inclusive).

    Excludes weekends and known NSE trading holidays.
    Accepts ``datetime.date`` objects or ISO-8601 strings.  Returns 0 on bad input.
    """
    try:
        if isinstance(from_date, str):
            from_date = dt.date.fromisoformat(from_date[:10])
        if isinstance(to_date, str):
            to_date = dt.date.fromisoformat(to_date[:10])
        if not from_date or not to_date or from_date > to_date:
            return 0
        count = 0
        current = from_date
        one_day = dt.timedelta(days=1)
        while current <= to_date:
            if current.weekday() < 5 and current.isoformat() not in _NSE_HOLIDAYS:
                count += 1
            current += one_day
        return count
    except Exception:
        return 0


def parse_excel_date(value):
    """Coerce *value* to a ``datetime.date``, or return None on failure.

    Accepts ``datetime.datetime``, ``datetime.date``, or ISO / common string
    formats.  Rejects dates outside the 1990-to-next-two-years range.
    """
    def _valid_year(d: dt.date) -> bool:
        return 1990 <= d.year <= (dt.date.today().year + 2)

    if value is None:
        return None
    if isinstance(value, dt.datetime):
        d = value.date()
        return d if _valid_year(d) else None
    if isinstance(value, dt.date):
        return value if _valid_year(value) else None
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%Y/%m/%d"):
            try:
                d = dt.datetime.strptime(raw[:10], fmt).date()
                if _valid_year(d):
                    return d
            except ValueError:
                continue
    return None
