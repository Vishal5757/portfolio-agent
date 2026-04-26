"""
portfolio_agent.quote_manager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Quote-source registry, scoring, and selection logic extracted from app.py.

All database-mutating helpers accept a live ``sqlite3.Connection``; callers
are responsible for transaction management (commit / context manager).

App-level constants and the ``MarketDataClient`` class are accessed lazily
via ``_core()`` so this module can be imported without triggering circular
dependency errors at startup.
"""

import datetime as dt
import random
import re

from portfolio_agent.utils import clamp, now_iso, parse_float


# ---------------------------------------------------------------------------
# Lazy back-reference to app module
# ---------------------------------------------------------------------------

def _core():
    import app  # noqa: PLC0415 — intentional late import
    return app


# ---------------------------------------------------------------------------
# Registry helpers
# ---------------------------------------------------------------------------

def discovered_quote_sources() -> list:
    """Return every known quote source name — built-ins plus any discovered
    via ``MarketDataClient.fetch_*_quote`` method names."""
    core = _core()
    sources = list(core.BUILTIN_LIVE_QUOTE_SOURCES)
    try:
        method_names = [
            m for m in dir(core.MarketDataClient)
            if m.startswith("fetch_") and m.endswith("_quote")
        ]
    except Exception:
        method_names = []

    aliases = {
        "nse": "nse_api",
        "bse": "bse_api",
        "nsetools": "nsetools_api",
        "stock_nse_india": "stock_nse_india_api",
        "yahoo": "yahoo_finance",
        "google": "google_scrape",
        "screener": "screener_scrape",
        "trendlyne": "trendlyne_scrape",
        "cnbc": "cnbc_scrape",
    }
    for m in method_names:
        core_name = str(m)[6:-6]
        if core_name in ("", "quote", "multi_source", "source"):
            continue
        src = aliases.get(core_name, core_name)
        if not src.endswith(("_api", "_finance", "_scrape")):
            src = f"{src}_scrape"
        if re.match(r"^[a-z0-9_:-]{2,64}$", src) and src not in sources:
            sources.append(src)
    return sources


def ensure_quote_source_registry(conn, candidate_sources=None) -> None:
    """Upsert all known sources into the ``quote_source_registry`` table."""
    core = _core()
    now = now_iso()
    catalog: list = []
    for s in discovered_quote_sources():
        if s not in catalog:
            catalog.append(s)
    for s in core.parse_source_list(
        ",".join(candidate_sources or []),
        core.DEFAULT_LIVE_QUOTE_SOURCES,
    ):
        if s not in catalog:
            catalog.append(s)
    for source in catalog:
        conn.execute(
            """
            INSERT INTO quote_source_registry(source, adapter, enabled, created_at, updated_at, notes)
            VALUES (?, ?, 1, ?, ?, NULL)
            ON CONFLICT(source) DO UPDATE SET updated_at=excluded.updated_at
            """,
            (source, source, now, now),
        )


def set_quote_source_enabled(conn, source: str, enabled: bool = True, notes: str = None) -> None:
    """Enable or disable a quote source in the registry."""
    src = str(source or "").strip().lower()
    if not src or not re.match(r"^[a-z0-9_:-]{2,64}$", src):
        raise ValueError("invalid_source")
    ensure_quote_source_registry(conn, [src])
    conn.execute(
        """
        UPDATE quote_source_registry
        SET enabled = ?, updated_at = ?, notes = COALESCE(?, notes)
        WHERE source = ?
        """,
        (1 if bool(enabled) else 0, now_iso(), notes, src),
    )


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def quote_source_score_from_totals(
    attempts,
    successes,
    total_latency_ms,
    total_accuracy_error_pct,
    accuracy_samples,
) -> float:
    """Compute a 0–100 composite score from cumulative stats.

    Formula: ``success_rate × (0.55 × accuracy_score + 0.45 × latency_score)``

    * Latency score uses a 1 200 ms half-decay point.
    * Accuracy score uses a 3 % error half-decay point.
    * Returns 0.0 when there are no successful attempts.
    """
    attempts_n = max(0, int(attempts))
    succ_n = max(0, int(successes))
    if attempts_n <= 0 or succ_n <= 0:
        return 0.0
    success_rate = succ_n / attempts_n
    avg_latency = (parse_float(total_latency_ms, 0.0) / succ_n) if succ_n > 0 else 2000.0
    latency_score = 1.0 / (1.0 + (max(1.0, avg_latency) / 1200.0))
    if int(accuracy_samples or 0) > 0:
        avg_err = parse_float(total_accuracy_error_pct, 0.0) / max(1, int(accuracy_samples))
        accuracy_score = 1.0 / (1.0 + (max(0.0, avg_err) / 3.0))
    else:
        accuracy_score = 0.5
    quality = (0.55 * accuracy_score) + (0.45 * latency_score)
    return round(success_rate * quality * 100.0, 4)


def recompute_quote_source_scores(conn, sources=None) -> None:
    """Re-derive the ``score`` column in ``quote_source_stats`` from raw totals."""
    params: list = []
    where = ""
    if sources:
        uniq = sorted(set(str(s).strip().lower() for s in sources if str(s).strip()))
        if uniq:
            ph = ",".join("?" for _ in uniq)
            where = f"WHERE source IN ({ph})"
            params = uniq
    rows = conn.execute(
        f"""
        SELECT source, attempts, successes, total_latency_ms,
               total_accuracy_error_pct, accuracy_samples
        FROM quote_source_stats
        {where}
        """,
        params,
    ).fetchall()
    for r in rows:
        score = quote_source_score_from_totals(
            r["attempts"],
            r["successes"],
            r["total_latency_ms"],
            r["total_accuracy_error_pct"],
            r["accuracy_samples"],
        )
        conn.execute(
            "UPDATE quote_source_stats SET score = ?, updated_at = ? WHERE source = ?",
            (score, now_iso(), r["source"]),
        )


# ---------------------------------------------------------------------------
# Metrics ingestion
# ---------------------------------------------------------------------------

def apply_quote_source_metrics(conn, metric_events) -> None:
    """Accumulate a batch of quote-fetch result events into the stats tables.

    Each event dict should contain at minimum ``source`` and ``success``.
    Optional fields: ``latency_ms``, ``accuracy_error_pct``, ``fetched_at``.
    """
    if not metric_events:
        return
    ensure_quote_source_registry(conn, [e.get("source") for e in metric_events])
    by_source: dict = {}
    for ev in metric_events:
        src = str(ev.get("source") or "").strip().lower()
        if not src:
            continue
        item = by_source.setdefault(
            src,
            {
                "attempts": 0,
                "successes": 0,
                "failures": 0,
                "total_latency_ms": 0.0,
                "total_accuracy_error_pct": 0.0,
                "accuracy_samples": 0,
                "last_success_at": None,
                "last_error_at": None,
            },
        )
        ok = bool(ev.get("success"))
        ts = str(ev.get("fetched_at") or now_iso())
        item["attempts"] += 1
        if ok:
            item["successes"] += 1
            item["last_success_at"] = ts
        else:
            item["failures"] += 1
            item["last_error_at"] = ts
        item["total_latency_ms"] += max(0.0, parse_float(ev.get("latency_ms"), 0.0))
        err = ev.get("accuracy_error_pct")
        if err is not None:
            item["total_accuracy_error_pct"] += max(0.0, parse_float(err, 0.0))
            item["accuracy_samples"] += 1

    for source, m in by_source.items():
        conn.execute(
            """
            INSERT INTO quote_source_stats(
              source, attempts, successes, failures, total_latency_ms,
              total_accuracy_error_pct, accuracy_samples, score,
              last_success_at, last_error_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
            ON CONFLICT(source) DO UPDATE SET
              attempts                  = quote_source_stats.attempts + excluded.attempts,
              successes                 = quote_source_stats.successes + excluded.successes,
              failures                  = quote_source_stats.failures + excluded.failures,
              total_latency_ms          = quote_source_stats.total_latency_ms + excluded.total_latency_ms,
              total_accuracy_error_pct  = quote_source_stats.total_accuracy_error_pct + excluded.total_accuracy_error_pct,
              accuracy_samples          = quote_source_stats.accuracy_samples + excluded.accuracy_samples,
              last_success_at           = COALESCE(excluded.last_success_at, quote_source_stats.last_success_at),
              last_error_at             = COALESCE(excluded.last_error_at, quote_source_stats.last_error_at),
              updated_at                = excluded.updated_at
            """,
            (
                source,
                int(m["attempts"]),
                int(m["successes"]),
                int(m["failures"]),
                round(parse_float(m["total_latency_ms"], 0.0), 4),
                round(parse_float(m["total_accuracy_error_pct"], 0.0), 6),
                int(m["accuracy_samples"]),
                m["last_success_at"],
                m["last_error_at"],
                now_iso(),
            ),
        )
    recompute_quote_source_scores(conn, list(by_source.keys()))


# ---------------------------------------------------------------------------
# Ranking / selection
# ---------------------------------------------------------------------------

def _is_hard_failed(row, cooldown_sec: float) -> bool:
    """True when a source has never succeeded and its last error is recent."""
    attempts = int(row["attempts"] or 0)
    successes = int(row["successes"] or 0)
    if attempts < 15 or successes > 0:
        return False
    last_error_at = str(row["last_error_at"] or "").strip()
    if not last_error_at:
        return True
    try:
        err_dt = dt.datetime.fromisoformat(last_error_at[:19])
    except Exception:
        return True
    return (dt.datetime.now() - err_dt).total_seconds() < cooldown_sec


def get_ranked_quote_sources(conn, policy: dict, exchange: str = None) -> list:
    """Return an ordered list of source names to try for a quote fetch.

    Selection strategy:
    1. Sources ranked by composite score (highest first).
    2. Untried sources next, then soft-failed, hard-failed last.
    3. Honour ``top_k`` from *policy*; add an explore probe at rate ``explore_ratio``.
    4. Always include the primary exchange source (NSE/BSE) when it has signal.
    """
    core = _core()
    configured = core.parse_source_list(
        ",".join(policy.get("sources") or []),
        core.DEFAULT_LIVE_QUOTE_SOURCES,
    )
    ex = str(exchange or "").upper()
    if ex in ("NSE", "BSE"):
        configured = [s for s in configured if s != "gold_rate_scrape"]

    ensure_quote_source_registry(conn, configured)
    rows = conn.execute(
        """
        SELECT
          r.source, r.adapter, r.enabled,
          COALESCE(s.score, 0)    AS score,
          COALESCE(s.attempts, 0) AS attempts,
          COALESCE(s.successes,0) AS successes,
          COALESCE(s.failures, 0) AS failures,
          s.last_error_at
        FROM quote_source_registry r
        LEFT JOIN quote_source_stats s ON s.source = r.source
        WHERE r.enabled = 1
        ORDER BY score DESC, attempts DESC, r.source
        """
    ).fetchall()
    if ex in ("NSE", "BSE"):
        rows = [r for r in rows if str(r["source"] or "").strip().lower() != "gold_rate_scrape"]

    cooldown = float(getattr(core, "QUOTE_SOURCE_HARD_FAIL_COOLDOWN_SEC", 6 * 60 * 60))
    positives = [str(r["source"]) for r in rows if parse_float(r["score"], 0.0) > 0]
    fresh = [
        str(r["source"]) for r in rows
        if parse_float(r["score"], 0.0) <= 0 and int(r["attempts"] or 0) == 0
    ]
    soft_failed = [
        str(r["source"]) for r in rows
        if parse_float(r["score"], 0.0) <= 0
        and int(r["attempts"] or 0) > 0
        and not _is_hard_failed(r, cooldown)
    ]
    hard_failed = [str(r["source"]) for r in rows if _is_hard_failed(r, cooldown)]
    hard_failed_set = set(hard_failed)

    ordered = positives + fresh + soft_failed
    for src in configured:
        if src not in ordered and src not in hard_failed_set:
            ordered.append(src)

    top_k_default = int(getattr(core, "LIVE_QUOTE_TOP_K_DEFAULT", 4))
    explore_default = float(getattr(core, "LIVE_QUOTE_EXPLORE_RATIO_DEFAULT", 0.2))

    top_k = max(1, min(8, int(parse_float(policy.get("top_k"), top_k_default))))
    selected = ordered[:top_k]

    if len(selected) < 2 and hard_failed:
        ex_pref = "nse_api" if ex == "NSE" else ("bse_api" if ex == "BSE" else None)
        backup = ex_pref if ex_pref in hard_failed else hard_failed[0]
        if backup and backup not in selected:
            selected.append(backup)

    explore_ratio = clamp(parse_float(policy.get("explore_ratio"), explore_default), 0.0, 0.8)
    remainder = ordered[top_k:] + [s for s in hard_failed if s not in ordered[top_k:]]
    if remainder and explore_ratio > 0 and random.random() < explore_ratio:
        probe = random.choice(remainder)
        if probe not in selected:
            selected.append(probe)

    must_include = "nse_api" if ex == "NSE" else ("bse_api" if ex == "BSE" else None)
    if must_include and must_include in ordered and must_include not in selected:
        must_row = next((r for r in rows if str(r["source"]) == must_include), None)
        must_score = parse_float(must_row["score"], 0.0) if must_row else 0.0
        must_attempts = int(must_row["attempts"] or 0) if must_row else 0
        must_successes = int(must_row["successes"] or 0) if must_row else 0
        if must_score > 0 or not positives or (must_attempts < 15 and must_successes <= 0):
            selected.insert(0, must_include)

    if not selected:
        selected = list(core.BUILTIN_LIVE_QUOTE_SOURCES)
    if ex == "NSE" and "screener_scrape" in configured and "screener_scrape" not in selected:
        selected.append("screener_scrape")

    out: list = []
    for s in selected:
        if s not in out:
            out.append(s)
    return out


def quote_source_ranking(conn) -> list:
    """Return a full stats snapshot for all registered sources, sorted by score."""
    ensure_quote_source_registry(conn, [])
    rows = conn.execute(
        """
        SELECT
          r.source, r.adapter, r.enabled,
          COALESCE(s.score, 0)                    AS score,
          COALESCE(s.attempts, 0)                 AS attempts,
          COALESCE(s.successes, 0)                AS successes,
          COALESCE(s.failures, 0)                 AS failures,
          COALESCE(s.total_latency_ms, 0)         AS total_latency_ms,
          COALESCE(s.total_accuracy_error_pct, 0) AS total_accuracy_error_pct,
          COALESCE(s.accuracy_samples, 0)         AS accuracy_samples,
          s.last_success_at,
          s.last_error_at,
          r.updated_at
        FROM quote_source_registry r
        LEFT JOIN quote_source_stats s ON s.source = r.source
        ORDER BY score DESC, attempts DESC, r.source
        """
    ).fetchall()
    out = []
    for r in rows:
        attempts = int(r["attempts"] or 0)
        succ = int(r["successes"] or 0)
        latency = parse_float(r["total_latency_ms"], 0.0)
        avg_latency = (latency / succ) if succ > 0 else None
        err_samples = int(r["accuracy_samples"] or 0)
        avg_err = (
            parse_float(r["total_accuracy_error_pct"], 0.0) / err_samples
            if err_samples > 0 else None
        )
        out.append(
            {
                "source": r["source"],
                "adapter": r["adapter"],
                "enabled": bool(int(r["enabled"] or 0)),
                "score": round(parse_float(r["score"], 0.0), 4),
                "attempts": attempts,
                "successes": succ,
                "failures": int(r["failures"] or 0),
                "success_rate_pct": round(succ * 100.0 / attempts, 2) if attempts > 0 else 0.0,
                "avg_latency_ms": round(avg_latency, 2) if avg_latency is not None else None,
                "avg_accuracy_error_pct": round(avg_err, 4) if avg_err is not None else None,
                "last_success_at": r["last_success_at"],
                "last_error_at": r["last_error_at"],
                "updated_at": r["updated_at"],
            }
        )
    return out


# ---------------------------------------------------------------------------
# Corroboration helper (used by quote plausibility checks)
# ---------------------------------------------------------------------------

def quote_corroboration_count(
    candidates,
    selected_source: str,
    ltp,
    tolerance_pct: float = 2.0,
) -> int:
    """Count how many *candidates* corroborate *ltp* within *tolerance_pct*."""
    anchor = parse_float(ltp, 0.0)
    if anchor <= 0:
        return 0
    tol = max(0.01, anchor * max(0.2, parse_float(tolerance_pct, 2.0)) / 100.0)
    src = str(selected_source or "").lower()
    cnt = 0
    for c in candidates or []:
        csrc = str(c.get("source") or "").lower()
        if csrc == src:
            continue
        cpx = parse_float(c.get("ltp"), 0.0)
        if cpx > 0 and abs(cpx - anchor) <= tol:
            cnt += 1
    return cnt
