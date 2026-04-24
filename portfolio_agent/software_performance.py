import datetime as dt
import time

SOFTWARE_PERF_RETENTION_DAYS_DEFAULT = 90
SOFTWARE_PERF_RETENTION_DAYS_MIN = 7
SOFTWARE_PERF_RETENTION_DAYS_MAX = 3650
SOFTWARE_PERF_CLEANUP_INTERVAL_SEC = 7 * 24 * 60 * 60


def _core():
    import app  # local import keeps module decoupled from import order during startup

    return app


def get_software_perf_agent_config(conn):
    core = _core()
    rows = conn.execute(
        """
        SELECT key, value
        FROM app_config
        WHERE key IN (
          'software_perf_agent_enabled',
          'software_perf_agent_interval_sec',
          'software_perf_agent_last_run_at',
          'software_perf_agent_last_heal_at',
          'software_perf_agent_last_improvement_at',
          'software_perf_agent_last_cleanup_at',
          'software_perf_agent_auto_tune',
          'software_perf_agent_write_changes',
          'software_perf_agent_core_objective',
          'software_perf_agent_retention_days'
        )
        """
    ).fetchall()
    cfg = {r["key"]: r["value"] for r in rows}
    enabled = str(cfg.get("software_perf_agent_enabled", "1")) == "1"
    auto_tune = str(cfg.get("software_perf_agent_auto_tune", "1")) == "1"
    write_changes = str(cfg.get("software_perf_agent_write_changes", "1")) == "1"
    try:
        interval_sec = int(
            float(cfg.get("software_perf_agent_interval_sec", str(core.SOFTWARE_PERF_AGENT_INTERVAL_DEFAULT_SEC)))
        )
    except Exception:
        interval_sec = core.SOFTWARE_PERF_AGENT_INTERVAL_DEFAULT_SEC
    try:
        retention_days = int(
            float(cfg.get("software_perf_agent_retention_days", str(SOFTWARE_PERF_RETENTION_DAYS_DEFAULT)))
        )
    except Exception:
        retention_days = SOFTWARE_PERF_RETENTION_DAYS_DEFAULT
    core_objective = str(
        cfg.get("software_perf_agent_core_objective", "Preserve portfolio data integrity and strategy objective.") or ""
    ).strip()
    if not core_objective:
        core_objective = "Preserve portfolio data integrity and strategy objective."
    return {
        "enabled": enabled,
        "interval_seconds": max(core.SOFTWARE_PERF_AGENT_MIN_INTERVAL_SEC, min(7 * 24 * 60 * 60, interval_sec)),
        "last_run_at": str(cfg.get("software_perf_agent_last_run_at", "") or ""),
        "last_heal_at": str(cfg.get("software_perf_agent_last_heal_at", "") or ""),
        "last_improvement_at": str(cfg.get("software_perf_agent_last_improvement_at", "") or ""),
        "last_cleanup_at": str(cfg.get("software_perf_agent_last_cleanup_at", "") or ""),
        "retention_days": max(SOFTWARE_PERF_RETENTION_DAYS_MIN, min(SOFTWARE_PERF_RETENTION_DAYS_MAX, retention_days)),
        "auto_tune": auto_tune,
        "write_changes": write_changes,
        "core_objective": core_objective,
    }


def set_software_perf_agent_config(
    enabled=None,
    interval_seconds=None,
    auto_tune=None,
    write_changes=None,
    core_objective=None,
    last_run_at=None,
    last_heal_at=None,
    last_improvement_at=None,
    retention_days=None,
    last_cleanup_at=None,
):
    core = _core()
    with core.db_connect() as conn:
        if enabled is not None:
            conn.execute(
                """
                INSERT INTO app_config(key, value, updated_at) VALUES ('software_perf_agent_enabled', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                ("1" if enabled else "0", core.now_iso()),
            )
        if interval_seconds is not None:
            v = max(core.SOFTWARE_PERF_AGENT_MIN_INTERVAL_SEC, min(7 * 24 * 60 * 60, int(interval_seconds)))
            conn.execute(
                """
                INSERT INTO app_config(key, value, updated_at) VALUES ('software_perf_agent_interval_sec', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (str(v), core.now_iso()),
            )
        if auto_tune is not None:
            conn.execute(
                """
                INSERT INTO app_config(key, value, updated_at) VALUES ('software_perf_agent_auto_tune', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                ("1" if auto_tune else "0", core.now_iso()),
            )
        if write_changes is not None:
            conn.execute(
                """
                INSERT INTO app_config(key, value, updated_at) VALUES ('software_perf_agent_write_changes', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                ("1" if write_changes else "0", core.now_iso()),
            )
        if core_objective is not None:
            val = str(core_objective or "").strip()
            if not val:
                val = "Preserve portfolio data integrity and strategy objective."
            conn.execute(
                """
                INSERT INTO app_config(key, value, updated_at) VALUES ('software_perf_agent_core_objective', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (val, core.now_iso()),
            )
        if last_run_at is not None:
            conn.execute(
                """
                INSERT INTO app_config(key, value, updated_at) VALUES ('software_perf_agent_last_run_at', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (str(last_run_at or ""), core.now_iso()),
            )
        if last_heal_at is not None:
            conn.execute(
                """
                INSERT INTO app_config(key, value, updated_at) VALUES ('software_perf_agent_last_heal_at', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (str(last_heal_at or ""), core.now_iso()),
            )
        if last_improvement_at is not None:
            conn.execute(
                """
                INSERT INTO app_config(key, value, updated_at) VALUES ('software_perf_agent_last_improvement_at', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (str(last_improvement_at or ""), core.now_iso()),
            )
        if retention_days is not None:
            v = max(
                SOFTWARE_PERF_RETENTION_DAYS_MIN,
                min(SOFTWARE_PERF_RETENTION_DAYS_MAX, int(retention_days)),
            )
            conn.execute(
                """
                INSERT INTO app_config(key, value, updated_at) VALUES ('software_perf_agent_retention_days', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (str(v), core.now_iso()),
            )
        if last_cleanup_at is not None:
            conn.execute(
                """
                INSERT INTO app_config(key, value, updated_at) VALUES ('software_perf_agent_last_cleanup_at', ?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
                """,
                (str(last_cleanup_at or ""), core.now_iso()),
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


def _software_perf_issue_count(diag):
    issues = [str(x or "").strip() for x in (diag or {}).get("issues", [])]
    if not issues:
        return 0
    filtered = [x for x in issues if x and ("no critical data-pipe issues detected" not in x.lower())]
    return len(filtered)


def _software_perf_recent_quote_metrics(conn, lookback_minutes=60):
    core = _core()
    cutoff = (dt.datetime.now() - dt.timedelta(minutes=max(5, int(lookback_minutes)))).replace(microsecond=0).isoformat()
    qrow = conn.execute(
        """
        SELECT
          COUNT(*) AS c,
          AVG(CASE WHEN latency_ms > 0 THEN latency_ms END) AS avg_latency_ms
        FROM quote_samples
        WHERE fetched_at >= ?
        """,
        (cutoff,),
    ).fetchone()
    stats = conn.execute(
        """
        SELECT
          COALESCE(SUM(successes), 0) AS succ,
          COALESCE(SUM(attempts), 0) AS att
        FROM quote_source_stats
        """
    ).fetchone()
    succ = core.parse_float(stats["succ"], 0.0) if stats else 0.0
    att = core.parse_float(stats["att"], 0.0) if stats else 0.0
    return {
        "sample_count": int(core.parse_float(qrow["c"], 0.0)) if qrow else 0,
        "avg_latency_ms": core.parse_float(qrow["avg_latency_ms"], 0.0) if qrow else 0.0,
        "success_rate": (succ / att) if att > 0 else 0.0,
    }


def _software_perf_collect_snapshot(conn, persist=True):
    core = _core()
    diag = core.build_data_pipe_diagnostics(conn)
    quote_m = _software_perf_recent_quote_metrics(conn, lookback_minutes=60)
    lp_row = conn.execute("SELECT MAX(updated_at) AS ts FROM latest_prices").fetchone()
    price_age = _iso_age_seconds(lp_row["ts"]) if lp_row else None
    if price_age is None:
        price_age = float(max(900, int(core.parse_float(diag.get("staleness_threshold_seconds"), 900))))
    issues = [str(x or "") for x in diag.get("issues", [])]
    issue_count = _software_perf_issue_count(diag)
    snap = {
        "created_at": core.now_iso(),
        "live_stale_symbols": int(core.parse_float(diag.get("stale_price_symbols"), 0.0)),
        "live_zero_ltp_symbols": int(core.parse_float(diag.get("zero_ltp_symbols"), 0.0)),
        "live_missing_price_symbols": int(core.parse_float(diag.get("missing_price_symbols"), 0.0)),
        "weak_sources_count": len(diag.get("weak_sources") or []),
        "avg_quote_latency_ms": round(core.parse_float(quote_m.get("avg_latency_ms"), 0.0), 3),
        "quote_success_rate": round(core.parse_float(quote_m.get("success_rate"), 0.0), 6),
        "last_price_age_sec": round(core.parse_float(price_age, 0.0), 3),
        "history_coverage_ratio": round(core.parse_float(diag.get("history_coverage_ratio"), 0.0), 6),
        "issue_count": int(issue_count),
        "issues": issues,
        "weak_sources": diag.get("weak_sources") or [],
        "diag": diag,
        "quote_metrics": quote_m,
    }
    if persist:
        conn.execute(
            """
            INSERT INTO software_perf_snapshots(
              created_at,
              live_stale_symbols,
              live_zero_ltp_symbols,
              live_missing_price_symbols,
              weak_sources_count,
              avg_quote_latency_ms,
              quote_success_rate,
              last_price_age_sec,
              history_coverage_ratio,
              issue_count,
              notes_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snap["created_at"],
                snap["live_stale_symbols"],
                snap["live_zero_ltp_symbols"],
                snap["live_missing_price_symbols"],
                snap["weak_sources_count"],
                snap["avg_quote_latency_ms"],
                snap["quote_success_rate"],
                snap["last_price_age_sec"],
                snap["history_coverage_ratio"],
                snap["issue_count"],
                core._safe_json_dumps(
                    {
                        "issues": issues[:12],
                        "weak_sources": (diag.get("weak_sources") or [])[:10],
                        "staleness_threshold_seconds": diag.get("staleness_threshold_seconds"),
                    }
                ),
            ),
        )
    return snap


def _software_perf_log_action(conn, action_type, status, summary, details=None):
    core = _core()
    conn.execute(
        """
        INSERT INTO software_perf_actions(created_at, action_type, status, summary, details_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            core.now_iso(),
            str(action_type or ""),
            str(status or ""),
            str(summary or ""),
            core._safe_json_dumps(details or {}),
        ),
    )


def _software_perf_cleanup_stale_logs(conn, retention_days=SOFTWARE_PERF_RETENTION_DAYS_DEFAULT):
    keep_days = max(SOFTWARE_PERF_RETENTION_DAYS_MIN, min(SOFTWARE_PERF_RETENTION_DAYS_MAX, int(retention_days)))
    cutoff_at = (dt.datetime.now() - dt.timedelta(days=keep_days)).replace(microsecond=0).isoformat()
    deleted_actions = int(
        conn.execute(
            "DELETE FROM software_perf_actions WHERE created_at < ?",
            (cutoff_at,),
        ).rowcount
        or 0
    )
    deleted_snapshots = int(
        conn.execute(
            "DELETE FROM software_perf_snapshots WHERE created_at < ?",
            (cutoff_at,),
        ).rowcount
        or 0
    )
    return {
        "retention_days": keep_days,
        "cutoff_at": cutoff_at,
        "deleted_actions": deleted_actions,
        "deleted_snapshots": deleted_snapshots,
        "deleted_total": deleted_actions + deleted_snapshots,
    }


def _software_perf_core_snapshot(conn):
    core = _core()
    row = conn.execute(
        """
        SELECT
          (SELECT COUNT(*) FROM instruments) AS instruments_total,
          (SELECT COUNT(*) FROM trades) AS trades_total,
          (SELECT COUNT(*) FROM strategy_parameters) AS strategy_param_total,
          (SELECT COUNT(*) FROM cash_ledger) AS cash_ledger_total
        """
    ).fetchone()
    return {
        "instruments_total": int(core.parse_float(row["instruments_total"], 0.0)) if row else 0,
        "trades_total": int(core.parse_float(row["trades_total"], 0.0)) if row else 0,
        "strategy_param_total": int(core.parse_float(row["strategy_param_total"], 0.0)) if row else 0,
        "cash_ledger_total": int(core.parse_float(row["cash_ledger_total"], 0.0)) if row else 0,
    }


def _software_perf_core_guard(before, after):
    core = _core()
    protected = ("instruments_total", "trades_total", "strategy_param_total", "cash_ledger_total")
    drift = {}
    for k in protected:
        b = int(core.parse_float((before or {}).get(k), 0.0))
        a = int(core.parse_float((after or {}).get(k), 0.0))
        if a != b:
            drift[k] = {"before": b, "after": a}
    return {"ok": len(drift) == 0, "drift": drift}


def _software_perf_base_proposal(snapshot, after_snapshot, cfg, actions=None, errors=None, core_guard=None):
    core = _core()
    remaining_issues = list((after_snapshot or {}).get("issues") or [])
    proposal = {
        "generated_at": core.now_iso(),
        "core_objective": str((cfg or {}).get("core_objective") or ""),
        "issues": list((snapshot or {}).get("issues") or []),
        "remaining_issues": remaining_issues,
        "analysis_summary": (
            f"Observed {int(core.parse_float((snapshot or {}).get('issue_count'), 0.0))} issue(s) before self-heal and "
            f"{int(core.parse_float((after_snapshot or {}).get('issue_count'), 0.0))} after self-heal."
        ),
        "suggested_live_config_updates": {},
        "suggested_code_changes": [],
        "tests_to_add": [
            "Add a regression test for the failing data-path or stale quote condition.",
            "Add an observability assertion so the same failure is visible before it becomes user-facing.",
        ],
        "observability_additions": [
            "Log the upstream source and fetch latency for quote refresh failures.",
            "Track stale/latest price age by source in the software-performance dashboard.",
        ],
        "action_context": list(actions or []),
        "errors": list(errors or []),
        "core_guard": dict(core_guard or {"ok": True, "drift": {}}),
        "analysis_engine": "local_rules",
        "notes": [
            "Draft only: this file is generated for review and is not auto-imported by the runtime.",
            "Core objective guard: do not edit trade/instrument/strategy schema or destructive paths.",
        ],
    }
    return proposal


def _software_perf_generate_local_proposal(snapshot, after_snapshot, cfg, actions=None, errors=None, core_guard=None):
    core = _core()
    proposal = _software_perf_base_proposal(
        snapshot,
        after_snapshot,
        cfg,
        actions=actions,
        errors=errors,
        core_guard=core_guard,
    )
    proposal["analysis_engine"] = "local_rules"
    try:
        with core.db_connect() as conn:
            llm_cfg = core.get_hosted_llm_config(conn)
            if llm_cfg.get("enabled"):
                prompt = (
                    "Return concise JSON for a software performance agent advisory. "
                    "Do not suggest destructive operations. Focus on root-cause hypotheses, safe tests, and safe code-review targets. "
                    "Schema: {\"summary\":\"short\",\"safe_actions\":[\"...\"],\"tests_to_add\":[\"...\"],\"risks\":[\"...\"]}.\n\n"
                    + core._safe_json_dumps(
                        {
                            "core_objective": proposal.get("core_objective"),
                            "before": snapshot,
                            "after": after_snapshot,
                            "actions": list(actions or []),
                            "errors": list(errors or []),
                            "core_guard": dict(core_guard or {}),
                        }
                    )
                )
                result = core.hosted_llm_generate(
                    conn,
                    prompt,
                    purpose="software_performance",
                    system_prompt="You are a cautious software reliability reviewer. Return only valid JSON. Never propose destructive data changes.",
                )
                proposal["hosted_llm_support"] = {
                    "ok": bool(result.get("ok")),
                    "status": str(result.get("status") or ""),
                    "provider": str(result.get("provider") or ""),
                    "model": str(result.get("model") or ""),
                    "latency_ms": core.parse_float(result.get("latency_ms"), 0.0),
                    "advisory": core._extract_json_object(result.get("text")) if result.get("ok") and hasattr(core, "_extract_json_object") else {},
                    "error": str(result.get("error") or ""),
                }
                if result.get("ok"):
                    proposal["analysis_engine"] = "local_rules+hosted_llm"
                    adv = proposal["hosted_llm_support"].get("advisory") or {}
                    if adv.get("summary"):
                        proposal["analysis_summary"] = f"{proposal['analysis_summary']} LLM advisory: {adv.get('summary')}"
                    proposal["tests_to_add"].extend([str(x) for x in adv.get("tests_to_add", []) if str(x).strip()][:3])
                    proposal["observability_additions"].extend([str(x) for x in adv.get("safe_actions", []) if str(x).strip()][:3])
    except Exception as ex:
        proposal["hosted_llm_support"] = {"ok": False, "status": "error", "error": str(ex)[:500]}
    return proposal


def _software_perf_write_improvement_draft(proposal, cfg):
    core = _core()
    tenant_data_dir = core.get_current_tenant_data_dir() if hasattr(core, "get_current_tenant_data_dir") else core.DATA_DIR
    out_dir = tenant_data_dir / "agent_improvements"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    target = out_dir / f"software_improvement_{stamp}.py"
    payload = dict(proposal or {})
    payload["generated_at"] = str(payload.get("generated_at") or core.now_iso())
    payload["core_objective"] = str(payload.get("core_objective") or str((cfg or {}).get("core_objective") or ""))
    py = (
        "\"\"\"Auto-generated software improvement draft.\n"
        "Review before use. This file is never auto-executed by the server.\n"
        "\"\"\"\n\n"
        f"PROPOSAL = {repr(payload)}\n\n"
        "def proposed_live_config_updates():\n"
        "    return dict(PROPOSAL.get('suggested_live_config_updates', {}))\n\n"
        "def rationale_lines():\n"
        "    lines = [str(PROPOSAL.get('analysis_summary') or '').strip()]\n"
        "    lines.extend([str(x) for x in PROPOSAL.get('issues', [])])\n"
        "    return [x for x in lines if x]\n"
    )
    target.write_text(py, encoding="utf-8")
    return str(target)


def list_software_perf_snapshots(conn, limit=40):
    core = _core()
    lim = max(1, min(500, int(limit)))
    rows = conn.execute(
        """
        SELECT
          id, created_at, live_stale_symbols, live_zero_ltp_symbols, live_missing_price_symbols,
          weak_sources_count, avg_quote_latency_ms, quote_success_rate, last_price_age_sec,
          history_coverage_ratio, issue_count, notes_json
        FROM software_perf_snapshots
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


def list_software_perf_actions(conn, limit=80):
    core = _core()
    lim = max(1, min(500, int(limit)))
    rows = conn.execute(
        """
        SELECT id, created_at, action_type, status, summary, details_json
        FROM software_perf_actions
        ORDER BY id DESC
        LIMIT ?
        """,
        (lim,),
    ).fetchall()
    out = []
    for r in rows:
        item = dict(r)
        item["details"] = core._safe_json_loads(item.get("details_json"), {})
        out.append(item)
    return out


def run_software_perf_agent_once(max_runtime_sec=60, force=False):
    core = _core()
    with core.db_connect() as conn:
        cfg = get_software_perf_agent_config(conn)
    if not cfg.get("enabled") and not force:
        return {"ok": True, "skipped": "disabled", "config": cfg}

    last_run_at = str(cfg.get("last_run_at") or "")
    if last_run_at and not force:
        age = _iso_age_seconds(last_run_at)
        if age is not None and age < int(cfg.get("interval_seconds", core.SOFTWARE_PERF_AGENT_INTERVAL_DEFAULT_SEC)):
            return {"ok": True, "skipped": "interval_not_elapsed", "seconds_since_last_run": round(age, 3)}

    t0 = time.time()
    runtime_cap = max(10, int(core.parse_float(max_runtime_sec, 60)))
    actions = []
    errors = []
    draft_path = ""
    core_guard = {"ok": True, "drift": {}}
    cleanup = {}
    improvement = {}

    cleanup_age = _iso_age_seconds(cfg.get("last_cleanup_at"))
    cleanup_due = force or cleanup_age is None or cleanup_age >= SOFTWARE_PERF_CLEANUP_INTERVAL_SEC
    if cleanup_due:
        try:
            with core.db_connect() as conn:
                cleanup = _software_perf_cleanup_stale_logs(conn, retention_days=cfg.get("retention_days"))
                _software_perf_log_action(
                    conn,
                    action_type="cleanup",
                    status="ok",
                    summary="Ran weekly stale-log cleanup for software-performance history.",
                    details=cleanup,
                )
                conn.commit()
            set_software_perf_agent_config(last_cleanup_at=core.now_iso())
            actions.append("weekly_stale_log_cleanup")
        except Exception as ex:
            errors.append(f"cleanup_failed:{str(ex)}")

    with core.db_connect() as conn:
        core_before = _software_perf_core_snapshot(conn)
        before = _software_perf_collect_snapshot(conn, persist=True)
        conn.commit()

    needs_heal = bool(
        int(core.parse_float(before.get("live_stale_symbols"), 0.0)) > 0
        or int(core.parse_float(before.get("live_missing_price_symbols"), 0.0)) > 0
        or int(core.parse_float(before.get("live_zero_ltp_symbols"), 0.0)) > 0
        or int(core.parse_float(before.get("issue_count"), 0.0)) > 0
    )
    last_heal_age = _iso_age_seconds(cfg.get("last_heal_at"))
    heal_cooldown = max(120, min(3600, int(core.parse_float(cfg.get("interval_seconds"), 900) // 2)))
    can_heal_now = (last_heal_age is None) or (last_heal_age >= heal_cooldown) or force

    if needs_heal and can_heal_now and (time.time() - t0) < runtime_cap:
        fix = core.apply_data_pipe_fixes()
        actions.extend([str(x) for x in fix.get("actions", [])])
        errors.extend([str(x) for x in fix.get("errors", [])])
        set_software_perf_agent_config(last_heal_at=core.now_iso())
        with core.db_connect() as conn:
            _software_perf_log_action(
                conn,
                action_type="self_heal",
                status="ok" if not fix.get("errors") else "partial",
                summary="Applied data-pipe self-healing actions.",
                details=fix,
            )
            conn.commit()

    with core.db_connect() as conn:
        after = _software_perf_collect_snapshot(conn, persist=True)
        core_after = _software_perf_core_snapshot(conn)
        core_guard = _software_perf_core_guard(core_before, core_after)
        if not core_guard.get("ok"):
            _software_perf_log_action(
                conn,
                action_type="core_guard",
                status="error",
                summary="Core objective guard detected protected-count drift.",
                details=core_guard,
            )
            errors.append("core_objective_guard_triggered")
        conn.commit()

    improve_age = _iso_age_seconds(cfg.get("last_improvement_at"))
    improve_due = improve_age is None or improve_age >= (6 * 60 * 60)
    if cfg.get("auto_tune") and cfg.get("write_changes") and (time.time() - t0) < runtime_cap and improve_due and (
        needs_heal or int(core.parse_float(after.get("issue_count"), 0.0)) > 0 or force
    ):
        improvement = _software_perf_generate_local_proposal(
            before,
            after,
            cfg,
            actions=actions,
            errors=errors,
            core_guard=core_guard,
        )
        details = {
            "analysis_engine": str((improvement or {}).get("analysis_engine") or "local_rules"),
            "hosted_llm_support": dict((improvement or {}).get("hosted_llm_support") or {}),
            "suggested_live_config_updates": dict((improvement or {}).get("suggested_live_config_updates") or {}),
            "suggested_code_changes": len((improvement or {}).get("suggested_code_changes") or []),
        }
        try:
            draft_path = _software_perf_write_improvement_draft(improvement, cfg)
            actions.append("write_improvement_draft")
            set_software_perf_agent_config(last_improvement_at=core.now_iso())
            with core.db_connect() as conn:
                _software_perf_log_action(
                    conn,
                    action_type="write_draft",
                    status="ok",
                    summary=f"Wrote {details['analysis_engine']} software-improvement draft file.",
                    details={**details, "path": draft_path},
                )
                conn.commit()
        except Exception as ex:
            errors.append(f"write_draft_failed:{str(ex)}")

    set_software_perf_agent_config(last_run_at=core.now_iso())
    return {
        "ok": len(errors) == 0,
        "agent": "software_performance",
        "core_guard_ok": bool(core_guard.get("ok")),
        "core_guard": core_guard,
        "actions": actions,
        "errors": errors,
        "tune_updates": {},
        "draft_path": draft_path,
        "cleanup": cleanup,
        "improvement": improvement,
        "before": before,
        "after": after,
    }


def maybe_run_software_perf_agent_once():
    core = _core()
    with core.db_connect() as conn:
        cfg = get_software_perf_agent_config(conn)
    if not cfg.get("enabled"):
        return None
    last = str(cfg.get("last_run_at") or "").strip()
    if last:
        age = _iso_age_seconds(last)
        if age is not None and age < int(cfg.get("interval_seconds", core.SOFTWARE_PERF_AGENT_INTERVAL_DEFAULT_SEC)):
            return None
    return run_software_perf_agent_once(max_runtime_sec=60, force=False)


def software_performance_worker(stop_event):
    core = _core()
    while not stop_event.is_set():
        sleep_for = core.SOFTWARE_PERF_AGENT_INTERVAL_DEFAULT_SEC
        try:
            with core.db_connect() as conn:
                cfg = get_software_perf_agent_config(conn)
            sleep_for = int(cfg.get("interval_seconds", core.SOFTWARE_PERF_AGENT_INTERVAL_DEFAULT_SEC))
            if cfg.get("enabled"):
                maybe_run_software_perf_agent_once()
        except Exception:
            pass
        stop_event.wait(timeout=max(core.SOFTWARE_PERF_AGENT_MIN_INTERVAL_SEC, int(sleep_for)))
