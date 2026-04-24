const $ = (id) => document.getElementById(id);
const LOCAL_MUTATION_HEADER = "X-Portfolio-Agent-Local";

const state = {
  symbols: [],
  selectedSymbol: null,
  selectedAssetClass: "EQUITY",
  strategySets: [],
  activeSetId: null,
  liveTimer: null,
  currentTab: "dashboard",
  holdingsRaw: [],
  tradesRaw: [],
  peakRaw: [],
  splitsRaw: [],
  cashflowRaw: [],
  dividendRaw: [],
  approvalsRaw: [],
  agentsRaw: [],
  softwarePerfSnapshotRaw: [],
  softwarePerfActionsRaw: [],
  softwarePerfIssueRows: [],
  intelSummary: null,
  backtestHistory: [],
  lastBacktestResult: null,
  holdingsSort: { key: "market_value", dir: "desc" },
  strategyInsights: null,
  tsPointsRaw: [],
  tsView: { start: 0, end: 0 },
  strategyProjView: { start: 0, end: 0 },
  splitChartView: { start: 0, end: 0 },
  skippedTradeItems: [],
  skippedOverrideAdded: 0,
  pendingPeakSplitCandidates: [],
  peakSplitPromptKey: "",
  tenantsRaw: [],
  activeTenant: "default",
  approvalVerification: null,
  rebalancePlanRaw: [],
  rebalancePlanMeta: null,
  rebalanceClosedHistoryRaw: [],
  assetSplitRaw: [],
  latestPriceUpdatedAt: "",
  harvestPlanRaw: null,
  harvestPlanMeta: null,
  strategyAuditRaw: null,
  attentionRaw: null,
  dailyTargetPlanRaw: null,
  dailyTargetHistoryRaw: [],
  dailyTargetDrafts: {},
};

const money = (n) =>
  Number(n || 0).toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
const pct = (n) => `${Number(n || 0).toFixed(2)}%`;
const pctWeight = (n) => `${(Number(n || 0) * 100).toFixed(2)}%`;
const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
const clsBySign = (v) => (Number(v) >= 0 ? "pos" : "neg");
const HOLDING_QTY_EPS = 1e-6;
const secToLabel = (sec) => {
  const n = Number(sec || 0);
  if (!n || n <= 0) return "-";
  if (n % 86400 === 0) return `${n / 86400}d`;
  if (n % 3600 === 0) return `${n / 3600}h`;
  if (n % 60 === 0) return `${n / 60}m`;
  return `${n}s`;
};
const looksLikeGoldSymbol = (symbol) => /\b(GOLD|SGB|SILVER)\b/i.test(String(symbol || "").trim());
const normalizeAssetClass = (raw, symbol = "") => {
  const s = String(raw || "").trim().toUpperCase();
  if (s === "GOLD") return "GOLD";
  if (looksLikeGoldSymbol(symbol)) return "GOLD";
  return "EQUITY";
};

function updateTradeUnitLabels(assetClass, symbol = "") {
  const isGold = normalizeAssetClass(assetClass, symbol) === "GOLD";
  if ($("tradeAddQtyLabel")) $("tradeAddQtyLabel").textContent = isGold ? "Qty (gms)" : "Qty";
  if ($("tradeAddPriceLabel")) $("tradeAddPriceLabel").textContent = isGold ? "Price (/gm)" : "Price";
  if ($("tradesQtyHeader")) $("tradesQtyHeader").textContent = isGold ? "Qty (gms)" : "Qty";
  if ($("tradesPriceHeader")) $("tradesPriceHeader").textContent = isGold ? "Price (/gm)" : "Price";
  if ($("tradesAmountHeader")) $("tradesAmountHeader").textContent = "Amount";
  if ($("tradesLtpHeader")) $("tradesLtpHeader").textContent = isGold ? "Ref LTP (/gm)" : "Ref LTP";
  if ($("sellSimQtyLabel")) $("sellSimQtyLabel").textContent = isGold ? "Qty To Sell (gms)" : "Qty To Sell";
  if ($("sellSimPriceLabel")) $("sellSimPriceLabel").textContent = isGold ? "Sell Price (/gm)" : "Sell Price";
}

async function api(path, opts = {}) {
  const headers = {
    "Content-Type": "application/json",
    [LOCAL_MUTATION_HEADER]: "1",
    ...(opts.headers || {}),
  };
  const res = await fetch(path, {
    ...opts,
    headers,
  });
  if (!res.ok) {
    const raw = await res.text();
    let parsed = null;
    try {
      parsed = JSON.parse(raw);
    } catch {
      parsed = null;
    }
    const err = new Error((parsed && (parsed.message || parsed.error)) || raw || `HTTP ${res.status}`);
    err.status = Number(res.status || 0);
    err.code = String((parsed && (parsed.code || parsed.error)) || `HTTP_${res.status}`);
    err.reason = String((parsed && (parsed.message || parsed.error)) || raw || err.message || "Request failed");
    err.payload = parsed;
    throw err;
  }
  return res.json();
}

function arrayBufferToBase64(buffer) {
  const bytes = new Uint8Array(buffer);
  const chunk = 0x8000;
  let binary = "";
  for (let i = 0; i < bytes.length; i += chunk) {
    const slice = bytes.subarray(i, i + chunk);
    binary += String.fromCharCode(...slice);
  }
  return btoa(binary);
}

function escapeHtml(s) {
  return String(s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

const boundButtonIds = new Set();

function normalizeUiError(error, fallbackCode = "UI_UNKNOWN_ERROR") {
  const statusVal = Number(error?.status || 0);
  const status = Number.isFinite(statusVal) && statusVal > 0 ? statusVal : null;
  const rawCode = String(error?.code || fallbackCode || "UI_UNKNOWN_ERROR")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9_:-]/g, "_");
  const code = rawCode || String(fallbackCode || "UI_UNKNOWN_ERROR").toUpperCase();
  const reason = String(error?.reason || error?.message || "Unknown error").trim() || "Unknown error";
  return { code, reason, status };
}

function showUiNotification(kind, title, message, options = {}) {
  const host = $("uiNotifier");
  if (!host) return;
  const type = String(kind || "info").toLowerCase();
  const note = document.createElement("div");
  note.className = `ui-note ${type}`;
  const code = String(options.code || "").trim();
  const status = Number(options.status || 0);
  const suffix = [
    code ? `Code: ${code}` : "",
    status > 0 ? `HTTP: ${status}` : "",
  ]
    .filter(Boolean)
    .join(" | ");
  note.innerHTML = `
    <div class="ui-note-title">${escapeHtml(String(title || "Notification"))}</div>
    <div class="ui-note-message">${escapeHtml(String(message || ""))}</div>
    ${suffix ? `<div class="ui-note-meta">${escapeHtml(suffix)}</div>` : ""}
  `;
  host.prepend(note);
  while (host.children.length > 6) host.removeChild(host.lastChild);
  const timeoutMs = Math.max(2500, Math.min(20000, Number(options.timeoutMs || 8000)));
  window.setTimeout(() => {
    if (note.parentElement === host) host.removeChild(note);
  }, timeoutMs);
}

function notifyActionFailure(actionName, error, fallbackCode = "UI_ACTION_FAILED") {
  const meta = normalizeUiError(error, fallbackCode);
  showUiNotification(
    "error",
    `${actionName} failed`,
    meta.reason,
    { code: meta.code, status: meta.status, timeoutMs: 10000 }
  );
}

function registerButton(buttonId, handler, options = {}) {
  const el = $(buttonId);
  if (!el) {
    console.warn(`button_not_found:${buttonId}`);
    return;
  }
  boundButtonIds.add(String(buttonId));
  const actionName = String(options.actionName || buttonId);
  const fallbackCode = String(options.errorCode || "UI_BUTTON_ACTION_FAILED");
  el.addEventListener("click", async (evt) => {
    try {
      await handler(evt);
    } catch (error) {
      notifyActionFailure(actionName, error, fallbackCode);
    }
  });
}

function verifyButtonBindings() {
  const ids = Array.from(document.querySelectorAll("button[id]"))
    .map((el) => String(el.id || "").trim())
    .filter(Boolean)
    .sort();
  const missing = ids.filter((id) => !boundButtonIds.has(id));
  if (missing.length) {
    const preview = missing.slice(0, 8).join(", ");
    const suffix = missing.length > 8 ? ", ..." : "";
    const reason = `Missing handlers for ${missing.length} button(s): ${preview}${suffix}`;
    console.error(`ui_button_binding_missing:${reason}`);
    showUiNotification("error", "UI Binding Check Failed", reason, {
      code: "UI_BTN_BIND_MISSING",
      timeoutMs: 12000,
    });
  }
  return missing;
}

function numOrNull(v) {
  if (v === null || typeof v === "undefined") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function appendAssistantMsg(role, text) {
  const box = $("assistantChatLog");
  if (!box) return;
  const div = document.createElement("div");
  div.className = `assistant-msg ${role === "user" ? "user" : "bot"}`;
  div.innerHTML = escapeHtml(text).replace(/\n/g, "<br>");
  box.appendChild(div);
  box.scrollTop = box.scrollHeight;
}

function assistantReplyText(res) {
  const lines = [];
  if (res && res.message) lines.push(String(res.message));
  if (Array.isArray(res?.query_catalog) && res.query_catalog.length) {
    lines.push("Suggested queries:");
    res.query_catalog.slice(0, 20).forEach((q) => {
      lines.push(`- ${q.query}: ${q.explanation}`);
    });
  }
  if (res?.intent === "cash_balance_explain" && res.cash_breakdown) {
    const c = res.cash_breakdown;
    lines.push(`Entries: ${c.entries}`);
    lines.push(`Deposits: ${Number(c.deposits_total || 0).toFixed(2)}`);
    lines.push(`Withdrawals: ${Number(c.withdrawals_total || 0).toFixed(2)}`);
    if (typeof c.net_hand_investment_total !== "undefined") {
      lines.push(`Investment: ${Number(c.net_hand_investment_total || 0).toFixed(2)}`);
    }
    if (typeof c.trade_credit_total !== "undefined") {
      lines.push(`Trade Credits: ${Number(c.trade_credit_total || 0).toFixed(2)}`);
    }
    if (typeof c.investment_spend_total !== "undefined") {
      lines.push(`Market Deployment (Settlement Debits): ${Number(c.investment_spend_total || 0).toFixed(2)}`);
    }
    if (typeof c.charges_total !== "undefined") {
      lines.push(`Charges: ${Number(c.charges_total || 0).toFixed(2)}`);
    }
    lines.push(`Cash Balance: ${Number(c.cash_balance || 0).toFixed(2)}`);
    if (res.formula) lines.push(`Formula: ${res.formula}`);
    if (res.note) lines.push(`Note: ${res.note}`);
  }
  if (res?.intent === "metric_explain" && res.explanation) {
    lines.push(`Formula: ${res.explanation}`);
  }
  if (res?.intent === "portfolio_summary" && res.summary) {
    lines.push(`As of: ${res.summary.as_of}`);
  }
  if (res?.intent === "price_status" && res.price_status) {
    lines.push(`Latest price time: ${res.price_status.updated_at || "N/A"}`);
  }
  if (res?.intent === "software_performance_status" && res.latest) {
    const s = res.latest;
    lines.push(
      `Software performance: issues=${Number(s.issue_count || 0)}, stale=${Number(s.live_stale_symbols || 0)}, missing=${Number(s.live_missing_price_symbols || 0)}, weak_sources=${Number(s.weak_sources_count || 0)}`
    );
    if (res.run_result) {
      lines.push(`Run result: actions=${(res.run_result.actions || []).length}, errors=${(res.run_result.errors || []).length}`);
    }
  }
  if (Array.isArray(res?.upload_batches) && res.upload_batches.length) {
    lines.push("Upload batches:");
    res.upload_batches.slice(0, 6).forEach((b) => {
      lines.push(`- ${b.notes}: trades=${b.trades}, scrips=${b.scrips}, ${b.from_date} to ${b.to_date}`);
    });
  }
  if (Array.isArray(res?.trade_id_duplicates) && res.trade_id_duplicates.length) {
    lines.push("Trade-ID duplicates:");
    res.trade_id_duplicates.slice(0, 6).forEach((d) => {
      lines.push(`- ${d.external_trade_id}: ${d.duplicates}`);
    });
  }
  if (Array.isArray(res?.value_duplicates) && res.value_duplicates.length) {
    lines.push("Value/date duplicates:");
    res.value_duplicates.slice(0, 6).forEach((d) => {
      lines.push(`- ${d.symbol} ${d.side} ${d.trade_date} qty=${d.quantity} price=${d.price} x${d.duplicates}`);
    });
  }
  if (Array.isArray(res?.items) && res.items.length && res.intent === "top_movers") {
    lines.push(`Top movers (${res.metric}):`);
    res.items.slice(0, 8).forEach((i) => {
      lines.push(`- ${i.symbol}: ${Number(i[res.metric] || 0).toFixed(2)}%`);
    });
  }
  if (res?.intent === "strategy_summary" && res.counts) {
    lines.push(
      `Strategy mix: TRIM=${res.counts.TRIM || 0}, ADD=${res.counts.ADD || 0}, HOLD=${res.counts.HOLD || 0}, REVIEW=${res.counts.REVIEW || 0}, WATCH_ADD=${res.counts.WATCH_ADD || 0}`
    );
    if (res.macro) {
      lines.push(
        `Macro: ${String(res.macro.regime || "neutral").toUpperCase()} score=${Number(res.macro.score || 0).toFixed(2)}`
      );
    }
    if (Array.isArray(res.items) && res.items.length) {
      lines.push("Top strategy actions:");
      res.items.slice(0, 8).forEach((i) => {
        lines.push(`- ${i.symbol}: ${i.action} @ ${money(i.price_now)} (${(Number(i.confidence || 0) * 100).toFixed(1)}%)`);
      });
    }
  }
  if (res?.intent === "strategy_projection" && Array.isArray(res?.projection_summary)) {
    lines.push("Projection summary:");
    res.projection_summary.forEach((p) => {
      lines.push(
        `- ${p.scenario}: year ${p.year_offset} value ${money(p.projected_value)} (ann ${pctWeight(p.annual_return)})`
      );
    });
  }
  if (res?.intent === "intel_summary") {
    lines.push(
      `Intelligence overlay: ${Number(res.portfolio_score || 0).toFixed(2)} ` +
      `(confidence ${(Number(res.portfolio_confidence || 0) * 100).toFixed(1)}%)`
    );
    if (Array.isArray(res.top_symbols) && res.top_symbols.length) {
      lines.push("Top symbol intelligence impacts:");
      res.top_symbols.slice(0, 8).forEach((s) => {
        lines.push(`- ${s.symbol}: ${Number(s.score || 0).toFixed(2)} (${(Number(s.confidence || 0) * 100).toFixed(1)}%)`);
      });
    }
    if (Array.isArray(res.cross_flows) && res.cross_flows.length) {
      lines.push("Cross-company flow links:");
      res.cross_flows.slice(0, 6).forEach((f) => {
        lines.push(`- ${f.from_symbol} -> ${f.to_symbol}: ${Number(f.flow_score || 0).toFixed(3)} (${f.period || ""})`);
      });
    }
  }
  if (res?.intent === "chart_summary") {
    if (Array.isArray(res.items) && res.items.length) {
      lines.push("Top chart signals:");
      res.items.slice(0, 8).forEach((s) => {
        lines.push(
          `- ${s.symbol}: ${String(s.signal || "NEUTRAL")} score ${Number(s.score || 0).toFixed(2)} ` +
          `(conf ${(Number(s.confidence || 0) * 100).toFixed(1)}%)`
        );
      });
    }
    if (res.run_result) {
      lines.push(
        `Chart run: updated=${Number(res.run_result.updated || 0)}, symbols=${Number(res.run_result.symbols_considered || 0)}`
      );
    }
  }
  if (res?.intent === "intel_symbol" && res.item) {
    lines.push(
      `${res.symbol}: score ${Number(res.item.score || 0).toFixed(2)}, ` +
      `confidence ${(Number(res.item.confidence || 0) * 100).toFixed(1)}%`
    );
    if (res.item.summary) lines.push(`Drivers: ${res.item.summary}`);
  }
  if (res?.intent === "intel_autopilot_status" && res.config) {
    const c = res.config;
    lines.push(
      `Autopilot: enabled=${!!c.enabled}, interval=${Number(c.interval_seconds || 0)}s, ` +
      `max_docs=${Number(c.max_docs || 0)}, symbols_limit=${Number(c.symbols_limit || 0)}`
    );
    if (res.run_result) {
      lines.push(
        `Run result: inserted=${Number(res.run_result.inserted_docs || 0)}, ` +
        `skipped=${Number(res.run_result.skipped_docs || 0)}`
      );
      const fc = res.run_result.online_financial_collection || {};
      if (fc && (Number(fc.inserted_financial_rows || 0) > 0 || Number(fc.updated_financial_rows || 0) > 0 || Number(fc.inserted_docs || 0) > 0)) {
        lines.push(
          `Online financial collection: inserted_rows=${Number(fc.inserted_financial_rows || 0)}, ` +
          `updated_rows=${Number(fc.updated_financial_rows || 0)}, inserted_docs=${Number(fc.inserted_docs || 0)}`
        );
      }
    }
  }
  if (res?.intent === "cashflow_summary" && res.cashflow_summary) {
    const s = res.cashflow_summary;
    lines.push(
      `Cashflow: entries=${s.entries}, hand_investment=${money(s.net_hand_investment_total)}, deposits=${money(s.deposits_total)}, withdrawals=${money(s.withdrawals_total)}, balance=${money(s.cash_balance)}`
    );
  }
  if (res?.intent === "cashflow_duplicates") {
    const a = Array.isArray(res.cashflow_id_duplicates) ? res.cashflow_id_duplicates : [];
    const b = Array.isArray(res.cashflow_value_duplicates) ? res.cashflow_value_duplicates : [];
    lines.push(`Cashflow duplicate groups: id=${a.length}, value/date=${b.length}`);
  }
  if (res?.intent === "approval_required" && res.approval) {
    lines.push(
      `Approval required: request #${res.approval.id} for ${res.approval.action_type || "action"}.`
    );
    if (typeof res.affected_trades === "number") {
      lines.push(`Affected trades: ${res.affected_trades}`);
    }
  }
  if (res?.intent === "approvals_list") {
    const items = Array.isArray(res.items) ? res.items : [];
    lines.push(`Pending approvals: ${items.length}`);
    items.slice(0, 8).forEach((i) => {
      lines.push(`- #${i.id} ${i.action_type}: ${i.summary || ""}`);
    });
  }
  if (res && res.executed && typeof res.deleted_trades === "number") {
    lines.push(`Deleted trades: ${res.deleted_trades}`);
    if (Array.isArray(res.deleted_symbols) && res.deleted_symbols.length) {
      lines.push(`Scrips impacted: ${res.deleted_symbols.join(", ")}`);
    }
  } else if (res && typeof res.affected_trades === "number") {
    lines.push(`Matching trades: ${res.affected_trades}`);
    if (Array.isArray(res.affected_symbols) && res.affected_symbols.length) {
      lines.push(`Scrips impacted: ${res.affected_symbols.join(", ")}`);
    }
  }
  if (Array.isArray(res?.notes_examples) && res.notes_examples.length) {
    lines.push(`Notes examples: ${res.notes_examples.join(" | ")}`);
  }
  return lines.join("\n");
}

async function sendAssistantChat() {
  const input = $("assistantChatInput");
  if (!input) return;
  const message = (input.value || "").trim();
  if (!message) return;
  appendAssistantMsg("user", message);
  input.value = "";
  try {
    const res = await api("/api/v1/assistant/chat", {
      method: "POST",
      body: JSON.stringify({ message }),
    });
    appendAssistantMsg("bot", assistantReplyText(res) || "Done.");
    await loadAssistantApprovals();
    await loadApprovalsTab();
    await loadApprovalVerification();
    if (res.executed && Number(res.deleted_trades || 0) > 0) {
      await loadDashboard();
      await loadPeakDiff();
      await loadSplits();
      if (state.selectedSymbol) {
        try {
          await loadScrip(state.selectedSymbol);
        } catch {
          // symbol may no longer have trades after cleanup
        }
      }
    }
  } catch (e) {
    appendAssistantMsg("bot", `Command failed: ${e.message}`);
  }
}

function renderAssistantApprovals(items, pendingCount) {
  state.approvalsRaw = items || [];
  $("approvalPendingCount").textContent = `Pending approvals: ${Number(pendingCount || 0)}`;
  $("approvalsTable").querySelector("tbody").innerHTML = state.approvalsRaw
    .map((a) => {
      const status = String(a.status || "").toUpperCase();
      const isPending = String(a.status || "").toLowerCase() === "pending";
      const query = String(a.query_text || "");
      const qShort = query.length > 90 ? `${query.slice(0, 90)}...` : query;
      return `
      <tr>
        <td>${a.id}</td>
        <td>${a.created_at || ""}</td>
        <td>${status}</td>
        <td>${a.action_type || ""}</td>
        <td>${a.summary || ""}</td>
        <td title="${escapeHtml(query)}">${escapeHtml(qShort)}</td>
        <td>${
          isPending
            ? `<button class="btn secondary approval-approve-btn" data-id="${a.id}">Approve</button>
               <button class="btn secondary approval-reject-btn" data-id="${a.id}">Reject</button>`
            : "<span class='metric'>-</span>"
        }</td>
      </tr>`;
    })
    .join("");

  document.querySelectorAll(".approval-approve-btn").forEach((b) => {
    b.addEventListener("click", () => decideAssistantApproval(Number(b.getAttribute("data-id")), "approve"));
  });
  document.querySelectorAll(".approval-reject-btn").forEach((b) => {
    b.addEventListener("click", () => decideAssistantApproval(Number(b.getAttribute("data-id")), "reject"));
  });
}

async function loadAssistantApprovals() {
  try {
    const res = await api("/api/v1/assistant/approvals?status=pending&limit=100");
    renderAssistantApprovals(res.items || [], Number(res.pending_count || 0));
  } catch (e) {
    console.error("approval load failed", e);
  }
}

async function decideAssistantApproval(approvalId, decision) {
  const action = decision === "approve" ? "Approve" : "Reject";
  if (!confirm(`${action} request #${approvalId}?`)) return;
  const res = await api(`/api/v1/assistant/approvals/${approvalId}/decision`, {
    method: "POST",
    body: JSON.stringify({ decision }),
  });
  appendAssistantMsg("bot", assistantReplyText(res) || `Request #${approvalId} ${decision}d.`);
  await loadAssistantApprovals();
  await loadApprovalsTab();
  await loadApprovalVerification();
  if (res.executed && Number(res.deleted_trades || 0) > 0) {
    await loadDashboard();
    await loadPeakDiff();
    await loadSplits();
    if (state.selectedSymbol) {
      try {
        await loadScrip(state.selectedSymbol);
      } catch {}
    }
  }
}

function formatVerificationValue(v) {
  if (v === null || typeof v === "undefined" || String(v) === "") return "-";
  if (typeof v === "number") {
    if (!Number.isFinite(v)) return "-";
    return Number.isInteger(v) ? String(v) : String(Number(v.toFixed(6)));
  }
  if (typeof v === "boolean") return v ? "true" : "false";
  return String(v);
}

function renderApprovalsTab(items, pendingCount) {
  const rows = Array.isArray(items) ? items : [];
  const pending = Number(pendingCount || 0);
  const pendingEl = $("approvalTabPendingCount");
  if (pendingEl) pendingEl.textContent = `Pending approvals: ${pending}`;
  const table = $("approvalsTabTable");
  if (!table) return;
  table.querySelector("tbody").innerHTML = rows.length
    ? rows
        .map((a) => {
          const status = String(a.status || "").toLowerCase();
          const statusLabel = status.toUpperCase() || "-";
          const isPending = status === "pending";
          const query = String(a.query_text || "");
          const qShort = query.length > 90 ? `${query.slice(0, 90)}...` : query;
          return `
            <tr>
              <td>${a.id}</td>
              <td>${a.created_at || "-"}</td>
              <td>${escapeHtml(statusLabel)}</td>
              <td>${escapeHtml(a.action_type || "-")}</td>
              <td title="${escapeHtml(a.summary || "")}">${escapeHtml(a.summary || "-")}</td>
              <td title="${escapeHtml(query)}">${escapeHtml(qShort)}</td>
              <td>${
                isPending
                  ? `<button class="btn secondary approvaltab-approve-btn" data-id="${a.id}">Approve</button>
                     <button class="btn secondary approvaltab-reject-btn" data-id="${a.id}">Reject</button>`
                  : `<span class="metric-inline">${escapeHtml(a.decided_at || a.executed_at || "-")}</span>`
              }</td>
            </tr>
          `;
        })
        .join("")
    : '<tr><td colspan="7">No approvals found.</td></tr>';

  document.querySelectorAll(".approvaltab-approve-btn").forEach((b) => {
    b.addEventListener("click", () => decideAssistantApproval(Number(b.getAttribute("data-id")), "approve"));
  });
  document.querySelectorAll(".approvaltab-reject-btn").forEach((b) => {
    b.addEventListener("click", () => decideAssistantApproval(Number(b.getAttribute("data-id")), "reject"));
  });
}

async function loadApprovalsTab() {
  try {
    const res = await api("/api/v1/assistant/approvals?limit=200");
    renderApprovalsTab(res.items || [], Number(res.pending_count || 0));
  } catch (e) {
    console.error("approvals tab load failed", e);
    const table = $("approvalsTabTable");
    if (table) table.querySelector("tbody").innerHTML = '<tr><td colspan="7">Failed to load approvals.</td></tr>';
  }
}

function renderApprovalVerification(payload) {
  state.approvalVerification = payload || null;
  const p = payload || {};
  const approvals = p.approvals || {};
  const counts = approvals.counts || {};
  const perf = p.software_performance || {};
  const summary = perf.summary || {};
  const draft = perf.latest_draft || {};
  const checks = Array.isArray(perf.verification_checks) ? perf.verification_checks : [];
  const changeSummary = Array.isArray(p.change_summary) ? p.change_summary : [];

  const stamp = $("approvalVerifyStamp");
  if (stamp) {
    stamp.textContent = `Verification refreshed: ${new Date().toLocaleString()}`;
  }

  const summaryEl = $("approvalVerifySummary");
  if (summaryEl) {
    summaryEl.innerHTML = `
      <div class="metric">Pending: ${Number(approvals.pending_count || 0)}</div>
      <div class="metric">Approved: ${Number(counts.approved || 0)}</div>
      <div class="metric">Executed: ${Number(counts.executed || 0)}</div>
      <div class="metric">Rejected: ${Number(counts.rejected || 0)}</div>
      <div class="metric">Draft Parsed: ${draft.proposal_loaded ? "Yes" : "No"}</div>
      <div class="metric">Draft At: ${draft.generated_at || "-"}</div>
      <div class="metric">Draft Path: ${escapeHtml(draft.path || "-")}</div>
      <div class="metric">Draft Updates Applied: ${Number(summary.draft_updates_applied || 0)} / ${Number(summary.draft_updates_total || 0)}</div>
      <div class="metric">Auto Updates Applied: ${Number(summary.auto_updates_applied || 0)} / ${Number(summary.auto_updates_total || 0)}</div>
    `;
  }

  const changeTable = $("approvalChangeSummaryTable");
  if (changeTable) {
    changeTable.querySelector("tbody").innerHTML = changeSummary.length
      ? changeSummary
          .map(
            (r) => `
            <tr>
              <td>${r.created_at || "-"}</td>
              <td>${escapeHtml(r.source || "-")}</td>
              <td>${escapeHtml(String(r.status || "-").toUpperCase())}</td>
              <td title="${escapeHtml(r.summary || "")}">${escapeHtml(r.summary || "-")}</td>
              <td title="${escapeHtml(r.details || "")}">${escapeHtml(r.details || "-")}</td>
            </tr>
          `
          )
          .join("")
      : '<tr><td colspan="5">No software-performance changes logged yet.</td></tr>';
  }

  const checksTable = $("approvalVerifyChecksTable");
  if (checksTable) {
    checksTable.querySelector("tbody").innerHTML = checks.length
      ? checks
          .map(
            (r) => `
            <tr>
              <td>${escapeHtml(r.source || "-")}</td>
              <td>${escapeHtml(r.key || "-")}</td>
              <td>${escapeHtml(formatVerificationValue(r.expected))}</td>
              <td>${escapeHtml(formatVerificationValue(r.current))}</td>
              <td><span class="${r.applied ? "pos" : "neg"}">${r.applied ? "YES" : "NO"}</span></td>
            </tr>
          `
          )
          .join("")
      : '<tr><td colspan="5">No verification checks available.</td></tr>';
  }
}

async function loadApprovalVerification(options = {}) {
  const throwOnError = !!options.throwOnError;
  try {
    const res = await api("/api/v1/assistant/verification?approval_limit=200&action_limit=220");
    renderApprovalVerification(res || {});
    const approvals = res?.approvals || {};
    renderApprovalsTab(approvals.items || [], Number(approvals.pending_count || 0));
  } catch (e) {
    console.error("approval verification load failed", e);
    const meta = normalizeUiError(e, "APPROVAL_VERIFICATION_LOAD_FAILED");
    const stamp = $("approvalVerifyStamp");
    if (stamp) stamp.textContent = `Verification error: ${meta.reason}`;
    const summaryEl = $("approvalVerifySummary");
    if (summaryEl) summaryEl.innerHTML = `<div class="metric neg">Failed to load verification: ${escapeHtml(meta.reason)}</div>`;
    const changeTable = $("approvalChangeSummaryTable");
    if (changeTable) changeTable.querySelector("tbody").innerHTML = '<tr><td colspan="5">Failed to load change summary.</td></tr>';
    const checksTable = $("approvalVerifyChecksTable");
    if (checksTable) checksTable.querySelector("tbody").innerHTML = '<tr><td colspan="5">Failed to load verification checks.</td></tr>';
    if (throwOnError) throw e;
  }
}

function renderAgents(items) {
  state.agentsRaw = items || [];
  $("agentsTable").querySelector("tbody").innerHTML = state.agentsRaw
    .map((a) => {
      const enabled = !!a.enabled;
      const badgeCls = enabled ? "pos" : "neg";
      const badgeText = enabled ? "Enabled" : "Paused";
      return `
      <tr>
        <td>${escapeHtml(a.label || a.agent || "")}</td>
        <td><span class="${badgeCls}">${badgeText}</span></td>
        <td>${secToLabel(a.interval_seconds)}</td>
        <td>${a.last_run_at || "-"}</td>
        <td title="${escapeHtml(a.details || "")}">${escapeHtml(a.details || "")}</td>
        <td>
          <button class="btn secondary agent-toggle-btn" data-agent="${a.agent}" data-enable="${enabled ? "0" : "1"}">${enabled ? "Disable" : "Enable"}</button>
          <button class="btn secondary agent-run-btn" data-agent="${a.agent}">Run Now</button>
        </td>
      </tr>`;
    })
    .join("");

  document.querySelectorAll(".agent-toggle-btn").forEach((b) => {
    b.addEventListener("click", async () => {
      const agent = b.getAttribute("data-agent");
      const enable = b.getAttribute("data-enable") === "1";
      b.disabled = true;
      try {
        await api(`/api/v1/agents/${encodeURIComponent(agent)}/control`, {
          method: "PUT",
          body: JSON.stringify({ enabled: enable }),
        });
        await loadAgentStatus();
        if (agent === "market") {
          await loadLiveConfig();
        } else if (agent === "software_performance") {
          await loadSoftwarePerfLogs();
        } else if (agent === "tax_monitor") {
          await loadAttentionConsole().catch(() => {});
        }
      } catch (e) {
        alert(`Agent update failed: ${e.message}`);
      } finally {
        b.disabled = false;
      }
    });
  });

  document.querySelectorAll(".agent-run-btn").forEach((b) => {
    b.addEventListener("click", async () => {
      const agent = b.getAttribute("data-agent");
      b.disabled = true;
      b.textContent = "Running...";
      try {
        await api(`/api/v1/agents/${encodeURIComponent(agent)}/control`, {
          method: "PUT",
          body: JSON.stringify({ run_now: true }),
        });
        await loadAgentStatus();
        if (agent === "market") {
          await loadDashboard();
          await loadPeakDiff();
          await loadPriceStatus();
          if (state.selectedSymbol) await loadScrip(state.selectedSymbol);
        } else if (agent === "strategy") {
          await loadStrategyInsights(false);
          await loadDashboard();
        } else if (agent === "history") {
          await loadDashboard();
        } else if (agent === "self_learning") {
          await loadStrategyInsights(false);
          await loadDashboard();
          await loadIntelSummary();
        } else if (agent === "intel_autopilot") {
          await loadIntelSummary();
          await loadStrategyInsights(false);
          await loadDashboard();
        } else if (agent === "chart_intel") {
          await loadIntelSummary();
          await loadStrategyInsights(false);
          await loadDashboard();
        } else if (agent === "software_performance") {
          await loadDashboard();
          await loadPeakDiff();
          await loadPriceStatus();
          if (state.selectedSymbol) await loadScrip(state.selectedSymbol);
          await loadSoftwarePerfLogs();
        } else if (agent === "tax_monitor") {
          await loadAttentionConsole();
        }
      } catch (e) {
        alert(`Agent run failed: ${e.message}`);
      } finally {
        b.disabled = false;
        b.textContent = "Run Now";
      }
    });
  });
}

async function loadAgentStatus() {
  const res = await api("/api/v1/agents/status");
  renderAgents(res.items || []);
  $("agentsStatusStamp").textContent = `Last refreshed: ${new Date().toLocaleString()}`;
}

function normalizeSoftwareIssues(snapshot) {
  const notes = snapshot?.notes || {};
  const issues = Array.isArray(notes?.issues) ? notes.issues : [];
  return issues
    .map((x) => String(x || "").trim())
    .filter((x) => x.length > 0 && x.toLowerCase() !== "no critical data-pipe issues detected.");
}

function buildSoftwareIssueRows(snapshots) {
  const items = Array.isArray(snapshots) ? snapshots : [];
  if (!items.length) return [];
  const ordered = [...items].reverse();
  const map = new Map();
  ordered.forEach((snap) => {
    const at = String(snap?.created_at || "");
    const issueSet = new Set(normalizeSoftwareIssues(snap));
    issueSet.forEach((issue) => {
      const cur = map.get(issue) || {
        issue,
        first_seen: at,
        last_seen: at,
        occurrences: 0,
        status: "open",
        resolved_at: "",
      };
      if (!cur.first_seen) cur.first_seen = at;
      cur.last_seen = at;
      cur.occurrences += 1;
      map.set(issue, cur);
    });
  });
  const latestIssueSet = new Set(normalizeSoftwareIssues(items[0]));
  const latestAt = String(items[0]?.created_at || "");
  const out = Array.from(map.values()).map((r) => {
    const open = latestIssueSet.has(r.issue);
    return {
      ...r,
      status: open ? "open" : "resolved",
      resolved_at: open ? "" : latestAt,
    };
  });
  out.sort((a, b) => {
    if (a.status !== b.status) return a.status === "open" ? -1 : 1;
    return String(b.last_seen || "").localeCompare(String(a.last_seen || ""));
  });
  return out;
}

function summarizeSoftwareActionDetails(details) {
  if (!details || typeof details !== "object") return "-";
  const bits = [];
  if (Array.isArray(details.actions) && details.actions.length) {
    bits.push(`actions=${details.actions.join(", ")}`);
  }
  if (Array.isArray(details.errors) && details.errors.length) {
    bits.push(`errors=${details.errors.join("; ")}`);
  }
  if (details.updates && typeof details.updates === "object") {
    const kv = Object.entries(details.updates)
      .slice(0, 8)
      .map(([k, v]) => `${k}:${v}`);
    if (kv.length) bits.push(`updates=${kv.join(", ")}`);
  }
  if (details.suggested_live_config_updates && typeof details.suggested_live_config_updates === "object") {
    const kv = Object.entries(details.suggested_live_config_updates)
      .slice(0, 8)
      .map(([k, v]) => `${k}:${v}`);
    if (kv.length) bits.push(`suggested_updates=${kv.join(", ")}`);
  }
  if (typeof details.suggested_code_changes === "number") bits.push(`code_changes=${details.suggested_code_changes}`);
  if (details.error) bits.push(`error=${details.error}`);
  if (details.path) bits.push(`path=${details.path}`);
  if (details.drift && typeof details.drift === "object" && Object.keys(details.drift).length) {
    bits.push(`drift=${Object.keys(details.drift).join(", ")}`);
  }
  if (!bits.length) {
    try {
      return JSON.stringify(details);
    } catch {
      return "-";
    }
  }
  return bits.join(" | ");
}

function renderSoftwarePerfLogs(payload) {
  const snapshots = Array.isArray(payload?.snapshots) ? payload.snapshots : [];
  const actions = Array.isArray(payload?.actions) ? payload.actions : [];
  const latest = payload?.latest || snapshots[0] || null;
  const cfg = payload?.config || {};
  const issueRows = buildSoftwareIssueRows(snapshots);
  const openCount = issueRows.filter((r) => r.status === "open").length;
  const resolvedCount = issueRows.filter((r) => r.status === "resolved").length;
  state.softwarePerfSnapshotRaw = snapshots;
  state.softwarePerfActionsRaw = actions;
  state.softwarePerfIssueRows = issueRows;

  $("softwarePerfSummary").innerHTML = `
    <div class="metric">Enabled: ${cfg.enabled ? "Yes" : "No"}</div>
    <div class="metric">Interval: ${secToLabel(cfg.interval_seconds)}</div>
    <div class="metric">Last Run: ${cfg.last_run_at || "-"}</div>
    <div class="metric">Last Heal: ${cfg.last_heal_at || "-"}</div>
    <div class="metric">Last Improvement Draft: ${cfg.last_improvement_at || "-"}</div>
    <div class="metric">Last Cleanup: ${cfg.last_cleanup_at || "-"}</div>
    <div class="metric">Retention: ${Number(cfg.retention_days || 0)} day(s)</div>
    <div class="metric">Local Improvement: ${cfg.auto_tune ? "Enabled" : "Disabled"}</div>
    <div class="metric">Write Changes: ${cfg.write_changes ? "Yes" : "No"}</div>
    <div class="metric">Snapshots: ${snapshots.length}</div>
    <div class="metric">Open Issues: ${openCount}</div>
    <div class="metric">Resolved Issues: ${resolvedCount}</div>
    <div class="metric">Latest Issue Count: ${Number(latest?.issue_count || 0)}</div>
    <div class="metric">Latest Success Rate: ${(Number(latest?.quote_success_rate || 0) * 100).toFixed(1)}%</div>
  `;

  $("softwarePerfOpenIssuesTable").querySelector("tbody").innerHTML = issueRows.length
    ? issueRows
        .map((r) => `
          <tr>
            <td><span class="${r.status === "open" ? "neg" : "pos"}">${r.status.toUpperCase()}</span></td>
            <td title="${escapeHtml(r.issue)}">${escapeHtml(r.issue)}</td>
            <td>${r.first_seen || "-"}</td>
            <td>${r.last_seen || "-"}</td>
            <td>${r.resolved_at || "-"}</td>
            <td>${Number(r.occurrences || 0)}</td>
          </tr>
        `)
        .join("")
    : '<tr><td colspan="6">No issue lifecycle yet. Run the Software Performance Agent to create logs.</td></tr>';

  $("softwarePerfActionsTable").querySelector("tbody").innerHTML = actions.length
    ? actions
        .map((a) => {
          const detailsText = summarizeSoftwareActionDetails(a.details || {});
          const status = String(a.status || "").toLowerCase();
          const statusCls = status === "ok" ? "pos" : status === "partial" ? "" : "neg";
          return `
            <tr>
              <td>${a.created_at || "-"}</td>
              <td>${escapeHtml(a.action_type || "-")}</td>
              <td><span class="${statusCls}">${escapeHtml(String(a.status || "-").toUpperCase())}</span></td>
              <td title="${escapeHtml(a.summary || "")}">${escapeHtml(a.summary || "-")}</td>
              <td title="${escapeHtml(detailsText)}">${escapeHtml(detailsText)}</td>
            </tr>
          `;
        })
        .join("")
    : '<tr><td colspan="5">No software-performance actions logged yet.</td></tr>';
}

async function loadSoftwarePerfLogs(options = {}) {
  const throwOnError = !!options.throwOnError;
  try {
    const res = await api("/api/v1/agents/software-performance?limit=120");
    renderSoftwarePerfLogs(res || {});
    $("softwarePerfStamp").textContent = `Software logs refreshed: ${new Date().toLocaleString()}`;
  } catch (e) {
    const meta = normalizeUiError(e, "PERF_LOG_REFRESH_FAILED");
    $("softwarePerfStamp").textContent = `Software logs error: ${meta.reason}`;
    $("softwarePerfSummary").innerHTML = `<div class="metric neg">Failed to load software-performance logs: ${escapeHtml(meta.reason)}</div>`;
    $("softwarePerfOpenIssuesTable").querySelector("tbody").innerHTML = '<tr><td colspan="6">Failed to load issues.</td></tr>';
    $("softwarePerfActionsTable").querySelector("tbody").innerHTML = '<tr><td colspan="5">Failed to load actions.</td></tr>';
    e.code = e.code || meta.code;
    e.reason = e.reason || meta.reason;
    e.status = e.status || meta.status;
    if (throwOnError) throw e;
  }
}

async function refreshSoftwarePerfNow() {
  const btn = $("refreshSoftwarePerfBtn");
  const stamp = $("softwarePerfStamp");
  const restoreText = "Refresh Performance Logs";
  const startedAt = Date.now();
  if (btn) {
    btn.disabled = true;
    btn.textContent = "Refreshing...";
  }
  if (stamp) {
    stamp.textContent = `Software logs refreshing: ${new Date().toLocaleString()}`;
  }
  try {
    await loadSoftwarePerfLogs({ throwOnError: true });
  } finally {
    const elapsed = Date.now() - startedAt;
    const minVisibleMs = 350;
    if (elapsed < minVisibleMs) {
      await new Promise((resolve) => setTimeout(resolve, minVisibleMs - elapsed));
    }
    if (btn) {
      btn.disabled = false;
      btn.textContent = restoreText;
    }
  }
}

function renderBacktestHistory(items) {
  state.backtestHistory = items || [];
  $("agentBacktestHistoryTable").querySelector("tbody").innerHTML = state.backtestHistory
    .map((r) => `
      <tr>
        <td>${r.id}</td>
        <td>${r.created_at || ""}</td>
        <td>${r.from_date || ""}</td>
        <td>${r.to_date || ""}</td>
        <td>${Number(r.horizon_days || 0)}</td>
        <td>${Number(r.sample_count || 0)}</td>
        <td>${(Number(r.hit_rate || 0) * 100).toFixed(1)}%</td>
        <td>${Number(r.applied_tuning || 0) ? "Yes" : "No"}</td>
      </tr>
    `)
    .join("");
}

function renderBacktestResult(res) {
  state.lastBacktestResult = res || null;
  if (!res) {
    $("agentBacktestSummary").innerHTML = "";
    $("agentBacktestSuggestionsTable").querySelector("tbody").innerHTML = "";
    $("agentBacktestDiagnosticsTable").querySelector("tbody").innerHTML = "";
    return;
  }
  const diagBefore = res.diagnostics?.before || {};
  const diagAfter = res.diagnostics?.after || {};
  const fixes = res.diagnostics?.fixes || {};
  const suggestions = res.suggestions || {};
  const updates = suggestions.updates || {};
  const before = res.params_before || {};
  const after = res.params_after || {};

  $("agentBacktestSummary").innerHTML = `
    <div class="metric">Run ID: ${res.run_id || "-"}</div>
    <div class="metric">Window: ${res.from_date || "-"} to ${res.to_date || "-"}</div>
    <div class="metric">Horizon: ${Number(res.horizon_days || 0)} day</div>
    <div class="metric">Samples: ${Number(res.sample_count || 0)}</div>
    <div class="metric">Hit Rate: ${(Number(res.hit_rate || 0) * 100).toFixed(2)}%</div>
    <div class="metric">Momentum Hit: ${(Number(res.momentum_hit_rate || 0) * 100).toFixed(2)}%</div>
    <div class="metric">Intel Hit: ${(Number(res.intel_hit_rate || 0) * 100).toFixed(2)}%</div>
    <div class="metric">Avg Future Return: ${Number(res.avg_future_return || 0).toFixed(3)}%</div>
    <div class="metric">Applied Tuning: ${res.applied_tuning ? "Yes" : "No"}</div>
    <div class="metric">Fixes: ${(fixes.actions || []).join(", ") || "-"}</div>
    <div class="metric" style="grid-column: 1 / -1;">Notes: ${(suggestions.notes || []).join(" | ") || "-"}</div>
  `;

  const keys = Object.keys(updates);
  $("agentBacktestSuggestionsTable").querySelector("tbody").innerHTML = keys
    .map((k) => `
      <tr>
        <td>${k}</td>
        <td>${typeof before[k] === "undefined" ? "-" : Number(before[k]).toFixed(6)}</td>
        <td>${Number(updates[k]).toFixed(6)}</td>
        <td>${typeof after[k] === "undefined" ? "-" : Number(after[k]).toFixed(6)}</td>
      </tr>
    `)
    .join("");
  if (!keys.length) {
    $("agentBacktestSuggestionsTable").querySelector("tbody").innerHTML = `
      <tr><td colspan="4">No parameter update suggestions from this run.</td></tr>
    `;
  }

  const diagRows = [
    ["Active Symbols", diagBefore.active_symbols, diagAfter.active_symbols],
    ["With Price", diagBefore.with_price_symbols, diagAfter.with_price_symbols],
    ["Zero LTP", diagBefore.zero_ltp_symbols, diagAfter.zero_ltp_symbols],
    ["Stale Prices", diagBefore.stale_price_symbols, diagAfter.stale_price_symbols],
    ["Missing Prices", diagBefore.missing_price_symbols, diagAfter.missing_price_symbols],
    ["History Coverage %", (Number(diagBefore.history_coverage_ratio || 0) * 100).toFixed(1), (Number(diagAfter.history_coverage_ratio || 0) * 100).toFixed(1)],
    ["History Latest Date", diagBefore.history_latest_date || "-", diagAfter.history_latest_date || "-"],
  ];
  $("agentBacktestDiagnosticsTable").querySelector("tbody").innerHTML = diagRows
    .map((r) => `
      <tr>
        <td>${r[0]}</td>
        <td>${r[1]}</td>
        <td>${r[2]}</td>
      </tr>
    `)
    .join("");
}

async function loadBacktestHistory() {
  try {
    const res = await api("/api/v1/agents/backtest/history?limit=40");
    renderBacktestHistory(res.items || []);
  } catch (e) {
    console.error("backtest history load failed", e);
  }
}

function openAgentBacktestModal() {
  $("agentBacktestModal").classList.remove("hidden");
  loadBacktestHistory().catch(() => {});
}

function closeAgentBacktestModal() {
  $("agentBacktestModal").classList.add("hidden");
}

async function runAgentBacktest() {
  const btn = $("runAgentBacktestBtn");
  btn.disabled = true;
  btn.textContent = "Running...";
  try {
    const payload = {
      from_date: $("btFromDate").value || null,
      to_date: $("btToDate").value || null,
      horizon_days: Number($("btHorizonDays").value || 20),
      min_samples: Number($("btMinSamples").value || 30),
      apply_tuning: $("btApplyTuning").checked,
      fix_data_pipes: $("btFixPipes").checked,
    };
    const res = await api("/api/v1/agents/backtest/run", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    renderBacktestResult(res);
    await loadBacktestHistory();
    await loadAgentStatus();
    await loadDashboard();
    await loadStrategyInsights(false);
    if (state.selectedSymbol) await loadScrip(state.selectedSymbol);
  } catch (e) {
    alert(`Backtest failed: ${e.message}`);
  } finally {
    btn.disabled = false;
    btn.textContent = "Run Backtest";
  }
}

function setTab(tabName) {
  state.currentTab = tabName;
  document.querySelectorAll(".tab-btn").forEach((b) => {
    b.classList.toggle("active", b.getAttribute("data-tab") === tabName);
  });
  document.querySelectorAll(".tab-section").forEach((sec) => {
    sec.classList.toggle("hidden", sec.getAttribute("data-tab") !== tabName);
  });
}

function updateSymbolSuggestions(symbols) {
  const uniq = Array.from(new Set((symbols || []).map((s) => String(s).toUpperCase()))).sort();
  $("stockSuggestions").innerHTML = uniq.map((s) => `<option value="${s}"></option>`).join("");
}

function isHoldingOrphan(r) {
  if (!r) return true;
  const qty = Number(r.qty || 0);
  const tradeCount = Number(r.trade_count || 0);
  const invested = Math.abs(Number(r.invested || 0));
  const dividend = Math.abs(Number(r.dividend_amount || 0));
  const market = Math.abs(Number(r.market_value || 0));
  const realized = Math.abs(Number(r.realized_pnl || 0));
  const unrealized = Math.abs(Number(r.unrealized_pnl || 0));
  if (Math.abs(qty) > HOLDING_QTY_EPS) return false;
  if (tradeCount > 0) return false;
  return invested <= HOLDING_QTY_EPS && dividend <= HOLDING_QTY_EPS && market <= HOLDING_QTY_EPS && realized <= HOLDING_QTY_EPS && unrealized <= HOLDING_QTY_EPS;
}

function applyHoldingsFilters(items) {
  const symbolQ = ($("holdingsFilterSymbol").value || "").trim().toUpperCase();
  const signalQ = $("holdingsFilterSignal").value;
  const minRet = parseFloat($("holdingsFilterMinRet").value);
  const maxRet = parseFloat($("holdingsFilterMaxRet").value);
  return (items || []).filter((r) => {
    if (isHoldingOrphan(r)) return false;
    if (symbolQ && !String(r.symbol || "").toUpperCase().includes(symbolQ)) return false;
    if (!Number.isNaN(minRet) && Number(r.total_return_pct) < minRet) return false;
    if (!Number.isNaN(maxRet) && Number(r.total_return_pct) > maxRet) return false;
    if (signalQ === "BUY") {
      if (!(r.buy_signal && String(r.buy_signal).startsWith("B"))) return false;
    }
    if (signalQ === "SELL") {
      if (!(r.sell_signal && String(r.sell_signal).startsWith("S"))) return false;
    }
    return true;
  });
}

function getSignalText(r) {
  return [r.buy_signal, r.sell_signal].filter(Boolean).join("/");
}

function getActionSignalText(r) {
  const action = String(r.strategy_action || "").toUpperCase();
  const signal = getSignalText(r);
  if (action && signal) return `${action} (${signal})`;
  if (action) return action;
  if (signal) return `(${signal})`;
  return "";
}

function peakPriceValue(r) {
  const v = Number(r?.peak_traded_price);
  if (Number.isFinite(v)) return v;
  return Number(r?.peak_buy_price || 0);
}

function peakPctValue(r) {
  const v = Number(r?.pct_from_peak_traded);
  if (Number.isFinite(v)) return v;
  return Number(r?.pct_from_peak_buy || 0);
}

function holdingsSortValue(row, key) {
  if (key === "symbol") return String(row.symbol || "").toUpperCase();
  if (key === "signal") return getActionSignalText(row).toUpperCase();
  if (key === "peak_traded_price" || key === "peak_buy_price") return peakPriceValue(row);
  if (key === "pct_from_peak_traded" || key === "pct_from_peak_buy") return peakPctValue(row);
  return Number(row[key] || 0);
}

function sortHoldings(items) {
  const { key, dir } = state.holdingsSort || {};
  if (!key) return [...(items || [])];
  const factor = dir === "asc" ? 1 : -1;
  return [...(items || [])].sort((a, b) => {
    const va = holdingsSortValue(a, key);
    const vb = holdingsSortValue(b, key);
    let cmp = 0;
    if (typeof va === "number" && typeof vb === "number") {
      if (va < vb) cmp = -1;
      else if (va > vb) cmp = 1;
    } else {
      cmp = String(va).localeCompare(String(vb), undefined, { sensitivity: "base", numeric: true });
    }
    if (cmp === 0) {
      cmp = String(a.symbol || "").localeCompare(String(b.symbol || ""), undefined, {
        sensitivity: "base",
        numeric: true,
      });
    }
    return cmp * factor;
  });
}

function getFilteredSortedHoldings() {
  return sortHoldings(applyHoldingsFilters(state.holdingsRaw));
}

function updateHoldingsSortHeaders() {
  document.querySelectorAll("#holdingsTable thead th[data-sort-key], #holdingsZeroTable thead th[data-sort-key]").forEach((th) => {
    const key = th.getAttribute("data-sort-key");
    const baseLabel = th.getAttribute("data-label") || th.textContent.replace(/\s*[\^v]$/, "");
    th.setAttribute("data-label", baseLabel);
    if (state.holdingsSort.key === key) {
      const arrow = state.holdingsSort.dir === "asc" ? "^" : "v";
      th.textContent = `${baseLabel} ${arrow}`;
      th.setAttribute("aria-sort", state.holdingsSort.dir === "asc" ? "ascending" : "descending");
    } else {
      th.textContent = baseLabel;
      th.setAttribute("aria-sort", "none");
    }
  });
}

function toggleHoldingsSort(key) {
  if (state.holdingsSort.key === key) {
    state.holdingsSort.dir = state.holdingsSort.dir === "asc" ? "desc" : "asc";
  } else {
    state.holdingsSort.key = key;
    state.holdingsSort.dir = "asc";
  }
  updateHoldingsSortHeaders();
  renderHoldings(getFilteredSortedHoldings());
}

function splitHoldingsByQty(items) {
  const active = [];
  const zero = [];
  (items || []).forEach((r) => {
    if (Number(r.qty || 0) > HOLDING_QTY_EPS) active.push(r);
    else zero.push(r);
  });
  return { active, zero };
}

function parseTimeToMs(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  if (/^\d{10,13}$/.test(raw)) {
    const n = Number(raw);
    if (Number.isFinite(n)) return raw.length === 13 ? n : n * 1000;
  }
  const direct = Date.parse(raw);
  if (Number.isFinite(direct)) return direct;
  const normalizedRaw = raw.replace(/\s+/, "T").replace(/\.(\d{3})\d+/, ".$1");
  const normalized = Date.parse(normalizedRaw);
  return Number.isFinite(normalized) ? normalized : null;
}

function getHoldingPriceUpdatedMs(r) {
  const rowTs = parseTimeToMs(r?.price_updated_at);
  if (Number.isFinite(rowTs)) return rowTs;
  const globalTs = parseTimeToMs(state.latestPriceUpdatedAt);
  if (Number.isFinite(globalTs)) return globalTs;
  return null;
}

function holdingSymbolFreshnessClass(r) {
  const intervalRaw = Number(($("liveIntervalSec")?.value || 10));
  const intervalSec = Number.isFinite(intervalRaw) && intervalRaw > 0 ? intervalRaw : 10;
  const liveEnabled = !!$("liveEnabled")?.checked;
  const multiplier = 20;
  const minWindowMs = liveEnabled ? 45000 : 120000;
  const freshnessWindowMs = Math.max(intervalSec * multiplier * 1000, minWindowMs);
  const updatedMs = getHoldingPriceUpdatedMs(r);
  if (!Number.isFinite(updatedMs)) return "holding-symbol-stale";
  const ageMs = Date.now() - updatedMs;
  if (ageMs >= -15000 && ageMs <= freshnessWindowMs) return "holding-symbol-fresh";
  return "holding-symbol-stale";
}

function holdingsRowHtml(r) {
  const action = String(r.strategy_action || "").toUpperCase();
  const signal = getSignalText(r);
  let signalCell = "-";
  if (action && signal) {
    signalCell = `<span class="action-pill ${strategyActionClass(action)}">${action}</span> <span class="metric-inline">(${signal})</span>`;
  } else if (action) {
    signalCell = `<span class="action-pill ${strategyActionClass(action)}">${action}</span>`;
  } else if (signal) {
    signalCell = `<span class="metric-inline">(${signal})</span>`;
  }
  return `
      <tr data-symbol="${r.symbol}">
        <td class="holding-symbol ${holdingSymbolFreshnessClass(r)}">${r.symbol}</td>
        <td>${money(r.qty)}</td>
        <td>${money(r.ltp)}</td>
        <td>${money(r.avg_cost)}</td>
        <td>${money(r.invested)}</td>
        <td class="pos">${money(r.dividend_amount)}</td>
        <td>${money(r.market_value)}</td>
        <td class="${clsBySign(r.realized_pnl)}">${money(r.realized_pnl)}</td>
        <td class="${clsBySign(r.unrealized_pnl)}">${money(r.unrealized_pnl)}</td>
        <td class="${clsBySign(r.abs_pnl)}">${money(r.abs_pnl)}</td>
        <td class="${clsBySign(r.upl_pct)}">${pct(r.upl_pct)}</td>
        <td class="${clsBySign(r.day_pnl)}">${money(r.day_pnl)}</td>
        <td class="${clsBySign(r.day_change_pct)}">${pct(r.day_change_pct)}</td>
        <td class="${clsBySign(r.total_return_pct)}">${pct(r.total_return_pct)}</td>
        <td>${money(peakPriceValue(r))}</td>
        <td class="${clsBySign(peakPctValue(r))}">${pct(peakPctValue(r))}</td>
        <td>${signalCell}</td>
      </tr>`;
}

function renderHoldingsTable(tableId, items) {
  const body = $(tableId).querySelector("tbody");
  const rows = items || [];
  body.innerHTML = rows.length
    ? rows.map((r) => holdingsRowHtml(r)).join("")
    : `<tr><td colspan="17">No rows.</td></tr>`;

  body.querySelectorAll("tr[data-symbol]").forEach((tr) => {
    tr.addEventListener("click", () => {
      const symbol = tr.getAttribute("data-symbol");
      $("symbolSelect").value = symbol;
      loadScrip(symbol);
      setTab("scrip");
    });
  });
}

function applyTradesFilters(items) {
  const side = $("tradesFilterSide").value;
  const from = $("tradesFilterFrom").value;
  const to = $("tradesFilterTo").value;
  const q = ($("tradesFilterText").value || "").trim().toLowerCase();
  return (items || []).filter((t) => {
    if (side && String(t.side).toUpperCase() !== side) return false;
    if (from && String(t.trade_date) < from) return false;
    if (to && String(t.trade_date) > to) return false;
    if (q) {
      const hay = `${t.trade_date} ${t.side} ${t.quantity} ${t.price} ${t.amount} ${t.current_ltp || ""} ${t.current_pnl || ""} ${t.notes || ""}`.toLowerCase();
      if (!hay.includes(q)) return false;
    }
    return true;
  });
}

function applyPeakFilters(items) {
  const symbolQ = ($("peakFilterSymbol").value || "").trim().toUpperCase();
  const minPct = parseFloat($("peakFilterMinPct").value);
  const maxPct = parseFloat($("peakFilterMaxPct").value);
  return (items || []).filter((r) => {
    if (symbolQ && !String(r.symbol || "").toUpperCase().includes(symbolQ)) return false;
    const peakPct = peakPctValue(r);
    if (!Number.isNaN(minPct) && peakPct < minPct) return false;
    if (!Number.isNaN(maxPct) && peakPct > maxPct) return false;
    return true;
  });
}

function applySplitsFilters(items) {
  const symbolQ = ($("splitsFilterSymbol").value || "").trim().toUpperCase();
  return (items || []).filter((r) =>
    symbolQ ? String(r.symbol || "").toUpperCase().includes(symbolQ) : true
  );
}

function renderKpis(summary) {
  $("asOfText").textContent = `As of ${summary.as_of}`;
  const handInvestment = summary.hand_invested ?? summary.invested;
  const marketDeployment = summary.market_deployment ?? 0;
  const items = [
    ["Investment", money(handInvestment)],
    ["Market Deployment", money(marketDeployment)],
    ["Market Value", money(summary.market_value)],
    ["Today's P/L", money(summary.today_pnl), clsBySign(summary.today_pnl)],
    ["Today's Change %", pct(summary.today_change_pct), clsBySign(summary.today_change_pct)],
    ["Realized P/L", money(summary.realized_pnl), clsBySign(summary.realized_pnl)],
    ["Unrealized P/L", money(summary.unrealized_pnl), clsBySign(summary.unrealized_pnl)],
    ["Total P/L", money(summary.total_pnl), clsBySign(summary.total_pnl)],
    ["Total Return (Hand %)", pct(summary.total_return_pct), clsBySign(summary.total_return_pct)],
    ["Total Return (Deployment %)", pct(summary.deployment_return_pct || 0), clsBySign(summary.deployment_return_pct || 0)],
    ["CAGR", pct(summary.cagr_pct || 0), clsBySign(summary.cagr_pct || 0)],
    ["XIRR", pct(summary.xirr_pct || 0), clsBySign(summary.xirr_pct || 0)],
    ["Cash Balance", money(summary.cash_balance)],
  ];
  $("kpiGrid").innerHTML = items
    .map(
      ([label, val, cls]) =>
        `<div class="kpi"><div class="label">${label}</div><div class="value ${cls || ""}">${val}</div></div>`
    )
    .join("");
}

function renderPortfolioPerf(perf) {
  $("perfSummary").innerHTML = `
    <div class="metric">From: ${perf.start_date}</div>
    <div class="metric">To: ${perf.end_date}</div>
    <div class="metric ${clsBySign(perf.pnl)}">P/L: ${money(perf.pnl)}</div>
    <div class="metric ${clsBySign(perf.return_pct)}">Return: ${pct(perf.return_pct)}</div>
  `;
}

function fmtDate(iso) {
  const d = new Date(`${iso}T00:00:00`);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleDateString(undefined, { day: "2-digit", month: "short", year: "2-digit" });
}

function setTimeseriesRange(range) {
  const pts = state.tsPointsRaw || [];
  if (!pts.length) return;
  if (range === "all") {
    state.tsView = { start: 0, end: pts.length - 1 };
    renderChart(pts, true);
    return;
  }
  const daysMap = { "1m": 30, "3m": 90, "6m": 180, "1y": 365 };
  const days = daysMap[range];
  if (!days) return;
  const endIdx = pts.length - 1;
  const endDate = new Date(`${pts[endIdx].date}T00:00:00`);
  let startIdx = 0;
  for (let i = endIdx; i >= 0; i -= 1) {
    const d = new Date(`${pts[i].date}T00:00:00`);
    const diff = (endDate - d) / (1000 * 60 * 60 * 24);
    if (diff >= days) {
      startIdx = i;
      break;
    }
  }
  state.tsView = { start: Math.max(0, startIdx), end: endIdx };
  renderChart(pts, true);
}

function resetTimeseriesView() {
  const pts = state.tsPointsRaw || [];
  if (!pts.length) return;
  state.tsView = { start: 0, end: pts.length - 1 };
  renderChart(pts, true);
}

function renderChart(points, preserveView = false) {
  const svg = $("tsChart");
  const tooltip = $("tsTooltip");
  svg.innerHTML = "";
  tooltip.classList.add("hidden");
  state.tsPointsRaw = points || [];
  if (!points || points.length < 2) return;

  if (!preserveView || !state.tsView || Number.isNaN(state.tsView.start) || Number.isNaN(state.tsView.end)) {
    state.tsView = { start: 0, end: points.length - 1 };
  }
  let viewStart = Math.max(0, Math.min(points.length - 2, state.tsView.start));
  let viewEnd = Math.max(viewStart + 1, Math.min(points.length - 1, state.tsView.end));
  state.tsView = { start: viewStart, end: viewEnd };
  const view = points.slice(viewStart, viewEnd + 1);

  const width = 800;
  const height = 260;
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  const m = { top: 20, right: 20, bottom: 34, left: 68 };
  const plotW = width - m.left - m.right;
  const plotH = height - m.top - m.bottom;
  const investedVals = view.map((p) => Number((p.investment ?? p.hand_invested ?? p.invested) || 0));
  const marketVals = view.map((p) => Number(p.market_value || 0));
  const allVals = investedVals.concat(marketVals);
  const minRaw = Math.min(...allVals);
  const maxRaw = Math.max(...allVals);
  const pad = Math.max(1, (maxRaw - minRaw) * 0.08);
  const min = minRaw - pad;
  const max = maxRaw + pad;
  const n = view.length;

  const x = (i) => m.left + (i / Math.max(1, n - 1)) * plotW;
  const y = (v) => m.top + (1 - (v - min) / (max - min || 1)) * plotH;

  function svgEl(tag, attrs = {}) {
    const el = document.createElementNS("http://www.w3.org/2000/svg", tag);
    Object.entries(attrs).forEach(([k, v]) => el.setAttribute(k, String(v)));
    return el;
  }

  const axisColor = "#7b8797";
  const gridColor = "#e3e8ee";
  const investedColor = "#26547c";
  const portfolioColor = "#0b7d6b";

  const yTicks = 5;
  for (let t = 0; t <= yTicks; t++) {
    const v = min + ((max - min) * t) / yTicks;
    const yy = y(v);
    svg.appendChild(svgEl("line", { x1: m.left, y1: yy, x2: m.left + plotW, y2: yy, stroke: gridColor, "stroke-width": 1 }));
    const lbl = svgEl("text", { x: m.left - 8, y: yy + 3, "text-anchor": "end", "font-size": 10, fill: axisColor });
    lbl.textContent = money(v);
    svg.appendChild(lbl);
  }

  svg.appendChild(svgEl("line", { x1: m.left, y1: m.top + plotH, x2: m.left + plotW, y2: m.top + plotH, stroke: axisColor, "stroke-width": 1 }));
  svg.appendChild(svgEl("line", { x1: m.left, y1: m.top, x2: m.left, y2: m.top + plotH, stroke: axisColor, "stroke-width": 1 }));

  const xTicks = Math.min(7, n - 1);
  for (let t = 0; t <= xTicks; t++) {
    const i = Math.round((t / Math.max(1, xTicks)) * (n - 1));
    const xx = x(i);
    svg.appendChild(svgEl("line", { x1: xx, y1: m.top + plotH, x2: xx, y2: m.top + plotH + 4, stroke: axisColor, "stroke-width": 1 }));
    const lbl = svgEl("text", { x: xx, y: m.top + plotH + 16, "text-anchor": "middle", "font-size": 10, fill: axisColor });
    lbl.textContent = fmtDate(view[i].date);
    svg.appendChild(lbl);
  }

  function pathFor(vals) {
    return vals.map((v, i) => `${i === 0 ? "M" : "L"} ${x(i)} ${y(v)}`).join(" ");
  }
  svg.appendChild(svgEl("path", { d: pathFor(investedVals), fill: "none", stroke: investedColor, "stroke-width": 2 }));
  svg.appendChild(svgEl("path", { d: pathFor(marketVals), fill: "none", stroke: portfolioColor, "stroke-width": 2.5 }));

  svg.appendChild(svgEl("line", { x1: m.left + 8, y1: m.top - 6, x2: m.left + 26, y2: m.top - 6, stroke: investedColor, "stroke-width": 2.6 }));
  const l1 = svgEl("text", { x: m.left + 30, y: m.top - 3, "font-size": 10, fill: "#384657" });
  l1.textContent = "Investment";
  svg.appendChild(l1);
  svg.appendChild(svgEl("line", { x1: m.left + 90, y1: m.top - 6, x2: m.left + 108, y2: m.top - 6, stroke: portfolioColor, "stroke-width": 2.6 }));
  const l2 = svgEl("text", { x: m.left + 112, y: m.top - 3, "font-size": 10, fill: "#384657" });
  l2.textContent = "Market Value";
  svg.appendChild(l2);

  const hoverLine = svgEl("line", {
    x1: m.left,
    y1: m.top,
    x2: m.left,
    y2: m.top + plotH,
    stroke: "#57677b",
    "stroke-width": 1,
    "stroke-dasharray": "3 3",
    visibility: "hidden",
  });
  const dotInvested = svgEl("circle", { cx: m.left, cy: m.top, r: 3.5, fill: investedColor, visibility: "hidden" });
  const dotMarket = svgEl("circle", { cx: m.left, cy: m.top, r: 3.5, fill: portfolioColor, visibility: "hidden" });
  svg.appendChild(hoverLine);
  svg.appendChild(dotInvested);
  svg.appendChild(dotMarket);

  const overlay = svgEl("rect", {
    x: m.left,
    y: m.top,
    width: plotW,
    height: plotH,
    fill: "transparent",
    style: "cursor:crosshair",
  });
  svg.appendChild(overlay);

  const hoverAt = (clientX, clientY) => {
    const rect = svg.getBoundingClientRect();
    const px = ((clientX - rect.left) / rect.width) * width;
    const py = ((clientY - rect.top) / rect.height) * height;
    if (px < m.left || px > m.left + plotW || py < m.top || py > m.top + plotH) return;
    const i = Math.max(0, Math.min(n - 1, Math.round(((px - m.left) / plotW) * (n - 1))));
    const xx = x(i);
    const yi = y(investedVals[i]);
    const yp = y(marketVals[i]);
    hoverLine.setAttribute("x1", xx);
    hoverLine.setAttribute("x2", xx);
    hoverLine.setAttribute("visibility", "visible");
    dotInvested.setAttribute("cx", xx);
    dotInvested.setAttribute("cy", yi);
    dotInvested.setAttribute("visibility", "visible");
    dotMarket.setAttribute("cx", xx);
    dotMarket.setAttribute("cy", yp);
    dotMarket.setAttribute("visibility", "visible");
    tooltip.innerHTML = `
      <div class="row"><span class="label">Date</span><strong>${fmtDate(view[i].date)}</strong></div>
      <div class="row"><span class="label">Investment</span><strong>${money(investedVals[i])}</strong></div>
      <div class="row"><span class="label">Market Value</span><strong>${money(marketVals[i])}</strong></div>
    `;
    tooltip.classList.remove("hidden");
    const wrap = svg.closest(".chart-wrap").getBoundingClientRect();
    let tx = clientX - wrap.left + 10;
    let ty = clientY - wrap.top - 10;
    if (tx > wrap.width - 190) tx = wrap.width - 190;
    if (ty < 10) ty = 10;
    tooltip.style.left = `${tx}px`;
    tooltip.style.top = `${ty}px`;
  };

  let drag = null;
  overlay.addEventListener("mousedown", (e) => {
    drag = { x: e.clientX, start: viewStart, end: viewEnd };
    overlay.style.cursor = "grabbing";
  });
  overlay.addEventListener("mouseup", () => {
    drag = null;
    overlay.style.cursor = "crosshair";
  });
  overlay.addEventListener("mousemove", (e) => {
    hoverAt(e.clientX, e.clientY);
    if (!drag) return;
    const dxPx = e.clientX - drag.x;
    const span = Math.max(2, drag.end - drag.start + 1);
    const shift = Math.round((-dxPx / plotW) * span);
    let ns = drag.start + shift;
    let ne = drag.end + shift;
    if (ns < 0) {
      ne += -ns;
      ns = 0;
    }
    if (ne > points.length - 1) {
      ns -= ne - (points.length - 1);
      ne = points.length - 1;
    }
    ns = Math.max(0, ns);
    ne = Math.max(ns + 1, ne);
    state.tsView = { start: ns, end: ne };
    renderChart(points, true);
  });
  overlay.addEventListener("mouseleave", () => {
    drag = null;
    overlay.style.cursor = "crosshair";
    hoverLine.setAttribute("visibility", "hidden");
    dotInvested.setAttribute("visibility", "hidden");
    dotMarket.setAttribute("visibility", "hidden");
    tooltip.classList.add("hidden");
  });

  overlay.addEventListener("wheel", (e) => {
    e.preventDefault();
    const zoomIn = e.deltaY < 0;
    const span = Math.max(2, viewEnd - viewStart + 1);
    const nextSpan = zoomIn ? Math.max(10, Math.round(span * 0.85)) : Math.min(points.length, Math.round(span * 1.15));
    const rect = svg.getBoundingClientRect();
    const px = ((e.clientX - rect.left) / rect.width) * width;
    const ratio = clamp((px - m.left) / plotW, 0, 1);
    const centerIndex = viewStart + Math.round(ratio * (span - 1));
    let ns = centerIndex - Math.round(ratio * (nextSpan - 1));
    let ne = ns + nextSpan - 1;
    if (ns < 0) {
      ne += -ns;
      ns = 0;
    }
    if (ne > points.length - 1) {
      ns -= ne - (points.length - 1);
      ne = points.length - 1;
    }
    ns = Math.max(0, ns);
    ne = Math.max(ns + 1, ne);
    state.tsView = { start: ns, end: ne };
    renderChart(points, true);
  }, { passive: false });

  overlay.addEventListener("dblclick", () => resetTimeseriesView());
}

function renderHoldings(items) {
  const grouped = splitHoldingsByQty(items);
  renderHoldingsTable("holdingsTable", grouped.active);
  renderHoldingsTable("holdingsZeroTable", grouped.zero);
  if ($("holdingsActiveCount")) $("holdingsActiveCount").textContent = String(grouped.active.length);
  if ($("holdingsZeroCount")) $("holdingsZeroCount").textContent = String(grouped.zero.length);
}

function renderAssetSplitTable(tableId, items) {
  const body = $(tableId)?.querySelector("tbody");
  if (!body) return;
  body.innerHTML = (items || []).length
    ? (items || [])
        .map(
          (r) => `
          <tr>
            <td>${escapeHtml(r.symbol || "")}</td>
            <td>${money(r.qty)}</td>
            <td>${money(r.ltp)}</td>
            <td>${money(r.avg_cost)}</td>
            <td>${money(r.invested)}</td>
            <td>${money(r.market_value)}</td>
            <td class="${clsBySign(r.unrealized_pnl)}">${money(r.unrealized_pnl)}</td>
            <td class="${clsBySign(r.total_return_pct)}">${pct(r.total_return_pct)}</td>
          </tr>`
        )
        .join("")
    : '<tr><td colspan="8">No holdings.</td></tr>';
}

function renderAssetSplit(items) {
  const all = Array.isArray(items) ? items : [];
  const active = all.filter((r) => Number(r.qty || 0) > HOLDING_QTY_EPS);
  const equityItems = active.filter((r) => normalizeAssetClass(r.asset_class, r.symbol) === "EQUITY");
  const goldItems = active.filter((r) => normalizeAssetClass(r.asset_class, r.symbol) === "GOLD");
  const eqValue = equityItems.reduce((s, r) => s + Number(r.market_value || 0), 0);
  const goldValue = goldItems.reduce((s, r) => s + Number(r.market_value || 0), 0);
  const totalValue = eqValue + goldValue;
  state.assetSplitRaw = active;
  if ($("assetSplitSummary")) {
    $("assetSplitSummary").innerHTML = `
      <div class="metric">Total Active Mkt Value: ${money(totalValue)}</div>
      <div class="metric">Equity Mkt Value: ${money(eqValue)} (${totalValue > 0 ? pct((eqValue * 100) / totalValue) : "0.00%"})</div>
      <div class="metric">Gold Mkt Value: ${money(goldValue)} (${totalValue > 0 ? pct((goldValue * 100) / totalValue) : "0.00%"})</div>
      <div class="metric">Equity Holdings: ${equityItems.length}</div>
      <div class="metric">Gold Holdings: ${goldItems.length}</div>
    `;
  }
  renderAssetSplitTable("assetEquityTable", equityItems);
  renderAssetSplitTable("assetGoldTable", goldItems);
}

async function loadAssetSplit() {
  const res = await api("/api/v1/scrips");
  renderAssetSplit(res.items || []);
}

function plannerNoteLabel(note) {
  const n = String(note || "").trim().toLowerCase();
  if (!n) return "-";
  if (n === "blocked_by_min_value") return "Blocked by min value";
  if (n === "capped_by_min_value") return "Capped by min value";
  if (n === "blocked_by_max_value") return "Blocked by max value";
  if (n === "capped_by_max_value") return "Capped by max value";
  if (n === "missing_ltp") return "Missing LTP";
  return n.replace(/_/g, " ");
}

function parseGuardInputValue(raw) {
  const s = String(raw || "").trim();
  if (!s) return null;
  const v = Number(s);
  if (!Number.isFinite(v) || v < 0) return NaN;
  return v;
}

function plannerDateLabel(raw) {
  const s = String(raw || "").trim();
  if (!s) return "-";
  const d = new Date(s);
  if (!Number.isFinite(d.getTime())) return s;
  return d.toLocaleString();
}

function isActiveRebalanceLot(payload) {
  const lot = payload && typeof payload === "object" ? payload.lot : null;
  if (!lot || typeof lot !== "object") return false;
  return String(lot.status || "").toLowerCase() === "active";
}

function setRebalancePlannerControlState() {
  const meta = state.rebalancePlanMeta || {};
  const active = isActiveRebalanceLot(meta);
  const sideEl = $("rebalanceSide");
  const pctEl = $("rebalancePercent");
  const lockBtn = $("rebalanceLockLotBtn");
  const resetBtn = $("rebalanceResetLotBtn");
  const sideVal = String(meta.side || "").toUpperCase();
  const percentVal = Number(meta.percent || 0);
  if (sideEl && (sideVal === "SELL" || sideVal === "BUY")) sideEl.value = sideVal;
  if (pctEl && Number.isFinite(percentVal) && percentVal > 0) pctEl.value = String(Number(percentVal.toFixed(2)));
  if (sideEl) sideEl.disabled = active;
  if (pctEl) pctEl.disabled = active;
  if (lockBtn) {
    lockBtn.disabled = active;
    lockBtn.textContent = active ? "Lot Locked" : "Lock Qty Lot";
  }
  if (resetBtn) {
    resetBtn.disabled = !active;
  }
}

async function updateRebalanceLotItemCompletion(itemId, state, note, executedPrice = null, executedAt = null, plannedQty = null) {
  if ($("rebalanceStamp")) $("rebalanceStamp").textContent = `Planner updating: item ${itemId}`;
  const payload = {
    state: String(state || "pending").toLowerCase(),
    note: String(note || "").trim(),
  };
  if (plannedQty !== null && typeof plannedQty !== "undefined") {
    payload.planned_qty = Number(plannedQty);
  }
  if (payload.state === "closed") {
    payload.executed_price = Number(executedPrice || 0);
    if (executedAt) payload.executed_at = String(executedAt);
  } else if (payload.state === "skipped" && executedAt) {
    payload.executed_at = String(executedAt);
  }
  const out = await api(`/api/v1/rebalance/lot/items/${itemId}`, {
    method: "PUT",
    body: JSON.stringify(payload),
  });
  renderRebalancePlanner(out || {});
  if (out && out.closed_history) renderRebalanceClosedHistory(out.closed_history);
  if ($("rebalanceStamp")) $("rebalanceStamp").textContent = `Planner updated: ${new Date().toLocaleString()}`;
}

function bindRebalanceCompletionToggles() {
  const submitRowCompletion = async (row) => {
    if (!row) return;
    const saveBtn = row.querySelector(".rebalance-complete-save-btn");
    const statusEl = row.querySelector(".rebalance-complete-select");
    const priceEl = row.querySelector(".rebalance-exec-price");
    const dateEl = row.querySelector(".rebalance-exec-date");
    const noteEl = row.querySelector(".rebalance-complete-note");
    const qtyEl = row.querySelector(".rebalance-locked-qty-input");
    const itemId = Number(saveBtn?.getAttribute("data-lot-item-id") || statusEl?.getAttribute("data-lot-item-id") || 0);
    if (!Number.isFinite(itemId) || itemId <= 0) return;
    const state = String(statusEl?.value || "pending").toLowerCase();
    const ltpFallback = Number(row.getAttribute("data-ltp") || 0);
    let plannedQty = null;
    if (qtyEl && !qtyEl.disabled) {
      const qRaw = Number(qtyEl.value || 0);
      if (!Number.isFinite(qRaw) || qRaw < 0) {
        throw Object.assign(new Error("Locked qty must be non-negative."), { code: "REBALANCE_LOCKED_QTY_INVALID" });
      }
      plannedQty = qRaw;
    }
    let executedPrice = null;
    if (state === "closed") {
      const pxRaw = Number(priceEl?.value || 0);
      executedPrice = Number.isFinite(pxRaw) && pxRaw > 0 ? pxRaw : (ltpFallback > 0 ? ltpFallback : null);
      if (!Number.isFinite(Number(executedPrice)) || Number(executedPrice) <= 0) {
        throw Object.assign(new Error("Closed state requires executed price."), { code: "REBALANCE_EXEC_PRICE_REQUIRED" });
      }
      if (priceEl && (!priceEl.value || Number(priceEl.value) <= 0)) {
        priceEl.value = String(Number(executedPrice).toFixed(2));
      }
    }
    const executedAt = String(dateEl?.value || "").trim() || null;
    const note = String(noteEl?.value || "");
    if (saveBtn) saveBtn.disabled = true;
    if (statusEl) statusEl.disabled = true;
    if (priceEl) priceEl.disabled = true;
    if (dateEl) dateEl.disabled = true;
    if (noteEl) noteEl.disabled = true;
    if (qtyEl) qtyEl.disabled = true;
    try {
      await updateRebalanceLotItemCompletion(itemId, state, note, executedPrice, executedAt, plannedQty);
    } catch (e) {
      const meta = normalizeUiError(e, "REBALANCE_ITEM_UPDATE_FAILED");
      if ($("rebalanceStamp")) $("rebalanceStamp").textContent = `Planner error: ${meta.reason}`;
      notifyActionFailure("Update Lot Completion", e, "REBALANCE_ITEM_UPDATE_FAILED");
    } finally {
      if (saveBtn) saveBtn.disabled = false;
      if (statusEl) statusEl.disabled = false;
      if (priceEl) priceEl.disabled = false;
      if (dateEl) dateEl.disabled = false;
      if (noteEl) noteEl.disabled = false;
      if (qtyEl) qtyEl.disabled = false;
    }
  };

  const buttons = Array.from(document.querySelectorAll("#rebalanceTable .rebalance-complete-save-btn"));
  buttons.forEach((btn) => {
    btn.addEventListener("click", async (evt) => {
      const target = evt.currentTarget;
      if (!(target instanceof HTMLButtonElement)) return;
      const row = target.closest("tr");
      await submitRowCompletion(row);
    });
  });

  const statusSelects = Array.from(document.querySelectorAll("#rebalanceTable .rebalance-complete-select"));
  statusSelects.forEach((select) => {
    const row = select.closest("tr");
    const priceEl = row?.querySelector(".rebalance-exec-price");
    const dateEl = row?.querySelector(".rebalance-exec-date");
    const setStateInputMode = () => {
      const state = String(select.value || "pending").toLowerCase();
      const needsPrice = state === "closed";
      if (priceEl) priceEl.disabled = !needsPrice;
      if (dateEl) dateEl.disabled = state === "pending";
    };
    setStateInputMode();
    select.addEventListener("change", async (evt) => {
      const row = evt.currentTarget.closest("tr");
      const rowPriceEl = row?.querySelector(".rebalance-exec-price");
      const rowDateEl = row?.querySelector(".rebalance-exec-date");
      const state = String(evt.currentTarget.value || "pending").toLowerCase();
      if (rowPriceEl) rowPriceEl.disabled = state !== "closed";
      if (rowDateEl) rowDateEl.disabled = state === "pending";
      await submitRowCompletion(row);
    });
  });

  const noteInputs = Array.from(document.querySelectorAll("#rebalanceTable .rebalance-complete-note"));
  noteInputs.forEach((input) => {
    input.addEventListener("keydown", (evt) => {
      if (evt.key !== "Enter") return;
      evt.preventDefault();
      const row = evt.currentTarget.closest("tr");
      if (!row) return;
      const saveBtn = row.querySelector(".rebalance-complete-save-btn");
      if (saveBtn instanceof HTMLButtonElement) saveBtn.click();
    });
  });

  const qtyInputs = Array.from(document.querySelectorAll("#rebalanceTable .rebalance-locked-qty-input"));
  qtyInputs.forEach((input) => {
    input.addEventListener("change", async (evt) => {
      const row = evt.currentTarget.closest("tr");
      await submitRowCompletion(row);
    });
    input.addEventListener("keydown", (evt) => {
      if (evt.key !== "Enter") return;
      evt.preventDefault();
      const row = evt.currentTarget.closest("tr");
      if (!row) return;
      const saveBtn = row.querySelector(".rebalance-complete-save-btn");
      if (saveBtn instanceof HTMLButtonElement) saveBtn.click();
    });
  });
}

function calcRebuySaved(executedPrice, rebuyPrice, qty) {
  const ep = Number(executedPrice || 0);
  const rp = Number(rebuyPrice || 0);
  const q = Number(qty || 0);
  if (!Number.isFinite(ep) || ep <= 0 || !Number.isFinite(rp) || rp <= 0 || !Number.isFinite(q) || q <= 0) return null;
  return (ep - rp) * q;
}

async function setRebalanceBuybackStatus(itemId, completed, options = {}) {
  const payload = {
    buyback_completed: !!completed,
    include_completed: !!options.includeCompleted,
  };
  if (typeof options.buybackPrice !== "undefined" && options.buybackPrice !== null && Number(options.buybackPrice) > 0) {
    payload.buyback_price = Number(options.buybackPrice);
  }
  if (options.buybackAt) payload.buyback_at = String(options.buybackAt);
  if (options.note) payload.note = String(options.note);
  const out = await api(`/api/v1/rebalance/closed-history/items/${itemId}/buyback`, {
    method: "PUT",
    body: JSON.stringify(payload),
  });
  if (out && out.history) {
    renderRebalanceClosedHistory(out.history);
  } else {
    await loadRebalanceClosedHistory({ throwOnError: true });
  }
  return out;
}

function renderRebalanceClosedHistory(payload) {
  const items = Array.isArray(payload?.items) ? payload.items : [];
  const summary = payload?.summary && typeof payload.summary === "object" ? payload.summary : {};
  state.rebalanceClosedHistoryRaw = items;
  if ($("rebalanceHistorySummary")) {
    const netSaved = Number(summary.net_saved_total || 0);
    $("rebalanceHistorySummary").innerHTML = `
      <div class="metric">Tracked Items: ${Number(summary.tracked_items || 0)}</div>
      <div class="metric">Buyback Completed: ${Number(summary.completed_items || 0)}</div>
      <div class="metric">Projected P/L Saved: ${money(summary.projected_saved_now_total || 0)}</div>
      <div class="metric">Realized P/L Saved: ${money(summary.realized_saved_total || 0)}</div>
      <div class="metric ${netSaved >= 0 ? "pos" : "neg"}">Net P/L Saved: ${money(netSaved)}</div>
    `;
  }
  const body = $("rebalanceHistoryTable")?.querySelector("tbody");
  if (!body) return;
  body.innerHTML = items.length
    ? items
        .map((r) => {
          const refPrice = Number(r.buyback_reference_price || 0);
          const manualPrice = refPrice > 0 ? refPrice : Number(r.executed_price || 0);
          const saved = calcRebuySaved(r.executed_price, manualPrice, r.qty);
          return `
          <tr data-history-item-id="${Number(r.lot_item_id || 0)}">
            <td>${Number(r.lot_id || 0)}</td>
            <td>${Number(r.lot_item_id || 0)}</td>
            <td>${escapeHtml(r.symbol || "")}</td>
            <td>${escapeHtml(r.side || "")}</td>
            <td>${money(r.qty)}</td>
            <td>${money(r.executed_price)}</td>
            <td>${escapeHtml(plannerDateLabel(r.executed_at))}</td>
            <td>${r.buyback_reference_price === null || typeof r.buyback_reference_price === "undefined" ? "-" : money(r.buyback_reference_price)}</td>
            <td><input class="rebalance-history-rebuy-price" type="number" min="0.01" step="0.01" value="${manualPrice > 0 ? Number(manualPrice).toFixed(2) : ""}" placeholder="re-buy price"></td>
            <td class="${(saved || 0) >= 0 ? "pos" : "neg"}">${
              r.buyback_completed
                ? (r.realized_saved === null || typeof r.realized_saved === "undefined" ? "-" : money(r.realized_saved))
                : (saved === null ? "-" : money(saved))
            }</td>
            <td>${r.buyback_completed ? `Completed${r.buyback_completed_at ? ` (${escapeHtml(plannerDateLabel(r.buyback_completed_at))})` : ""}` : "Tracking"}</td>
            <td>${escapeHtml(String(r.note || "")) || "-"}</td>
            <td>
              <button class="btn secondary rebalance-history-prepare-buy-btn" data-symbol="${escapeHtml(String(r.symbol || ""))}" data-qty="${Number(r.qty || 0)}">Prepare Buy</button>
              <button class="btn secondary rebalance-history-buyback-complete-btn" data-lot-item-id="${Number(r.lot_item_id || 0)}" data-complete="${r.buyback_completed ? "0" : "1"}">${r.buyback_completed ? "Reopen Track" : "Mark Buyback Done"}</button>
            </td>
          </tr>
          `;
        })
        .join("")
    : '<tr><td colspan="13">No closed lot history available.</td></tr>';

  Array.from(document.querySelectorAll("#rebalanceHistoryTable .rebalance-history-rebuy-price")).forEach((inp) => {
    inp.addEventListener("input", (evt) => {
      const row = evt.currentTarget.closest("tr");
      if (!row) return;
      const itemId = Number(row.getAttribute("data-history-item-id") || 0);
      const item = state.rebalanceClosedHistoryRaw.find((x) => Number(x.lot_item_id || 0) === itemId);
      if (!item) return;
      const saved = calcRebuySaved(item.executed_price, Number(evt.currentTarget.value || 0), item.qty);
      const td = row.children[9];
      if (!td) return;
      td.className = (saved || 0) >= 0 ? "pos" : "neg";
      if (item.buyback_completed) {
        td.textContent = item.realized_saved === null || typeof item.realized_saved === "undefined" ? "-" : money(item.realized_saved);
      } else {
        td.textContent = saved === null ? "-" : money(saved);
      }
    });
  });

  Array.from(document.querySelectorAll("#rebalanceHistoryTable .rebalance-history-prepare-buy-btn")).forEach((btn) => {
    btn.addEventListener("click", (evt) => {
      const target = evt.currentTarget;
      if (!(target instanceof HTMLButtonElement)) return;
      const row = target.closest("tr");
      if (!row) return;
      const symbol = String(target.getAttribute("data-symbol") || "").trim().toUpperCase();
      const qty = Number(target.getAttribute("data-qty") || 0);
      const rebuyEl = row.querySelector(".rebalance-history-rebuy-price");
      const price = Number(rebuyEl?.value || 0);
      if (!symbol || !Number.isFinite(qty) || qty <= 0) return;
      if ($("symbolSelect")) $("symbolSelect").value = symbol;
      if ($("tradeAddSide")) $("tradeAddSide").value = "BUY";
      if ($("tradeAddQty")) $("tradeAddQty").value = String(Number(qty.toFixed(4)));
      if ($("tradeAddPrice")) $("tradeAddPrice").value = Number.isFinite(price) && price > 0 ? String(Number(price.toFixed(2))) : "";
      setTab("scrip");
      loadScrip(symbol).catch(() => {});
      if ($("rebalanceHistoryStamp")) $("rebalanceHistoryStamp").textContent = `Prepared BUY for ${symbol} @ ${new Date().toLocaleString()}`;
    });
  });

  Array.from(document.querySelectorAll("#rebalanceHistoryTable .rebalance-history-buyback-complete-btn")).forEach((btn) => {
    btn.addEventListener("click", async (evt) => {
      const target = evt.currentTarget;
      if (!(target instanceof HTMLButtonElement)) return;
      const row = target.closest("tr");
      if (!row) return;
      const itemId = Number(target.getAttribute("data-lot-item-id") || 0);
      if (!Number.isFinite(itemId) || itemId <= 0) return;
      const complete = String(target.getAttribute("data-complete") || "1") === "1";
      const rebuyEl = row.querySelector(".rebalance-history-rebuy-price");
      const buybackPrice = Number(rebuyEl?.value || 0);
      const includeCompleted = !!$("rebalanceHistoryShowCompleted")?.checked;
      target.disabled = true;
      if ($("rebalanceHistoryStamp")) $("rebalanceHistoryStamp").textContent = `Updating buyback status: ${new Date().toLocaleString()}`;
      try {
        await setRebalanceBuybackStatus(itemId, complete, {
          buybackPrice: complete && Number.isFinite(buybackPrice) && buybackPrice > 0 ? buybackPrice : null,
          includeCompleted,
        });
        if ($("rebalanceHistoryStamp")) {
          $("rebalanceHistoryStamp").textContent = complete
            ? `Buyback marked complete: ${new Date().toLocaleString()}`
            : `Buyback tracking reopened: ${new Date().toLocaleString()}`;
        }
      } catch (e) {
        const meta = normalizeUiError(e, "REBALANCE_BUYBACK_UPDATE_FAILED");
        if ($("rebalanceHistoryStamp")) $("rebalanceHistoryStamp").textContent = `Buyback status error: ${meta.reason}`;
        notifyActionFailure("Update Buyback Status", e, "REBALANCE_BUYBACK_UPDATE_FAILED");
      } finally {
        target.disabled = false;
      }
    });
  });
}

async function loadRebalanceClosedHistory(options = {}) {
  const throwOnError = !!options.throwOnError;
  const includeCompleted = !!$("rebalanceHistoryShowCompleted")?.checked;
  if ($("rebalanceHistoryStamp")) $("rebalanceHistoryStamp").textContent = `Closed history refreshing: ${new Date().toLocaleString()}`;
  try {
    const res = await api(`/api/v1/rebalance/closed-history?limit=300&include_completed=${includeCompleted ? "1" : "0"}`);
    renderRebalanceClosedHistory(res || {});
    if ($("rebalanceHistoryStamp")) $("rebalanceHistoryStamp").textContent = `Closed history refreshed: ${new Date().toLocaleString()}`;
  } catch (e) {
    const meta = normalizeUiError(e, "REBALANCE_CLOSED_HISTORY_FAILED");
    if ($("rebalanceHistoryStamp")) $("rebalanceHistoryStamp").textContent = `Closed history error: ${meta.reason}`;
    if ($("rebalanceHistorySummary")) {
      $("rebalanceHistorySummary").innerHTML = '<div class="metric neg">Closed history summary unavailable.</div>';
    }
    const body = $("rebalanceHistoryTable")?.querySelector("tbody");
    if (body) body.innerHTML = '<tr><td colspan="13">Failed to load closed history.</td></tr>';
    if (throwOnError) throw e;
  }
}

function renderRebalancePlanner(payload) {
  const p = payload || {};
  const lot = p.lot && typeof p.lot === "object" ? p.lot : null;
  const items = Array.isArray(p.items) ? p.items : [];
  state.rebalancePlanRaw = items;
  state.rebalancePlanMeta = {
    side: String(p.side || "SELL").toUpperCase(),
    percent: Number(p.percent || 0),
    allocation_basis: String(p.allocation_basis || "portfolio_market_value"),
    target_trade_value: Number(p.target_trade_value || 0),
    active_nonzero_count: Number(p.active_nonzero_count || 0),
    total_current_market_value: Number(p.total_current_market_value || 0),
    total_suggested_trade_value: Number(p.total_suggested_trade_value || 0),
    total_remaining_trade_value: Number(p.total_remaining_trade_value || 0),
    completed_items: Number(p.completed_items || 0),
    closed_items: Number(p.closed_items || 0),
    skipped_items: Number(p.skipped_items || 0),
    remaining_items: Number(p.remaining_items || 0),
    lot,
  };
  if ($("rebalanceSummary")) {
    const lotSummary = lot
      ? `Lot #${Number(lot.id || 0)} (${escapeHtml(String(lot.status || "active"))}) | Created ${escapeHtml(plannerDateLabel(lot.created_at))}`
      : "No active locked lot";
    $("rebalanceSummary").innerHTML = `
      <div class="metric">Side: ${state.rebalancePlanMeta.side}</div>
      <div class="metric">Percent: ${Number(state.rebalancePlanMeta.percent || 0).toFixed(2)}%</div>
      <div class="metric">Basis: ${escapeHtml(state.rebalancePlanMeta.allocation_basis)}</div>
      <div class="metric">Lot: ${lotSummary}</div>
      <div class="metric">Active Non-Zero Holdings: ${state.rebalancePlanMeta.active_nonzero_count}</div>
      <div class="metric">Completed Items: ${state.rebalancePlanMeta.completed_items}</div>
      <div class="metric">Closed Items: ${state.rebalancePlanMeta.closed_items}</div>
      <div class="metric">Skipped Items: ${state.rebalancePlanMeta.skipped_items}</div>
      <div class="metric">Remaining Items: ${state.rebalancePlanMeta.remaining_items}</div>
      <div class="metric">Current Mkt Value Total: ${money(state.rebalancePlanMeta.total_current_market_value)}</div>
      <div class="metric">Target Trade Value: ${money(state.rebalancePlanMeta.target_trade_value)}</div>
      <div class="metric">Suggested Trade Value Total: ${money(state.rebalancePlanMeta.total_suggested_trade_value)}</div>
      <div class="metric">Remaining Trade Value: ${money(state.rebalancePlanMeta.total_remaining_trade_value)}</div>
    `;
  }
  const body = $("rebalanceTable")?.querySelector("tbody");
  if (!body) return;
  body.innerHTML = items.length
    ? items
        .map(
          (r) => `
          <tr data-symbol="${escapeHtml(r.symbol)}" data-ltp="${Number(r.ltp || 0)}" class="${r.completed ? "rebalance-completed-row" : ""}">
            <td>${escapeHtml(r.symbol || "")}</td>
            <td>${money(r.qty)}</td>
            <td>${money(r.ltp)}</td>
            <td>${money(r.market_value)}</td>
            <td><input class="guard-input rebalance-min-input" type="number" min="0" step="0.01" data-symbol="${escapeHtml(r.symbol)}" value="${Number(r.min_value || 0).toFixed(2)}"></td>
            <td><input class="guard-input rebalance-max-input" type="number" min="0" step="0.01" data-symbol="${escapeHtml(r.symbol)}" value="${r.max_value === null || typeof r.max_value === "undefined" ? "" : Number(r.max_value).toFixed(2)}" placeholder="optional"></td>
            <td>${money(r.desired_trade_value)}</td>
            <td>${
              Number(r.lot_item_id || 0) > 0
                ? `<input class="guard-input rebalance-locked-qty-input" type="number" min="0" step="0.0001" value="${Number(typeof r.locked_qty === "undefined" ? r.suggested_qty : r.locked_qty).toFixed(4)}" ${String(r.execution_state || "pending").toLowerCase() === "pending" ? "" : "disabled"}>`
                : money(typeof r.locked_qty === "undefined" ? r.suggested_qty : r.locked_qty)
            }</td>
            <td>${money(typeof r.remaining_qty === "undefined" ? r.suggested_qty : r.remaining_qty)}</td>
            <td>${money(r.suggested_trade_value)}</td>
            <td>${money(r.post_trade_market_value)}</td>
            <td>${escapeHtml(plannerNoteLabel(r.note))}</td>
            <td>${
              Number(r.lot_item_id || 0) > 0
                ? `
                <div class="rebalance-complete-cell">
                  <select class="rebalance-complete-select" data-lot-item-id="${Number(r.lot_item_id || 0)}">
                    <option value="pending" ${(String(r.execution_state || "pending").toLowerCase() === "pending") ? "selected" : ""}>Pending</option>
                    <option value="closed" ${(String(r.execution_state || "").toLowerCase() === "closed") ? "selected" : ""}>Closed</option>
                    <option value="skipped" ${(String(r.execution_state || "").toLowerCase() === "skipped") ? "selected" : ""}>Skipped</option>
                  </select>
                  <input class="rebalance-exec-price" type="number" min="0.01" step="0.01" placeholder="close price" value="${r.executed_price === null || typeof r.executed_price === "undefined" ? "" : Number(r.executed_price).toFixed(2)}">
                  <input class="rebalance-exec-date" type="date" value="${r.executed_at ? String(r.executed_at).slice(0, 10) : ""}">
                  <input class="rebalance-complete-note" type="text" maxlength="160" placeholder="optional note" value="${escapeHtml(String(r.completion_note || ""))}">
                  <button class="btn secondary rebalance-complete-save-btn" data-lot-item-id="${Number(r.lot_item_id || 0)}">Update</button>
                </div>
                `
                : "<span class='metric-inline'>-</span>"
            }</td>
          </tr>
        `
        )
        .join("")
    : '<tr><td colspan="13">No active non-zero holdings for planning.</td></tr>';
  bindRebalanceCompletionToggles();
  setRebalancePlannerControlState();
  if (p.closed_history && typeof p.closed_history === "object") {
    renderRebalanceClosedHistory(p.closed_history);
  }
}

async function loadRebalanceSuggestions(options = {}) {
  const throwOnError = !!options.throwOnError;
  const sideEl = $("rebalanceSide");
  const pctEl = $("rebalancePercent");
  const side = String(sideEl?.value || "SELL").toUpperCase();
  const percent = Number(pctEl?.value || 0);
  if (!Number.isFinite(percent) || percent <= 0) {
    const err = new Error("Percent must be greater than 0.");
    err.code = "REBALANCE_PERCENT_INVALID";
    if (throwOnError) throw err;
    if ($("rebalanceStamp")) $("rebalanceStamp").textContent = "Planner error: invalid percent";
    return;
  }
  if ($("rebalanceStamp")) $("rebalanceStamp").textContent = `Planner refreshing: ${new Date().toLocaleString()}`;
  try {
    const res = await api(`/api/v1/rebalance/suggestions?side=${encodeURIComponent(side)}&percent=${encodeURIComponent(percent)}`);
    renderRebalancePlanner(res || {});
    if (!(res && res.closed_history)) {
      await loadRebalanceClosedHistory();
    }
    if ($("rebalanceStamp")) $("rebalanceStamp").textContent = `Planner refreshed: ${new Date().toLocaleString()}`;
  } catch (e) {
    const meta = normalizeUiError(e, "REBALANCE_SUGGEST_FAILED");
    if ($("rebalanceStamp")) $("rebalanceStamp").textContent = `Planner error: ${meta.reason}`;
    if ($("rebalanceSummary")) $("rebalanceSummary").innerHTML = `<div class="metric neg">Failed to load planner: ${escapeHtml(meta.reason)}</div>`;
    const body = $("rebalanceTable")?.querySelector("tbody");
    if (body) body.innerHTML = '<tr><td colspan="13">Failed to load suggestions.</td></tr>';
    setRebalancePlannerControlState();
    loadRebalanceClosedHistory().catch(() => {});
    if (throwOnError) throw e;
  }
}

async function lockRebalanceLot() {
  const side = String($("rebalanceSide")?.value || "SELL").toUpperCase();
  const percent = Number($("rebalancePercent")?.value || 0);
  if (!Number.isFinite(percent) || percent <= 0) {
    throw Object.assign(new Error("Percent must be greater than 0."), { code: "REBALANCE_PERCENT_INVALID" });
  }
  const btn = $("rebalanceLockLotBtn");
  if (btn) {
    btn.disabled = true;
    btn.textContent = "Locking...";
  }
  if ($("rebalanceStamp")) $("rebalanceStamp").textContent = `Planner locking: ${new Date().toLocaleString()}`;
  try {
    const out = await api("/api/v1/rebalance/lot/lock", {
      method: "POST",
      body: JSON.stringify({ side, percent }),
    });
    renderRebalancePlanner(out || {});
    if ($("rebalanceStamp")) $("rebalanceStamp").textContent = `Lot locked: ${new Date().toLocaleString()}`;
  } finally {
    setRebalancePlannerControlState();
  }
}

async function resetRebalanceLot() {
  const btn = $("rebalanceResetLotBtn");
  if (btn) {
    btn.disabled = true;
    btn.textContent = "Resetting...";
  }
  if ($("rebalanceStamp")) $("rebalanceStamp").textContent = `Planner reset requested: ${new Date().toLocaleString()}`;
  try {
    await api("/api/v1/rebalance/lot/reset", { method: "POST" });
    await loadRebalanceSuggestions({ throwOnError: true });
    if ($("rebalanceStamp")) $("rebalanceStamp").textContent = `Lot reset: ${new Date().toLocaleString()}`;
  } finally {
    if (btn) btn.textContent = "Reset Lot";
    setRebalancePlannerControlState();
  }
}

async function saveRebalanceGuards() {
  const rows = Array.from($("rebalanceTable")?.querySelectorAll("tbody tr[data-symbol]") || []);
  if (!rows.length) {
    if ($("rebalanceStamp")) $("rebalanceStamp").textContent = "Planner: no rows to save";
    return;
  }
  const items = [];
  for (const tr of rows) {
    const symbol = String(tr.getAttribute("data-symbol") || "").trim().toUpperCase();
    if (!symbol) continue;
    const minEl = tr.querySelector(".rebalance-min-input");
    const maxEl = tr.querySelector(".rebalance-max-input");
    const minValue = parseGuardInputValue(minEl?.value);
    const maxValue = parseGuardInputValue(maxEl?.value);
    if (Number.isNaN(minValue) || Number.isNaN(maxValue)) {
      throw Object.assign(new Error(`Invalid min/max for ${symbol}`), { code: "REBALANCE_GUARD_INVALID" });
    }
    const minVal = minValue === null ? 0 : minValue;
    const maxVal = maxValue;
    if (maxVal !== null && maxVal < minVal) {
      throw Object.assign(new Error(`Max value cannot be less than min value for ${symbol}`), { code: "REBALANCE_GUARD_RANGE_INVALID" });
    }
    items.push({ symbol, min_value: minVal, max_value: maxVal });
  }
  await api("/api/v1/scrips/position-guards", {
    method: "PUT",
    body: JSON.stringify({ items }),
  });
  if ($("rebalanceStamp")) $("rebalanceStamp").textContent = `Limits saved: ${new Date().toLocaleString()}`;
  await loadRebalanceSuggestions({ throwOnError: true });
}

function renderPeakDiff(items) {
  const pendingBySymbol = {};
  (state.pendingPeakSplitCandidates || []).forEach((c) => {
    const sym = String(c.symbol || "").toUpperCase();
    if (!sym) return;
    pendingBySymbol[sym] = (pendingBySymbol[sym] || 0) + 1;
  });
  $("peakTable").querySelector("tbody").innerHTML = items
    .map(
      (r) => `
      <tr data-symbol="${r.symbol}">
        <td>${r.symbol}</td>
        <td>${money(r.ltp)}</td>
        <td>${money(peakPriceValue(r))}</td>
        <td class="${clsBySign(peakPctValue(r))}">${pct(peakPctValue(r))}</td>
        <td>${(pendingBySymbol[String(r.symbol || "").toUpperCase()] || 0) > 0 ? `<button class="btn secondary peak-split-review-btn" data-symbol="${r.symbol}">Review (${pendingBySymbol[String(r.symbol || "").toUpperCase()]})</button>` : "<span class='metric'>-</span>"}</td>
      </tr>`
    )
    .join("");
  document.querySelectorAll("#peakTable tbody tr").forEach((tr) => {
    tr.addEventListener("click", () => {
      const symbol = tr.getAttribute("data-symbol");
      $("symbolSelect").value = symbol;
      loadScrip(symbol);
      setTab("scrip");
    });
  });
  document.querySelectorAll(".peak-split-review-btn").forEach((b) => {
    b.addEventListener("click", () => {
      openPeakSplitReviewModal(b.getAttribute("data-symbol"));
    });
  });
}

function harvestDirectionClass(value) {
  const v = String(value || "").toUpperCase();
  if (v === "DOWN") return "neg";
  if (v === "UP") return "pos";
  return "";
}

function harvestHeldDaysLabel(row) {
  const minDays = Number(row?.held_days_min || 0);
  const maxDays = Number(row?.held_days_max || 0);
  if (maxDays > minDays) return `${minDays}-${maxDays}`;
  return `${minDays}`;
}

function renderHarvestLossBucketTable(tableId, rows, emptyText) {
  const body = $(tableId)?.querySelector("tbody");
  if (!body) return;
  body.innerHTML = rows.length
    ? rows
        .map(
          (r) => `
            <tr>
              <td>${escapeHtml(String(r.symbol || ""))}</td>
              <td>${escapeHtml(String(r.buy_date || "-"))}</td>
              <td>${money(r.qty)}</td>
              <td>${money(r.ltp)}</td>
              <td>${money(r.avg_cost)}</td>
              <td>${escapeHtml(harvestHeldDaysLabel(r))}</td>
              <td class="neg">${money(r.loss_available)}</td>
              <td>${money(r.suggested_qty)}</td>
              <td class="${clsBySign(-Number(r.suggested_realized_loss || 0))}">${money(r.suggested_realized_loss)}</td>
              <td class="reason-cell">${escapeHtml(String(r.reason || ""))}</td>
            </tr>`
        )
        .join("")
    : `<tr><td colspan="10">${escapeHtml(emptyText)}</td></tr>`;
}

function renderLossLotsTable(tableId, rows, emptyText) {
  const body = $(tableId)?.querySelector("tbody");
  if (!body) return;
  body.innerHTML = rows.length
    ? rows
        .map(
          (r) => `
            <tr>
              <td>${escapeHtml(String(r.symbol || ""))}</td>
              <td>${escapeHtml(String(r.buy_date || "-"))}</td>
              <td>${money(r.qty)}</td>
              <td>${money(r.ltp)}</td>
              <td>${money(r.avg_cost)}</td>
              <td>${escapeHtml(harvestHeldDaysLabel(r))}</td>
              <td class="neg">${money(r.loss_available)}</td>
              <td class="reason-cell">${escapeHtml(String(r.reason || ""))}</td>
            </tr>`
        )
        .join("")
    : `<tr><td colspan="8">${escapeHtml(emptyText)}</td></tr>`;
}

function renderProfitLotsTable(tableId, rows, emptyText) {
  const body = $(tableId)?.querySelector("tbody");
  if (!body) return;
  body.innerHTML = rows.length
    ? rows
        .map(
          (r) => `
            <tr>
              <td>${escapeHtml(String(r.symbol || ""))}</td>
              <td>${escapeHtml(String(r.buy_date || "-"))}</td>
              <td>${money(r.qty)}</td>
              <td>${money(r.ltp)}</td>
              <td>${money(r.avg_cost)}</td>
              <td>${escapeHtml(harvestHeldDaysLabel(r))}</td>
              <td class="pos">${money(r.profit_available)}</td>
              <td class="reason-cell">${escapeHtml(String(r.reason || ""))}</td>
            </tr>`
        )
        .join("")
    : `<tr><td colspan="8">${escapeHtml(emptyText)}</td></tr>`;
}

function renderLossLots(payload) {
  const p = payload || {};
  const summary = p.summary || {};
  const stcg = Array.isArray(p.stcg_items) ? p.stcg_items : [];
  const ltcg = Array.isArray(p.ltcg_items) ? p.ltcg_items : [];
  const stcgProfit = Array.isArray(p.stcg_profit_items) ? p.stcg_profit_items : [];
  const ltcgProfit = Array.isArray(p.ltcg_profit_items) ? p.ltcg_profit_items : [];
  if ($("lossLotsSummary")) {
    $("lossLotsSummary").innerHTML = `
      <div class="metric">Total Loss Lots: ${Number(summary.total_loss_lots || 0)}</div>
      <div class="metric">Total Loss Qty: ${money(summary.total_loss_qty)}</div>
      <div class="metric ${clsBySign(-Number(summary.total_loss_available || 0))}">Total Loss Available: ${money(summary.total_loss_available)}</div>
      <div class="metric">STCG Lots: ${Number(summary.stcg_loss_lots || 0)}</div>
      <div class="metric">STCG Qty: ${money(summary.stcg_loss_qty)}</div>
      <div class="metric ${clsBySign(-Number(summary.stcg_loss_available || 0))}">STCG Loss: ${money(summary.stcg_loss_available)}</div>
      <div class="metric">LTCG Lots: ${Number(summary.ltcg_loss_lots || 0)}</div>
      <div class="metric">LTCG Qty: ${money(summary.ltcg_loss_qty)}</div>
      <div class="metric ${clsBySign(-Number(summary.ltcg_loss_available || 0))}">LTCG Loss: ${money(summary.ltcg_loss_available)}</div>
      <div class="metric">Total Profit Lots: ${Number(summary.total_profit_lots || 0)}</div>
      <div class="metric">Total Profit Qty: ${money(summary.total_profit_qty)}</div>
      <div class="metric pos">Total Profit Available: ${money(summary.total_profit_available)}</div>
      <div class="metric">STCG Profit Lots: ${Number(summary.stcg_profit_lots || 0)}</div>
      <div class="metric">STCG Profit Qty: ${money(summary.stcg_profit_qty)}</div>
      <div class="metric pos">STCG Profit: ${money(summary.stcg_profit_available)}</div>
      <div class="metric">LTCG Profit Lots: ${Number(summary.ltcg_profit_lots || 0)}</div>
      <div class="metric">LTCG Profit Qty: ${money(summary.ltcg_profit_qty)}</div>
      <div class="metric pos">LTCG Profit: ${money(summary.ltcg_profit_available)}</div>
    `;
  }
  renderLossLotsTable("lossLotsStcgTable", stcg, "No STCG loss lots.");
  renderLossLotsTable("lossLotsLtcgTable", ltcg, "No LTCG loss lots.");
  renderProfitLotsTable("lossLotsStcgProfitTable", stcgProfit, "No STCG profit lots.");
  renderProfitLotsTable("lossLotsLtcgProfitTable", ltcgProfit, "No LTCG profit lots.");
}

function renderHarvestPlan(payload) {
  const p = payload || {};
  const summary = p.summary || {};
  const harvest = Array.isArray(p.harvest_candidates) ? p.harvest_candidates : [];
  const profits = Array.isArray(p.profit_offset_candidates) ? p.profit_offset_candidates : [];
  const analysis = p.analysis || {};
  const macro = p.macro || {};
  state.harvestPlanRaw = p;
  state.harvestPlanMeta = { summary, analysis, macro };

  if ($("harvestSummary")) {
    $("harvestSummary").innerHTML = `
      <div class="metric">Target Loss: ${money(summary.target_loss)}</div>
      <div class="metric ${clsBySign(-Number(summary.total_loss_available || 0))}">Loss Available: ${money(summary.total_loss_available)}</div>
      <div class="metric ${clsBySign(-Number(summary.total_loss_available_stcg || 0))}">STCG Loss Available: ${money(summary.total_loss_available_stcg)}</div>
      <div class="metric ${clsBySign(-Number(summary.total_loss_available_ltcg || 0))}">LTCG Loss Available: ${money(summary.total_loss_available_ltcg)}</div>
      <div class="metric pos">Profit Available: ${money(summary.total_profit_available)}</div>
      <div class="metric pos">STCG Profit Available: ${money(summary.total_profit_available_stcg)}</div>
      <div class="metric pos">LTCG Profit Available: ${money(summary.total_profit_available_ltcg)}</div>
      <div class="metric ${clsBySign(-Number(summary.suggested_harvest_loss || 0))}">Suggested Harvest Loss: ${money(summary.suggested_harvest_loss)}</div>
      <div class="metric ${clsBySign(-Number(summary.suggested_harvest_loss_stcg || 0))}">Suggested STCG Harvest: ${money(summary.suggested_harvest_loss_stcg)}</div>
      <div class="metric ${clsBySign(-Number(summary.suggested_harvest_loss_ltcg || 0))}">Suggested LTCG Harvest: ${money(summary.suggested_harvest_loss_ltcg)}</div>
      <div class="metric pos">Suggested Offset Profit: ${money(summary.suggested_offset_profit)}</div>
      <div class="metric pos">Suggested STCG Offset: ${money(summary.suggested_offset_profit_stcg)}</div>
      <div class="metric pos">Suggested LTCG Offset: ${money(summary.suggested_offset_profit_ltcg)}</div>
      <div class="metric">Harvest Lines: ${Number(summary.suggested_harvest_count || 0)}</div>
      <div class="metric">Offset Lines: ${Number(summary.suggested_offset_count || 0)}</div>
      <div class="metric">Unfilled Loss Target: ${money(summary.unfilled_harvest_loss)}</div>
      <div class="metric">Unfilled Offset Profit: ${money(summary.unfilled_offset_profit)}</div>
      <div class="metric">Unfilled STCG Offset: ${money(summary.unfilled_offset_profit_stcg)}</div>
      <div class="metric">Unfilled LTCG Offset: ${money(summary.unfilled_offset_profit_ltcg)}</div>
      <div class="metric">Macro: ${String(macro.regime || "neutral").toUpperCase()} (${Number(macro.score || 0).toFixed(2)})</div>
    `;
  }

  const lossBody = $("harvestLossTable")?.querySelector("tbody");
  if (lossBody) {
    lossBody.innerHTML = harvest.length
      ? harvest
          .map(
            (r) => `
            <tr>
              <td>${escapeHtml(String(r.symbol || ""))}</td>
              <td>${escapeHtml(String(r.buy_date || "-"))}</td>
              <td>${escapeHtml(String(r.tax_bucket || "-"))}</td>
              <td>${money(r.qty)}</td>
              <td>${money(r.ltp)}</td>
              <td>${money(r.avg_cost)}</td>
              <td>${escapeHtml(harvestHeldDaysLabel(r))}</td>
              <td class="${clsBySign(r.unrealized_pnl)}">${money(r.unrealized_pnl)}</td>
              <td class="neg">${money(r.loss_available)}</td>
              <td class="${harvestDirectionClass(r.likely_direction)}">${escapeHtml(String(r.likely_direction || "-"))}</td>
              <td>${escapeHtml(String(r.strategy_action || "-"))}</td>
              <td>${money(r.suggested_qty)}</td>
              <td class="${clsBySign(-Number(r.suggested_realized_loss || 0))}">${money(r.suggested_realized_loss)}</td>
              <td class="reason-cell">${escapeHtml(String(r.reason || ""))}</td>
            </tr>`
          )
          .join("")
      : '<tr><td colspan="14">No harvestable loss positions.</td></tr>';
  }

  const profitBody = $("harvestProfitTable")?.querySelector("tbody");
  if (profitBody) {
    profitBody.innerHTML = profits.length
      ? profits
          .map(
            (r) => `
            <tr>
              <td>${escapeHtml(String(r.symbol || ""))}</td>
              <td>${escapeHtml(String(r.buy_date || "-"))}</td>
              <td>${escapeHtml(String(r.tax_bucket || "-"))}</td>
              <td>${money(r.qty)}</td>
              <td>${money(r.ltp)}</td>
              <td>${money(r.avg_cost)}</td>
              <td>${escapeHtml(harvestHeldDaysLabel(r))}</td>
              <td class="${clsBySign(r.unrealized_pnl)}">${money(r.unrealized_pnl)}</td>
              <td class="pos">${money(r.profit_available)}</td>
              <td class="${harvestDirectionClass(r.likely_direction)}">${escapeHtml(String(r.likely_direction || "-"))}</td>
              <td>${escapeHtml(String(r.strategy_action || "-"))}</td>
              <td>${money(r.suggested_qty)}</td>
              <td class="${clsBySign(r.suggested_realized_profit)}">${money(r.suggested_realized_profit)}</td>
              <td class="reason-cell">${escapeHtml(String(r.reason || ""))}</td>
            </tr>`
          )
          .join("")
      : '<tr><td colspan="14">No profitable offset candidates.</td></tr>';
  }

  if ($("harvestAnalysisMeta")) {
    const mode = String(analysis.mode || "none").toUpperCase();
    const model = analysis.model ? ` | Model: ${analysis.model}` : "";
    const provider = analysis.provider ? ` | Provider: ${analysis.provider}` : "";
    const err = analysis.error ? ` | ${analysis.error}` : "";
    $("harvestAnalysisMeta").textContent = `Mode: ${mode}${provider}${model}${err}`;
  }
  if ($("harvestAnalysisText")) {
    $("harvestAnalysisText").textContent = String(analysis.text || "No analysis yet.");
  }
}

async function loadHarvestPlan(options = {}) {
  const runAnalysis = !!options.runAnalysis;
  const throwOnError = !!options.throwOnError;
  const targetLoss = Math.max(0, Number($("harvestTargetLoss")?.value || 0));
  if ($("harvestStatusText")) {
    $("harvestStatusText").textContent = runAnalysis
      ? `Harvest planner running dynamic analysis: ${new Date().toLocaleString()}`
      : `Harvest planner refreshing: ${new Date().toLocaleString()}`;
  }
  const params = new URLSearchParams();
  params.set("target_loss", String(Number(targetLoss.toFixed(2))));
  if (runAnalysis) params.set("run_analysis", "1");
  try {
    const res = await api(`/api/v1/harvest/plan?${params.toString()}`);
    renderHarvestPlan(res || {});
    if ($("harvestStatusText")) {
      $("harvestStatusText").textContent = runAnalysis
        ? `Harvest dynamic analysis refreshed: ${new Date().toLocaleString()}`
        : `Harvest planner refreshed: ${new Date().toLocaleString()}`;
    }
  } catch (e) {
    const meta = normalizeUiError(e, runAnalysis ? "HARVEST_ANALYSIS_FAILED" : "HARVEST_PLAN_FAILED");
    if ($("harvestStatusText")) $("harvestStatusText").textContent = `Harvest planner error: ${meta.reason}`;
    if ($("harvestSummary")) $("harvestSummary").innerHTML = `<div class="metric neg">Harvest planner unavailable: ${escapeHtml(meta.reason)}</div>`;
    if ($("harvestAnalysisText")) $("harvestAnalysisText").textContent = meta.reason;
    const lossBody = $("harvestLossTable")?.querySelector("tbody");
    const profitBody = $("harvestProfitTable")?.querySelector("tbody");
    if (lossBody) lossBody.innerHTML = '<tr><td colspan="14">Failed to load harvest candidates.</td></tr>';
    if (profitBody) profitBody.innerHTML = '<tr><td colspan="14">Failed to load profit candidates.</td></tr>';
    if (throwOnError) throw e;
  }
}

async function loadLossLots(options = {}) {
  const throwOnError = !!options.throwOnError;
  if ($("lossLotsStatusText")) {
    $("lossLotsStatusText").textContent = `Loss lots refreshing: ${new Date().toLocaleString()}`;
  }
  try {
    const res = await api("/api/v1/loss-lots");
    renderLossLots(res || {});
    if ($("lossLotsStatusText")) {
      $("lossLotsStatusText").textContent = `Loss lots refreshed: ${new Date().toLocaleString()}`;
    }
  } catch (e) {
    const meta = normalizeUiError(e, "LOSS_LOTS_FAILED");
    if ($("lossLotsStatusText")) $("lossLotsStatusText").textContent = `Loss lots error: ${meta.reason}`;
    if ($("lossLotsSummary")) $("lossLotsSummary").innerHTML = `<div class="metric neg">Loss lots unavailable: ${escapeHtml(meta.reason)}</div>`;
    const stcgBody = $("lossLotsStcgTable")?.querySelector("tbody");
    const ltcgBody = $("lossLotsLtcgTable")?.querySelector("tbody");
    const stcgProfitBody = $("lossLotsStcgProfitTable")?.querySelector("tbody");
    const ltcgProfitBody = $("lossLotsLtcgProfitTable")?.querySelector("tbody");
    if (stcgBody) stcgBody.innerHTML = '<tr><td colspan="8">Failed to load STCG loss lots.</td></tr>';
    if (ltcgBody) ltcgBody.innerHTML = '<tr><td colspan="8">Failed to load LTCG loss lots.</td></tr>';
    if (stcgProfitBody) stcgProfitBody.innerHTML = '<tr><td colspan="8">Failed to load STCG profit lots.</td></tr>';
    if (ltcgProfitBody) ltcgProfitBody.innerHTML = '<tr><td colspan="8">Failed to load LTCG profit lots.</td></tr>';
    if (throwOnError) throw e;
  }
}

function dailyTargetStateOptions(currentState) {
  const current = String(currentState || "pending").toLowerCase();
  return [
    ["pending", "Pending"],
    ["sell_done", "Sell Done"],
    ["buy_done", "Buy Done"],
    ["executed", "Done"],
    ["skipped", "Skipped"],
  ]
    .map(([value, label]) => `<option value="${value}" ${value === current ? "selected" : ""}>${label}</option>`)
    .join("");
}

function isClosedDailyTargetState(rawState) {
  const s = String(rawState || "").toLowerCase();
  return s === "executed" || s === "skipped" || s === "replaced";
}

function renderDailyTargetHistory(payload) {
  const body = $("dailyTargetHistoryTable")?.querySelector("tbody");
  if (!body) return;
  const items = Array.isArray(payload?.items) ? payload.items : [];
  const summary = payload?.summary || {};
  state.dailyTargetHistoryRaw = items;
  if ($("dailyTargetHistorySummary")) {
    $("dailyTargetHistorySummary").innerHTML = `
      <div class="metric">Shown: ${Number(summary.filtered_count ?? items.length)}</div>
      <div class="metric">Total: ${Number(summary.total_count ?? items.length)}</div>
      <div class="metric">Done: ${Number(summary.executed || 0)}</div>
      <div class="metric">Pending: ${Number(summary.pending || 0)}</div>
      <div class="metric">Sell Done: ${Number(summary.sell_done || 0)}</div>
      <div class="metric">Buy Done: ${Number(summary.buy_done || 0)}</div>
      <div class="metric">Skipped: ${Number(summary.skipped || 0)}</div>
      <div class="metric">Replaced: ${Number(summary.replaced || 0)}</div>
      <div class="metric">From: ${escapeHtml(String(summary.date_from || "-"))}</div>
      <div class="metric">To: ${escapeHtml(String(summary.date_to || "-"))}</div>
      <div class="metric">State Filter: ${escapeHtml(String(summary.state_filter || "all"))}</div>
    `;
  }
  body.innerHTML = items.length
    ? items
        .map(
          (r) => `
            <tr>
              <td>${escapeHtml(String(r.updated_at || "-"))}</td>
              <td>${Number(r.plan_id || 0)}</td>
              <td>${Number(r.priority_rank || 0)}</td>
              <td>${escapeHtml(String(r.state || "-"))}</td>
              <td>${escapeHtml(String(r.sell_symbol || ""))} x ${money(r.sell_qty)}</td>
              <td>${escapeHtml(String(r.buy_symbol || ""))} x ${money(r.buy_qty)}</td>
              <td>${money(r.seed_capital)}</td>
              <td>${pct(r.target_profit_pct)}</td>
              <td class="${clsBySign(r.expected_profit_value)}">${money(r.expected_profit_value)}</td>
              <td>${money(r.current_buy_value)}</td>
              <td class="${clsBySign(r.live_mtm_pnl)}">${money(r.live_mtm_pnl)}</td>
              <td>${r.executed_sell_value ? money(r.executed_sell_value) : "-"}</td>
              <td>${r.executed_buy_value ? money(r.executed_buy_value) : "-"}</td>
              <td class="reason-cell">${escapeHtml(String(r.completion_note || ""))}</td>
            </tr>`
        )
        .join("")
    : '<tr><td colspan="14">No tracked rotation history yet.</td></tr>';
}

function bindDailyTargetTableActions() {
  const rememberDraft = (row) => {
    const pairId = Number(row?.querySelector(".daily-target-save-btn")?.getAttribute("data-pair-id") || 0);
    if (!pairId) return;
    state.dailyTargetDrafts[pairId] = {
      state: row?.querySelector(".daily-target-state-select")?.value || "pending",
      executed_sell_price: row?.querySelector(".daily-target-sell-price")?.value || "",
      executed_sell_at: row?.querySelector(".daily-target-sell-date")?.value || "",
      executed_buy_price: row?.querySelector(".daily-target-buy-price")?.value || "",
      executed_buy_at: row?.querySelector(".daily-target-buy-date")?.value || "",
      note: row?.querySelector(".daily-target-note")?.value || "",
    };
  };
  Array.from(
    document.querySelectorAll(
      "#dailyTargetTable .daily-target-state-select, #dailyTargetTable .daily-target-sell-price, #dailyTargetTable .daily-target-sell-date, #dailyTargetTable .daily-target-buy-price, #dailyTargetTable .daily-target-buy-date, #dailyTargetTable .daily-target-note"
    )
  ).forEach((el) => {
    const eventName = el.matches("select, input[type='date']") ? "change" : "input";
    el.addEventListener(eventName, () => {
      const row = el.closest("tr");
      if (row) rememberDraft(row);
    });
    if (eventName !== "change") {
      el.addEventListener("change", () => {
        const row = el.closest("tr");
        if (row) rememberDraft(row);
      });
    }
  });
  Array.from(document.querySelectorAll("#dailyTargetTable .daily-target-save-btn")).forEach((btn) => {
    btn.addEventListener("click", async () => {
      const itemId = Number(btn.getAttribute("data-pair-id") || 0);
      const row = btn.closest("tr");
      const stateEl = row?.querySelector(".daily-target-state-select");
      const sellPriceEl = row?.querySelector(".daily-target-sell-price");
      const sellDateEl = row?.querySelector(".daily-target-sell-date");
      const buyPriceEl = row?.querySelector(".daily-target-buy-price");
      const buyDateEl = row?.querySelector(".daily-target-buy-date");
      const noteEl = row?.querySelector(".daily-target-note");
      if (!itemId || !stateEl) return;
      if ($("dailyTargetStatusText")) {
        $("dailyTargetStatusText").textContent = `Updating rotation item ${itemId}: ${new Date().toLocaleString()}`;
      }
      try {
        const out = await api(`/api/v1/daily-target/pairs/${itemId}`, {
          method: "PUT",
          body: JSON.stringify({
            state: stateEl.value,
            executed_sell_price: sellPriceEl?.value || null,
            executed_sell_at: sellDateEl?.value || null,
            executed_buy_price: buyPriceEl?.value || null,
            executed_buy_at: buyDateEl?.value || null,
            note: noteEl?.value || "",
          }),
        });
        delete state.dailyTargetDrafts[itemId];
        renderDailyTargetPlan(out || {});
        await loadDailyTargetHistory();
        const savedRow = Array.isArray(out?.pairs) ? out.pairs.find((r) => Number(r?.pair_id || 0) === itemId) : null;
        const savedStateLabel = savedRow?.state ? String(savedRow.state).replaceAll("_", " ") : "updated";
        if ($("dailyTargetStatusText")) {
          $("dailyTargetStatusText").textContent = `Rotation item ${savedStateLabel}: ${new Date().toLocaleString()}`;
        }
      } catch (e) {
        const meta = normalizeUiError(e, "DAILY_TARGET_UPDATE_FAILED");
        if ($("dailyTargetStatusText")) $("dailyTargetStatusText").textContent = `Daily target error: ${meta.reason}`;
        throw e;
      }
    });
  });
}

function renderDailyTargetPlan(payload) {
  const p = payload || {};
  const plan = p.plan || {};
  const summary = p.summary || {};
  const perf = p.performance || {};
  const pairs = Array.isArray(p.pairs) ? p.pairs : [];
  const activePairs = pairs.filter((r) => !isClosedDailyTargetState(r?.state));
  const completedPairs = pairs.filter((r) => isClosedDailyTargetState(r?.state));
  const snapshots = Array.isArray(p.snapshots) ? p.snapshots : [];
  const fullCycles = Array.isArray(p.full_cycles) ? p.full_cycles.slice(0, 10) : [];
  const llmReview = p.llm_review || null;
  state.dailyTargetPlanRaw = p;
  if ($("dailyTargetSummary")) {
    const taxMode = String(summary.tax_mode || "-").replaceAll("_", " ");
    const zerodhaCostModel = String(summary.zerodha_cost_model || "-").replaceAll("_", " ");
    const effectiveSeed = summary.effective_seed_capital || summary.suggested_next_seed_capital || summary.seed_capital;
    const effectiveTradeCapital = summary.effective_trade_capital || summary.economic_min_trade_value || effectiveSeed;
    const effectiveTarget = summary.effective_target_profit_value || summary.target_profit_value;
    const tradeSizeAdvice = String(summary.trade_size_advice || "-");
    $("dailyTargetSummary").innerHTML = `
      <div class="metric">Plan: ${plan.id ? `#${Number(plan.id)}` : "-"}</div>
      <div class="metric">Starting Capital: ${money(summary.seed_capital)}</div>
      <div class="metric pos" title="Compounded capital used for today's pair sizing = Starting Capital + all realized profits">Effective Capital: ${money(effectiveSeed)}</div>
      <div class="metric ${Number(effectiveTradeCapital || 0) > Number(effectiveSeed || 0) ? "warn" : "pos"}" title="Charge-aware trade value. If fixed charges eat too much of the target, the planner recommends a larger lot.">Economical Trade Value: ${money(effectiveTradeCapital)}</div>
      <div class="metric">Target %: ${pct(summary.target_profit_pct)}</div>
      <div class="metric ${clsBySign(effectiveTarget)}" title="Today's profit target = charge-aware trade value x Target %">Today's Target Profit: ${money(effectiveTarget)}</div>
      <div class="metric ${Number(summary.charge_drag_pct_at_seed || 0) > Number(summary.max_charge_drag_pct || 45) ? "warn" : "pos"}" title="Estimated sell-now + buy-entry + future-sell charges as % of target profit at the starting seed.">Charge Drag @ Seed: ${pct(summary.charge_drag_pct_at_seed)}</div>
      <div class="metric ${Number(summary.charge_drag_pct_at_effective || 0) > Number(summary.max_charge_drag_pct || 45) ? "warn" : "pos"}" title="Estimated charges as % of target profit at the recommended trade size.">Charge Drag @ Trade: ${pct(summary.charge_drag_pct_at_effective)}</div>
      <div class="metric">Est. Charges @ Trade: ${money(summary.estimated_charges_at_effective)}</div>
      <div class="metric" title="${escapeHtml(tradeSizeAdvice)}">Size Advice: ${escapeHtml(tradeSizeAdvice)}</div>
      <div class="metric ${clsBySign(summary.projected_pending_profit)}">Projected Pending Profit: ${money(summary.projected_pending_profit)}</div>
      <div class="metric">Pending: ${Number(summary.pending_pairs || 0)}</div>
      <div class="metric">Sell Done: ${Number(summary.sell_done_pairs || 0)}</div>
      <div class="metric">Buy Done: ${Number(summary.buy_done_pairs || 0)}</div>
      <div class="metric">Executed: ${Number(summary.executed_pairs || 0)}</div>
      <div class="metric">Skipped: ${Number(summary.skipped_pairs || 0)}</div>
      <div class="metric">Replaced: ${Number(summary.replaced_pairs || 0)}</div>
      <div class="metric">Status: ${escapeHtml(String(plan.status || "-"))}</div>
      <div class="metric">Tax Mode: ${escapeHtml(taxMode)}</div>
      <div class="metric">STCG Tax: ${pct(summary.equity_stcg_tax_pct)}</div>
      <div class="metric">LTCG Tax: ${pct(summary.equity_ltcg_tax_pct)}</div>
      <div class="metric">LTCG Exemption Limit: ${money(summary.equity_ltcg_exemption_limit)}</div>
      <div class="metric">FY: ${escapeHtml(String(summary.fy_label || "-"))}</div>
      <div class="metric">Realized LTCG This FY: ${money(summary.realized_ltcg_net_gain)}</div>
      <div class="metric ${Number(summary.remaining_ltcg_exemption || 0) > 0 ? "pos" : "neg"}">Remaining LTCG Exemption: ${money(summary.remaining_ltcg_exemption)}</div>
      <div class="metric">Tax Bracket Ref: ${pct(summary.investor_tax_bracket_pct)}</div>
      <div class="metric">Broker Cost Model: ${escapeHtml(zerodhaCostModel)}</div>
      <div class="metric ${llmReview?.ok ? "pos" : llmReview ? "warn" : ""}">LLM Review: ${escapeHtml(String(llmReview?.status || "not requested"))}</div>
      <div class="metric">LLM Provider: ${escapeHtml(String(llmReview?.provider || "-"))}</div>
      <div class="metric">Created: ${escapeHtml(String(summary.created_at || "-"))}</div>
      <div class="metric">Last Recalibrated: ${escapeHtml(String(summary.last_recalibrated_at || "-"))}</div>
    `;
  }
  if ($("dailyTargetLlmReview")) {
    if (!llmReview) {
      $("dailyTargetLlmReview").textContent = "LLM pair judgment not requested.";
    } else if (llmReview.ok) {
      $("dailyTargetLlmReview").textContent = `LLM pair judgment applied via ${llmReview.provider || "-"} (${Number(llmReview.latency_ms || 0).toFixed(0)} ms). See Agent Note column for pair-level verdicts.`;
    } else {
      $("dailyTargetLlmReview").textContent = `LLM pair judgment unavailable: ${llmReview.error || llmReview.status || "unknown"}. Deterministic Daily Target planner output is still shown.`;
    }
  }
  if ($("dailyTargetPerformance")) {
    $("dailyTargetPerformance").innerHTML = `
      <div class="metric">Starting Capital: ${money(perf.starting_capital)}</div>
      <div class="metric pos" title="Starting Capital + all realized profits to date">Compounded Capital: ${money(perf.realized_compounded_capital)}</div>
      <div class="metric ${clsBySign(perf.realized_profit_value)}">Realized Profit: ${money(perf.realized_profit_value)}</div>
      <div class="metric ${clsBySign(perf.realized_profit_pct)}">Realized Return %: ${pct(perf.realized_profit_pct)}</div>
      <div class="metric">MTM Capital (incl. open): ${money(perf.current_compounded_capital)}</div>
      <div class="metric ${clsBySign(perf.compounded_return_value)}">Total Strategy P/L: ${money(perf.compounded_return_value)}</div>
      <div class="metric ${clsBySign(perf.compounded_return_pct)}">Total Strategy Return %: ${pct(perf.compounded_return_pct)}</div>
      <div class="metric">Executed Rotations: ${Number(perf.executed_rotation_count || 0)}</div>
      <div class="metric">Open Tracked Positions: ${Number(perf.open_position_count || 0)}</div>
      <div class="metric">Cumulative Sell Value: ${money(perf.cumulative_sell_value)}</div>
      <div class="metric">Cumulative Buy Value: ${money(perf.cumulative_buy_value)}</div>
      <div class="metric">Open Trade Cost Basis: ${money(perf.live_mtm_basis_value)}</div>
      <div class="metric ${clsBySign(perf.live_mtm_pnl)}">Net Live P/L: ${money(perf.live_mtm_pnl)}</div>
      <div class="metric ${clsBySign(perf.live_mtm_return_pct)}">Net Live Return %: ${pct(perf.live_mtm_return_pct)}</div>
      <div class="metric">Latest Symbol: ${escapeHtml(String(perf.latest_symbol || "-"))}</div>
      <div class="metric">Latest Trade Date: ${escapeHtml(String(perf.latest_trade_date || "-"))}</div>
    `;
  }
  const fullCycleBody = $("dailyTargetFullCycleTable")?.querySelector("tbody");
  if (fullCycleBody) {
    fullCycleBody.innerHTML = fullCycles.length
      ? fullCycles
          .map(
            (r) => `
              <tr>
                <td>${escapeHtml(String(r.exit_at || "-"))}</td>
                <td>${escapeHtml(String(r.symbol || ""))}</td>
                <td>${money(r.qty)}</td>
                <td>${money(r.entry_price)}</td>
                <td>${money(r.exit_price)}</td>
                <td>${money(r.entry_value)}</td>
                <td>${money(r.exit_value)}</td>
                <td class="${clsBySign(r.realized_profit)}">${money(r.realized_profit)}</td>
                <td>#${Number(r.source_pair_id || 0)} -> #${Number(r.exit_pair_id || 0)}</td>
                <td class="reason-cell">${escapeHtml(String(r.comment || ""))}</td>
              </tr>`
          )
          .join("")
      : '<tr><td colspan="10">No full cycle completed yet.</td></tr>';
  }
  const body = $("dailyTargetTable")?.querySelector("tbody");
  if (body) {
    body.innerHTML = activePairs.length
      ? activePairs
          .map(
            (r) => {
              const pairId = Number(r.pair_id || 0);
              const draft = state.dailyTargetDrafts[pairId] || {};
              const stateValue = draft.state || r.state || "pending";
              const sellPriceValue =
                draft.executed_sell_price !== undefined
                  ? draft.executed_sell_price
                  : r.executed_sell_price
                    ? Number(r.executed_sell_price).toFixed(2)
                    : "";
              const sellDateValue =
                draft.executed_sell_at !== undefined ? draft.executed_sell_at : r.executed_sell_at ? String(r.executed_sell_at).slice(0, 10) : "";
              const buyPriceValue =
                draft.executed_buy_price !== undefined
                  ? draft.executed_buy_price
                  : r.executed_buy_price
                    ? Number(r.executed_buy_price).toFixed(2)
                    : "";
              const buyDateValue =
                draft.executed_buy_at !== undefined ? draft.executed_buy_at : r.executed_buy_at ? String(r.executed_buy_at).slice(0, 10) : "";
              const noteValue = draft.note !== undefined ? draft.note : String(r.completion_note || "");
              return `
              <tr>
                <td>${Number(r.priority_rank || 0)}</td>
                <td>${escapeHtml(String(r.state || "-"))}</td>
                <td>${escapeHtml(String(r.sell_symbol || ""))}</td>
                <td>${money(r.sell_qty)}</td>
                <td>${money(r.current_sell_ref_price || r.sell_ref_price)}</td>
                <td>${money(r.sell_trade_value)}</td>
                <td>${escapeHtml(String(r.buy_symbol || ""))}</td>
                <td>${money(r.buy_qty)}</td>
                <td>${money(r.current_buy_ref_price || r.buy_ref_price)}</td>
                <td>${money(r.buy_trade_value)}</td>
                <td>${money(r.buy_target_exit_price)}</td>
                <td class="${clsBySign(r.expected_profit_value)}">${money(r.expected_profit_value)}</td>
                <td>${Number(r.rotation_score || 0).toFixed(2)}</td>
                <td class="reason-cell ${String(r.llm_verdict || "").toUpperCase() === "AVOID" ? "neg" : String(r.llm_verdict || "").toUpperCase() === "GO" ? "pos" : "warn"}">${escapeHtml(`${r.llm_verdict ? `${r.llm_verdict}: ` : ""}${r.llm_note || ""}` || "-")}</td>
                <td class="${Number(r.target_progress_pct || 0) >= 100 ? "exit-now" : clsBySign(r.target_progress_pct)}">${Number(r.target_progress_pct || 0) >= 100 ? `&#9889; EXIT NOW (${pct(r.target_progress_pct)})` : pct(r.target_progress_pct)}</td>
                <td class="reason-cell">${escapeHtml(String(r.sell_reason || ""))}</td>
                <td class="reason-cell">${escapeHtml(String(r.buy_reason || ""))}</td>
                <td>
                  <div class="daily-target-exec-cell">
                    <select class="daily-target-state-select" data-pair-id="${pairId}">
                      ${dailyTargetStateOptions(stateValue)}
                    </select>
                    <input class="daily-target-sell-price" type="number" min="0.01" step="0.01" placeholder="sell px" value="${escapeHtml(String(sellPriceValue || ""))}">
                    <input class="daily-target-sell-date" type="date" value="${escapeHtml(String(sellDateValue || ""))}">
                    <input class="daily-target-buy-price" type="number" min="0.01" step="0.01" placeholder="buy px" value="${escapeHtml(String(buyPriceValue || ""))}">
                    <input class="daily-target-buy-date" type="date" value="${escapeHtml(String(buyDateValue || ""))}">
                    <input class="daily-target-note" type="text" maxlength="180" placeholder="note" value="${escapeHtml(String(noteValue || ""))}">
                    <button class="btn secondary daily-target-save-btn" data-pair-id="${pairId}">Update</button>
                  </div>
                </td>
              </tr>`;
            }
          )
          .join("")
      : '<tr><td colspan="18">No active daily target rotation ideas.</td></tr>';
  }
  const completedBody = $("dailyTargetCompletedTable")?.querySelector("tbody");
  if (completedBody) {
    completedBody.innerHTML = completedPairs.length
      ? completedPairs
          .map(
            (r) => `
              <tr>
                <td>${Number(r.priority_rank || 0)}</td>
                <td>${escapeHtml(String(r.state || "-"))}</td>
                <td>${escapeHtml(String(r.sell_symbol || ""))} x ${money(r.sell_qty)}</td>
                <td>${escapeHtml(String(r.buy_symbol || ""))} x ${money(r.buy_qty)}</td>
                <td>${r.executed_sell_price ? `${money(r.executed_sell_price)} / ${money(r.executed_sell_value || 0)}` : "-"}</td>
                <td>${r.executed_buy_price ? `${money(r.executed_buy_price)} / ${money(r.executed_buy_value || 0)}` : "-"}</td>
                <td>${escapeHtml(String(r.executed_buy_at || r.executed_sell_at || r.updated_at || "-"))}</td>
                <td class="reason-cell">${escapeHtml(String(r.completion_note || ""))}</td>
              </tr>`
          )
          .join("")
      : '<tr><td colspan="8">No completed rotations in the current cycle.</td></tr>';
  }
  const snapBody = $("dailyTargetSnapshotsTable")?.querySelector("tbody");
  if (snapBody) {
    snapBody.innerHTML = snapshots.length
      ? snapshots
          .map(
            (r) => `
              <tr>
                <td>${escapeHtml(String(r.captured_at || "-"))}</td>
                <td>${Number(r.priority_rank || 0)}</td>
                <td>${escapeHtml(`${String(r.sell_symbol || "")} -> ${String(r.buy_symbol || "")}`)}</td>
                <td>${money(r.sell_ref_price)}</td>
                <td>${money(r.buy_ref_price)}</td>
                <td>${money(r.buy_target_exit_price)}</td>
                <td class="${clsBySign(r.expected_profit_value)}">${money(r.expected_profit_value)}</td>
                <td>${Number(r.rotation_score || 0).toFixed(2)}</td>
                <td class="reason-cell">${escapeHtml(String(r.snapshot_note || ""))}</td>
              </tr>`
          )
          .join("")
      : '<tr><td colspan="9">No recalibration log yet.</td></tr>';
  }
  bindDailyTargetTableActions();
}

async function loadDailyTargetHistory(options = {}) {
  const throwOnError = !!options.throwOnError;
  const params = new URLSearchParams();
  params.set("limit", "250");
  const from = String($("dailyTargetHistoryFrom")?.value || "").trim();
  const to = String($("dailyTargetHistoryTo")?.value || "").trim();
  const stateFilter = String($("dailyTargetHistoryState")?.value || "all").trim().toLowerCase();
  if (from) params.set("date_from", from);
  if (to) params.set("date_to", to);
  if (stateFilter && stateFilter !== "all") params.set("state", stateFilter);
  try {
    const res = await api(`/api/v1/daily-target/history?${params.toString()}`);
    renderDailyTargetHistory(res || {});
  } catch (e) {
    const body = $("dailyTargetHistoryTable")?.querySelector("tbody");
    const meta = normalizeUiError(e, "DAILY_TARGET_HISTORY_FAILED");
    if (body) body.innerHTML = `<tr><td colspan="15">Failed to load history: ${escapeHtml(meta.reason)}</td></tr>`;
    if ($("dailyTargetHistorySummary")) $("dailyTargetHistorySummary").innerHTML = `<div class="metric neg">History unavailable: ${escapeHtml(meta.reason)}</div>`;
    if (throwOnError) throw e;
  }
}

async function loadDailyTargetPlan(options = {}) {
  const throwOnError = !!options.throwOnError;
  const recalibrate = options.recalibrate !== false;
  const seedCapital = Math.max(1000, Number($("dailyTargetSeedCapital")?.value || 10000));
  const targetProfitPct = Math.max(0.1, Number($("dailyTargetProfitPct")?.value || 1));
  const topN = Math.max(1, Math.min(10, Number($("dailyTargetTopN")?.value || 5)));
  const useHostedLlm = !!$("dailyTargetUseHostedLlm")?.checked;
  if ($("dailyTargetStatusText")) {
    $("dailyTargetStatusText").textContent = recalibrate
      ? `Refreshing ideas${useHostedLlm ? " + LLM review" : ""}: ${new Date().toLocaleString()}`
      : `Loading: ${new Date().toLocaleString()}`;
  }
  const params = new URLSearchParams();
  params.set("seed_capital", String(Number(seedCapital.toFixed(2))));
  params.set("target_profit_pct", String(Number(targetProfitPct.toFixed(2))));
  params.set("top_n", String(topN));
  params.set("recalibrate", recalibrate ? "1" : "0");
  params.set("use_hosted_llm", useHostedLlm ? "1" : "0");
  try {
    const res = await api(`/api/v1/daily-target/plan?${params.toString()}`);
    // On live-poll refreshes (recalibrate=false), preserve any in-progress
    // field values so typing is not clobbered. If focus is inside the table,
    // skip the re-render entirely to avoid interrupting active input.
    if (!recalibrate) {
      const table = $("dailyTargetTable");
      const focused = table?.contains(document.activeElement);
      if (focused) {
        // User is actively editing â€” skip render, just update status
      } else {
        // Save filled-in fields keyed by pair ID before re-render
        const saved = {};
        (table?.querySelectorAll(".daily-target-save-btn") || []).forEach((btn) => {
          const pid = btn.getAttribute("data-pair-id");
          const row = btn.closest("tr");
          if (!row) return;
          const sp = row.querySelector(".daily-target-sell-price")?.value || "";
          const sd = row.querySelector(".daily-target-sell-date")?.value || "";
          const bp = row.querySelector(".daily-target-buy-price")?.value || "";
          const bd = row.querySelector(".daily-target-buy-date")?.value || "";
          const nt = row.querySelector(".daily-target-note")?.value || "";
          const st = row.querySelector(".daily-target-state-select")?.value || "";
          if (sp || sd || bp || bd || nt || st) saved[pid] = { sp, sd, bp, bd, nt, st };
        });
        renderDailyTargetPlan(res || {});
        // Restore any values the user had typed
        Object.entries(saved).forEach(([pid, v]) => {
          const btn = table?.querySelector(`[data-pair-id="${pid}"]`);
          const row = btn?.closest("tr");
          if (!row) return;
          if (v.sp) { const el = row.querySelector(".daily-target-sell-price"); if (el) el.value = v.sp; }
          if (v.sd) { const el = row.querySelector(".daily-target-sell-date"); if (el) el.value = v.sd; }
          if (v.bp) { const el = row.querySelector(".daily-target-buy-price"); if (el) el.value = v.bp; }
          if (v.bd) { const el = row.querySelector(".daily-target-buy-date"); if (el) el.value = v.bd; }
          if (v.nt) { const el = row.querySelector(".daily-target-note"); if (el) el.value = v.nt; }
          if (v.st) { const el = row.querySelector(".daily-target-state-select"); if (el) el.value = v.st; }
        });
      }
    } else {
      renderDailyTargetPlan(res || {});
    }
    await loadDailyTargetHistory();
    if (useHostedLlm) await loadHostedLlmMetrics();
    if ($("dailyTargetStatusText")) {
      $("dailyTargetStatusText").textContent = recalibrate
        ? `Ideas refreshed${useHostedLlm ? " with LLM review" : ""}: ${new Date().toLocaleString()}`
        : `Loaded: ${new Date().toLocaleString()}`;
    }
  } catch (e) {
    const meta = normalizeUiError(e, "DAILY_TARGET_PLAN_FAILED");
    if ($("dailyTargetStatusText")) $("dailyTargetStatusText").textContent = `Daily target error: ${meta.reason}`;
    if ($("dailyTargetSummary")) $("dailyTargetSummary").innerHTML = `<div class="metric neg">Daily target unavailable: ${escapeHtml(meta.reason)}</div>`;
    const body = $("dailyTargetTable")?.querySelector("tbody");
    const completedBody = $("dailyTargetCompletedTable")?.querySelector("tbody");
    const fullCycleBody = $("dailyTargetFullCycleTable")?.querySelector("tbody");
    const snapBody = $("dailyTargetSnapshotsTable")?.querySelector("tbody");
    if (body) body.innerHTML = '<tr><td colspan="17">Failed to load daily target ideas.</td></tr>';
    if (completedBody) completedBody.innerHTML = '<tr><td colspan="9">Failed to load completed current-cycle items.</td></tr>';
    if (fullCycleBody) fullCycleBody.innerHTML = '<tr><td colspan="10">Failed to load full cycle history.</td></tr>';
    if (snapBody) snapBody.innerHTML = '<tr><td colspan="9">Failed to load recalibration log.</td></tr>';
    if (throwOnError) throw e;
  }
}

async function resetDailyTargetPlan() {
  if ($("dailyTargetStatusText")) {
    $("dailyTargetStatusText").textContent = `Daily target resetting: ${new Date().toLocaleString()}`;
  }
  await api("/api/v1/daily-target/reset", { method: "POST", body: JSON.stringify({}) });
  await loadDailyTargetPlan({ recalibrate: true });
}

function pendingPeakSplitSignature(items) {
  return (items || [])
    .map((x) => Number(x.id || 0))
    .filter((x) => x > 0)
    .sort((a, b) => a - b)
    .join(",");
}

function updatePeakSplitReviewStatus() {
  const total = (state.pendingPeakSplitCandidates || []).length;
  $("peakSplitReviewStatus").textContent = total > 0
    ? `Pending split confirmations: ${total}`
    : "Pending split confirmations: 0";
}

function closePeakSplitReviewModal() {
  $("peakSplitReviewModal").classList.add("hidden");
}

async function decidePeakSplitCandidate(id, decision) {
  const row = (state.pendingPeakSplitCandidates || []).find((x) => Number(x.id) === Number(id));
  if (!row) return;
  const verb = decision === "apply" ? "apply" : "ignore";
  const ok = confirm(
    `Confirm ${verb.toUpperCase()} split for peak traded calculations?\nSymbol: ${row.symbol}\nYear: ${row.split_year}\nEffective: ${row.effective_date}\nFactor: ${row.factor}`
  );
  if (!ok) return;
  await api(`/api/v1/analytics/peak-splits/${id}/review`, {
    method: "PUT",
    body: JSON.stringify({ decision }),
  });
  await loadPeakDiff();
  await loadDashboard();
  if (state.selectedSymbol) await loadScrip(state.selectedSymbol);
}

function renderPeakSplitReviewItems(symbolFilter = "") {
  const symQ = String(symbolFilter || "").trim().toUpperCase();
  const items = (state.pendingPeakSplitCandidates || []).filter((x) => {
    if (!symQ) return true;
    return String(x.symbol || "").toUpperCase() === symQ;
  });
  $("peakSplitReviewSummary").textContent = items.length
    ? `Review ${items.length} split candidate(s)${symQ ? ` for ${symQ}` : ""}.`
    : `No pending split confirmation${symQ ? ` for ${symQ}` : ""}.`;
  const body = $("peakSplitReviewTable").querySelector("tbody");
  body.innerHTML = items
    .map(
      (r) => `
      <tr>
        <td>${r.symbol}</td>
        <td>${r.split_year || "-"}</td>
        <td>${r.effective_date || "-"}</td>
        <td>${r.factor}</td>
        <td>${Number(r.buys_before_split || 0)}</td>
        <td>${r.first_buy_date || "-"}</td>
        <td>${r.last_buy_date_before_split || "-"}</td>
        <td>${r.note || ""}</td>
        <td>
          <button class="btn secondary peak-split-apply-btn" data-id="${r.id}">Apply</button>
          <button class="btn secondary peak-split-ignore-btn" data-id="${r.id}">Ignore</button>
        </td>
      </tr>`
    )
    .join("");
  body.querySelectorAll(".peak-split-apply-btn").forEach((b) => {
    b.addEventListener("click", () => decidePeakSplitCandidate(Number(b.getAttribute("data-id")), "apply"));
  });
  body.querySelectorAll(".peak-split-ignore-btn").forEach((b) => {
    b.addEventListener("click", () => decidePeakSplitCandidate(Number(b.getAttribute("data-id")), "ignore"));
  });
}

function openPeakSplitReviewModal(symbol = "") {
  renderPeakSplitReviewItems(symbol);
  $("peakSplitReviewModal").classList.remove("hidden");
}

function renderSymbolSelect(items) {
  const sel = $("symbolSelect");
  sel.innerHTML = items.map((r) => `<option value="${r.symbol}">${r.symbol}</option>`).join("");
  state.symbols = items.map((r) => r.symbol);
  updateSymbolSuggestions(state.symbols);
  renderBulkDeleteSymbols(state.symbols);
  if (!state.selectedSymbol && state.symbols.length) state.selectedSymbol = state.symbols[0];
  if (state.selectedSymbol) sel.value = state.selectedSymbol;
}

function renderBulkDeleteSymbols(symbols) {
  const sel = $("bulkDeleteSymbols");
  if (!sel) return;
  const selected = new Set(Array.from(sel.selectedOptions).map((o) => o.value));
  sel.innerHTML = (symbols || [])
    .slice()
    .sort((a, b) => String(a).localeCompare(String(b), undefined, { sensitivity: "base", numeric: true }))
    .map((s) => `<option value="${s}" ${selected.has(s) ? "selected" : ""}>${s}</option>`)
    .join("");
}

function renderScripStats(s) {
  const isGold = normalizeAssetClass(s?.asset_class, s?.symbol) === "GOLD";
  const ltpLabel = isGold ? "LTP / gm" : "LTP";
  const qtyLabel = isGold ? "Qty (gms)" : "Qty";
  const avgLabel = isGold ? "Avg / gm" : "Avg";
  $("scripStats").innerHTML = `
    <div class="metric">${ltpLabel}: ${money(s.ltp)}</div>
    <div class="metric ${clsBySign(s.day_change_pct)}">Day %: ${pct(s.day_change_pct)}</div>
    <div class="metric ${clsBySign(s.day_pnl)}">Day P/L: ${money(s.day_pnl)}</div>
    <div class="metric">${qtyLabel}: ${money(s.qty)}</div>
    <div class="metric">${avgLabel}: ${money(s.avg_cost)}</div>
    <div class="metric">Invested: ${money(s.invested)}</div>
    <div class="metric">Dividend: ${money(s.dividend_amount)}</div>
    <div class="metric ${clsBySign(s.realized_pnl)}">RPL: ${money(s.realized_pnl)}</div>
    <div class="metric ${clsBySign(s.unrealized_pnl)}">UPL: ${money(s.unrealized_pnl)}</div>
    <div class="metric ${clsBySign(s.abs_pnl)}">Abs P/L: ${money(s.abs_pnl)}</div>
    <div class="metric ${clsBySign(s.upl_pct)}">UPL %: ${pct(s.upl_pct)}</div>
    <div class="metric ${clsBySign(s.total_return_pct)}">Return: ${pct(s.total_return_pct)}</div>
    <div class="metric">Peak Traded: ${money(peakPriceValue(s))}</div>
    <div class="metric ${clsBySign(peakPctValue(s))}">% from Peak Traded: ${pct(peakPctValue(s))}</div>
    <div class="metric">Signal: ${(s.signal?.buy_signal || "") + (s.signal?.sell_signal ? "/" + s.signal.sell_signal : "") || "-"}</div>
  `;
}

function renderScripPerf(perf) {
  $("scripPerf").innerHTML = `
    <div class="metric">From: ${perf.start_date}</div>
    <div class="metric">To: ${perf.end_date}</div>
    <div class="metric ${clsBySign(perf.pnl)}">P/L: ${money(perf.pnl)}</div>
    <div class="metric ${clsBySign(perf.return_pct)}">Return: ${pct(perf.return_pct)}</div>
    <div class="metric ${clsBySign(perf.realized_delta)}">Realized Delta: ${money(perf.realized_delta)}</div>
    <div class="metric ${clsBySign(perf.unrealized_delta)}">Unrealized Delta: ${money(perf.unrealized_delta)}</div>
  `;
}

function renderSellSimulation(sim) {
  if (!sim) {
    $("sellSimSummary").innerHTML = "";
    $("sellSimTable").querySelector("tbody").innerHTML = "";
    return;
  }
  $("sellSimSummary").innerHTML = `
    <div class="metric">Requested Qty: ${money(sim.requested_qty)}</div>
    <div class="metric">Available Qty: ${money(sim.available_qty)}</div>
    <div class="metric">Matched Qty: ${money(sim.matched_qty)}</div>
    <div class="metric ${sim.unmatched_qty > 0 ? "neg" : ""}">Unmatched Qty: ${money(sim.unmatched_qty)}</div>
    <div class="metric">Sell Price: ${money(sim.sell_price)}</div>
    <div class="metric">LTP: ${money(sim.ltp)}</div>
    <div class="metric">Cost (Matched): ${money(sim.total_cost)}</div>
    <div class="metric">Proceeds: ${money(sim.total_proceeds)}</div>
    <div class="metric ${clsBySign(sim.total_profit)}">Profit: ${money(sim.total_profit)}</div>
    <div class="metric ${clsBySign(sim.total_profit_pct)}">Profit %: ${pct(sim.total_profit_pct)}</div>
  `;
  $("sellSimTable").querySelector("tbody").innerHTML = (sim.lines || [])
    .map(
      (l) => `
      <tr>
        <td>${l.buy_date}</td>
        <td>${money(l.buy_price)}</td>
        <td>${money(l.qty_sold)}</td>
        <td>${money(l.cost)}</td>
        <td>${money(l.sell_price)}</td>
        <td>${money(l.proceeds)}</td>
        <td class="${clsBySign(l.profit)}">${money(l.profit)}</td>
      </tr>`
    )
    .join("");
}

async function simulateSellForSelected() {
  const symbol = state.selectedSymbol;
  if (!symbol) {
    alert("Select a scrip first.");
    return;
  }
  const quantity = Number($("sellSimQty").value);
  const sellPriceRaw = $("sellSimPrice").value;
  const sell_price = sellPriceRaw ? Number(sellPriceRaw) : null;
  if (!quantity || quantity <= 0) {
    alert("Enter a valid quantity to sell.");
    return;
  }
  const res = await api(`/api/v1/scrips/${symbol}/sell-simulate`, {
    method: "POST",
    body: JSON.stringify({ quantity, sell_price }),
  });
  renderSellSimulation(res);
}

function renderTrades(items) {
  const rows = items || [];
  const isGold = String(state.selectedAssetClass || "EQUITY").toUpperCase() === "GOLD";
  const totalCurrent = rows.reduce((acc, t) => acc + Number(t.current_pnl || 0), 0);
  const totalBuyMtm = rows
    .filter((t) => String(t.side || "").toUpperCase() === "BUY")
    .reduce((acc, t) => acc + Number(t.current_pnl || 0), 0);
  const totalSellRealized = rows
    .filter((t) => String(t.side || "").toUpperCase() === "SELL")
    .reduce((acc, t) => acc + Number(t.current_pnl || 0), 0);
  const firstNonZeroLtp = rows.find(
    (t) => String(t.side || "").toUpperCase() === "BUY" && Number(t.current_ltp || 0) > 0
  );
  const currentLtp = firstNonZeroLtp ? Number(firstNonZeroLtp.current_ltp || 0) : 0;
  if ($("tradesCurrentProfitText")) {
    const buyLtpLabel = isGold ? "BUY@LTP/gm" : "BUY@LTP";
    $("tradesCurrentProfitText").textContent =
      `Trade P/L ${money(totalCurrent)} | ${buyLtpLabel} ${money(currentLtp)}: ${money(totalBuyMtm)} | SELL@FIFO: ${money(totalSellRealized)}`;
    $("tradesCurrentProfitText").className = `metric ${clsBySign(totalCurrent)}`;
  }
  $("tradesTable").querySelector("tbody").innerHTML = rows
    .map(
      (t) => `
      <tr>
        <td>${t.id}</td>
        <td>${t.trade_date}</td>
        <td class="${t.side === "BUY" ? "pos" : "neg"}">${t.side}</td>
        <td>${money(t.quantity)}</td>
        <td>${money(t.price)}</td>
        <td>${money(t.amount)}</td>
        <td>${String(t.side || "").toUpperCase() === "BUY" ? money(t.current_ltp) : "-"}</td>
        <td class="${clsBySign(t.current_pnl)}">${money(t.current_pnl)}</td>
        <td>${t.notes || ""}</td>
        <td><button class="btn secondary trade-del-btn" data-id="${t.id}">Delete</button></td>
      </tr>`
    )
    .join("");
  document.querySelectorAll(".trade-del-btn").forEach((b) => {
    b.addEventListener("click", async () => {
      const id = b.getAttribute("data-id");
      if (!confirm(`Delete trade #${id}?`)) return;
      await api(`/api/v1/trades/${id}`, { method: "DELETE" });
      if (state.selectedSymbol) await loadScrip(state.selectedSymbol);
      await loadDashboard();
      await loadPeakDiff();
    });
  });
}

function normalizeTradeRowsForCurrentPnl(items, currentLtpHint = 0) {
  const hint = Number(currentLtpHint || 0);
  return (items || []).map((row) => {
    const t = { ...(row || {}) };
    const qty = Number(t.quantity || 0);
    const tradePx = Number(t.price || 0);
    const side = String(t.side || "").trim().toUpperCase();
    const hasServerBasis = String(t.pnl_basis || "").trim().length > 0;
    const ltp = Number(t.current_ltp || 0) > 0 ? Number(t.current_ltp || 0) : hint;
    let pnl = Number(t.current_pnl || 0);
    if ((!Number.isFinite(pnl) || Math.abs(pnl) <= 1e-12) && !hasServerBasis) {
      if (side === "BUY") {
        if (ltp > 0 && qty > 0 && tradePx > 0) {
          pnl = (ltp - tradePx) * qty;
          t.pnl_basis = "mark_to_market_fallback";
        } else {
          pnl = 0;
          t.pnl_basis = "mark_to_market_fallback";
        }
      } else if (side === "SELL") {
        const matchedQty = Number(t.matched_qty || qty || 0);
        const matchedAvgBuy = Number(t.matched_avg_buy_price || 0);
        if (matchedQty > 0 && tradePx > 0 && matchedAvgBuy > 0) {
          pnl = (tradePx - matchedAvgBuy) * matchedQty;
        } else {
          pnl = 0;
        }
        t.pnl_basis = "realized_fifo_fallback";
      } else {
        pnl = 0;
      }
    }
    t.current_ltp = ltp > 0 ? ltp : 0;
    t.current_pnl = Number.isFinite(pnl) ? pnl : 0;
    return t;
  });
}

function renderStrategySets(sets) {
  state.strategySets = sets;
  const sel = $("strategySelect");
  sel.innerHTML = sets
    .map((s) => `<option value="${s.id}">${s.name}${s.is_active ? " (Active)" : ""}</option>`)
    .join("");
  const active = sets.find((s) => s.is_active === 1) || sets[0];
  if (active) {
    state.activeSetId = active.id;
    sel.value = String(active.id);
    renderParams(active.parameters);
  }
}

function renderParams(params) {
  $("paramsGrid").innerHTML = params
    .map(
      (p) => `
      <div class="param">
        <label>${p.key}</label>
        <input data-key="${p.key}" type="number" step="0.0001" value="${p.value}">
      </div>`
    )
    .join("");
}

function strategyActionClass(action) {
  const a = String(action || "").toUpperCase();
  if (a === "ADD") return "action-add";
  if (a === "TRIM") return "action-trim";
  if (a === "REVIEW") return "action-review";
  if (a === "WATCH_ADD") return "action-watch_add";
  return "action-hold";
}

function resetStrategyProjectionView() {
  state.strategyProjView = { start: 0, end: 0 };
  if (state.strategyInsights?.projections) {
    renderStrategyProjection(state.strategyInsights.projections, false);
  }
}

function renderStrategyProjection(projections, preserveView = false) {
  const svg = $("strategyProjectionChart");
  const tooltip = $("strategyTooltip");
  svg.innerHTML = "";
  tooltip.classList.add("hidden");
  const scenarios = (projections && projections.scenarios) || [];
  if (!scenarios.length) return;

  const width = 800;
  const height = 260;
  const m = { top: 18, right: 20, bottom: 30, left: 72 };
  const plotW = width - m.left - m.right;
  const plotH = height - m.top - m.bottom;
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);

  const allPts = [];
  scenarios.forEach((s) => (s.points || []).forEach((p) => allPts.push(Number(p.projected_value || 0))));
  if (!allPts.length) return;
  const allYears = Array.from(
    new Set(
      scenarios.flatMap((s) => (s.points || []).map((p) => Number(p.year_offset || 0)))
    )
  )
    .filter((x) => Number.isFinite(x))
    .sort((a, b) => a - b);
  if (!allYears.length) return;

  if (!preserveView || !state.strategyProjView || state.strategyProjView.end <= state.strategyProjView.start) {
    state.strategyProjView = { start: 0, end: allYears.length - 1 };
  }
  let viewStart = Math.max(0, Math.min(allYears.length - 1, Number(state.strategyProjView.start || 0)));
  let viewEnd = Math.max(viewStart, Math.min(allYears.length - 1, Number(state.strategyProjView.end || (allYears.length - 1))));
  if (viewEnd === viewStart && allYears.length > 1) {
    viewEnd = Math.min(allYears.length - 1, viewStart + 1);
  }
  const yearsView = allYears.slice(viewStart, viewEnd + 1);
  const yearToIndex = new Map(yearsView.map((y, i) => [y, i]));

  const valuesView = [];
  scenarios.forEach((sc) => {
    (sc.points || []).forEach((p) => {
      const yr = Number(p.year_offset || 0);
      if (yearToIndex.has(yr)) valuesView.push(Number(p.projected_value || 0));
    });
  });
  if (!valuesView.length) return;
  const minRaw = Math.min(...valuesView);
  const maxRaw = Math.max(...valuesView);
  const pad = Math.max(1, (maxRaw - minRaw) * 0.08);
  const min = minRaw - pad;
  const max = maxRaw + pad;
  const x = (i) => m.left + ((Number(i || 0) / Math.max(1, yearsView.length - 1)) * plotW);
  const y = (val) => m.top + (1 - (Number(val || 0) - min) / (max - min || 1)) * plotH;

  function svgEl(tag, attrs = {}) {
    const el = document.createElementNS("http://www.w3.org/2000/svg", tag);
    Object.entries(attrs).forEach(([k, v]) => el.setAttribute(k, String(v)));
    return el;
  }

  const axisColor = "#7b8797";
  const gridColor = "#e3e8ee";
  const palette = {
    conservative: "#bf2f1f",
    base: "#0b7d6b",
    aggressive: "#26547c",
  };

  const yTicks = 5;
  for (let t = 0; t <= yTicks; t++) {
    const v = min + ((max - min) * t) / yTicks;
    const yy = y(v);
    svg.appendChild(svgEl("line", { x1: m.left, y1: yy, x2: m.left + plotW, y2: yy, stroke: gridColor, "stroke-width": 1 }));
    const lbl = svgEl("text", { x: m.left - 8, y: yy + 3, "text-anchor": "end", "font-size": 10, fill: axisColor });
    lbl.textContent = money(v);
    svg.appendChild(lbl);
  }

  svg.appendChild(svgEl("line", { x1: m.left, y1: m.top + plotH, x2: m.left + plotW, y2: m.top + plotH, stroke: axisColor, "stroke-width": 1 }));
  svg.appendChild(svgEl("line", { x1: m.left, y1: m.top, x2: m.left, y2: m.top + plotH, stroke: axisColor, "stroke-width": 1 }));

  for (let t = 0; t < yearsView.length; t++) {
    const xx = x(t);
    svg.appendChild(svgEl("line", { x1: xx, y1: m.top + plotH, x2: xx, y2: m.top + plotH + 4, stroke: axisColor, "stroke-width": 1 }));
    const lbl = svgEl("text", { x: xx, y: m.top + plotH + 16, "text-anchor": "middle", "font-size": 10, fill: axisColor });
    lbl.textContent = `Y${yearsView[t]}`;
    svg.appendChild(lbl);
  }

  const seriesByScenario = {};
  scenarios.forEach((sc) => {
    const ptsRaw = sc.points || [];
    const vals = yearsView.map((yr) => {
      const hit = ptsRaw.find((p) => Number(p.year_offset || 0) === yr);
      return hit ? Number(hit.projected_value || 0) : null;
    });
    seriesByScenario[sc.scenario] = vals;
    const usable = vals.map((v, i) => ({ v, i })).filter((x) => x.v != null);
    if (!usable.length) return;
    const d = usable
      .map((p, i) => `${i === 0 ? "M" : "L"} ${x(p.i)} ${y(p.v)}`)
      .join(" ");
    const color = palette[sc.scenario] || "#5b6770";
    svg.appendChild(svgEl("path", { d, fill: "none", stroke: color, "stroke-width": 2.4 }));
    usable.forEach((p) => {
      svg.appendChild(svgEl("circle", { cx: x(p.i), cy: y(p.v), r: 2.7, fill: color, opacity: 0.9 }));
    });
  });

  let lx = m.left + 8;
  const ly = m.top + 8;
  scenarios.forEach((sc) => {
    const color = palette[sc.scenario] || "#5b6770";
    svg.appendChild(svgEl("line", { x1: lx, y1: ly, x2: lx + 16, y2: ly, stroke: color, "stroke-width": 2.4 }));
    const lbl = svgEl("text", { x: lx + 20, y: ly + 3, "font-size": 10, fill: "#384657" });
    lbl.textContent = `${sc.scenario} (${pctWeight(sc.annual_return)})`;
    svg.appendChild(lbl);
    lx += 130;
  });

  const hoverLine = svgEl("line", {
    x1: m.left,
    y1: m.top,
    x2: m.left,
    y2: m.top + plotH,
    stroke: "#6a7785",
    "stroke-width": 1,
    "stroke-dasharray": "4 3",
    visibility: "hidden",
  });
  svg.appendChild(hoverLine);
  const overlay = svgEl("rect", {
    x: m.left,
    y: m.top,
    width: plotW,
    height: plotH,
    fill: "transparent",
    style: "cursor:crosshair",
  });
  svg.appendChild(overlay);

  const hoverAt = (clientX, clientY) => {
    const rect = svg.getBoundingClientRect();
    const px = ((clientX - rect.left) / rect.width) * width;
    const py = ((clientY - rect.top) / rect.height) * height;
    if (px < m.left || px > m.left + plotW || py < m.top || py > m.top + plotH) return;
    const i = Math.max(0, Math.min(yearsView.length - 1, Math.round(((px - m.left) / plotW) * (yearsView.length - 1))));
    const xx = x(i);
    hoverLine.setAttribute("x1", xx);
    hoverLine.setAttribute("x2", xx);
    hoverLine.setAttribute("visibility", "visible");
    const lines = [
      `<div class="row"><span class="label">Year</span><strong>Y${yearsView[i]}</strong></div>`,
    ];
    scenarios.forEach((sc) => {
      const key = sc.scenario;
      const val = (seriesByScenario[key] || [])[i];
      if (val == null) return;
      lines.push(`<div class="row"><span class="label">${String(key || "").toUpperCase()}</span><strong>${money(val)}</strong></div>`);
    });
    tooltip.innerHTML = lines.join("");
    tooltip.classList.remove("hidden");
    const wrap = svg.closest(".chart-wrap").getBoundingClientRect();
    let tx = clientX - wrap.left + 10;
    let ty = clientY - wrap.top - 10;
    if (tx > wrap.width - 200) tx = wrap.width - 200;
    if (ty < 10) ty = 10;
    tooltip.style.left = `${tx}px`;
    tooltip.style.top = `${ty}px`;
  };

  let drag = null;
  overlay.addEventListener("mousedown", (e) => {
    drag = { x: e.clientX, start: viewStart, end: viewEnd };
    overlay.style.cursor = "grabbing";
  });
  overlay.addEventListener("mouseup", () => {
    drag = null;
    overlay.style.cursor = "crosshair";
  });
  overlay.addEventListener("mousemove", (e) => {
    hoverAt(e.clientX, e.clientY);
    if (!drag) return;
    const dxPx = e.clientX - drag.x;
    const span = Math.max(2, drag.end - drag.start + 1);
    const shift = Math.round((-dxPx / plotW) * span);
    let ns = drag.start + shift;
    let ne = drag.end + shift;
    if (ns < 0) {
      ne += -ns;
      ns = 0;
    }
    if (ne > allYears.length - 1) {
      ns -= ne - (allYears.length - 1);
      ne = allYears.length - 1;
    }
    ns = Math.max(0, ns);
    ne = Math.max(ns + 1, ne);
    state.strategyProjView = { start: ns, end: ne };
    renderStrategyProjection(projections, true);
  });
  overlay.addEventListener("mouseleave", () => {
    drag = null;
    overlay.style.cursor = "crosshair";
    hoverLine.setAttribute("visibility", "hidden");
    tooltip.classList.add("hidden");
  });
  overlay.addEventListener("wheel", (e) => {
    e.preventDefault();
    const zoomIn = e.deltaY < 0;
    const span = Math.max(2, viewEnd - viewStart + 1);
    const nextSpan = zoomIn ? Math.max(2, Math.round(span * 0.85)) : Math.min(allYears.length, Math.round(span * 1.15));
    const rect = svg.getBoundingClientRect();
    const px = ((e.clientX - rect.left) / rect.width) * width;
    const ratio = clamp((px - m.left) / plotW, 0, 1);
    const centerIndex = viewStart + Math.round(ratio * (span - 1));
    let ns = centerIndex - Math.round(ratio * (nextSpan - 1));
    let ne = ns + nextSpan - 1;
    if (ns < 0) {
      ne += -ns;
      ns = 0;
    }
    if (ne > allYears.length - 1) {
      ns -= ne - (allYears.length - 1);
      ne = allYears.length - 1;
    }
    ns = Math.max(0, ns);
    ne = Math.max(ns + 1, ne);
    state.strategyProjView = { start: ns, end: ne };
    renderStrategyProjection(projections, true);
  }, { passive: false });
  overlay.addEventListener("dblclick", resetStrategyProjectionView);
}

function renderStrategyInsights(item) {
  state.strategyInsights = item || null;
  if (!item) {
    $("strategySummary").innerHTML = "<div class='metric'>No strategy analytics yet.</div>";
    $("strategyRecoTable").querySelector("tbody").innerHTML = "";
    $("strategyProjectionChart").innerHTML = "";
    $("strategyTooltip").classList.add("hidden");
    return;
  }
  const c = item.counts || {};
  const m = item.macro || {};
  const intel = item.intelligence || {};
  $("strategySummary").innerHTML = `
    <div class="metric">Run Date: ${item.run_date || "-"}</div>
    <div class="metric">Generated: ${item.generated_at || "-"}</div>
    <div class="metric">Start Value: ${money(item.projected_start_value)}</div>
    <div class="metric">Market Value: ${money(item.market_value)}</div>
    <div class="metric">Cash: ${money(item.cash_balance)}</div>
    <div class="metric">Macro: ${String(m.regime || "neutral").toUpperCase()}</div>
    <div class="metric">Macro Score: ${Number(m.score || 0).toFixed(2)}</div>
    <div class="metric">Macro Confidence: ${(Number(m.confidence || 0) * 100).toFixed(1)}%</div>
    <div class="metric">Intel Score: ${Number(intel.score || 0).toFixed(2)}</div>
    <div class="metric">Intel Confidence: ${(Number(intel.confidence || 0) * 100).toFixed(1)}%</div>
    <div class="metric" style="grid-column: 1 / -1;">Macro Thought: ${m.thought || "-"}</div>
    <div class="metric" style="grid-column: 1 / -1;">Intel Thought: ${intel.thought || "-"}</div>
    <div class="metric">TRIM: ${Number(c.TRIM || 0)}</div>
    <div class="metric">ADD: ${Number(c.ADD || 0)}</div>
    <div class="metric">HOLD: ${Number(c.HOLD || 0)}</div>
    <div class="metric">REVIEW: ${Number(c.REVIEW || 0)}</div>
    <div class="metric">WATCH_ADD: ${Number(c.WATCH_ADD || 0)}</div>
  `;

  const rows = (item.recommendations || []).map((r) => {
    const action = String(r.action || "").toUpperCase();
    return `
      <tr>
        <td>${r.symbol}</td>
        <td><span class="action-pill ${strategyActionClass(action)}">${action}</span></td>
        <td>${Number(r.priority || 0)}</td>
        <td>${pctWeight(r.weight_current)}</td>
        <td>${pctWeight(r.weight_target)}</td>
        <td class="${clsBySign(r.delta_weight)}">${pctWeight(r.delta_weight)}</td>
        <td>${money(r.price_now)}</td>
        <td>${money(r.buy_price_2)} - ${money(r.buy_price_1)}</td>
        <td>${money(r.sell_price_1)} - ${money(r.sell_price_2)}</td>
        <td>${(Number(r.confidence || 0) * 100).toFixed(1)}%</td>
        <td class="${clsBySign(r.expected_annual_return)}">${pctWeight(r.expected_annual_return)}</td>
        <td class="${clsBySign(r.intel_score)}">${Number(r.intel_score || 0).toFixed(2)}</td>
        <td>${(Number(r.intel_confidence || 0) * 100).toFixed(1)}%</td>
        <td class="reason-cell">${r.intel_summary || "-"}</td>
        <td class="reason-cell">${r.reason || ""}</td>
      </tr>
    `;
  });
  $("strategyRecoTable").querySelector("tbody").innerHTML = rows.join("");
  renderStrategyProjection(item.projections || { scenarios: [] }, false);
}

function strategyAuditStatusClass(status) {
  const s = String(status || "").toLowerCase();
  if (s === "critical") return "neg";
  if (s === "warn") return "";
  return "pos";
}

function strategyAuditSeverityClass(severity) {
  const s = String(severity || "").toLowerCase();
  if (s === "critical" || s === "warn") return "neg";
  return "";
}

function hostedProviderByName(config, name) {
  const providers = Array.isArray(config?.providers) ? config.providers : [];
  return providers.find((p) => String(p.provider || "") === name) || {};
}

function renderHostedLlmConfig(config) {
  const cfg = config || {};
  const openrouter = hostedProviderByName(cfg, "openrouter");
  const groq = hostedProviderByName(cfg, "groq");
  const huggingface = hostedProviderByName(cfg, "huggingface");
  if ($("hostedLlmEnabled")) $("hostedLlmEnabled").checked = !!cfg.enabled;
  if ($("strategyAuditUseHostedLlm")) $("strategyAuditUseHostedLlm").checked = !!cfg.enabled;
  if ($("hostedLlmProviderOrder")) $("hostedLlmProviderOrder").value = String(cfg.provider_order || "openrouter,groq,huggingface");
  if ($("hostedLlmTimeoutSec")) $("hostedLlmTimeoutSec").value = Number(cfg.timeout_sec || 45);
  if ($("hostedLlmOpenrouterModel")) $("hostedLlmOpenrouterModel").value = String(openrouter.model || "openrouter/free");
  if ($("hostedLlmGroqModel")) $("hostedLlmGroqModel").value = String(groq.model || "llama-3.1-8b-instant");
  if ($("hostedLlmHuggingfaceModel")) $("hostedLlmHuggingfaceModel").value = String(huggingface.model || "Qwen/Qwen2.5-7B-Instruct");
  if ($("hostedLlmOpenrouterKey")) $("hostedLlmOpenrouterKey").value = "";
  if ($("hostedLlmGroqKey")) $("hostedLlmGroqKey").value = "";
  if ($("hostedLlmHuggingfaceKey")) $("hostedLlmHuggingfaceKey").value = "";
  if ($("hostedLlmSummary")) {
    const providers = Array.isArray(cfg.providers) ? cfg.providers : [];
    $("hostedLlmSummary").innerHTML = `
      <div class="metric ${cfg.enabled ? "pos" : ""}">Mode: ${cfg.enabled ? "Enabled" : "Disabled"}</div>
      <div class="metric">Order: ${escapeHtml(String(cfg.provider_order || "-"))}</div>
      <div class="metric">Timeout: ${Number(cfg.timeout_sec || 0)} sec</div>
      <div class="metric" style="grid-column: 1 / -1;">Privacy: ${escapeHtml(String(cfg.privacy_note || "-"))}</div>
      ${providers
        .map(
          (p) => `
            <div class="metric ${p.configured ? "pos" : "warn"}">
              ${escapeHtml(String(p.label || p.provider || ""))}: ${p.configured ? "configured" : "missing key"}<br>
              Model: ${escapeHtml(String(p.model || "-"))}<br>
              Last: ${escapeHtml(String(p.last_status || "-"))} ${p.last_error ? `| ${escapeHtml(String(p.last_error))}` : ""}
            </div>`
        )
        .join("")}
    `;
  }
}

function renderHostedLlmMetrics(payload) {
  const p = payload || {};
  const summary = p.summary || {};
  const providers = Array.isArray(p.providers) ? p.providers : [];
  const items = Array.isArray(p.items) ? p.items : [];
  if ($("hostedLlmMetricsSummary")) {
    $("hostedLlmMetricsSummary").innerHTML = `
      <div class="metric">Attempts: ${Number(summary.total_attempts || 0)}</div>
      <div class="metric pos">OK: ${Number(summary.ok_attempts || 0)}</div>
      <div class="metric ${Number(summary.error_attempts || 0) > 0 ? "neg" : ""}">Errors: ${Number(summary.error_attempts || 0)}</div>
      <div class="metric">Skipped: ${Number(summary.skipped_attempts || 0)}</div>
      <div class="metric ${Number(summary.success_rate_pct || 0) >= 60 ? "pos" : "warn"}">Success Rate: ${pct(summary.success_rate_pct)}</div>
      <div class="metric">Avg OK Latency: ${Number(summary.avg_ok_latency_ms || 0).toFixed(0)} ms</div>
      <div class="metric">Last Run: ${escapeHtml(String(summary.last_run_at || "-"))}</div>
      <div class="metric" style="grid-column: 1 / -1;">Provider Mix: ${providers
        .map((x) => `${x.provider}: ${Number(x.ok_count || 0)}/${Number(x.attempts || 0)} ok, ${Number(x.avg_ok_latency_ms || 0).toFixed(0)}ms`)
        .join(" | ") || "-"}</div>
    `;
  }
  const body = $("hostedLlmMetricsTable")?.querySelector("tbody");
  if (body) {
    body.innerHTML = items.length
      ? items
          .map(
            (r) => `
              <tr>
                <td>${escapeHtml(String(r.created_at || "-"))}</td>
                <td>${escapeHtml(String(r.purpose || "-"))}</td>
                <td>${escapeHtml(String(r.provider || "-"))}</td>
                <td>${escapeHtml(String(r.model || "-"))}</td>
                <td class="${String(r.status || "") === "ok" ? "pos" : String(r.status || "") === "error" ? "neg" : "warn"}">${escapeHtml(String(r.status || "-").toUpperCase())}</td>
                <td>${Number(r.latency_ms || 0).toFixed(0)} ms</td>
                <td>${Number(r.prompt_chars || 0)}</td>
                <td>${Number(r.response_chars || 0)}</td>
                <td class="reason-cell">${escapeHtml(String(r.error || ""))}</td>
              </tr>`
          )
          .join("")
      : '<tr><td colspan="9">No hosted LLM calls recorded yet.</td></tr>';
  }
}

async function loadHostedLlmMetrics(options = {}) {
  const throwOnError = !!options.throwOnError;
  try {
    const metrics = await api("/api/v1/hosted-llm/metrics?limit=60");
    renderHostedLlmMetrics(metrics || {});
  } catch (e) {
    const meta = normalizeUiError(e, "HOSTED_LLM_METRICS_LOAD_FAILED");
    if ($("hostedLlmMetricsSummary")) $("hostedLlmMetricsSummary").innerHTML = `<div class="metric neg">Hosted LLM metrics unavailable: ${escapeHtml(meta.reason)}</div>`;
    const body = $("hostedLlmMetricsTable")?.querySelector("tbody");
    if (body) body.innerHTML = '<tr><td colspan="9">Failed to load hosted LLM metrics.</td></tr>';
    if (throwOnError) throw e;
  }
}

async function loadHostedLlmConfig(options = {}) {
  const throwOnError = !!options.throwOnError;
  try {
    const cfg = await api("/api/v1/hosted-llm/config");
    renderHostedLlmConfig(cfg || {});
    await loadHostedLlmMetrics();
    if ($("hostedLlmStatusText")) $("hostedLlmStatusText").textContent = `Hosted LLM: ${cfg?.enabled ? "enabled" : "disabled"}`;
  } catch (e) {
    const meta = normalizeUiError(e, "HOSTED_LLM_CONFIG_LOAD_FAILED");
    if ($("hostedLlmStatusText")) $("hostedLlmStatusText").textContent = `Hosted LLM error: ${meta.reason}`;
    if (throwOnError) throw e;
  }
}

async function saveHostedLlmConfig() {
  const providers = [
    {
      provider: "openrouter",
      api_key: $("hostedLlmOpenrouterKey")?.value || "",
      model: $("hostedLlmOpenrouterModel")?.value || "",
    },
    {
      provider: "groq",
      api_key: $("hostedLlmGroqKey")?.value || "",
      model: $("hostedLlmGroqModel")?.value || "",
    },
    {
      provider: "huggingface",
      api_key: $("hostedLlmHuggingfaceKey")?.value || "",
      model: $("hostedLlmHuggingfaceModel")?.value || "",
    },
  ];
  const cfg = await api("/api/v1/hosted-llm/config", {
    method: "POST",
    body: JSON.stringify({
      enabled: !!$("hostedLlmEnabled")?.checked,
      provider_order: $("hostedLlmProviderOrder")?.value || "openrouter,groq,huggingface",
      timeout_sec: Number($("hostedLlmTimeoutSec")?.value || 45),
      providers,
    }),
  });
  renderHostedLlmConfig(cfg || {});
  if ($("hostedLlmStatusText")) $("hostedLlmStatusText").textContent = `Hosted LLM saved: ${new Date().toLocaleString()}`;
}

async function testHostedLlmConfig() {
  if ($("hostedLlmStatusText")) $("hostedLlmStatusText").textContent = "Hosted LLM test running...";
  const res = await api("/api/v1/hosted-llm/test", { method: "POST", body: JSON.stringify({}) });
  renderHostedLlmConfig(res?.config || {});
  await loadHostedLlmMetrics();
  if ($("hostedLlmStatusText")) {
    $("hostedLlmStatusText").textContent = res?.ok
      ? `Hosted LLM OK via ${String(res.provider || "-")}`
      : `Hosted LLM failed: ${String(res.message || res.status || "-")}`;
  }
}

function renderStrategyAudit(payload) {
  const p = payload || {};
  state.strategyAuditRaw = p;
  const latest = p.latest || null;
  const items = Array.isArray(p.items) ? p.items : [];
  const summaryEl = $("strategyAuditSummary");
  if (summaryEl) {
    if (!latest) {
      summaryEl.innerHTML = "<div class='metric'>No audit runs yet.</div>";
    } else {
      const stats = latest.stats || {};
      summaryEl.innerHTML = `
        <div class="metric">Mode: ${escapeHtml(String(latest.audit_mode || "heuristic"))}</div>
        <div class="metric">Run At: ${escapeHtml(String(latest.created_at || "-"))}</div>
        <div class="metric">Strategy Date: ${escapeHtml(String(latest.strategy_run_date || "-"))}</div>
        <div class="metric ${strategyAuditStatusClass(latest.overall_status)}">Status: ${escapeHtml(String(latest.overall_status || "ok").toUpperCase())}</div>
        <div class="metric ${clsBySign(Number(latest.overall_score || 0) - 60)}">Audit Score: ${Number(latest.overall_score || 0).toFixed(2)}</div>
        <div class="metric">Recommendations: ${Number(stats.recommendation_count || 0)}</div>
        <div class="metric">ADD/WATCH: ${Number(stats.add_count || 0)}</div>
        <div class="metric">TRIM: ${Number(stats.trim_count || 0)}</div>
        <div class="metric">REVIEW: ${Number(stats.review_count || 0)}</div>
        <div class="metric">Low Confidence: ${Number(stats.low_confidence_count || 0)}</div>
        <div class="metric">Backtest Hit Rate: ${pct((Number(stats.backtest_hit_rate || 0)) * 100)}</div>
        <div class="metric">Backtest Samples: ${Number(stats.backtest_sample_count || 0)}</div>
        <div class="metric">Target Weight Sum: ${Number(stats.target_weight_sum || 0).toFixed(2)}</div>
        <div class="metric">Macro Conf: ${pct((Number(stats.macro_confidence || 0)) * 100)}</div>
        <div class="metric">Intel Conf: ${pct((Number(stats.intel_confidence || 0)) * 100)}</div>
        <div class="metric" style="grid-column: 1 / -1;">Summary: ${escapeHtml(String(latest.summary || "-"))}</div>
        <div class="metric" style="grid-column: 1 / -1;">Recommendation: ${escapeHtml(String(latest.recommendation || "-"))}</div>
      `;
    }
  }
  const findingsBody = $("strategyAuditFindingsTable")?.querySelector("tbody");
  if (findingsBody) {
    const findings = Array.isArray(latest?.findings) ? latest.findings : [];
    findingsBody.innerHTML = findings.length
      ? findings
          .map(
            (f) => `
              <tr>
                <td class="${strategyAuditSeverityClass(f.severity)}">${escapeHtml(String(f.severity || "-").toUpperCase())}</td>
                <td>${escapeHtml(String(f.code || ""))}</td>
                <td>${escapeHtml(String(f.title || ""))}</td>
                <td>${escapeHtml(String(f.symbol || "-"))}</td>
                <td>${f.metric_value === null || typeof f.metric_value === "undefined" ? "-" : money(f.metric_value)}</td>
                <td>${escapeHtml(String(f.expected_range || "-"))}</td>
                <td class="reason-cell">${escapeHtml(String(f.detail || ""))}</td>
              </tr>`
          )
          .join("")
      : '<tr><td colspan="7">No audit findings yet.</td></tr>';
  }
  const historyBody = $("strategyAuditHistoryTable")?.querySelector("tbody");
  if (historyBody) {
    historyBody.innerHTML = items.length
      ? items
          .map(
            (r) => `
              <tr>
                <td>${escapeHtml(String(r.created_at || "-"))}</td>
                <td>${escapeHtml(String(r.strategy_run_date || "-"))}</td>
                <td class="${strategyAuditStatusClass(r.overall_status)}">${escapeHtml(String(r.overall_status || "").toUpperCase())}</td>
                <td>${Number(r.overall_score || 0).toFixed(2)}</td>
                <td>${Number(r.critical_count || 0)}</td>
                <td>${Number(r.warn_count || 0)}</td>
                <td>${Number(r.info_count || 0)}</td>
                <td class="reason-cell">${escapeHtml(String(r.summary || ""))}</td>
                <td class="reason-cell">${escapeHtml(String(r.recommendation || ""))}</td>
              </tr>`
          )
          .join("")
      : '<tr><td colspan="9">No strategy audit history yet.</td></tr>';
  }
}

async function loadStrategyAudit(options = {}) {
  const throwOnError = !!options.throwOnError;
  try {
    const res = await api("/api/v1/strategy/audits?limit=40");
    renderStrategyAudit(res || {});
    if ($("strategyAuditStatusText")) {
      const latest = res?.latest || null;
      $("strategyAuditStatusText").textContent = latest
        ? `Strategy audit: ${String(latest.overall_status || "ok").toUpperCase()} | ${new Date().toLocaleString()}`
        : `Strategy audit: no runs yet`;
    }
  } catch (e) {
    const meta = normalizeUiError(e, "STRATEGY_AUDIT_LOAD_FAILED");
    if ($("strategyAuditStatusText")) $("strategyAuditStatusText").textContent = `Strategy audit error: ${meta.reason}`;
    const findingsBody = $("strategyAuditFindingsTable")?.querySelector("tbody");
    const historyBody = $("strategyAuditHistoryTable")?.querySelector("tbody");
    if ($("strategyAuditSummary")) $("strategyAuditSummary").innerHTML = `<div class="metric neg">Strategy audit unavailable: ${escapeHtml(meta.reason)}</div>`;
    if (findingsBody) findingsBody.innerHTML = '<tr><td colspan="7">Failed to load audit findings.</td></tr>';
    if (historyBody) historyBody.innerHTML = '<tr><td colspan="9">Failed to load audit history.</td></tr>';
    if (throwOnError) throw e;
  }
}

async function runStrategyAudit() {
  if ($("strategyAuditStatusText")) {
    $("strategyAuditStatusText").textContent = `Strategy audit running: ${new Date().toLocaleString()}`;
  }
  const refreshStrategy = !!$("strategyAuditRefreshFirst")?.checked;
  const useHostedLlm = !!$("strategyAuditUseHostedLlm")?.checked;
  const res = await api("/api/v1/strategy/audits/run", {
    method: "POST",
    body: JSON.stringify({ refresh_strategy: refreshStrategy, use_hosted_llm: useHostedLlm }),
  });
  await loadStrategyAudit();
  await loadHostedLlmMetrics();
  if ($("strategyAuditStatusText")) {
    $("strategyAuditStatusText").textContent =
      `Strategy audit ${String(res?.overall_status || "ok").toUpperCase()}: ${new Date().toLocaleString()}`;
  }
}

function attentionSeverityClass(severity) {
  const s = String(severity || "").toLowerCase();
  if (s === "critical") return "neg";
  if (s === "warning") return "warn";
  return "";
}

function smallRatePct(rateDecimal) {
  return `${(Number(rateDecimal || 0) * 100).toFixed(3)}%`;
}

function renderAttentionConsole(payload) {
  const p = payload || {};
  const summary = p.summary || {};
  const tax = p.tax_profile || {};
  const openAlerts = Array.isArray(p.open_alerts) ? p.open_alerts : [];
  const resolvedAlerts = Array.isArray(p.resolved_alerts) ? p.resolved_alerts : [];
  const taxRuns = Array.isArray(p.tax_sync_runs) ? p.tax_sync_runs : [];
  state.attentionRaw = p;
  if ($("attentionSummary")) {
    $("attentionSummary").innerHTML = `
      <div class="metric ${Number(summary.open_count || 0) > 0 ? "neg" : "pos"}">Open Alerts: ${Number(summary.open_count || 0)}</div>
      <div class="metric">Critical Open: ${Number(summary.critical_open || 0)}</div>
      <div class="metric">Warning Open: ${Number(summary.warning_open || 0)}</div>
      <div class="metric">Info Open: ${Number(summary.info_open || 0)}</div>
      <div class="metric">Resolved Shown: ${Number(summary.resolved_count || 0)}</div>
      <div class="metric">Latest Tax Sync: ${escapeHtml(String(summary.latest_tax_sync_at || "-"))}</div>
      <div class="metric">Latest Tax Sync Status: ${escapeHtml(String(summary.latest_tax_sync_status || "-"))}</div>
    `;
  }
  if ($("attentionTaxProfile")) {
    $("attentionTaxProfile").innerHTML = `
      <div class="metric">FY: ${escapeHtml(String(tax.fy_label || "-"))}</div>
      <div class="metric">FY Range: ${escapeHtml(String(tax.fy_start_date || "-"))} to ${escapeHtml(String(tax.fy_end_date || "-"))}</div>
      <div class="metric">STCG Rate: ${pct(tax.stcg_rate_pct)}</div>
      <div class="metric">LTCG Rate: ${pct(tax.ltcg_rate_pct)}</div>
      <div class="metric">LTCG Exemption Limit: ${money(tax.ltcg_exemption_limit)}</div>
      <div class="metric ${clsBySign(-Number(tax.realized_ltcg_net_gain || 0))}">Realized LTCG This FY: ${money(tax.realized_ltcg_net_gain)}</div>
      <div class="metric ${Number(tax.remaining_ltcg_exemption || 0) > 0 ? "pos" : "neg"}">Remaining LTCG Exemption: ${money(tax.remaining_ltcg_exemption)}</div>
      <div class="metric">Stamp Buy Rate: ${smallRatePct(tax.stamp_buy_rate)}</div>
      <div class="metric">STT Delivery Rate: ${smallRatePct(tax.stt_delivery_rate)}</div>
      <div class="metric">GST Rate: ${pct(Number(tax.gst_rate || 0) * 100)}</div>
      <div class="metric">DP Charge Sell: ${money(tax.dp_charge_sell_incl_gst)}</div>
    `;
  }
  const openBody = $("attentionOpenTable")?.querySelector("tbody");
  if (openBody) {
    openBody.innerHTML = openAlerts.length
      ? openAlerts
          .map(
            (r, idx) => `
              <tr>
                <td>${idx + 1}</td>
                <td class="${attentionSeverityClass(r.severity_label)}">${escapeHtml(String(r.severity_label || "-").toUpperCase())}</td>
                <td>${escapeHtml(String(r.category || "-"))}</td>
                <td>${escapeHtml(String(r.title || "-"))}</td>
                <td class="reason-cell">${escapeHtml(String(r.detail || ""))}</td>
                <td>${escapeHtml(String(r.source_ref || "-"))}</td>
                <td>${escapeHtml(String(r.detected_at || "-"))}</td>
                <td>${escapeHtml(String(r.last_seen_at || "-"))}</td>
                <td>${Number(r.occurrence_count || 0)}</td>
              </tr>`
          )
          .join("")
      : '<tr><td colspan="9">No open attention items.</td></tr>';
  }
  const taxRunsBody = $("attentionTaxRunsTable")?.querySelector("tbody");
  if (taxRunsBody) {
    taxRunsBody.innerHTML = taxRuns.length
      ? taxRuns
          .map(
            (r) => `
              <tr>
                <td>${escapeHtml(String(r.created_at || "-"))}</td>
                <td class="${String(r.status || "").toLowerCase() === "error" ? "neg" : "pos"}">${escapeHtml(String(r.status || "-").toUpperCase())}</td>
                <td>${r.stcg_rate_pct != null ? pct(r.stcg_rate_pct) : "-"}</td>
                <td>${r.ltcg_rate_pct != null ? pct(r.ltcg_rate_pct) : "-"}</td>
                <td>${r.ltcg_exemption_limit != null ? money(r.ltcg_exemption_limit) : "-"}</td>
                <td>${r.stt_delivery_rate != null ? smallRatePct(r.stt_delivery_rate) : "-"}</td>
                <td>${r.stamp_buy_rate != null ? smallRatePct(r.stamp_buy_rate) : "-"}</td>
                <td>${r.gst_rate != null ? pct(Number(r.gst_rate || 0) * 100) : "-"}</td>
                <td>${r.dp_charge_sell != null ? money(r.dp_charge_sell) : "-"}</td>
                <td class="reason-cell">${escapeHtml(String(r.error || r.detail || ""))}</td>
              </tr>`
          )
          .join("")
      : '<tr><td colspan="10">No tax monitor runs yet.</td></tr>';
  }
  const resolvedBody = $("attentionResolvedTable")?.querySelector("tbody");
  if (resolvedBody) {
    resolvedBody.innerHTML = resolvedAlerts.length
      ? resolvedAlerts
          .map(
            (r) => `
              <tr>
                <td class="${attentionSeverityClass(r.severity_label)}">${escapeHtml(String(r.severity_label || "-").toUpperCase())}</td>
                <td>${escapeHtml(String(r.category || "-"))}</td>
                <td>${escapeHtml(String(r.title || "-"))}</td>
                <td>${escapeHtml(String(r.resolved_at || "-"))}</td>
                <td class="reason-cell">${escapeHtml(String(r.detail || ""))}</td>
              </tr>`
          )
          .join("")
      : '<tr><td colspan="5">No recently resolved items.</td></tr>';
  }
}

async function loadAttentionConsole(options = {}) {
  const throwOnError = !!options.throwOnError;
  try {
    const res = await api("/api/v1/attention");
    renderAttentionConsole(res || {});
    if ($("attentionStatusText")) {
      const openCount = Number((res?.summary || {}).open_count || 0);
      $("attentionStatusText").textContent = openCount > 0
        ? `Attention: ${openCount} open item(s)`
        : `Attention: clear`;
    }
  } catch (e) {
    const meta = normalizeUiError(e, "ATTENTION_LOAD_FAILED");
    if ($("attentionStatusText")) $("attentionStatusText").textContent = `Attention error: ${meta.reason}`;
    if ($("attentionSummary")) $("attentionSummary").innerHTML = `<div class="metric neg">Attention console unavailable: ${escapeHtml(meta.reason)}</div>`;
    const openBody = $("attentionOpenTable")?.querySelector("tbody");
    const runsBody = $("attentionTaxRunsTable")?.querySelector("tbody");
    const resolvedBody = $("attentionResolvedTable")?.querySelector("tbody");
    if (openBody) openBody.innerHTML = '<tr><td colspan="9">Failed to load open alerts.</td></tr>';
    if (runsBody) runsBody.innerHTML = '<tr><td colspan="10">Failed to load tax sync runs.</td></tr>';
    if (resolvedBody) resolvedBody.innerHTML = '<tr><td colspan="5">Failed to load resolved alerts.</td></tr>';
    if (throwOnError) throw e;
  }
}

async function runAttentionTaxMonitor() {
  if ($("attentionStatusText")) $("attentionStatusText").textContent = `Attention: tax monitor running`;
  await api("/api/v1/agents/tax_monitor/control", {
    method: "PUT",
    body: JSON.stringify({ run_now: true }),
  });
  await loadAttentionConsole();
  await loadAgentStatus();
  if ($("attentionStatusText")) $("attentionStatusText").textContent = `Attention: tax monitor refreshed`;
}

function renderIntelSummaryBlock(summary) {
  state.intelSummary = summary || null;
  if (!summary) {
    $("intelStatusText").textContent = "Intelligence: no data";
    $("intelSummary").innerHTML = "";
    $("intelImpactTable").querySelector("tbody").innerHTML = "";
    $("intelFlowTable").querySelector("tbody").innerHTML = "";
    return;
  }
  $("intelStatusText").textContent =
    `Intelligence: score ${Number(summary.portfolio_score || 0).toFixed(2)} | ` +
    `confidence ${(Number(summary.portfolio_confidence || 0) * 100).toFixed(1)}%`;
  const finBackfill = summary.financial_backfill || {};
  const finBackfillText = finBackfill.executed
    ? `financial backfill: symbols=${Number(finBackfill.symbols_considered || 0)}, rows+${Number(finBackfill.inserted_financial_rows || 0)}, rows~${Number(finBackfill.updated_financial_rows || 0)}`
    : `financial backfill: ${String(finBackfill.reason || "idle")}`;
  $("intelSummary").innerHTML = `
    <div class="metric">Portfolio Intel Score: ${Number(summary.portfolio_score || 0).toFixed(2)}</div>
    <div class="metric">Portfolio Intel Confidence: ${(Number(summary.portfolio_confidence || 0) * 100).toFixed(1)}%</div>
    <div class="metric">Recent Docs: ${Number(summary.documents_recent || 0)}</div>
    <div class="metric">Recent Impacts: ${Number(summary.impacts_recent || 0)}</div>
    <div class="metric">Commentary Weight: ${(Number(summary.weights?.commentary || 0) * 100).toFixed(1)}%</div>
    <div class="metric">Policy Weight: ${(Number(summary.weights?.policy || 0) * 100).toFixed(1)}%</div>
    <div class="metric">Financials Weight: ${(Number(summary.weights?.financials || 0) * 100).toFixed(1)}%</div>
    <div class="metric">Chart Weight: ${(Number(summary.weights?.chart || 0) * 100).toFixed(1)}%</div>
    <div class="metric">Decay Days: ${Number(summary.weights?.decay_days || 0)}</div>
    <div class="metric" style="grid-column: 1 / -1;">${escapeHtml(finBackfillText)}</div>
    <div class="metric" style="grid-column: 1 / -1;">Thought: ${summary.thought || "-"}</div>
  `;

  const symbols = Array.isArray(summary.symbol_scores) ? summary.symbol_scores : [];
  $("intelImpactTable").querySelector("tbody").innerHTML = symbols
    .slice(0, 120)
    .map((s) => `
      <tr>
        <td>${s.symbol || ""}</td>
        <td class="${clsBySign(s.score)}">${Number(s.score || 0).toFixed(2)}</td>
        <td>${(Number(s.confidence || 0) * 100).toFixed(1)}%</td>
        <td class="${clsBySign(s.commentary_score)}">${Number(s.commentary_score || 0).toFixed(2)}</td>
        <td class="${clsBySign(s.policy_score)}">${Number(s.policy_score || 0).toFixed(2)}</td>
        <td class="${clsBySign(s.financial_score)}">${Number(s.financial_score || 0).toFixed(2)}</td>
        <td class="reason-cell">${s.summary || ""}</td>
      </tr>
    `)
    .join("");

  const flows = Array.isArray(summary.cross_flows) ? summary.cross_flows : [];
  $("intelFlowTable").querySelector("tbody").innerHTML = flows
    .slice(0, 80)
    .map((f) => `
      <tr>
        <td>${f.from_symbol || ""}</td>
        <td>${f.to_symbol || ""}</td>
        <td>${Number(f.flow_score || 0).toFixed(3)}</td>
        <td>${f.period || ""}</td>
        <td>${f.reason || ""}</td>
      </tr>
    `)
    .join("");
}

async function loadIntelSummary() {
  try {
    const res = await api("/api/v1/intel/summary?limit=80");
    renderIntelSummaryBlock(res);
  } catch (e) {
    $("intelStatusText").textContent = `Intelligence load failed: ${e.message}`;
  }
}

async function readIntelDocContentFromInput() {
  const manual = $("intelDocContent").value || "";
  const fileInput = $("intelDocFile");
  const file = fileInput && fileInput.files && fileInput.files[0];
  if (file) {
    try {
      const txt = await file.text();
      if (txt && txt.trim()) return txt.trim();
    } catch {
      // fallback to manual text
    }
  }
  return manual.trim();
}

async function analyzeIntelDocument() {
  const btn = $("intelAnalyzeBtn");
  btn.disabled = true;
  btn.textContent = "Analyzing...";
  try {
    const content = await readIntelDocContentFromInput();
    if (!content) {
      alert("Provide transcript/policy text or upload a text file.");
      return;
    }
    const payload = {
      doc_type: $("intelDocType").value,
      source: $("intelDocSource").value.trim(),
      source_ref: $("intelDocRef").value.trim(),
      doc_date: $("intelDocDate").value || new Date().toISOString().slice(0, 10),
      title: $("intelDocTitle").value.trim(),
      content,
      run_strategy: true,
    };
    const res = await api("/api/v1/intel/docs", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    $("intelDocContent").value = "";
    $("intelDocFile").value = "";
    await loadIntelSummary();
    await loadStrategyInsights(false);
    await loadDashboard();
    alert(
      `Document analyzed.\nDoc ID: ${res.result?.doc_id || "-"}\n` +
      `Sentiment: ${Number(res.result?.sentiment_score || 0).toFixed(2)}\n` +
      `Impacted symbols: ${Number(res.result?.impact_count || 0)}`
    );
  } catch (e) {
    alert(`Intelligence analysis failed: ${e.message}`);
  } finally {
    btn.disabled = false;
    btn.textContent = "Analyze Document";
  }
}

async function addIntelFinancialRow() {
  const symbol = $("intelFinSymbol").value.trim().toUpperCase();
  const fiscal_period = $("intelFinPeriod").value.trim();
  if (!symbol || !fiscal_period) {
    alert("Financial QoQ update needs Symbol and Fiscal Period.");
    return;
  }
  const payload = {
    symbol,
    fiscal_period,
    report_date: $("intelFinDate").value || new Date().toISOString().slice(0, 10),
    revenue: numOrNull($("intelFinRevenue").value),
    pat: numOrNull($("intelFinPAT").value),
    operating_cash_flow: numOrNull($("intelFinOCF").value),
    investing_cash_flow: numOrNull($("intelFinICF").value),
    financing_cash_flow: numOrNull($("intelFinFCF").value),
    debt: numOrNull($("intelFinDebt").value),
    fii_holding_pct: numOrNull($("intelFinFII").value),
    dii_holding_pct: numOrNull($("intelFinDII").value),
    promoter_holding_pct: numOrNull($("intelFinProm").value),
    source: $("intelFinSource").value.trim() || "manual",
    notes: $("intelFinNotes").value.trim(),
    run_strategy: true,
  };
  const res = await api("/api/v1/intel/financials", {
    method: "POST",
    body: JSON.stringify(payload),
  });
  await loadIntelSummary();
  await loadStrategyInsights(false);
  await loadDashboard();
  alert(
    `Financial row updated for ${symbol}.\n` +
    `Signal score: ${Number(res.financial_signal?.score || 0).toFixed(2)}\n` +
    `Confidence: ${(Number(res.financial_signal?.confidence || 0) * 100).toFixed(1)}%`
  );
}

function splitColorForSymbol(symbol) {
  const s = String(symbol || "").toUpperCase();
  let h = 0;
  for (let i = 0; i < s.length; i++) h = ((h * 31) + s.charCodeAt(i)) % 360;
  return `hsl(${h}, 58%, 42%)`;
}

function resetSplitChartView() {
  state.splitChartView = { start: 0, end: 0 };
  renderSplitTimeline(applySplitsFilters(state.splitsRaw), false);
}

function renderSplitTimeline(items, preserveView = false) {
  const svg = $("splitTimelineChart");
  const tooltip = $("splitChartTooltip");
  if (!svg || !tooltip) return;
  svg.innerHTML = "";
  tooltip.classList.add("hidden");

  const pointsAll = (items || [])
    .map((s) => {
      const rawDate = String(s.effective_date || "");
      const d = new Date(`${rawDate}T00:00:00`);
      const ts = d.getTime();
      if (Number.isNaN(ts)) return null;
      return {
        symbol: String(s.symbol || "").toUpperCase(),
        date: rawDate,
        ts,
        factor: Number(s.factor || 0),
        note: s.note || "",
      };
    })
    .filter((x) => x && x.factor > 0)
    .sort((a, b) => (a.ts - b.ts) || String(a.symbol).localeCompare(String(b.symbol)));

  const width = 800;
  const height = 240;
  const m = { top: 18, right: 20, bottom: 36, left: 58 };
  const plotW = width - m.left - m.right;
  const plotH = height - m.top - m.bottom;
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);

  function svgEl(tag, attrs = {}) {
    const el = document.createElementNS("http://www.w3.org/2000/svg", tag);
    Object.entries(attrs).forEach(([k, v]) => el.setAttribute(k, String(v)));
    return el;
  }

  if (!pointsAll.length) {
    const txt = svgEl("text", { x: width / 2, y: height / 2, "text-anchor": "middle", "font-size": 12, fill: "#7b8797" });
    txt.textContent = "No split points for selected filter";
    svg.appendChild(txt);
    return;
  }

  if (!preserveView || !state.splitChartView || state.splitChartView.end <= state.splitChartView.start) {
    state.splitChartView = { start: 0, end: pointsAll.length - 1 };
  }
  let viewStart = Math.max(0, Math.min(pointsAll.length - 1, Number(state.splitChartView.start || 0)));
  let viewEnd = Math.max(viewStart, Math.min(pointsAll.length - 1, Number(state.splitChartView.end || (pointsAll.length - 1))));
  if (viewEnd === viewStart && pointsAll.length > 1) {
    viewEnd = Math.min(pointsAll.length - 1, viewStart + 1);
  }
  const view = pointsAll.slice(viewStart, viewEnd + 1);

  const axisColor = "#7b8797";
  const gridColor = "#e3e8ee";
  const vals = view.map((p) => p.factor);
  const minRaw = Math.min(...vals);
  const maxRaw = Math.max(...vals);
  const pad = Math.max(0.1, (maxRaw - minRaw) * 0.12);
  const min = Math.max(0, minRaw - pad);
  const max = maxRaw + pad;
  const x = (i) => m.left + (i / Math.max(1, view.length - 1)) * plotW;
  const y = (v) => m.top + (1 - (v - min) / (max - min || 1)) * plotH;

  const yTicks = 5;
  for (let t = 0; t <= yTicks; t++) {
    const v = min + ((max - min) * t) / yTicks;
    const yy = y(v);
    svg.appendChild(svgEl("line", { x1: m.left, y1: yy, x2: m.left + plotW, y2: yy, stroke: gridColor, "stroke-width": 1 }));
    const lbl = svgEl("text", { x: m.left - 8, y: yy + 3, "text-anchor": "end", "font-size": 10, fill: axisColor });
    lbl.textContent = Number(v).toFixed(2);
    svg.appendChild(lbl);
  }
  svg.appendChild(svgEl("line", { x1: m.left, y1: m.top + plotH, x2: m.left + plotW, y2: m.top + plotH, stroke: axisColor, "stroke-width": 1 }));
  svg.appendChild(svgEl("line", { x1: m.left, y1: m.top, x2: m.left, y2: m.top + plotH, stroke: axisColor, "stroke-width": 1 }));

  const xTicks = Math.min(6, view.length - 1);
  for (let t = 0; t <= xTicks; t++) {
    const i = Math.round((t / Math.max(1, xTicks)) * (view.length - 1));
    const xx = x(i);
    svg.appendChild(svgEl("line", { x1: xx, y1: m.top + plotH, x2: xx, y2: m.top + plotH + 4, stroke: axisColor, "stroke-width": 1 }));
    const lbl = svgEl("text", { x: xx, y: m.top + plotH + 16, "text-anchor": "middle", "font-size": 10, fill: axisColor });
    lbl.textContent = fmtDate(view[i].date);
    svg.appendChild(lbl);
  }

  const pathD = view.map((p, i) => `${i === 0 ? "M" : "L"} ${x(i)} ${y(p.factor)}`).join(" ");
  svg.appendChild(svgEl("path", { d: pathD, fill: "none", stroke: "#445868", "stroke-width": 1.4, "stroke-dasharray": "4 3", opacity: 0.7 }));
  view.forEach((p, i) => {
    svg.appendChild(svgEl("circle", { cx: x(i), cy: y(p.factor), r: 3.7, fill: splitColorForSymbol(p.symbol), opacity: 0.95 }));
  });

  const hoverLine = svgEl("line", {
    x1: m.left,
    y1: m.top,
    x2: m.left,
    y2: m.top + plotH,
    stroke: "#6a7785",
    "stroke-width": 1,
    "stroke-dasharray": "4 3",
    visibility: "hidden",
  });
  svg.appendChild(hoverLine);
  const overlay = svgEl("rect", {
    x: m.left,
    y: m.top,
    width: plotW,
    height: plotH,
    fill: "transparent",
    style: "cursor:crosshair",
  });
  svg.appendChild(overlay);

  const hoverAt = (clientX, clientY) => {
    const rect = svg.getBoundingClientRect();
    const px = ((clientX - rect.left) / rect.width) * width;
    const py = ((clientY - rect.top) / rect.height) * height;
    if (px < m.left || px > m.left + plotW || py < m.top || py > m.top + plotH) return;
    const i = Math.max(0, Math.min(view.length - 1, Math.round(((px - m.left) / plotW) * (view.length - 1))));
    const p = view[i];
    const xx = x(i);
    hoverLine.setAttribute("x1", xx);
    hoverLine.setAttribute("x2", xx);
    hoverLine.setAttribute("visibility", "visible");
    tooltip.innerHTML = `
      <div class="row"><span class="label">Symbol</span><strong>${p.symbol}</strong></div>
      <div class="row"><span class="label">Date</span><strong>${fmtDate(p.date)}</strong></div>
      <div class="row"><span class="label">Factor</span><strong>${Number(p.factor).toFixed(4)}</strong></div>
      <div class="row"><span class="label">Note</span><strong>${escapeHtml(p.note || "-")}</strong></div>
    `;
    tooltip.classList.remove("hidden");
    const wrap = svg.closest(".chart-wrap").getBoundingClientRect();
    let tx = clientX - wrap.left + 10;
    let ty = clientY - wrap.top - 10;
    if (tx > wrap.width - 220) tx = wrap.width - 220;
    if (ty < 10) ty = 10;
    tooltip.style.left = `${tx}px`;
    tooltip.style.top = `${ty}px`;
  };

  let drag = null;
  overlay.addEventListener("mousedown", (e) => {
    drag = { x: e.clientX, start: viewStart, end: viewEnd };
    overlay.style.cursor = "grabbing";
  });
  overlay.addEventListener("mouseup", () => {
    drag = null;
    overlay.style.cursor = "crosshair";
  });
  overlay.addEventListener("mousemove", (e) => {
    hoverAt(e.clientX, e.clientY);
    if (!drag) return;
    const dxPx = e.clientX - drag.x;
    const span = Math.max(2, drag.end - drag.start + 1);
    const shift = Math.round((-dxPx / plotW) * span);
    let ns = drag.start + shift;
    let ne = drag.end + shift;
    if (ns < 0) {
      ne += -ns;
      ns = 0;
    }
    if (ne > pointsAll.length - 1) {
      ns -= ne - (pointsAll.length - 1);
      ne = pointsAll.length - 1;
    }
    ns = Math.max(0, ns);
    ne = Math.max(ns + 1, ne);
    state.splitChartView = { start: ns, end: ne };
    renderSplitTimeline(items, true);
  });
  overlay.addEventListener("mouseleave", () => {
    drag = null;
    overlay.style.cursor = "crosshair";
    hoverLine.setAttribute("visibility", "hidden");
    tooltip.classList.add("hidden");
  });
  overlay.addEventListener("wheel", (e) => {
    e.preventDefault();
    const zoomIn = e.deltaY < 0;
    const span = Math.max(2, viewEnd - viewStart + 1);
    const nextSpan = zoomIn ? Math.max(2, Math.round(span * 0.85)) : Math.min(pointsAll.length, Math.round(span * 1.15));
    const rect = svg.getBoundingClientRect();
    const px = ((e.clientX - rect.left) / rect.width) * width;
    const ratio = clamp((px - m.left) / plotW, 0, 1);
    const centerIndex = viewStart + Math.round(ratio * (span - 1));
    let ns = centerIndex - Math.round(ratio * (nextSpan - 1));
    let ne = ns + nextSpan - 1;
    if (ns < 0) {
      ne += -ns;
      ns = 0;
    }
    if (ne > pointsAll.length - 1) {
      ns -= ne - (pointsAll.length - 1);
      ne = pointsAll.length - 1;
    }
    ns = Math.max(0, ns);
    ne = Math.max(ns + 1, ne);
    state.splitChartView = { start: ns, end: ne };
    renderSplitTimeline(items, true);
  }, { passive: false });
  overlay.addEventListener("dblclick", resetSplitChartView);
}

function renderSplits(items) {
  $("splitsTable").querySelector("tbody").innerHTML = items
    .map(
      (s) => `
      <tr>
        <td>${s.id}</td>
        <td>${s.symbol}</td>
        <td>${s.effective_date}</td>
        <td>${s.factor}</td>
        <td>${s.note || ""}</td>
        <td><button class="btn secondary split-del-btn" data-id="${s.id}">Delete</button></td>
      </tr>`
    )
    .join("");
  document.querySelectorAll(".split-del-btn").forEach((b) => {
    b.addEventListener("click", async () => {
      await api(`/api/v1/corporate-actions/splits/${b.getAttribute("data-id")}`, { method: "DELETE" });
      await loadSplits();
      await loadDashboard();
      if (state.selectedSymbol) await loadScrip(state.selectedSymbol);
    });
  });
  renderSplitTimeline(items, false);
}

function renderCashflowSummary(s) {
  if (!s) {
    $("cashflowSummary").innerHTML = "";
    return;
  }
  $("cashflowSummary").innerHTML = `
    <div class="metric">Entries: ${Number(s.entries || 0)}</div>
    <div class="metric">Deposits: ${money(s.deposits_total)}</div>
    <div class="metric">Withdrawals: ${money(s.withdrawals_total)}</div>
    <div class="metric">Investment: ${money(s.net_hand_investment_total)}</div>
    <div class="metric">Hand Investment After Charges: ${money(s.net_hand_after_charges_total)}</div>
    <div class="metric">Trade Credits: ${money(s.trade_credit_total)}</div>
    <div class="metric">Market Deployment (Settlement Debits): ${money(s.investment_spend_total)}</div>
    <div class="metric">Charges: ${money(s.charges_total)}</div>
    <div class="metric">Deployment + Charges: ${money(s.invested_plus_charges_total)}</div>
    <div class="metric">Cash Balance: ${money(s.cash_balance)}</div>
    <div class="metric">From: ${s.from_date || "-"}</div>
    <div class="metric">To: ${s.to_date || "-"}</div>
  `;
}

function renderCashflows(items) {
  $("cashflowTable").querySelector("tbody").innerHTML = (items || [])
    .map((r) => {
      const t = String(r.entry_type || "").toLowerCase();
      const typeLabel = t === "trade_credit" ? "TRADE CREDIT" : t === "investment" ? "INVESTMENT" : t === "charge" ? "CHARGE" : t.toUpperCase();
      const typeCls = t === "deposit" || t === "trade_credit" ? "pos" : "neg";
      return `
      <tr>
        <td>${r.id}</td>
        <td>${r.entry_date}</td>
        <td class="${typeCls}">${typeLabel}</td>
        <td class="${Number(r.amount || 0) >= 0 ? "pos" : "neg"}">${money(r.amount)}</td>
        <td>${r.reference_text || ""}</td>
        <td>${r.external_entry_id || ""}</td>
        <td>${r.source || ""}</td>
      </tr>
      `;
    })
    .join("");
}

function renderDividendSummary(s) {
  if (!s) {
    $("dividendSummary").innerHTML = "";
    return;
  }
  const top = Array.isArray(s.top_symbols) ? s.top_symbols : [];
  const topTxt = top.length
    ? top.map((x) => `${x.symbol}: ${money(x.total_dividend)}`).join(" | ")
    : "-";
  const byYear = Array.isArray(s.by_year) ? s.by_year : [];
  const yearRows = byYear.map((y) => `
    <tr>
      <td>${y.year || "-"}</td>
      <td class="num">${y.entries}</td>
      <td class="num pos">${money(y.total_dividend)}</td>
    </tr>`).join("");
  const yearTable = byYear.length ? `
    <table class="year-dividend-table">
      <thead>
        <tr>
          <th>Year</th>
          <th class="num">Entries</th>
          <th class="num">Total Dividend</th>
        </tr>
      </thead>
      <tbody>${yearRows}</tbody>
    </table>` : "";
  $("dividendSummary").innerHTML = `
    <div class="metric">Entries: ${Number(s.entries || 0)}</div>
    <div class="metric">Total Dividend Profit: ${money(s.total_dividend)}</div>
    <div class="metric">From: ${s.from_date || "-"}</div>
    <div class="metric">To: ${s.to_date || "-"}</div>
    <div class="metric">Top Scrips: ${topTxt}</div>
    ${yearTable}
  `;
}

function renderDividends(items) {
  $("dividendTable").querySelector("tbody").innerHTML = (items || [])
    .map((r) => `
      <tr>
        <td>${r.id}</td>
        <td>${r.entry_date}</td>
        <td>${r.symbol}</td>
        <td class="pos">${money(r.amount)}</td>
        <td>${r.reference_text || ""}</td>
        <td>${r.external_entry_id || ""}</td>
        <td>${r.source || ""}</td>
      </tr>
    `)
    .join("");
}

function applyLivePolling(intervalSec) {
  if (state.liveTimer) clearInterval(state.liveTimer);
  if (!$("liveEnabled").checked) return;
  state.liveTimer = setInterval(async () => {
    try {
      await loadDashboard();
      if (state.selectedSymbol) await loadScrip(state.selectedSymbol);
      await loadPeakDiff();
    } catch (e) {
      console.error("live refresh error", e);
    }
  }, intervalSec * 1000);
}

async function loadLiveConfig() {
  const cfg = await api("/api/v1/config/live");
  $("liveEnabled").checked = !!cfg.enabled;
  $("liveIntervalSec").value = cfg.interval_seconds;
  applyLivePolling(cfg.interval_seconds);
  await loadPriceStatus();
}

async function saveLiveConfig() {
  const enabled = $("liveEnabled").checked;
  const interval = Math.max(5, Number($("liveIntervalSec").value || 10));
  const cfg = await api("/api/v1/config/live", {
    method: "PUT",
    body: JSON.stringify({ enabled, interval_seconds: interval }),
  });
  $("liveEnabled").checked = !!cfg.enabled;
  $("liveIntervalSec").value = cfg.interval_seconds;
  applyLivePolling(cfg.interval_seconds);
}

async function loadPriceStatus() {
  const st = await api("/api/v1/prices/status");
  const ts = st.updated_at || "N/A";
  state.latestPriceUpdatedAt = st.updated_at || "";
  $("pricesStatusText").textContent = `Prices updated at: ${ts} | symbols: ${st.scrips_with_price}`;
}

async function refreshPricesNow() {
  $("refreshPricesBtn").disabled = true;
  $("refreshPricesBtn").textContent = "Refreshing...";
  try {
    await api("/api/v1/prices/refresh", { method: "POST", body: JSON.stringify({}) });
    await loadDashboard();
    await loadPeakDiff();
    await loadPriceStatus();
  } finally {
    $("refreshPricesBtn").disabled = false;
    $("refreshPricesBtn").textContent = "Refresh Live Prices Now";
  }
}

async function refreshStrategyNow() {
  const btn = $("refreshStrategyBtn");
  btn.disabled = true;
  btn.textContent = "Refreshing...";
  try {
    await loadStrategyInsights(true);
  } finally {
    btn.disabled = false;
    btn.textContent = "Refresh Strategy Insights";
  }
}

function skippedReasonText(reason) {
  const r = String(reason || "").toLowerCase();
  if (r === "duplicate_trade_id") return "Duplicate Trade ID";
  if (r === "duplicate_value_date") return "Duplicate by value/date";
  if (r === "non_equity") return "Non-equity row";
  if (r === "invalid") return "Invalid row";
  return r || "Skipped";
}

function canOverrideSkippedTrade(it) {
  if (!it) return false;
  if (!String(it.symbol || "").trim()) return false;
  if (!String(it.trade_date || "").trim()) return false;
  const side = String(it.side || "").toUpperCase();
  if (!(side === "BUY" || side === "SELL")) return false;
  if (!(Number(it.quantity || 0) > 0)) return false;
  if (!(Number(it.price || 0) > 0)) return false;
  return true;
}

function closeSkipOverrideModal(refresh = false) {
  $("skipOverrideModal").classList.add("hidden");
  if (refresh && state.skippedOverrideAdded > 0) {
    Promise.all([loadDashboard(), loadPeakDiff(), state.selectedSymbol ? loadScrip(state.selectedSymbol) : Promise.resolve()])
      .catch(() => {});
  }
}

async function overrideSkippedTradeAt(index) {
  const item = state.skippedTradeItems[index];
  if (!item) return;
  if (!canOverrideSkippedTrade(item)) {
    alert("This skipped row is not override-capable (missing symbol/date/side/qty/price).");
    return;
  }
  const ok = confirm(
    `Override add trade?\nSymbol: ${item.symbol}\nDate: ${item.trade_date}\nSide: ${item.side}\nQty: ${item.quantity}\nPrice: ${item.price}\nReason: ${skippedReasonText(item.reason)}`
  );
  if (!ok) return;
  try {
    await api("/api/v1/trades/override", {
      method: "POST",
      body: JSON.stringify({
        symbol: item.symbol,
        side: item.side,
        trade_date: item.trade_date,
        quantity: Number(item.quantity),
        price: Number(item.price),
        external_trade_id: item.external_trade_id || null,
        notes: `override_from_upload; reason:${item.reason || "skipped"}`,
      }),
    });
    state.skippedOverrideAdded += 1;
    item.__added = true;
    renderSkipOverrideItems();
  } catch (e) {
    alert(`Override failed: ${e.message}`);
  }
}

function renderSkipOverrideItems() {
  const body = $("skipOverrideTable").querySelector("tbody");
  body.innerHTML = (state.skippedTradeItems || [])
    .map((it, idx) => {
      const overrideable = canOverrideSkippedTrade(it);
      const added = !!it.__added;
      return `
      <tr>
        <td>${it.row_number || "-"}</td>
        <td title="${it.message || ""}">${skippedReasonText(it.reason)}</td>
        <td>${it.symbol || ""}</td>
        <td>${it.trade_date || ""}</td>
        <td>${String(it.side || "").toUpperCase()}</td>
        <td>${money(it.quantity)}</td>
        <td>${money(it.price)}</td>
        <td>${it.external_trade_id || ""}</td>
        <td>${added ? "<span class='pill'>Added</span>" : (overrideable ? `<button class=\"btn secondary skip-override-add\" data-i=\"${idx}\">Override Add</button>` : "<span class='metric'>N/A</span>")}</td>
      </tr>`;
    })
    .join("");

  body.querySelectorAll(".skip-override-add").forEach((b) => {
    b.addEventListener("click", () => overrideSkippedTradeAt(Number(b.getAttribute("data-i"))));
  });
}

function openSkipOverrideModal(items, truncated = false) {
  state.skippedTradeItems = (items || []).slice();
  state.skippedOverrideAdded = 0;
  const total = state.skippedTradeItems.length;
  const overrideable = state.skippedTradeItems.filter(canOverrideSkippedTrade).length;
  $("skipOverrideSummary").textContent = `Skipped rows: ${total}. Override-capable: ${overrideable}.${truncated ? " Showing limited sample." : ""}`;
  renderSkipOverrideItems();
  $("skipOverrideModal").classList.remove("hidden");
}

async function uploadTradebook() {
  const fileInput = $("tradebookFile");
  const file = fileInput.files && fileInput.files[0];
  if (!file) {
    alert("Select a tradebook .xlsx file first.");
    return;
  }
  $("uploadTradebookBtn").disabled = true;
  $("uploadTradebookBtn").textContent = "Uploading...";
  try {
    const buffer = await file.arrayBuffer();
    const content_base64 = arrayBufferToBase64(buffer);
    const res = await api("/api/v1/upload/tradebook", {
      method: "POST",
      body: JSON.stringify({ filename: file.name, content_base64, include_skipped: true }),
    });
    await loadDashboard();
    await loadPeakDiff();
    await loadPriceStatus();
    const st = res.stats || {};
    alert(
      `Upload complete.\nInserted: ${st.inserted || 0}\nSkipped duplicates: ${st.skipped_duplicates || 0}\nSkipped by Trade ID: ${st.skipped_trade_id_duplicates || 0}\nTrade ID column detected: ${st.trade_id_column_detected ? "Yes" : "No"}\nSkipped non-equity rows: ${st.skipped_non_equity || 0}\nSkipped invalid: ${st.skipped_invalid || 0}\nCross-source duplicates removed: ${st.cross_source_dedup_removed || 0}`
    );
    const skippedItems = Array.isArray(st.skipped_items) ? st.skipped_items : [];
    if (skippedItems.length > 0) {
      const wants = confirm(
        `There are ${skippedItems.length} skipped rows. Do you want to review and override-add individually?`
      );
      if (wants) {
        openSkipOverrideModal(skippedItems, !!st.skipped_items_truncated);
      }
    }
    fileInput.value = "";
  } finally {
    $("uploadTradebookBtn").disabled = false;
    $("uploadTradebookBtn").textContent = "Upload Tradebook";
  }
}

async function uploadCashflow() {
  const fileInput = $("cashflowFile");
  const file = fileInput.files && fileInput.files[0];
  if (!file) {
    alert("Select a cashflow .xlsx file first.");
    return;
  }
  $("uploadCashflowBtn").disabled = true;
  $("uploadCashflowBtn").textContent = "Uploading...";
  try {
    const buffer = await file.arrayBuffer();
    const content_base64 = arrayBufferToBase64(buffer);
    const replace_all = $("cashflowReplaceAll").checked;
    const res = await api("/api/v1/upload/cashflow", {
      method: "POST",
      body: JSON.stringify({ filename: file.name, content_base64, replace_all }),
    });
    await loadCashflows();
    await loadDashboard();
    await loadStrategyInsights(true);
    const st = res.stats || {};
    alert(
      `Cashflow upload complete.\nReplaced existing: ${res.replaced_existing ? "Yes" : "No"}\nInserted: ${st.inserted || 0}\nSkipped duplicates: ${st.skipped_duplicates || 0}\nSkipped by Entry ID: ${st.skipped_id_duplicates || 0}\nEntry ID column detected: ${st.entry_id_column_detected ? "Yes" : "No"}\nSkipped invalid: ${st.skipped_invalid || 0}\nCross-source duplicates removed: ${st.cross_source_dedup_removed || 0}`
    );
    fileInput.value = "";
    $("cashflowReplaceAll").checked = false;
  } finally {
    $("uploadCashflowBtn").disabled = false;
    $("uploadCashflowBtn").textContent = "Upload Cashflow";
  }
}

async function uploadDividends() {
  const fileInput = $("dividendFile");
  const file = fileInput.files && fileInput.files[0];
  if (!file) {
    alert("Select a dividend .xlsx file first.");
    return;
  }
  $("uploadDividendBtn").disabled = true;
  $("uploadDividendBtn").textContent = "Uploading...";
  try {
    const buffer = await file.arrayBuffer();
    const content_base64 = arrayBufferToBase64(buffer);
    const replace_all = $("dividendReplaceAll").checked;
    const res = await api("/api/v1/upload/dividends", {
      method: "POST",
      body: JSON.stringify({ filename: file.name, content_base64, replace_all }),
    });
    await loadDividends();
    await loadDashboard();
    await loadStrategyInsights(true);
    const st = res.stats || {};
    alert(
      `Dividend upload complete.\nReplaced existing: ${res.replaced_existing ? "Yes" : "No"}\nInserted: ${st.inserted || 0}\nSkipped duplicates: ${st.skipped_duplicates || 0}\nSkipped by Entry ID: ${st.skipped_id_duplicates || 0}\nEntry ID column detected: ${st.entry_id_column_detected ? "Yes" : "No"}\nSkipped invalid: ${st.skipped_invalid || 0}\nCross-source duplicates removed: ${st.cross_source_dedup_removed || 0}`
    );
    fileInput.value = "";
    $("dividendReplaceAll").checked = false;
  } finally {
    $("uploadDividendBtn").disabled = false;
    $("uploadDividendBtn").textContent = "Upload Dividends";
  }
}

async function addScrip() {
  const symbol = $("newSymbol").value.trim().toUpperCase();
  const exchange = $("newExchange").value;
  const feed_code = $("newFeedCode").value.trim();
  const asset_class = String($("newAssetClass")?.value || "EQUITY").toUpperCase();
  if (!symbol) {
    alert("Enter symbol");
    return;
  }
  await api("/api/v1/scrips", {
    method: "POST",
    body: JSON.stringify({ symbol, exchange, feed_code, asset_class }),
  });
  $("newSymbol").value = "";
  $("newFeedCode").value = "";
  if ($("newAssetClass")) $("newAssetClass").value = "EQUITY";
  await loadDashboard();
  await loadPeakDiff();
}

async function deleteScrip() {
  const typed = $("deleteSymbol").value.trim();
  const symbol = typed.toUpperCase();
  if (!symbol) {
    alert("Enter symbol");
    return;
  }
  const exact = (state.symbols || []).find((s) => String(s).toUpperCase() === symbol);
  if (!exact) {
    alert("Select the exact symbol from suggestions to delete.");
    return;
  }
  if (!confirm(`Delete ${exact} and entire history?`)) return;
  await api(`/api/v1/scrips/${encodeURIComponent(exact)}`, { method: "DELETE" });
  $("deleteSymbol").value = "";
  await loadDashboard();
  await loadPeakDiff();
}

async function bulkDeleteScrips() {
  const sel = $("bulkDeleteSymbols");
  const symbols = Array.from(sel.selectedOptions).map((o) => o.value);
  if (!symbols.length) {
    alert("Select one or more scrips for bulk delete.");
    return;
  }
  if (!confirm(`Delete ${symbols.length} scrip(s) and full history?`)) return;
  const res = await api("/api/v1/scrips/bulk-delete", {
    method: "POST",
    body: JSON.stringify({ symbols }),
  });
  const deleted = (res.deleted_symbols || []).length;
  const missing = (res.not_found_symbols || []).length;
  alert(`Bulk delete completed.\nDeleted symbols: ${deleted}\nNot found: ${missing}`);
  await loadDashboard();
  await loadPeakDiff();
  await loadSplits();
}

async function addManualTrade() {
  const symbol = state.selectedSymbol;
  if (!symbol) {
    alert("Select a scrip first.");
    return;
  }
  const trade_date = $("tradeAddDate").value;
  const side = $("tradeAddSide").value;
  const quantity = Number($("tradeAddQty").value);
  const price = Number($("tradeAddPrice").value);
  const notes = $("tradeAddNotes").value.trim();
  if (!trade_date || !quantity || !price) {
    alert("Provide trade date, quantity and price.");
    return;
  }
  try {
    await api(`/api/v1/scrips/${symbol}/trades`, {
      method: "POST",
      body: JSON.stringify({ trade_date, side, quantity, price, notes }),
    });
  } catch (e) {
    alert(`Add trade failed: ${e.message}`);
    return;
  }
  $("tradeAddQty").value = "";
  $("tradeAddPrice").value = "";
  $("tradeAddNotes").value = "";
  await loadScrip(symbol);
  await loadDashboard();
  await loadPeakDiff();
}

async function loadDashboard() {
  const basis = $("basisSelect").value;
  const [summary, holdings, perf, ts] = await Promise.all([
    api("/api/v1/portfolio/summary"),
    api("/api/v1/scrips"),
    api(`/api/v1/portfolio/performance?basis=${basis}`),
    api("/api/v1/portfolio/timeseries"),
  ]);
  renderKpis(summary);
  state.holdingsRaw = holdings.items || [];
  renderHoldings(getFilteredSortedHoldings());
  updateHoldingsSortHeaders();
  await loadRebalanceSuggestions();
  renderPortfolioPerf(perf);
  renderChart(ts.points, Array.isArray(state.tsPointsRaw) && state.tsPointsRaw.length > 0);
  renderSymbolSelect(state.holdingsRaw);
  if (state.selectedSymbol) {
    try {
      await loadScrip(state.selectedSymbol);
    } catch {
      state.selectedSymbol = state.symbols[0] || null;
      if (state.selectedSymbol) await loadScrip(state.selectedSymbol);
    }
  }
  if (state.currentTab === "assets") {
    await loadAssetSplit();
  }
  if (state.currentTab === "harvest") {
    await loadHarvestPlan();
  }
  if (state.currentTab === "dailytarget") {
    await loadDailyTargetPlan({ recalibrate: false });
  }
  if (state.currentTab === "losslots") {
    await loadLossLots();
  }
}

function renderTenantControls(payload) {
  const items = Array.isArray(payload?.items) ? payload.items : [];
  const active = String(payload?.active_tenant || "default");
  state.tenantsRaw = items;
  state.activeTenant = active;
  const sel = $("tenantSelect");
  if (sel) {
    sel.innerHTML = items
      .map((t) => {
        const key = String(t.key || "");
        const name = String(t.name || key || "tenant");
        const activeMark = key === active ? " (Active)" : "";
        return `<option value="${escapeHtml(key)}">${escapeHtml(`${name} [${key}]${activeMark}`)}</option>`;
      })
      .join("");
    if (active) sel.value = active;
  }
  if ($("tenantStatusText")) {
    const count = items.length;
    $("tenantStatusText").textContent = `Tenant: ${active} (${count}/5)`;
  }
}

async function loadTenants() {
  const res = await api("/api/v1/tenants");
  renderTenantControls(res || {});
}

async function switchTenant() {
  const sel = $("tenantSelect");
  const key = String(sel?.value || "").trim();
  if (!key) return;
  await api("/api/v1/tenants/active", {
    method: "PUT",
    body: JSON.stringify({ tenant: key }),
  });
  await loadTenants();
  await loadLiveConfig();
  await loadStrategy();
  await loadIntelSummary();
  await loadSplits();
  await loadCashflows();
  await loadDividends();
  await loadAgentStatus();
  await loadAssistantApprovals();
  await loadApprovalsTab();
  await loadApprovalVerification();
  await loadDashboard();
  await loadPeakDiff();
}

async function createTenantFromInput() {
  const keyRaw = String($("tenantNewKey")?.value || "").trim();
  if (!keyRaw) {
    alert("Enter tenant key (example: client-a).");
    return;
  }
  await api("/api/v1/tenants", {
    method: "POST",
    body: JSON.stringify({
      key: keyRaw,
      name: keyRaw,
      activate: true,
    }),
  });
  if ($("tenantNewKey")) $("tenantNewKey").value = "";
  await loadTenants();
  await loadLiveConfig();
  await loadStrategy();
  await loadIntelSummary();
  await loadSplits();
  await loadCashflows();
  await loadDividends();
  await loadAgentStatus();
  await loadAssistantApprovals();
  await loadDashboard();
  await loadPeakDiff();
}

async function loadPeakDiff() {
  const res = await api("/api/v1/analytics/peak-diff");
  state.peakRaw = res.items || [];
  state.pendingPeakSplitCandidates = Array.isArray(res.pending_split_candidates) ? res.pending_split_candidates : [];
  updatePeakSplitReviewStatus();
  renderPeakDiff(applyPeakFilters(state.peakRaw));
  const sig = pendingPeakSplitSignature(state.pendingPeakSplitCandidates);
  if (!sig) {
    state.peakSplitPromptKey = "";
    return;
  }
  if (state.currentTab === "peak" && sig !== state.peakSplitPromptKey) {
    state.peakSplitPromptKey = sig;
    openPeakSplitReviewModal();
  }
}

async function loadScrip(symbol) {
  state.selectedSymbol = symbol;
  const basis = $("scripBasisSelect").value;
  const [s, trades, perf] = await Promise.all([
    api(`/api/v1/scrips/${symbol}`),
    api(`/api/v1/scrips/${symbol}/trades`),
    api(`/api/v1/scrips/${symbol}/performance?basis=${basis}`),
  ]);
  state.selectedAssetClass = normalizeAssetClass(s?.asset_class, symbol);
  updateTradeUnitLabels(state.selectedAssetClass, symbol);
  renderScripStats(s);
  const ltpHint = Number(s?.ltp || trades?.current_ltp || 0);
  state.tradesRaw = normalizeTradeRowsForCurrentPnl(trades.items || [], ltpHint);
  renderTrades(applyTradesFilters(state.tradesRaw));
  renderScripPerf(perf);
  $("tradeEditFor").textContent = `For Scrip: ${symbol}`;
  if (!$("sellSimPrice").value || Number($("sellSimPrice").value) <= 0) {
    $("sellSimPrice").value = Number(s.ltp || 0).toFixed(2);
  }
  renderSellSimulation(null);
  if (!$("tradeAddDate").value) {
    $("tradeAddDate").value = new Date().toISOString().slice(0, 10);
  }
}

async function loadStrategy() {
  const res = await api("/api/v1/strategy/sets");
  renderStrategySets(res.items);
  await loadStrategyInsights(false);
}

async function loadStrategyInsights(force = false) {
  if (force) {
    const res = await api("/api/v1/strategy/refresh", {
      method: "POST",
      body: JSON.stringify({}),
    });
    renderStrategyInsights(res.item || null);
    return;
  }
  const res = await api("/api/v1/strategy/insights");
  renderStrategyInsights(res.item || null);
}

async function loadSplits() {
  const res = await api("/api/v1/corporate-actions/splits");
  state.splitsRaw = res.items || [];
  renderSplits(applySplitsFilters(state.splitsRaw));
}

async function loadCashflows() {
  const from = $("cashflowFilterFrom").value;
  const to = $("cashflowFilterTo").value;
  const entry_type = $("cashflowFilterType").value;
  const q = $("cashflowFilterText").value.trim();
  const params = new URLSearchParams();
  if (from) params.set("from", from);
  if (to) params.set("to", to);
  if (entry_type) params.set("entry_type", entry_type);
  if (q) params.set("q", q);
  const path = `/api/v1/cashflows${params.toString() ? `?${params.toString()}` : ""}`;
  const res = await api(path);
  state.cashflowRaw = res.items || [];
  renderCashflows(state.cashflowRaw);
  renderCashflowSummary(res.summary || null);
}

async function loadDividends() {
  const from = $("dividendFilterFrom").value;
  const to = $("dividendFilterTo").value;
  const symbol = $("dividendFilterSymbol").value.trim();
  const q = $("dividendFilterText").value.trim();
  const params = new URLSearchParams();
  if (from) params.set("from", from);
  if (to) params.set("to", to);
  if (symbol) params.set("symbol", symbol);
  if (q) params.set("q", q);
  const path = `/api/v1/dividends${params.toString() ? `?${params.toString()}` : ""}`;
  const res = await api(path);
  state.dividendRaw = res.items || [];
  renderDividends(state.dividendRaw);
  renderDividendSummary(res.summary || null);
}

async function saveParams() {
  const setId = Number($("strategySelect").value);
  const params = Array.from(document.querySelectorAll("#paramsGrid input")).map((i) => ({
    key: i.getAttribute("data-key"),
    value: Number(i.value),
  }));
  await api(`/api/v1/strategy/sets/${setId}/parameters`, {
    method: "PUT",
    body: JSON.stringify({ parameters: params }),
  });
  await loadDashboard();
  await loadPeakDiff();
  await loadStrategy();
  await loadStrategyInsights(true);
}

async function activateSet() {
  const id = Number($("strategySelect").value);
  await api("/api/v1/strategy/sets/active", {
    method: "PUT",
    body: JSON.stringify({ id }),
  });
  await loadDashboard();
  await loadPeakDiff();
  await loadStrategy();
  await loadStrategyInsights(true);
}

async function addSplit() {
  const symbol = $("splitSymbol").value.trim().toUpperCase();
  const effective_date = $("splitDate").value;
  const factor = Number($("splitFactor").value);
  if (!symbol || !effective_date || !factor) {
    alert("Enter symbol, effective date, and factor.");
    return;
  }
  await api("/api/v1/corporate-actions/splits", {
    method: "POST",
    body: JSON.stringify({ symbol, effective_date, factor }),
  });
  $("splitSymbol").value = "";
  $("splitDate").value = "";
  $("splitFactor").value = "";
  await loadSplits();
  await loadDashboard();
  await loadPeakDiff();
  if (state.selectedSymbol) await loadScrip(state.selectedSymbol);
}

function bindEvents() {
  document.querySelectorAll(".tab-btn").forEach((b) => {
    b.addEventListener("click", async () => {
      const tab = b.getAttribute("data-tab");
      setTab(tab);
      if (tab === "cashflow") {
        await loadCashflows();
      }
      if (tab === "dividends") {
        await loadDividends();
      }
      if (tab === "strategy") {
        await loadStrategyInsights(false);
        await loadIntelSummary();
      }
      if (tab === "strategyaudit") {
        await loadStrategyAudit();
        await loadHostedLlmConfig();
      }
      if (tab === "attention") {
        await loadAttentionConsole();
      }
      if (tab === "peak") {
        await loadPeakDiff();
      }
      if (tab === "assets") {
        await loadAssetSplit();
      }
      if (tab === "harvest") {
        await loadHarvestPlan();
      }
      if (tab === "dailytarget") {
        await loadDailyTargetPlan({ recalibrate: false });
        await loadDailyTargetHistory();
      }
      if (tab === "losslots") {
        await loadLossLots();
      }
      if (tab === "assistant") {
        await loadAssistantApprovals();
      }
      if (tab === "approvals") {
        await loadApprovalsTab();
        await loadApprovalVerification();
      }
      if (tab === "agents") {
        await loadAgentStatus();
        await loadBacktestHistory();
        await loadSoftwarePerfLogs();
      }
    });
  });
  $("basisSelect").addEventListener("change", loadDashboard);
  $("scripBasisSelect").addEventListener("change", () => {
    if (state.selectedSymbol) loadScrip(state.selectedSymbol);
  });
  $("symbolSelect").addEventListener("change", (e) => loadScrip(e.target.value));
  $("newSymbol")?.addEventListener("input", (e) => {
    const v = String(e?.target?.value || "").trim().toUpperCase();
    if ($("newAssetClass")) $("newAssetClass").value = looksLikeGoldSymbol(v) ? "GOLD" : "EQUITY";
  });
  registerButton("tenantSwitchBtn", switchTenant, { actionName: "Switch Tenant", errorCode: "TENANT_SWITCH_FAILED" });
  registerButton("tenantCreateBtn", createTenantFromInput, { actionName: "Create Tenant", errorCode: "TENANT_CREATE_FAILED" });
  registerButton("refreshPricesBtn", refreshPricesNow, { actionName: "Refresh Live Prices", errorCode: "PRICES_REFRESH_FAILED" });
  registerButton("uploadTradebookBtn", uploadTradebook, { actionName: "Upload Tradebook", errorCode: "TRADEBOOK_UPLOAD_FAILED" });
  registerButton("uploadCashflowBtn", uploadCashflow, { actionName: "Upload Cashflow", errorCode: "CASHFLOW_UPLOAD_FAILED" });
  registerButton("uploadDividendBtn", uploadDividends, { actionName: "Upload Dividends", errorCode: "DIVIDEND_UPLOAD_FAILED" });
  registerButton("tradeAddBtn", addManualTrade, { actionName: "Add Trade", errorCode: "TRADE_ADD_FAILED" });
  registerButton("saveParamsBtn", saveParams, { actionName: "Save Strategy Parameters", errorCode: "STRATEGY_PARAMS_SAVE_FAILED" });
  registerButton("activateSetBtn", activateSet, { actionName: "Activate Strategy Set", errorCode: "STRATEGY_SET_ACTIVATE_FAILED" });
  registerButton("refreshStrategyBtn", refreshStrategyNow, { actionName: "Refresh Strategy", errorCode: "STRATEGY_REFRESH_FAILED" });
  registerButton("strategyAuditRunBtn", runStrategyAudit, { actionName: "Run Strategy Audit", errorCode: "STRATEGY_AUDIT_RUN_FAILED" });
  registerButton("strategyAuditRefreshBtn", () => loadStrategyAudit({ throwOnError: true }), { actionName: "Refresh Strategy Audit", errorCode: "STRATEGY_AUDIT_LOAD_FAILED" });
  registerButton("hostedLlmSaveBtn", saveHostedLlmConfig, { actionName: "Save Hosted LLM", errorCode: "HOSTED_LLM_SAVE_FAILED" });
  registerButton("hostedLlmTestBtn", testHostedLlmConfig, { actionName: "Test Hosted LLM", errorCode: "HOSTED_LLM_TEST_FAILED" });
  registerButton("attentionRefreshBtn", () => loadAttentionConsole({ throwOnError: true }), { actionName: "Refresh Attention Console", errorCode: "ATTENTION_LOAD_FAILED" });
  registerButton("attentionRunTaxMonitorBtn", runAttentionTaxMonitor, { actionName: "Run Tax Monitor", errorCode: "TAX_MONITOR_RUN_FAILED" });
  registerButton("intelRefreshBtn", loadIntelSummary, { actionName: "Refresh Intelligence", errorCode: "INTEL_REFRESH_FAILED" });
  registerButton("intelAnalyzeBtn", analyzeIntelDocument, { actionName: "Analyze Intelligence Document", errorCode: "INTEL_DOC_ANALYZE_FAILED" });
  registerButton("intelFinancialAddBtn", addIntelFinancialRow, { actionName: "Add Financial QoQ", errorCode: "INTEL_FINANCIAL_ADD_FAILED" });
  registerButton("addSplitBtn", addSplit, { actionName: "Add Split", errorCode: "SPLIT_ADD_FAILED" });
  registerButton("splitChartResetBtn", resetSplitChartView, { actionName: "Reset Split Chart", errorCode: "SPLIT_CHART_RESET_FAILED" });
  registerButton("saveLiveCfgBtn", saveLiveConfig, { actionName: "Apply Refresh Config", errorCode: "LIVE_CONFIG_SAVE_FAILED" });
  registerButton("addScripBtn", addScrip, { actionName: "Add Scrip", errorCode: "SCRIP_ADD_FAILED" });
  registerButton("deleteScripBtn", deleteScrip, { actionName: "Delete Scrip", errorCode: "SCRIP_DELETE_FAILED" });
  registerButton("bulkDeleteScripBtn", bulkDeleteScrips, { actionName: "Bulk Delete Scrips", errorCode: "SCRIP_BULK_DELETE_FAILED" });
  registerButton("sellSimBtn", simulateSellForSelected, { actionName: "Simulate Sell", errorCode: "SELL_SIM_FAILED" });
  registerButton("rebalanceSuggestBtn", () => loadRebalanceSuggestions({ throwOnError: true }), { actionName: "Suggest Quantity", errorCode: "REBALANCE_SUGGEST_FAILED" });
  registerButton("rebalanceLockLotBtn", lockRebalanceLot, { actionName: "Lock Rebalance Lot", errorCode: "REBALANCE_LOCK_FAILED" });
  registerButton("rebalanceResetLotBtn", resetRebalanceLot, { actionName: "Reset Rebalance Lot", errorCode: "REBALANCE_RESET_FAILED" });
  registerButton("rebalanceSaveGuardsBtn", saveRebalanceGuards, { actionName: "Save Min/Max Limits", errorCode: "REBALANCE_GUARD_SAVE_FAILED" });
  registerButton("rebalanceHistoryRefreshBtn", () => loadRebalanceClosedHistory({ throwOnError: true }), { actionName: "Refresh Closed History", errorCode: "REBALANCE_CLOSED_HISTORY_FAILED" });
  registerButton("dailyTargetRefreshBtn", () => loadDailyTargetPlan({ throwOnError: true, recalibrate: true }), { actionName: "Refresh Daily Target Ideas", errorCode: "DAILY_TARGET_PLAN_FAILED" });
  registerButton("dailyTargetResetBtn", resetDailyTargetPlan, { actionName: "Start New Day â€” Fresh Ideas", errorCode: "DAILY_TARGET_RESET_FAILED" });
  registerButton("harvestRefreshBtn", () => loadHarvestPlan({ throwOnError: true }), { actionName: "Refresh Harvest Plan", errorCode: "HARVEST_PLAN_FAILED" });
  registerButton("harvestRunAnalysisBtn", () => loadHarvestPlan({ throwOnError: true, runAnalysis: true }), { actionName: "Run Harvest Dynamic Analysis", errorCode: "HARVEST_ANALYSIS_FAILED" });
  registerButton("lossLotsRefreshBtn", () => loadLossLots({ throwOnError: true }), { actionName: "Refresh Loss Lots", errorCode: "LOSS_LOTS_FAILED" });
  registerButton("assistantChatSendBtn", sendAssistantChat, { actionName: "Send Assistant Message", errorCode: "ASSISTANT_SEND_FAILED" });
  registerButton("refreshApprovalsBtn", loadAssistantApprovals, { actionName: "Refresh Approvals", errorCode: "APPROVALS_REFRESH_FAILED" });
  registerButton("refreshApprovalTabBtn", loadApprovalsTab, { actionName: "Refresh Approvals Tab", errorCode: "APPROVALS_TAB_REFRESH_FAILED" });
  registerButton("refreshApprovalVerifyBtn", loadApprovalVerification, { actionName: "Refresh Approval Verification", errorCode: "APPROVAL_VERIFICATION_REFRESH_FAILED" });
  registerButton(
    "refreshAgentsBtn",
    async () => {
      await loadAgentStatus();
      await loadSoftwarePerfLogs();
    },
    { actionName: "Refresh Agent Status", errorCode: "AGENTS_REFRESH_FAILED" }
  );
  registerButton(
    "refreshSoftwarePerfBtn",
    refreshSoftwarePerfNow,
    { actionName: "Refresh Performance Logs", errorCode: "PERF_LOG_REFRESH_FAILED" }
  );
  registerButton("openAgentBacktestBtn", openAgentBacktestModal, { actionName: "Open Agent Backtest", errorCode: "BACKTEST_MODAL_OPEN_FAILED" });
  registerButton("runAgentBacktestBtn", runAgentBacktest, { actionName: "Run Agent Backtest", errorCode: "BACKTEST_RUN_FAILED" });
  registerButton("agentBacktestCloseBtn", closeAgentBacktestModal, { actionName: "Close Agent Backtest", errorCode: "BACKTEST_MODAL_CLOSE_FAILED" });
  $("assistantChatInput").addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      sendAssistantChat();
    }
  });
  $("strategySelect").addEventListener("change", (e) => {
    const selected = state.strategySets.find((s) => s.id === Number(e.target.value));
    if (selected) renderParams(selected.parameters);
  });

  $("rebalanceSide")?.addEventListener("change", () => loadRebalanceSuggestions().catch(() => {}));
  $("rebalancePercent")?.addEventListener("change", () => loadRebalanceSuggestions().catch(() => {}));
  $("rebalanceHistoryShowCompleted")?.addEventListener("change", () => loadRebalanceClosedHistory().catch(() => {}));
  $("dailyTargetSeedCapital")?.addEventListener("change", () => loadDailyTargetPlan().catch(() => {}));
  $("dailyTargetProfitPct")?.addEventListener("change", () => loadDailyTargetPlan().catch(() => {}));
  $("dailyTargetTopN")?.addEventListener("change", () => loadDailyTargetPlan().catch(() => {}));
  ["dailyTargetHistoryFrom", "dailyTargetHistoryTo", "dailyTargetHistoryState"].forEach((id) => {
    $(id)?.addEventListener("input", () => loadDailyTargetHistory().catch(() => {}));
    $(id)?.addEventListener("change", () => loadDailyTargetHistory().catch(() => {}));
  });
  $("harvestTargetLoss")?.addEventListener("change", () => loadHarvestPlan().catch(() => {}));

  ["holdingsFilterSymbol", "holdingsFilterSignal", "holdingsFilterMinRet", "holdingsFilterMaxRet"].forEach((id) => {
    $(id).addEventListener("input", () => renderHoldings(getFilteredSortedHoldings()));
    $(id).addEventListener("change", () => renderHoldings(getFilteredSortedHoldings()));
  });
  ["tradesFilterSide", "tradesFilterFrom", "tradesFilterTo", "tradesFilterText"].forEach((id) => {
    $(id).addEventListener("input", () => renderTrades(applyTradesFilters(state.tradesRaw)));
    $(id).addEventListener("change", () => renderTrades(applyTradesFilters(state.tradesRaw)));
  });
  ["peakFilterSymbol", "peakFilterMinPct", "peakFilterMaxPct"].forEach((id) => {
    $(id).addEventListener("input", () => renderPeakDiff(applyPeakFilters(state.peakRaw)));
    $(id).addEventListener("change", () => renderPeakDiff(applyPeakFilters(state.peakRaw)));
  });
  $("splitsFilterSymbol").addEventListener("input", () => renderSplits(applySplitsFilters(state.splitsRaw)));
  ["cashflowFilterType", "cashflowFilterFrom", "cashflowFilterTo", "cashflowFilterText"].forEach((id) => {
    $(id).addEventListener("input", loadCashflows);
    $(id).addEventListener("change", loadCashflows);
  });
  ["dividendFilterSymbol", "dividendFilterFrom", "dividendFilterTo", "dividendFilterText"].forEach((id) => {
    $(id).addEventListener("input", loadDividends);
    $(id).addEventListener("change", loadDividends);
  });

  registerButton("tsRange1M", () => setTimeseriesRange("1m"), { actionName: "Set Range 1M", errorCode: "TS_RANGE_SET_FAILED" });
  registerButton("tsRange3M", () => setTimeseriesRange("3m"), { actionName: "Set Range 3M", errorCode: "TS_RANGE_SET_FAILED" });
  registerButton("tsRange6M", () => setTimeseriesRange("6m"), { actionName: "Set Range 6M", errorCode: "TS_RANGE_SET_FAILED" });
  registerButton("tsRange1Y", () => setTimeseriesRange("1y"), { actionName: "Set Range 1Y", errorCode: "TS_RANGE_SET_FAILED" });
  registerButton("tsRangeAll", () => setTimeseriesRange("all"), { actionName: "Set Range All", errorCode: "TS_RANGE_SET_FAILED" });
  registerButton("tsResetView", resetTimeseriesView, { actionName: "Reset Timeseries View", errorCode: "TS_VIEW_RESET_FAILED" });
  registerButton("skipOverrideCloseBtn", () => closeSkipOverrideModal(true), { actionName: "Close Override Modal", errorCode: "OVERRIDE_MODAL_CLOSE_FAILED" });
  $("skipOverrideModal").addEventListener("click", (e) => {
    if (e.target && e.target.id === "skipOverrideModal") {
      closeSkipOverrideModal(true);
    }
  });
  registerButton("peakSplitReviewCloseBtn", closePeakSplitReviewModal, { actionName: "Close Split Review", errorCode: "SPLIT_REVIEW_CLOSE_FAILED" });
  $("peakSplitReviewModal").addEventListener("click", (e) => {
    if (e.target && e.target.id === "peakSplitReviewModal") {
      closePeakSplitReviewModal();
    }
  });
  $("agentBacktestModal").addEventListener("click", (e) => {
    if (e.target && e.target.id === "agentBacktestModal") {
      closeAgentBacktestModal();
    }
  });

  document.querySelectorAll("#holdingsTable thead th[data-sort-key], #holdingsZeroTable thead th[data-sort-key]").forEach((th) => {
    th.addEventListener("click", () => toggleHoldingsSort(th.getAttribute("data-sort-key")));
  });
  updateHoldingsSortHeaders();
  setRebalancePlannerControlState();
}

async function init() {
  bindEvents();
  verifyButtonBindings();
  setTab("dashboard");
  const today = new Date().toISOString().slice(0, 10);
  const from = new Date(Date.now() - 365 * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  if ($("intelDocDate")) $("intelDocDate").value = today;
  if ($("intelFinDate")) $("intelFinDate").value = today;
  if ($("btToDate")) $("btToDate").value = today;
  if ($("btFromDate")) $("btFromDate").value = from;
  appendAssistantMsg(
    "bot",
    'Assistant ready.\nTry:\nhelp\nhow is cash balance calculated\ncashflow summary\ndividend summary\nportfolio summary\nstrategy summary\nstrategy projection\nintel summary\nchart summary\nchart signal for KITEX\npolicy impact for KITEX\nfund flow links\nrefresh gold price\nsoftware performance status\nrun software performance agent\nintel autopilot status\nrun intel autopilot\nrun chart agent\nrefresh strategy\nupload summary\ncashflow duplicates\nshow duplicates\nshow pending approvals\npreview notes like "upload:tradebook-OWY330.xlsx"\nerase trades notes like "upload:tradebook-OWY330.xlsx"'
  );
  await loadTenants();
  await loadLiveConfig();
  await loadStrategy();
  await loadIntelSummary();
  await loadSplits();
  await loadCashflows();
  await loadDividends();
  await loadAgentStatus();
  await loadAssistantApprovals();
  await loadApprovalsTab();
  await loadApprovalVerification();
  await loadDashboard();
  await loadPeakDiff();
}

init().catch((e) => {
  console.error(e);
  alert(`Initialization failed: ${e.message}`);
});
