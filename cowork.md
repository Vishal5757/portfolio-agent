# Cowork Chat

Shared coordination log for Codex and Claude. Keep messages short and append-only so both agents can sync without re-reading the full repo.

## Protocol

- Use newest entries first when catching up.
- Do not paste long diffs. Reference files, functions, commits, and test names.
- Claim work before editing to avoid overlap.
- Prefer disjoint file ownership when splitting work.
- Every handoff should include `State`, `Files`, `Tests`, and `Next`.
- If you see unexpected git changes, stop and ask the user before editing those files.
- Do not include API keys, broker data secrets, or private tokens.

## Message Format

```text
### YYYY-MM-DD HH:mm IST | Agent | Type
State: one-line status.
Files: path1, path2
Tests: command/result
Next: concrete next step or handoff request.
Blockers: none or short blocker.
```

## Task Board

| Status | Owner | Task | Files | Notes |
|---|---|---|---|---|
| open | unassigned | Pick next task from user request | - | Claim before editing |

## Chat

### 2026-04-26 23:26 IST | Codex | setup
State: Created shared cowork channel and protocol for Codex/Claude coordination.
Files: `cowork.md`, `tools/cowork.ps1`, `README.md`
Tests: pending.
Next: Claude can append a claim/handoff using `tools/cowork.ps1 add -Agent Claude -Type claim -Message "..."`
Blockers: none.

### 2026-04-26 23:27 IST | Codex | verify
State: Verified cowork helper can append and tail entries.
Files: cowork.md, tools/cowork.ps1, README.md
Tests: tools/cowork.ps1 tail passed
Next: Claude should claim files before editing.
Blockers: none
