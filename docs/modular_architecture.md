# Modular Architecture Plan

## Goals

- Replace single-file backend with domain modules.
- Keep API behavior stable during migration.
- Improve maintainability, testability, and horizontal feature scaling.

## Target Layout

```text
portfolio_agent/
  __init__.py
  app_shell.py                # thin assembly + dependency wiring
  config/
    settings.py               # constants, env, feature flags
  storage/
    sqlite.py                 # connections, migrations, repositories
  market/
    quotes.py                 # quote adapters + selection policy
    history.py                # market history sync/backfill
  strategy/
    engine.py                 # recommendation + projection logic
    backtest.py               # self-learning backtest/tuning
  intelligence/
    autopilot.py              # doc ingestion + analysis
    chart_agent.py            # chart signal analysis
  agents/
    software_performance.py   # runtime diagnostics/self-heal/improvement drafts
    scheduler.py              # worker loop orchestration
  api/
    handler.py                # HTTP request routing
    serializers.py            # payload format helpers
```

## Migration Strategy

1. Extract one domain at a time behind stable function names.
2. Keep `app.py` as compatibility façade during rollout.
3. Add regression tests before each extraction.
4. Move to module-local tests once domain is isolated.
5. Remove duplicated monolith code only after all call sites are switched.

## Current Status

- Extracted: software-performance agent into `portfolio_agent/software_performance.py`.
- Compatibility preserved via function aliasing in `app.py`.
- Next high-value candidates:
  - quote-source ranking + refresh flow
  - strategy engine + projections
  - HTTP handler splitting by route groups

