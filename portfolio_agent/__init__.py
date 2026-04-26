"""
portfolio_agent
~~~~~~~~~~~~~~~
Domain-logic modules extracted from app.py.

Importing this package does NOT import app.py — each sub-module uses a lazy
``_core()`` back-reference when it needs app-level state at call time.

Public surface
--------------
* utils          — pure helpers (parse_float, clamp, now_iso, …)
* quote_manager  — quote-source registry, scoring, ranking
* risk_analysis  — portfolio risk metrics (VaR, drawdown, HHI, …)
* software_performance — runtime health monitoring and auto-tuning
"""

from portfolio_agent import utils, quote_manager, risk_analysis, software_performance

__all__ = ["utils", "quote_manager", "risk_analysis", "software_performance"]
