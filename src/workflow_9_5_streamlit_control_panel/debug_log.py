"""
Workflow 9.5 — Debug Logging Module

Centralized terminal logging for the Streamlit Control Panel.
All output goes to sys.stderr (unbuffered) so it appears in the terminal
immediately even when Streamlit captures stdout.

Usage:
    from src.workflow_9_5_streamlit_control_panel.debug_log import log, DEBUG

    log.app("Server starting up")
    log.action("Run Campaign clicked", city="Toronto", mode="dry_run")
    log.error("Pipeline failed", exc=some_exception)

Toggle:
    Set DEBUG = True  for verbose tracing (every loader, every state read).
    Set DEBUG = False for essential logs only (actions, errors, warnings).
"""
from __future__ import annotations

import sys
import traceback
from datetime import datetime

# ── Master switch ────────────────────────────────────────────────────────────
DEBUG = True


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def _emit(prefix: str, msg: str, **kw: object) -> None:
    """Write a single log line to stderr, flushed immediately."""
    parts = [f"[{prefix}] {_ts()} {msg}"]
    for k, v in kw.items():
        parts.append(f"  {k}={v}")
    line = " | ".join(parts)
    print(line, file=sys.stderr, flush=True)


class _Logger:
    """Structured logger with category methods."""

    # ── Always-on (essential) ────────────────────────────────────────────────

    def app(self, msg: str, **kw: object) -> None:
        _emit("APP", msg, **kw)

    def action(self, msg: str, **kw: object) -> None:
        _emit("ACTION", msg, **kw)

    def pipeline(self, msg: str, **kw: object) -> None:
        _emit("PIPELINE", msg, **kw)

    def error(self, msg: str, exc: BaseException | None = None, **kw: object) -> None:
        _emit("ERROR", msg, **kw)
        if exc is not None:
            tb = traceback.format_exception(type(exc), exc, exc.__traceback__)
            for line in tb:
                print(f"[ERROR] {line}", end="", file=sys.stderr, flush=True)

    def warn(self, msg: str, **kw: object) -> None:
        _emit("WARN", msg, **kw)

    def success(self, msg: str, **kw: object) -> None:
        _emit("SUCCESS", msg, **kw)

    # ── Debug-only (verbose) ─────────────────────────────────────────────────

    def ui(self, msg: str, **kw: object) -> None:
        if DEBUG:
            _emit("UI", msg, **kw)

    def state(self, msg: str, **kw: object) -> None:
        if DEBUG:
            _emit("STATE", msg, **kw)

    def data(self, msg: str, **kw: object) -> None:
        """Verbose data-loader tracing."""
        if DEBUG:
            _emit("DATA", msg, **kw)


log = _Logger()
