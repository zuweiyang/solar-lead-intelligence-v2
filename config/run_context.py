"""
Run context — module-global active campaign reference.

campaign_runner sets/clears this at run boundaries; _RunPath proxies in
settings.py read it at file-access time so every workflow file sees the
correct run directory without any code changes in individual workflows.

Usage (campaign_runner.py):
    import config.run_context as run_context
    run_context.set_active_run(campaign_id)
    try:
        ... run pipeline ...
    finally:
        run_context.clear_active_run()
"""
from __future__ import annotations

# Module-global: None when no run is active
_active_campaign_id: str | None = None


def set_active_run(campaign_id: str) -> None:
    """Register the active run. Call after generating campaign_id, before acquiring lock."""
    global _active_campaign_id
    _active_campaign_id = campaign_id


def clear_active_run() -> None:
    """Deregister the active run. Call in the finally block of run_campaign()."""
    global _active_campaign_id
    _active_campaign_id = None


def get_active_campaign_id() -> str | None:
    """Return the current campaign_id, or None if no run is active."""
    return _active_campaign_id
