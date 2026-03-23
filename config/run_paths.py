"""
Run-scoped file paths — explicit, concrete, no lazy proxy.

RunPaths is constructed ONCE per campaign run by campaign_runner.run_campaign()
and holds concrete pathlib.Path objects for all files that belong to a specific
campaign run directory.

Unlike _RunPath, these paths are FIXED at construction time and never re-resolve
based on module-global state.  Using RunPaths eliminates two failure modes:
  1. Silent fallback to DATA_DIR when campaign context is not set
  2. exists() / open() inconsistency when run context changes between calls

Design
------
- RunPaths is a frozen dataclass → immutable after construction.
- A module-level _active_run_paths mirrors the existing _active_campaign_id
  pattern in run_context.py, but ONLY for use by campaign orchestration.
  UI code (ui_state.py) does NOT touch this global — it continues to use
  _active_campaign_id from run_context.py for display context only.
- Pipeline workflows call require_active_run_paths() which raises immediately
  if no run is active instead of silently writing to DATA_DIR.

Usage (pipeline steps, via campaign_steps.py)
---------------------------------------------
    from config.run_paths import require_active_run_paths
    paths = require_active_run_paths()
    with open(paths.qualified_leads_file) as f:
        ...

Usage (campaign_runner.run_campaign)
-------------------------------------
    run_paths = RunPaths.for_campaign(campaign_id)
    set_active_run_paths(run_paths)
    try:
        ... run pipeline ...
    finally:
        clear_active_run_paths()

First-batch coverage (score → enrich → verify → signals)
---------------------------------------------------------
These workflows now accept an explicit `paths: RunPaths` parameter and use it
for ALL file I/O — no _RunPath constants, no DATA_DIR fallback possible.

Other workflows (crawl, analyse, email generation, send, etc.) continue to use
_RunPath constants for backward compatibility.  They will be migrated in a
second batch.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


def _runs_dir() -> Path:
    """Lazy import to avoid circular dependency at module load time."""
    from config.settings import RUNS_DIR  # noqa: PLC0415
    return RUNS_DIR


# ---------------------------------------------------------------------------
# RunPaths dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RunPaths:
    """Concrete, campaign-scoped file paths.  All fields are plain Path objects."""

    campaign_id: str
    run_dir: Path

    # Workflow 4.5 — buyer filter (INPUT for scorer)
    company_analysis_file: Path
    buyer_filter_file: Path

    # Workflow 5 — scoring
    qualified_leads_file: Path
    disqualified_leads_file: Path

    # Workflow 5.5 — enrichment
    enriched_leads_file: Path
    enriched_contacts_file: Path

    # Workflow 5.6 — contact scoring (P1-2B)
    scored_contacts_file: Path

    # Workflow 5.9 — verification
    verified_enriched_leads_file: Path

    # Workflow 5.8 — signals
    research_signal_raw_file: Path
    research_signals_file: Path

    # Workflow 6 — queue policy (P1-3A)
    queue_policy_file: Path

    # Workflow 6 — policy summary / visibility (P1-3C)
    policy_summary_file: Path

    @classmethod
    def for_campaign(cls, campaign_id: str) -> "RunPaths":
        """Construct RunPaths for the given campaign_id.  Creates run_dir if needed."""
        if not campaign_id:
            raise ValueError(
                "RunPaths.for_campaign() requires a non-empty campaign_id"
            )
        run_dir = _runs_dir() / campaign_id
        run_dir.mkdir(parents=True, exist_ok=True)
        return cls(
            campaign_id=campaign_id,
            run_dir=run_dir,
            company_analysis_file=run_dir / "company_analysis.json",
            buyer_filter_file=run_dir / "buyer_filter.json",
            qualified_leads_file=run_dir / "qualified_leads.csv",
            disqualified_leads_file=run_dir / "disqualified_leads.csv",
            enriched_leads_file=run_dir / "enriched_leads.csv",
            enriched_contacts_file=run_dir / "enriched_contacts.csv",
            scored_contacts_file=run_dir / "scored_contacts.csv",
            verified_enriched_leads_file=run_dir / "verified_enriched_leads.csv",
            research_signal_raw_file=run_dir / "research_signal_raw.json",
            research_signals_file=run_dir / "research_signals.json",
            queue_policy_file=run_dir / "queue_policy.csv",
            policy_summary_file=run_dir / "policy_summary.json",
        )

    def log_summary(self, step: str = "") -> None:
        """Print a one-line path summary useful for debugging."""
        label = f"[{step}] " if step else ""
        print(
            f"{label}RunPaths: campaign={self.campaign_id!r}  "
            f"run_dir={self.run_dir}"
        )


# ---------------------------------------------------------------------------
# Module-level active RunPaths
# Set ONLY by campaign_runner; read by campaign_steps and first-batch workflows.
# UI code must NOT call set_active_run_paths / clear_active_run_paths.
# ---------------------------------------------------------------------------

_active_run_paths: RunPaths | None = None


def set_active_run_paths(run_paths: RunPaths) -> None:
    """Register the active RunPaths.  Call after campaign_id is known."""
    global _active_run_paths
    _active_run_paths = run_paths


def clear_active_run_paths() -> None:
    """Deregister the active RunPaths.  Call in the finally block of run_campaign()."""
    global _active_run_paths
    _active_run_paths = None


def get_active_run_paths() -> RunPaths | None:
    """Return the active RunPaths, or None if no run is active."""
    return _active_run_paths


def require_active_run_paths() -> RunPaths:
    """
    Return the active RunPaths, or raise RuntimeError if no run is active.

    This is the fail-fast guard for pipeline workflows.  They must never
    silently fall back to DATA_DIR; instead they fail immediately with a
    clear message pointing to the root cause.
    """
    rp = _active_run_paths
    if rp is None:
        raise RuntimeError(
            "No active RunPaths — pipeline workflow called outside a campaign run context.\n"
            "Ensure campaign_runner.run_campaign() has been called and RunPaths are active.\n"
            "Do NOT call pipeline workflow functions directly without a campaign context."
        )
    return rp
