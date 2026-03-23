"""
Workflow 9.5 — Streamlit Campaign Control Panel: Runner Bridge

Connects the Streamlit UI to Workflow 9 — Campaign Runner.
Handles exceptions and returns structured results for display.
"""
from __future__ import annotations

import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.workflow_9_5_streamlit_control_panel.debug_log import log
from src.workflow_9_campaign_runner.campaign_runner import run_campaign, resume_campaign
from src.workflow_9_5_streamlit_control_panel.ui_config import build_campaign_config


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

class RunResult:
    """Holds the outcome of a campaign run attempt."""

    def __init__(
        self,
        success: bool,
        campaign_id: str = "",
        status: str = "",
        completed_steps: list[str] | None = None,
        last_completed_step: str | None = None,
        error: str = "",
    ):
        self.success            = success
        self.campaign_id        = campaign_id
        self.status             = status
        self.completed_steps    = completed_steps or []
        self.last_completed_step = last_completed_step
        self.error              = error

    def __repr__(self) -> str:
        return (
            f"RunResult(success={self.success}, campaign_id={self.campaign_id!r}, "
            f"status={self.status!r}, last={self.last_completed_step!r})"
        )


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def run_campaign_from_ui(form_values: dict) -> RunResult:
    """
    Build a CampaignConfig from UI form values and run Workflow 9.

    Returns a RunResult suitable for display in the Streamlit UI.
    """
    log.pipeline("run_campaign_from_ui() — building config")
    config, errors = build_campaign_config(form_values)
    if errors:
        log.warn("Config validation failed", errors=errors)
        return RunResult(
            success=False,
            error="Configuration errors:\n" + "\n".join(f"• {e}" for e in errors),
        )

    log.pipeline("Config built successfully — starting run_campaign()",
                 city=config.city, country=config.country,
                 send_mode=config.send_mode, run_until=config.run_until)

    try:
        result = run_campaign(config)
        log.pipeline("run_campaign() returned",
                     status=result.get("status"),
                     campaign_id=result.get("campaign_id"),
                     completed_steps=result.get("completed_steps"),
                     error=str(result.get("error", ""))[:200])
        return RunResult(
            success             = result.get("status") == "completed",
            campaign_id         = result.get("campaign_id", ""),
            status              = result.get("status", ""),
            completed_steps     = result.get("completed_steps", []),
            last_completed_step = result.get("last_completed_step"),
            error               = result.get("error") or "",
        )
    except Exception as exc:
        log.error("run_campaign() raised exception", exc=exc)
        return RunResult(
            success=False,
            error=traceback.format_exc(),
        )


def resume_campaign_from_ui() -> RunResult:
    """
    Resume the last interrupted campaign via Workflow 9.

    Returns a RunResult suitable for display in the Streamlit UI.
    """
    log.pipeline("resume_campaign_from_ui() — calling resume_campaign()")
    try:
        result = resume_campaign()
        log.pipeline("resume_campaign() returned",
                     status=result.get("status"),
                     campaign_id=result.get("campaign_id"),
                     completed_steps=result.get("completed_steps"),
                     error=str(result.get("error", ""))[:200])
        return RunResult(
            success             = result.get("status") == "completed",
            campaign_id         = result.get("campaign_id", ""),
            status              = result.get("status", ""),
            completed_steps     = result.get("completed_steps", []),
            last_completed_step = result.get("last_completed_step"),
            error               = result.get("error") or "",
        )
    except RuntimeError as exc:
        log.error("resume_campaign() RuntimeError", exc=exc)
        return RunResult(success=False, error=str(exc))
    except Exception as exc:
        log.error("resume_campaign() unexpected exception", exc=exc)
        return RunResult(success=False, error=traceback.format_exc())
