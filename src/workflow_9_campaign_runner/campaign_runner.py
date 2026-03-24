"""
Workflow 9 — Campaign Runner: Orchestration Engine

Executes the full pipeline (or a partial run) in the correct order.
Supports:
  - full pipeline runs
  - partial runs (stop after a named step)
  - resume after interruption
  - dry-run mode

Usage:
    from src.workflow_9_campaign_runner.campaign_config import CampaignConfig
    from src.workflow_9_campaign_runner.campaign_runner import run_campaign

    config = CampaignConfig(city="Vancouver", country="Canada")
    run_campaign(config)
"""
from __future__ import annotations

import os
import subprocess
import sys
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import config.run_context as run_context
import config.run_paths as _run_paths
from config.settings import (
    CAMPAIGN_LOCK_FILE,
    CLOUD_AUTO_DEPLOY_ON_COMPLETE,
    CLOUD_SEND_ENABLED,
    FINAL_SEND_QUEUE_FILE,
)
from src.workflow_9_campaign_runner.campaign_config import (
    CampaignConfig,
    PIPELINE_STEPS,
    validate_config,
)
from src.workflow_9_campaign_runner.campaign_state import (
    initialize_campaign_state,
    load_campaign_state,
    save_campaign_state,
    sync_cloud_deploy_status,
    update_campaign_state,
    CLOUD_DEPLOY_FAILED,
    CLOUD_DEPLOY_PENDING,
    STATUS_RUNNING,
    STATUS_COMPLETED,
    STATUS_FAILED,
)
from src.workflow_9_campaign_runner.campaign_logger import (
    append_campaign_log,
    LOG_STARTED,
    LOG_COMPLETED,
    LOG_SKIPPED,
    LOG_FAILED,
)
from src.workflow_9_campaign_runner import campaign_steps as _steps


# ---------------------------------------------------------------------------
# Lock helpers — prevent concurrent campaign runs
# ---------------------------------------------------------------------------

_STALE_LOCK_THRESHOLD = timedelta(hours=2)


def _should_auto_deploy(config: CampaignConfig, completed_steps: list[str]) -> bool:
    if not CLOUD_SEND_ENABLED:
        return False
    auto_enabled = CLOUD_AUTO_DEPLOY_ON_COMPLETE if config.auto_cloud_deploy is None else bool(config.auto_cloud_deploy)
    if not auto_enabled:
        return False
    if config.send_mode == "dry_run" or config.dry_run:
        return False
    if not completed_steps or completed_steps[-1] != "campaign_status":
        return False
    if config.run_until != "campaign_status":
        return False
    return FINAL_SEND_QUEUE_FILE.exists()


def _trigger_cloud_deploy(campaign_id: str) -> None:
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "deploy_run_to_gcloud.py"
    if not script_path.exists():
        print(f"[Workflow 9] Cloud deploy skipped - script not found: {script_path}")
        return

    cmd = [sys.executable, str(script_path), "--campaign", campaign_id]
    popen_kwargs: dict[str, Any] = {
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "cwd": str(Path(__file__).resolve().parents[2]),
        "env": os.environ.copy(),
    }
    if os.name == "nt":
        popen_kwargs["creationflags"] = getattr(subprocess, "DETACHED_PROCESS", 0) | getattr(
            subprocess, "CREATE_NEW_PROCESS_GROUP", 0
        )
    else:
        popen_kwargs["start_new_session"] = True

    subprocess.Popen(cmd, **popen_kwargs)
    print(f"[Workflow 9] Triggered background cloud deploy for {campaign_id}")


def _parse_state_timestamp(raw: str) -> datetime | None:
    """Parse campaign state timestamps stored as UTC-naive strings."""
    try:
        dt = datetime.strptime((raw or "").strip(), "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def _recover_stale_lock_if_needed() -> bool:
    """
    Clear a stale campaign lock left behind by a crashed process.

    Safety rules:
    - If the lock file is missing, do nothing.
    - If campaign state exists and is not "running", the lock is stale.
    - If campaign state says "running" but the last state heartbeat is older
      than the threshold, the lock is considered stale.

    Returns True if a stale lock was removed.
    """
    if not CAMPAIGN_LOCK_FILE.exists():
        return False

    state = load_campaign_state()
    if state and state.get("status") != STATUS_RUNNING:
        CAMPAIGN_LOCK_FILE.unlink(missing_ok=True)
        return True

    last_update = _parse_state_timestamp((state or {}).get("updated_at", ""))
    if last_update is None:
        lock_mtime = datetime.fromtimestamp(CAMPAIGN_LOCK_FILE.stat().st_mtime, tz=timezone.utc)
        last_update = lock_mtime

    if datetime.now(timezone.utc) - last_update <= _STALE_LOCK_THRESHOLD:
        return False

    CAMPAIGN_LOCK_FILE.unlink(missing_ok=True)
    if state and state.get("status") == STATUS_RUNNING:
        update_campaign_state(
            state.get("last_completed_step") or "",
            STATUS_FAILED,
            error_message=(
                "Recovered stale campaign lock after no progress heartbeat was "
                f"seen for {int(_STALE_LOCK_THRESHOLD.total_seconds() // 60)} minutes."
            ),
        )
    return True

def is_campaign_running() -> bool:
    """Return True if a campaign lock file exists (another run is in progress)."""
    _recover_stale_lock_if_needed()
    return CAMPAIGN_LOCK_FILE.exists()


def _acquire_lock(campaign_id: str) -> None:
    CAMPAIGN_LOCK_FILE.write_text(campaign_id, encoding="utf-8")


def _release_lock() -> None:
    try:
        CAMPAIGN_LOCK_FILE.unlink(missing_ok=True)
    except Exception:
        pass



# ---------------------------------------------------------------------------
# Step registry — ordered list of (step_name, callable)
# ---------------------------------------------------------------------------

_STEP_REGISTRY: list[tuple[str, Callable[[CampaignConfig], Any]]] = [
    ("search_tasks",    _steps.run_step_1_search_tasks),
    ("scrape",          _steps.run_step_2_scrape),
    ("crawl",           _steps.run_step_3_crawl),
    ("analyze",         _steps.run_step_4_analyze),
    ("buyer_filter",    _steps.run_step_4_5_buyer_filter),
    ("score",           _steps.run_step_5_score),
    ("enrich",          _steps.run_step_5_5_enrich),
    ("contact_scoring", _steps.run_step_5_6_contact_scoring),
    ("verify",          _steps.run_step_5_9_verify),
    ("signals",         _steps.run_step_5_8_signals),
    ("queue_policy",    _steps.run_step_6_queue_policy),
    ("personalization", _steps.run_step_6_2_personalization),
    ("email_generation",_steps.run_step_6_generate),
    ("email_quality",   _steps.run_step_6_5_quality),
    ("email_repair",    _steps.run_step_6_7_repair),
    ("send",            _steps.run_step_7_send),
    ("tracking",        _steps.run_step_7_5_tracking),
    ("followup",        _steps.run_step_8_followup),
    ("campaign_status", _steps.run_step_8_5_campaign_status),
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_campaign(config: CampaignConfig) -> dict:
    """
    Run (or resume) a campaign with the given config.

    Returns a summary dict:
      {campaign_id, status, completed_steps, last_completed_step, error}
    """
    # Validate
    errors = validate_config(config)
    if errors:
        raise ValueError("Invalid campaign config:\n" + "\n".join(f"  - {e}" for e in errors))

    # Prevent concurrent runs
    if is_campaign_running():
        raise RuntimeError(
            "A campaign is already running. "
            "Wait for it to complete or delete data/campaign_run.lock to force-unlock."
        )

    # Resume or start fresh — determine campaign_id first, then activate run context
    if config.resume:
        state = load_campaign_state()
        if state is None:
            print("[Workflow 9] No existing state found — starting fresh run.")
            config.resume = False
            state = initialize_campaign_state(config)
        else:
            print(
                f"[Workflow 9] Resuming campaign {state['campaign_id']} "
                f"(last completed: {state.get('last_completed_step') or 'none'})"
            )
    else:
        state = initialize_campaign_state(config)

    campaign_id = state["campaign_id"]

    # Activate run-scoped file paths — all _RunPath constants now resolve to
    # data/runs/<campaign_id>/ for the duration of this run.
    run_context.set_active_run(campaign_id)

    # Build explicit RunPaths for the first-batch workflows (score → enrich →
    # verify → signals).  These hold concrete Path objects so those workflows
    # never re-resolve based on global state and never fall back to DATA_DIR.
    from config.run_paths import RunPaths
    run_paths = RunPaths.for_campaign(campaign_id)
    _run_paths.set_active_run_paths(run_paths)
    print(f"[Workflow 9] RunPaths active: campaign={campaign_id!r}  run_dir={run_paths.run_dir}")

    state["status"] = STATUS_RUNNING
    save_campaign_state(state)

    last_done         = state.get("last_completed_step")
    run_until         = config.run_until
    completed_steps: list[str] = []

    print(f"\n{'='*60}")
    print(f"[Workflow 9] Campaign {campaign_id}")
    print(f"  Location  : {config.city or config.region or config.country}")
    print(f"  Run until : {run_until}")
    print(f"  Send mode : {config.send_mode}")
    print(f"  Resume    : {config.resume}")
    print(f"{'='*60}\n")

    # Acquire lock for the duration of the run
    _acquire_lock(campaign_id)
    try:
        # Determine which steps to skip (already done in a previous run)
        skip_until: str | None = last_done
        past_last_done = (skip_until is None)

        for step_name, step_fn in _STEP_REGISTRY:
            # Check if we've passed the last completed step yet
            if not past_last_done:
                if step_name == skip_until:
                    past_last_done = True
                    completed_steps.append(step_name)
                    append_campaign_log(
                        campaign_id, step_name, LOG_SKIPPED,
                        "Already completed in previous run"
                    )
                    print(f"[Workflow 9]   SKIP  {step_name} (already done)")
                else:
                    append_campaign_log(
                        campaign_id, step_name, LOG_SKIPPED,
                        "Already completed in previous run"
                    )
                    print(f"[Workflow 9]   SKIP  {step_name} (already done)")
                # Check if run_until was an already-skipped step
                if step_name == run_until:
                    break
                continue

            # Log step start
            print(f"[Workflow 9]   START {step_name}")
            append_campaign_log(campaign_id, step_name, LOG_STARTED)

            try:
                step_fn(config)
            except Exception as exc:
                msg = str(exc)
                tb  = traceback.format_exc()
                print(f"[Workflow 9]   FAIL  {step_name}: {msg}")
                print(f"[Workflow 9]   TRACEBACK:\n{tb}")
                append_campaign_log(campaign_id, step_name, LOG_FAILED, msg)
                update_campaign_state(step_name, "failed", error_message=msg)
                return {
                    "campaign_id":         campaign_id,
                    "status":              "failed",
                    "completed_steps":     completed_steps,
                    "last_completed_step": completed_steps[-1] if completed_steps else None,
                    "error":               msg,
                }

            # Step succeeded
            completed_steps.append(step_name)
            append_campaign_log(campaign_id, step_name, LOG_COMPLETED, f"Step {step_name} done")
            update_campaign_state(step_name, "completed")
            print(f"[Workflow 9]   DONE  {step_name}")

            # Stop if we've reached the requested end step
            if step_name == run_until:
                break

    finally:
        _release_lock()
        run_context.clear_active_run()
        _run_paths.clear_active_run_paths()

    # Final state — persist completed status and correct last step
    state = load_campaign_state()
    if state:
        if completed_steps:
            state["last_completed_step"] = completed_steps[-1]
        state["status"] = STATUS_COMPLETED
        save_campaign_state(state)

    print(f"\n{'='*60}")
    print(f"[Workflow 9] Campaign {campaign_id} completed.")
    print(f"  Last step  : {completed_steps[-1] if completed_steps else 'none'}")
    print(f"{'='*60}\n")

    if _should_auto_deploy(config, completed_steps):
        sync_cloud_deploy_status(
            campaign_id,
            CLOUD_DEPLOY_PENDING,
            details={"cloud_deploy_trigger": "auto_on_complete"},
        )
        try:
            _trigger_cloud_deploy(campaign_id)
        except Exception as exc:
            sync_cloud_deploy_status(campaign_id, CLOUD_DEPLOY_FAILED, error_message=str(exc))
            print(f"[Workflow 9] Cloud auto-deploy skipped due to error: {exc}")

    return {
        "campaign_id":         campaign_id,
        "status":              STATUS_COMPLETED,
        "completed_steps":     completed_steps,
        "last_completed_step": completed_steps[-1] if completed_steps else None,
        "error":               None,
    }


def resume_campaign() -> dict:
    """
    Convenience wrapper: load the last state and resume from where it left off.
    """
    state = load_campaign_state()
    if state is None:
        raise RuntimeError("No campaign state found. Run a new campaign first.")

    saved_cfg = state.get("config", {})
    config = CampaignConfig(
        country           = saved_cfg.get("country",          "Canada"),
        region            = saved_cfg.get("region",           ""),
        city              = saved_cfg.get("city",             ""),
        base_city         = saved_cfg.get("base_city",        ""),
        metro_mode        = saved_cfg.get("metro_mode",       "base_only"),
        metro_sub_cities  = saved_cfg.get("metro_sub_cities", []),
        search_cities     = saved_cfg.get("search_cities",    []),
        keyword_mode      = saved_cfg.get("keyword_mode",     "default"),
        keywords          = saved_cfg.get("keywords",         []),
        company_limit     = saved_cfg.get("company_limit",    0),
        crawl_limit       = saved_cfg.get("crawl_limit",      0),
        enrich_limit      = saved_cfg.get("enrich_limit",     0),
        send_mode         = saved_cfg.get("send_mode",        "dry_run"),
        auto_cloud_deploy = saved_cfg.get("auto_cloud_deploy"),
        run_until         = saved_cfg.get("run_until",        "campaign_status"),
        resume            = True,
        dry_run           = saved_cfg.get("dry_run",          True),
    )
    return run_campaign(config)
