"""
Workflow 9 Queue Scheduler - Queue Runner

Single-worker scheduler loop that:
  1. Picks the next pending job (highest priority, then oldest)
  2. Marks it running
  3. Calls run_campaign() from Workflow 9
  4. Marks it completed or failed
  5. Moves to the next job automatically

This runner is controlled by the Streamlit control panel. If the control
panel disappears, the runner pauses the queue and exits so work never
continues unattended in the background.
"""
from __future__ import annotations

import sys
import time
import traceback
from pathlib import Path

# Force UTF-8 for all I/O so Arabic / CJK characters in company names,
# addresses, and web content don't crash on Windows systems whose default
# codec is GBK or another non-Unicode encoding.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Allow running as __main__ from any working directory
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config.settings import CAMPAIGN_LOCK_FILE
from src.workflow_9_campaign_runner.campaign_config import CampaignConfig
from src.workflow_9_campaign_runner.campaign_runner import (
    ControlPanelDisconnected,
    is_campaign_running,
    run_campaign,
)
from src.workflow_9_campaign_runner.campaign_state import load_campaign_state
from src.workflow_9_queue_scheduler.control_panel_heartbeat import (
    get_control_panel_heartbeat_age_seconds,
    is_control_panel_heartbeat_stale,
)
from src.workflow_9_queue_scheduler.queue_store import (
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_RUNNING,
    _now,
    get_next_pending,
    is_queue_paused,
    list_jobs,
    pause_queue,
    update_job,
)


def _job_to_config(job: dict) -> CampaignConfig:
    """Build a CampaignConfig from a queue job dict."""
    resume = bool(str(job.get("campaign_id") or "").strip())
    return CampaignConfig(
        country=job.get("country", ""),
        region=job.get("region", ""),
        city=job.get("location", ""),
        base_city=job.get("location", ""),
        metro_mode=job.get("metro_mode", "base_only"),
        metro_sub_cities=list(job.get("metro_sub_cities") or []),
        keyword_mode=job.get("keyword_mode", "default"),
        keywords=list(job.get("keywords") or []),
        company_limit=int(job.get("company_limit") or 0),
        crawl_limit=int(job.get("crawl_limit") or 0),
        enrich_limit=int(job.get("enrich_limit") or 0),
        send_mode=job.get("send_mode", "dry_run"),
        auto_cloud_deploy=job.get("auto_cloud_deploy"),
        run_until=job.get("run_until", "campaign_status"),
        resume=resume,
        dry_run=job.get("send_mode", "dry_run") == "dry_run",
    )


def _recover_stale_running_job() -> None:
    """
    If a job is stuck in 'running' but no campaign lock file exists, the
    previous scheduler process crashed without cleanup. Reset the job to
    failed so the queue can continue.
    """
    running_jobs = list_jobs(status=STATUS_RUNNING)
    if not running_jobs:
        return
    if is_campaign_running():
        return
    for job in running_jobs:
        print(
            f"[Queue] WARNING: job {job['job_id']} ({job['location']}, {job['country']}) "
            "was stuck in 'running' with no campaign lock - marking failed (stale)"
        )
        update_job(
            job["job_id"],
            status=STATUS_FAILED,
            completed_at=_now(),
            error="Scheduler crashed without cleanup - job reset to failed",
        )


def _pause_runner_for_missing_control_panel(job: dict) -> None:
    """Move the current queue job back to pending and pause the queue."""
    state = load_campaign_state() or {}
    campaign_id = str(state.get("campaign_id") or job.get("campaign_id") or "").strip()
    age = get_control_panel_heartbeat_age_seconds()
    age_text = f"{int(age)}s" if age is not None else "unknown"
    message = (
        "Paused automatically because the Streamlit control panel closed or "
        f"stopped heartbeating (last heartbeat age: {age_text})."
    )
    update_job(
        job["job_id"],
        status=STATUS_PENDING,
        campaign_id=campaign_id,
        started_at="",
        completed_at="",
        error=message,
    )
    pause_queue()
    print(
        f"[Queue] Paused job {job['job_id']} ({job['location']}, {job['country']}) "
        "because the control panel heartbeat disappeared."
    )


def run_scheduler(poll_interval: float = 5.0) -> None:
    """
    Run the queue scheduler loop indefinitely.

    poll_interval - seconds to sleep between queue checks when idle or paused.
    Exits cleanly on KeyboardInterrupt.
    """
    print("[Queue] Scheduler started. Press Ctrl+C to stop.")
    print(f"[Queue] Poll interval: {poll_interval}s")

    try:
        while True:
            _recover_stale_running_job()

            if is_queue_paused():
                print("[Queue] Queue is paused - waiting...")
                time.sleep(poll_interval)
                continue

            if is_control_panel_heartbeat_stale():
                age = get_control_panel_heartbeat_age_seconds()
                age_text = f"{int(age)}s" if age is not None else "unknown"
                running_jobs = list_jobs(status=STATUS_RUNNING)
                if running_jobs:
                    pause_queue()
                    print(
                        "[Queue] Control panel heartbeat is stale while a job is marked "
                        f"running (age {age_text}). Waiting for the active run to pause."
                    )
                    time.sleep(poll_interval)
                    continue
                if get_next_pending() is not None:
                    pause_queue()
                    print(
                        "[Queue] Control panel heartbeat is stale "
                        f"(age {age_text}) - pausing queue and stopping runner."
                    )
                    break

            running_jobs = list_jobs(status=STATUS_RUNNING)
            if running_jobs:
                running = running_jobs[0]
                print(
                    f"[Queue] Job {running['job_id']} "
                    f"({running['location']}, {running['country']}) is running - waiting..."
                )
                time.sleep(poll_interval)
                continue

            job = get_next_pending()
            if job is None:
                print("[Queue] No pending jobs - idle.")
                time.sleep(poll_interval)
                continue

            job_id = job["job_id"]
            location = job["location"]
            country = job["country"]
            print(f"\n[Queue] {'=' * 50}")
            print(f"[Queue] Starting job {job_id}: {location}, {country}")
            print(
                f"[Queue]   priority={job['priority']} send_mode={job['send_mode']} "
                f"run_until={job['run_until']}"
            )
            print(f"[Queue] {'=' * 50}")

            update_job(job_id, status=STATUS_RUNNING, started_at=_now())

            try:
                config = _job_to_config(job)
                result = run_campaign(config)

                campaign_id = result.get("campaign_id", "")
                final_status = result.get("status", "")

                if final_status == "completed":
                    update_job(
                        job_id,
                        status=STATUS_COMPLETED,
                        campaign_id=campaign_id,
                        completed_at=_now(),
                        error="",
                    )
                    print(f"[Queue] Job {job_id} ({location}) COMPLETED (campaign {campaign_id})")
                else:
                    error_msg = result.get("error") or f"campaign status: {final_status}"
                    update_job(
                        job_id,
                        status=STATUS_FAILED,
                        campaign_id=campaign_id,
                        completed_at=_now(),
                        error=error_msg,
                    )
                    print(f"[Queue] Job {job_id} ({location}) FAILED: {error_msg}")
                    print("[Queue] Continuing to next job.")

            except ControlPanelDisconnected as exc:
                _pause_runner_for_missing_control_panel(job)
                print(f"[Queue] Job {job_id} ({location}) paused: {exc}")
                print("[Queue] Runner will exit until the operator resumes the queue.")
                break

            except Exception as exc:
                tb = traceback.format_exc()
                msg = str(exc)
                update_job(
                    job_id,
                    status=STATUS_FAILED,
                    completed_at=_now(),
                    error=msg,
                )
                print(f"[Queue] Job {job_id} ({location}) CRASHED: {msg}")
                print(f"[Queue] Traceback:\n{tb}")
                print("[Queue] Continuing to next job.")

            time.sleep(2)

    except KeyboardInterrupt:
        print("\n[Queue] Scheduler stopped by user.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="OmniSol Campaign Queue Scheduler")
    parser.add_argument(
        "--poll",
        type=float,
        default=5.0,
        help="Poll interval in seconds when idle (default: 5)",
    )
    args = parser.parse_args()
    run_scheduler(poll_interval=args.poll)

