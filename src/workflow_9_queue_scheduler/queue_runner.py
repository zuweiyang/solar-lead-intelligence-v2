"""
Workflow 9 Queue Scheduler — Queue Runner

Single-worker scheduler loop that:
  1. Picks the next pending job (highest priority, then oldest)
  2. Marks it running
  3. Calls run_campaign() from Workflow 9
  4. Marks it completed or failed
  5. Moves to the next job automatically

Safety rules
------------
- Only one job runs at a time (enforced by status check + CAMPAIGN_LOCK_FILE)
- A failed job is marked failed and skipped; the queue continues
- If the queue-level pause flag is set, no new jobs are started
- If a crash leaves a job stuck in "running" state, re-running the scheduler
  will detect the stale lock (CAMPAIGN_LOCK_FILE absent) and reset it

Usage
-----
    # Standalone (recommended for production):
    py -m src.workflow_9_queue_scheduler.queue_runner

    # Or import and call from your own script:
    from src.workflow_9_queue_scheduler.queue_runner import run_scheduler
    run_scheduler()
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

from src.workflow_9_queue_scheduler.queue_store import (
    get_next_pending,
    list_jobs,
    update_job,
    is_queue_paused,
    STATUS_RUNNING,
    STATUS_COMPLETED,
    STATUS_FAILED,
    _now,
)
from src.workflow_9_campaign_runner.campaign_config import CampaignConfig
from src.workflow_9_campaign_runner.campaign_runner import run_campaign, is_campaign_running
from config.settings import CAMPAIGN_LOCK_FILE


# ---------------------------------------------------------------------------
# Config → CampaignConfig conversion
# ---------------------------------------------------------------------------

def _job_to_config(job: dict) -> CampaignConfig:
    """Build a CampaignConfig from a queue job dict."""
    return CampaignConfig(
        country          = job.get("country", ""),
        region           = job.get("region", ""),
        city             = job.get("location", ""),
        base_city        = job.get("location", ""),
        metro_mode       = job.get("metro_mode", "base_only"),
        metro_sub_cities = list(job.get("metro_sub_cities") or []),
        keyword_mode     = job.get("keyword_mode", "default"),
        keywords         = list(job.get("keywords") or []),
        company_limit    = int(job.get("company_limit") or 0),
        crawl_limit      = int(job.get("crawl_limit") or 0),
        enrich_limit     = int(job.get("enrich_limit") or 0),
        send_mode        = job.get("send_mode", "dry_run"),
        run_until        = job.get("run_until", "campaign_status"),
        resume           = False,
        dry_run          = job.get("send_mode", "dry_run") == "dry_run",
    )


# ---------------------------------------------------------------------------
# Stale lock recovery
# ---------------------------------------------------------------------------

def _recover_stale_running_job() -> None:
    """
    If a job is stuck in 'running' but no campaign lock file exists, the
    previous scheduler process crashed without cleanup.  Reset the job to
    failed so the queue can continue.
    """
    running_jobs = list_jobs(status=STATUS_RUNNING)
    if not running_jobs:
        return
    if is_campaign_running():
        return  # lock file present — a real run is in progress
    for job in running_jobs:
        print(
            f"[Queue] WARNING: job {job['job_id']} ({job['location']}, {job['country']}) "
            f"was stuck in 'running' with no campaign lock — marking failed (stale)"
        )
        update_job(
            job["job_id"],
            status=STATUS_FAILED,
            completed_at=_now(),
            error="Scheduler crashed without cleanup — job reset to failed",
        )


# ---------------------------------------------------------------------------
# Main scheduler loop
# ---------------------------------------------------------------------------

def run_scheduler(poll_interval: float = 5.0) -> None:
    """
    Run the queue scheduler loop indefinitely.

    poll_interval — seconds to sleep between queue checks when idle or paused.
    Exits cleanly on KeyboardInterrupt.
    """
    print("[Queue] Scheduler started. Press Ctrl+C to stop.")
    print(f"[Queue] Poll interval: {poll_interval}s")

    try:
        while True:
            # ---- Stale lock recovery on each iteration (cheap check) --------
            _recover_stale_running_job()

            # ---- Pause check ------------------------------------------------
            if is_queue_paused():
                print("[Queue] Queue is paused — waiting...")
                time.sleep(poll_interval)
                continue

            # ---- Concurrent-run guard ---------------------------------------
            # run_campaign() enforces its own file lock, but we also check
            # the running job count so we never pick a second job while one
            # is already claimed in the queue.
            running_jobs = list_jobs(status=STATUS_RUNNING)
            if running_jobs:
                running = running_jobs[0]
                print(
                    f"[Queue] Job {running['job_id']} "
                    f"({running['location']}, {running['country']}) is running — waiting..."
                )
                time.sleep(poll_interval)
                continue

            # ---- Pick next pending job --------------------------------------
            job = get_next_pending()
            if job is None:
                print("[Queue] No pending jobs — idle.")
                time.sleep(poll_interval)
                continue

            job_id   = job["job_id"]
            location = job["location"]
            country  = job["country"]
            print(f"\n[Queue] {'='*50}")
            print(f"[Queue] Starting job {job_id}: {location}, {country}")
            print(f"[Queue]   priority={job['priority']} send_mode={job['send_mode']} run_until={job['run_until']}")
            print(f"[Queue] {'='*50}")

            # Mark running before calling run_campaign so the UI reflects it
            update_job(job_id, status=STATUS_RUNNING, started_at=_now())

            # ---- Execute ----------------------------------------------------
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

            except Exception as exc:
                tb  = traceback.format_exc()
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

            # Brief pause between jobs (let file I/O settle)
            time.sleep(2)

    except KeyboardInterrupt:
        print("\n[Queue] Scheduler stopped by user.")


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="OmniSol Campaign Queue Scheduler")
    parser.add_argument(
        "--poll", type=float, default=5.0,
        help="Poll interval in seconds when idle (default: 5)",
    )
    args = parser.parse_args()
    run_scheduler(poll_interval=args.poll)
