"""
Workflow 9 Queue Scheduler — Job Queue Store

Stores campaign jobs in data/campaign_queue.json.
Provides CRUD operations for the scheduler and UI layer.

Job schema
----------
  job_id           unique identifier  (q-XXXXXXXX)
  location         city name          e.g. "Dubai"
  country          country name       e.g. "UAE"
  region           region / state     optional
  status           pending | running | completed | failed | paused
  priority         int, lower = higher priority (default 10)
  send_mode        dry_run | smtp | gmail_api
  run_until        pipeline step to stop after (default campaign_status)
  campaign_id      filled in when job starts running
  created_at       ISO UTC timestamp
  started_at       ISO UTC timestamp (empty until running)
  completed_at     ISO UTC timestamp (empty until done/failed)
  error            error message if failed
  keyword_mode     default | custom
  keywords         list[str]  (used when keyword_mode = custom)
  company_limit    int  (0 = no limit)
  crawl_limit      int  (0 = no limit)
  enrich_limit     int  (0 = no limit)
  metro_mode       base_only | recommended | custom
  metro_sub_cities list[str]

Writes are atomic: data is written to a temp file then renamed.
The queue file is a plain JSON list so it is human-readable and manually editable.
"""
from __future__ import annotations

import json
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from config.settings import CAMPAIGN_QUEUE_FILE, CAMPAIGN_QUEUE_PAUSE_FLAG

# ---------------------------------------------------------------------------
# Status constants
# ---------------------------------------------------------------------------

STATUS_PENDING   = "pending"
STATUS_RUNNING   = "running"
STATUS_COMPLETED = "completed"
STATUS_FAILED    = "failed"
STATUS_PAUSED    = "paused"   # individual job paused (not the whole queue)

ALL_STATUSES = (STATUS_PENDING, STATUS_RUNNING, STATUS_COMPLETED, STATUS_FAILED, STATUS_PAUSED)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _new_job_id() -> str:
    return "q-" + uuid.uuid4().hex[:8]


def _load_raw() -> list[dict]:
    if not CAMPAIGN_QUEUE_FILE.exists():
        return []
    with open(CAMPAIGN_QUEUE_FILE, encoding="utf-8") as f:
        try:
            return json.load(f)
        except (json.JSONDecodeError, ValueError):
            return []


def _save_raw(jobs: list[dict]) -> None:
    """Atomic write: write to temp file, then rename over queue file."""
    CAMPAIGN_QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=str(CAMPAIGN_QUEUE_FILE.parent),
        prefix=".queue_tmp_",
        suffix=".json",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(jobs, f, indent=2, ensure_ascii=False)
        Path(tmp_path).replace(CAMPAIGN_QUEUE_FILE)
    except Exception:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
        raise


# ---------------------------------------------------------------------------
# Public CRUD
# ---------------------------------------------------------------------------

def add_job(
    location: str,
    country: str,
    region: str = "",
    priority: int = 10,
    send_mode: str = "dry_run",
    run_until: str = "campaign_status",
    keyword_mode: str = "default",
    keywords: list[str] | None = None,
    company_limit: int = 0,
    crawl_limit: int = 0,
    enrich_limit: int = 0,
    metro_mode: str = "base_only",
    metro_sub_cities: list[str] | None = None,
) -> dict:
    """Add a new pending job to the queue. Returns the created job dict."""
    job: dict = {
        "job_id":          _new_job_id(),
        "location":        location.strip(),
        "country":         country.strip(),
        "region":          region.strip(),
        "status":          STATUS_PENDING,
        "priority":        int(priority),
        "send_mode":       send_mode,
        "run_until":       run_until,
        "campaign_id":     "",
        "created_at":      _now(),
        "started_at":      "",
        "completed_at":    "",
        "error":           "",
        "keyword_mode":    keyword_mode,
        "keywords":        list(keywords or []),
        "company_limit":   int(company_limit),
        "crawl_limit":     int(crawl_limit),
        "enrich_limit":    int(enrich_limit),
        "metro_mode":      metro_mode,
        "metro_sub_cities": list(metro_sub_cities or []),
    }
    jobs = _load_raw()
    jobs.append(job)
    _save_raw(jobs)
    return job


def get_job(job_id: str) -> dict | None:
    """Return a single job by ID, or None if not found."""
    for job in _load_raw():
        if job.get("job_id") == job_id:
            return job
    return None


def list_jobs(status: str | None = None) -> list[dict]:
    """
    Return all jobs, optionally filtered by status.
    Results are sorted by: priority (asc), then created_at (asc).
    """
    jobs = _load_raw()
    if status is not None:
        jobs = [j for j in jobs if j.get("status") == status]
    return sorted(jobs, key=lambda j: (int(j.get("priority", 99)), j.get("created_at", "")))


def get_next_pending() -> dict | None:
    """
    Return the highest-priority pending job (lowest priority number, then oldest).
    Returns None if no pending jobs exist.
    """
    pending = list_jobs(status=STATUS_PENDING)
    return pending[0] if pending else None


def update_job(job_id: str, **fields) -> dict | None:
    """
    Update arbitrary fields on a job by ID.
    Always updates the job in place; does not change fields not passed.
    Returns the updated job, or None if job_id not found.
    """
    jobs = _load_raw()
    updated = None
    for job in jobs:
        if job.get("job_id") == job_id:
            job.update(fields)
            updated = job
            break
    if updated is not None:
        _save_raw(jobs)
    return updated


def remove_job(job_id: str) -> bool:
    """Remove a job by ID. Returns True if found and removed."""
    jobs = _load_raw()
    new_jobs = [j for j in jobs if j.get("job_id") != job_id]
    if len(new_jobs) == len(jobs):
        return False
    _save_raw(new_jobs)
    return True


def requeue_job(job_id: str) -> dict | None:
    """
    Reset a failed or completed job back to pending so it runs again.
    Clears started_at, completed_at, error, campaign_id.
    """
    return update_job(
        job_id,
        status=STATUS_PENDING,
        campaign_id="",
        started_at="",
        completed_at="",
        error="",
    )


# ---------------------------------------------------------------------------
# Queue-level pause / resume
# ---------------------------------------------------------------------------

def pause_queue() -> None:
    """Pause the entire queue. The scheduler will stop picking new jobs."""
    CAMPAIGN_QUEUE_PAUSE_FLAG.parent.mkdir(parents=True, exist_ok=True)
    CAMPAIGN_QUEUE_PAUSE_FLAG.touch()


def resume_queue() -> None:
    """Resume a paused queue."""
    CAMPAIGN_QUEUE_PAUSE_FLAG.unlink(missing_ok=True)


def is_queue_paused() -> bool:
    """Return True if the queue-level pause flag is set."""
    return CAMPAIGN_QUEUE_PAUSE_FLAG.exists()


# ---------------------------------------------------------------------------
# Queue summary (used by UI)
# ---------------------------------------------------------------------------

def queue_summary() -> dict:
    """
    Return a lightweight summary dict for the UI:
      pending, running, completed, failed, paused_flag, running_job, next_job
    """
    jobs = _load_raw()
    by_status: dict[str, list[dict]] = {s: [] for s in ALL_STATUSES}
    for j in jobs:
        s = j.get("status", STATUS_PENDING)
        by_status.setdefault(s, []).append(j)

    pending_sorted = sorted(
        by_status[STATUS_PENDING],
        key=lambda j: (int(j.get("priority", 99)), j.get("created_at", "")),
    )
    running_list = by_status[STATUS_RUNNING]

    return {
        "pending":     len(by_status[STATUS_PENDING]),
        "running":     len(running_list),
        "completed":   len(by_status[STATUS_COMPLETED]),
        "failed":      len(by_status[STATUS_FAILED]),
        "paused_flag": is_queue_paused(),
        "running_job": running_list[0] if running_list else None,
        "next_job":    pending_sorted[0] if pending_sorted else None,
        "total":       len(jobs),
    }
