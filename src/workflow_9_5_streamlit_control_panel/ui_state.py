"""
Workflow 9.5 / 9.6 — Streamlit Campaign Control Panel: State Reader

Reads current pipeline state from output files and exposes it to the UI.
All functions tolerate missing files and return empty structures instead of crashing.
"""
from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.workflow_9_5_streamlit_control_panel.debug_log import log

import config.run_context as _run_context
from config.settings import (
    CAMPAIGN_RUN_STATE_FILE,
    CAMPAIGN_RUNNER_LOGS_FILE,
    CAMPAIGN_STATUS_FILE,
    CAMPAIGN_STATUS_SUMMARY,
    CAMPAIGN_QUEUE_FILE,
    SEARCH_TASKS_FILE,
    RAW_LEADS_FILE,
    COMPANY_PAGES_FILE,
    COMPANY_TEXT_FILE,
    COMPANY_ANALYSIS_FILE,
    QUALIFIED_LEADS_FILE,
    ENRICHED_LEADS_FILE,
    ENRICHED_CONTACTS_FILE,
    GENERATED_EMAILS_FILE,
    MANUAL_REVIEW_QUEUE_FILE,
    SEND_LOGS_FILE,
    ENGAGEMENT_LOGS_FILE,
    ENGAGEMENT_SUMMARY_FILE,
    FOLLOWUP_QUEUE_FILE,
    CLOUD_DEPLOY_STATUS_FILE,
    CLOUD_SEND_STATUS_FILE,
    CLOUD_WORKER_POLL_SECONDS,
    DATA_DIR,
    REPLY_LOGS_FILE,
    RUNS_DIR,
)
from src.workflow_9_campaign_runner.campaign_state import load_cloud_deploy_status
from src.workflow_9_campaign_runner.campaign_state import load_cloud_send_status

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _activate_display_context() -> None:
    """
    Point _RunPath constants at the last completed campaign's directory so that
    dashboard reads see actual pipeline outputs rather than the DATA_DIR fallback.

    Rules:
    - No-op if a campaign run is currently in progress (context already correct).
    - Reads campaign_id from campaign_run_state.json (process-level path, always
      accessible regardless of run context).
    - Safe to call multiple times — each call re-reads state so the dashboard
      automatically tracks the most recently completed campaign.
    """
    if _run_context.get_active_campaign_id() is not None:
        log.state("Display context: campaign already active, skipping")
        return  # campaign runner has the context — don't disturb it
    if not CAMPAIGN_RUN_STATE_FILE.exists():
        log.state("Display context: no campaign_run_state.json found")
        return
    try:
        with open(CAMPAIGN_RUN_STATE_FILE, encoding="utf-8") as f:
            state = json.load(f)
        cid = (state.get("campaign_id") or "").strip()
        if cid:
            log.state("Display context: activating run context", campaign_id=cid)
            _run_context.set_active_run(cid)
        else:
            log.state("Display context: state file has no campaign_id")
    except Exception as exc:
        log.warn("Display context: failed to read campaign_run_state.json", exc=exc)


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        log.data(f"_read_csv: file not found: {path.name}")
        return []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        log.data(f"_read_csv: {path.name}", rows=len(rows))
        return rows
    except Exception as exc:
        log.error(f"_read_csv: failed to read {path.name}", exc=exc)
        return []


def _count_csv(path: Path) -> int:
    rows = _read_csv(path)
    return len(rows)


def _count_csv_where(path: Path, col: str, value: str) -> int:
    if not path.exists():
        return 0
    try:
        with open(path, newline="", encoding="utf-8") as f:
            return sum(
                1 for r in csv.DictReader(f)
                if r.get(col, "").strip().lower() == value.lower()
            )
    except Exception:
        return 0


def _sum_col(path: Path, col: str) -> int:
    """Sum a numeric column in a CSV file."""
    if not path.exists():
        return 0
    try:
        with open(path, newline="", encoding="utf-8") as f:
            total = 0
            for r in csv.DictReader(f):
                try:
                    total += int(r.get(col) or 0)
                except (ValueError, TypeError):
                    pass
            return total
    except Exception:
        return 0


def _mtime(path: Path) -> str:
    """Return last-modified time as a readable string, or '' if missing."""
    if not path.exists():
        return ""
    try:
        ts = os.path.getmtime(path)
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return ""


def _read_json(path: Path) -> dict:
    """Return parsed JSON dict, or {} on missing/invalid content."""
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _load_run_config(run_dir: Path) -> dict:
    state_path = run_dir / "campaign_run_state.json"
    if not state_path.exists():
        return {}
    try:
        with open(state_path, encoding="utf-8") as f:
            data = json.load(f)
        cfg = data.get("config", {})
        return cfg if isinstance(cfg, dict) else {}
    except Exception:
        return {}


def _parse_dt(value: str) -> datetime | None:
    """Best-effort timestamp parser for mixed pipeline/UI timestamp formats."""
    text = str(value or "").strip()
    if not text:
        return None

    candidates = [
        text.replace(" UTC", "+00:00"),
        text.replace(" UTC", ""),
        text,
    ]
    for candidate in candidates:
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            pass

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
    ):
        try:
            return datetime.strptime(text.replace(" UTC", ""), fmt)
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Campaign run state
# ---------------------------------------------------------------------------

def load_current_campaign_state() -> dict:
    """Load data/campaign_run_state.json.  Returns {} if missing."""
    log.data("load_current_campaign_state()")
    if not CAMPAIGN_RUN_STATE_FILE.exists():
        log.data("campaign_run_state.json not found")
        return {}
    try:
        with open(CAMPAIGN_RUN_STATE_FILE, encoding="utf-8") as f:
            state = json.load(f)

        # Self-heal stale "running" UI state. campaign_runner.is_campaign_running()
        # now clears stale locks, but the state file itself may still say
        # "running" from an old crashed session. If no real lock remains, mark
        # the state failed so the control panel stops showing a phantom run.
        if state.get("status") == "running":
            try:
                from src.workflow_9_campaign_runner.campaign_runner import is_campaign_running
                if not is_campaign_running():
                    state["status"] = "failed"
                    state["error_message"] = (
                        "Recovered stale campaign state: no active campaign lock "
                        "was detected for the previous run."
                    )
                    with open(CAMPAIGN_RUN_STATE_FILE, "w", encoding="utf-8") as f:
                        json.dump(state, f, indent=2, ensure_ascii=False)
                    log.state("Recovered stale running campaign state", campaign_id=state.get("campaign_id"))
            except Exception as exc:
                log.warn("Stale campaign state recovery check failed", exc=exc)

        campaign_id = str(state.get("campaign_id") or "").strip()
        cloud_deploy = load_cloud_deploy_status(campaign_id) if campaign_id else {}
        cloud_send = load_cloud_send_status(campaign_id) if campaign_id else {}
        state.setdefault("cloud_deploy_status", "not_enabled")
        state.setdefault("cloud_deploy_updated_at", "")
        state.setdefault("cloud_deploy_error", None)
        state.setdefault("cloud_send_status", "not_queued")
        state.setdefault("cloud_send_updated_at", "")
        state.setdefault("cloud_send_error", None)
        if cloud_deploy:
            state["cloud_deploy_status"] = cloud_deploy.get("cloud_deploy_status", state["cloud_deploy_status"])
            state["cloud_deploy_updated_at"] = cloud_deploy.get(
                "cloud_deploy_updated_at",
                state.get("cloud_deploy_updated_at", ""),
            )
            state["cloud_deploy_error"] = cloud_deploy.get(
                "cloud_deploy_error",
                state.get("cloud_deploy_error"),
            )
            for key in (
                "cloud_deploy_run_uri",
                "cloud_deploy_manifest_uri",
                "cloud_deploy_uploaded_at",
                "cloud_deploy_upload_mode",
                "cloud_deploy_file_count",
                "cloud_deploy_bytes",
                "cloud_deploy_elapsed_seconds",
            ):
                if key in cloud_deploy:
                    state[key] = cloud_deploy[key]
        if cloud_send:
            state["cloud_send_status"] = cloud_send.get("cloud_send_status", state["cloud_send_status"])
            state["cloud_send_updated_at"] = cloud_send.get(
                "cloud_send_updated_at",
                state.get("cloud_send_updated_at", ""),
            )
            state["cloud_send_error"] = cloud_send.get(
                "cloud_send_error",
                state.get("cloud_send_error"),
            )
            for key in (
                "cloud_send_manifest_uri",
                "cloud_send_run_uri",
                "cloud_send_queued_at",
                "cloud_send_synced_at",
                "cloud_send_due_at",
                "cloud_send_wait_seconds",
                "cloud_send_market",
                "cloud_send_timezone",
                "cloud_send_started_at",
                "cloud_send_completed_at",
                "cloud_send_processed_manifest_uri",
                "cloud_send_failed_manifest_uri",
                "cloud_send_upload_mode",
                "cloud_send_uploaded_file_count",
                "cloud_send_uploaded_bytes",
                "cloud_send_upload_elapsed_seconds",
                "cloud_send_failed_at",
                "cloud_send_failed_stage",
            ):
                if key in cloud_send:
                    state[key] = cloud_send[key]

        log.data("campaign_run_state.json loaded",
                 campaign_id=state.get("campaign_id"),
                 status=state.get("status"))
        return state
    except Exception as exc:
        log.error("Failed to load campaign_run_state.json", exc=exc)
        return {}


# ---------------------------------------------------------------------------
# Campaign runner logs
# ---------------------------------------------------------------------------

def load_campaign_logs(limit: int = 100) -> list[dict]:
    """Load recent rows from data/campaign_runner_logs.csv, newest first."""
    log.data("load_campaign_logs()", limit=limit)
    _activate_display_context()
    if not CAMPAIGN_RUNNER_LOGS_FILE.exists():
        log.data("campaign_runner_logs.csv not found")
        return []
    try:
        with open(CAMPAIGN_RUNNER_LOGS_FILE, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        rows.reverse()
        log.data("campaign_runner_logs.csv loaded", total_rows=len(rows), returning=min(limit, len(rows)))
        return rows[:limit]
    except Exception as exc:
        log.error("Failed to load campaign_runner_logs.csv", exc=exc)
        return []


# ---------------------------------------------------------------------------
# Campaign status
# ---------------------------------------------------------------------------

def load_campaign_status(limit: int = 500) -> list[dict]:
    """Load data/campaign_status.csv.  Returns [] if missing."""
    log.data("load_campaign_status()")
    _activate_display_context()
    if not CAMPAIGN_STATUS_FILE.exists():
        log.data("campaign_status.csv not found")
        return []
    try:
        with open(CAMPAIGN_STATUS_FILE, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))[:limit]
        log.data("campaign_status.csv loaded", rows=len(rows))
        return rows
    except Exception as exc:
        log.error("Failed to load campaign_status.csv", exc=exc)
        return []


def load_campaign_summary() -> dict:
    """Load data/campaign_status_summary.json.  Returns {} if missing."""
    log.data("load_campaign_summary()")
    _activate_display_context()
    if not CAMPAIGN_STATUS_SUMMARY.exists():
        log.data("campaign_status_summary.json not found")
        return {}
    try:
        with open(CAMPAIGN_STATUS_SUMMARY, encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        log.error("Failed to load campaign_status_summary.json", exc=exc)
        return {}


# ---------------------------------------------------------------------------
# KPI metrics (enhanced — includes rates, blocked count)
# ---------------------------------------------------------------------------

def load_pipeline_metrics() -> dict:
    """
    Compute top-line KPI metrics from available output files.
    """
    log.data("load_pipeline_metrics()")
    _activate_display_context()
    total_companies  = _count_csv(RAW_LEADS_FILE)
    qualified_leads  = _count_csv(QUALIFIED_LEADS_FILE)
    total_contacts   = _count_csv(ENRICHED_CONTACTS_FILE)
    emails_generated = _count_csv(GENERATED_EMAILS_FILE)

    emails_sent = (
        _count_csv_where(SEND_LOGS_FILE, "send_status", "sent")
        + _count_csv_where(SEND_LOGS_FILE, "send_status", "dry_run")
    )
    review_required = _count_csv_where(SEND_LOGS_FILE, "send_status", "review_required")
    send_hard_blocked = _count_csv_where(SEND_LOGS_FILE, "send_status", "blocked")
    open_count  = _sum_col(ENGAGEMENT_SUMMARY_FILE, "open_count")
    click_count = _sum_col(ENGAGEMENT_SUMMARY_FILE, "click_count")

    # followup_queued: count directly from followup_queue.csv (authoritative)
    followup_queued = _count_csv(FOLLOWUP_QUEUE_FILE)

    # blocked_count: from campaign_status aggregated view
    status_rows = load_campaign_status()
    blocked_count = sum(
        1 for r in status_rows if r.get("lifecycle_status", "") == "followup_blocked"
    )

    open_rate  = round(open_count  / emails_sent * 100, 1) if emails_sent else 0.0
    click_rate = round(click_count / emails_sent * 100, 1) if emails_sent else 0.0

    qualification_rate  = round(qualified_leads  / total_companies * 100, 1) if total_companies else 0.0
    contact_rate        = round(total_contacts   / total_companies * 100, 1) if total_companies else 0.0
    email_gen_rate      = round(emails_generated / total_contacts  * 100, 1) if total_contacts  else 0.0

    return {
        "total_companies":     total_companies,
        "qualified_leads":     qualified_leads,
        "total_contacts":      total_contacts,
        "emails_generated":    emails_generated,
        "emails_sent":         emails_sent,
        "review_required":     review_required,
        "send_hard_blocked":   send_hard_blocked,
        "open_count":          open_count,
        "click_count":         click_count,
        "followup_queued":     followup_queued,
        "blocked_count":       blocked_count,
        "open_rate":           open_rate,
        "click_rate":          click_rate,
        "qualification_rate":  qualification_rate,
        "contact_rate":        contact_rate,
        "email_gen_rate":      email_gen_rate,
    }


def load_delivery_ops_snapshot() -> dict:
    """
    Build an operator-friendly cloud sending snapshot for the dashboard.

    Values are aggregated across queue jobs that ran in gmail_api mode and
    reached a completed cloud deploy state.
    """
    log.data("load_delivery_ops_snapshot()")

    state = load_current_campaign_state()
    now = datetime.now()
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    seven_days_ago = now - timedelta(days=7)

    try:
        with open(CAMPAIGN_QUEUE_FILE, encoding="utf-8") as f:
            jobs = json.load(f)
        if not isinstance(jobs, list):
            jobs = []
    except Exception:
        jobs = []

    current_job = next((j for j in jobs if j.get("status") == "running"), None)
    current_country = ""
    current_location = ""
    current_status = ""

    if current_job:
        current_country = str(current_job.get("country") or "").strip()
        current_location = str(current_job.get("location") or "").strip()
        current_status = "running"
    else:
        cfg = state.get("config") or {}
        cloud_send_status = str(state.get("cloud_send_status") or "").strip()
        if state.get("status") == "running":
            current_country = str(cfg.get("country") or "").strip()
            current_location = str(cfg.get("base_city") or cfg.get("city") or "").strip()
            current_status = "running"
        elif cloud_send_status and cloud_send_status != "not_queued":
            current_country = str(cfg.get("country") or "").strip()
            current_location = str(cfg.get("base_city") or cfg.get("city") or "").strip()
            current_status = cloud_send_status
        elif state:
            current_country = str(cfg.get("country") or "").strip()
            current_location = str(cfg.get("base_city") or cfg.get("city") or "").strip()
            current_status = str(state.get("status") or "").strip()

    cloud_delegated_emails = 0
    sent_successfully = 0
    uploaded_yesterday_runs = 0
    uploaded_yesterday_emails = 0
    cloud_run_count = 0
    cloud_queued_runs = 0
    cloud_waiting_runs = 0
    cloud_sending_runs = 0
    cloud_failed_runs = 0

    for job in jobs:
        campaign_id = str(job.get("campaign_id") or "").strip()
        send_mode = str(job.get("send_mode") or "").strip().lower()
        if not campaign_id or send_mode != "gmail_api":
            continue

        run_dir = RUNS_DIR / campaign_id
        cloud_status = _read_json(run_dir / "cloud_deploy_status.json")
        cloud_send = _read_json(run_dir / "cloud_send_status.json")
        send_summary = _read_json(run_dir / "send_batch_summary.json")
        deploy_state = str(cloud_status.get("cloud_deploy_status") or "").strip().lower()
        if deploy_state != "completed":
            continue

        send_state = str(cloud_send.get("cloud_send_status") or "").strip().lower()
        if send_state == "queued":
            cloud_queued_runs += 1
        elif send_state in {"synced", "waiting_window"}:
            cloud_waiting_runs += 1
        elif send_state == "sending":
            cloud_sending_runs += 1
        elif send_state == "failed":
            cloud_failed_runs += 1

        cloud_run_count += 1
        send_total = int(send_summary.get("total") or 0)
        sent_total = int(send_summary.get("sent") or 0)
        cloud_delegated_emails += send_total
        sent_successfully += sent_total

        uploaded_at = (
            cloud_status.get("cloud_deploy_uploaded_at")
            or cloud_status.get("cloud_deploy_updated_at")
            or job.get("completed_at")
            or ""
        )
        uploaded_dt = _parse_dt(str(uploaded_at))
        if uploaded_dt and uploaded_dt.date() == yesterday:
            uploaded_yesterday_runs += 1
            uploaded_yesterday_emails += send_total

    send_log_rows = _read_csv(Path(str(SEND_LOGS_FILE)))
    engagement_rows = _read_csv(Path(str(ENGAGEMENT_LOGS_FILE)))
    reply_rows = _read_csv(Path(str(REPLY_LOGS_FILE)))

    sent_7d = 0
    for row in send_log_rows:
        ts = _parse_dt(row.get("timestamp", ""))
        if ts and ts.replace(tzinfo=None) >= seven_days_ago and (row.get("send_status") or "").strip().lower() == "sent":
            sent_7d += 1

    bounces_7d = 0
    last_bounce_at = ""
    last_bounce_dt: datetime | None = None
    for row in engagement_rows:
        if (row.get("event_type") or "").strip().lower() != "bounce":
            continue
        ts = _parse_dt(row.get("timestamp", ""))
        if ts and ts.replace(tzinfo=None) >= seven_days_ago:
            bounces_7d += 1
        if ts and (last_bounce_dt is None or ts > last_bounce_dt):
            last_bounce_dt = ts
            last_bounce_at = ts.strftime("%Y-%m-%d %H:%M")

    suppressed_addresses: set[str] = set()
    bounce_addresses: set[str] = set()
    for row in reply_rows:
        email_key = (
            (row.get("matched_kp_email") or "").strip().lower()
            or (row.get("from_email") or "").strip().lower()
        )
        if not email_key:
            continue
        if (row.get("suppression_status") or "").strip().lower() == "suppressed":
            suppressed_addresses.add(email_key)
        if (row.get("reply_type") or "").strip().lower() == "bounce":
            bounce_addresses.add(email_key)

    bounce_rate_7d = round(bounces_7d / sent_7d * 100, 1) if sent_7d else 0.0

    return {
        "cloud_delegated_emails": cloud_delegated_emails,
        "sent_successfully": sent_successfully,
        "current_country": current_country,
        "current_location": current_location,
        "current_status": current_status,
        "uploaded_yesterday_runs": uploaded_yesterday_runs,
        "uploaded_yesterday_emails": uploaded_yesterday_emails,
        "cloud_run_count": cloud_run_count,
        "cloud_queued_runs": cloud_queued_runs,
        "cloud_waiting_runs": cloud_waiting_runs,
        "cloud_sending_runs": cloud_sending_runs,
        "cloud_failed_runs": cloud_failed_runs,
        "bounces_7d": bounces_7d,
        "bounce_rate_7d": bounce_rate_7d,
        "sent_7d": sent_7d,
        "suppressed_addresses": len(suppressed_addresses),
        "bounce_addresses": len(bounce_addresses),
        "last_bounce_at": last_bounce_at,
        "snapshot_date": today.isoformat(),
        "yesterday_date": yesterday.isoformat(),
    }


def load_cloud_worker_health() -> dict:
    """Return a health snapshot for the cloud worker and its recent alerts."""
    state_path = DATA_DIR / "cloud_send_worker_state.json"
    alerts_path = DATA_DIR / "cloud_worker_alerts.jsonl"
    release_path = DATA_DIR / "deploy_release.json"

    worker_state = _read_json(state_path)
    release = _read_json(release_path)

    now = datetime.now()
    last_poll = _parse_dt(worker_state.get("last_poll_at", ""))
    stale_after_seconds = max(int(CLOUD_WORKER_POLL_SECONDS * 4), 300)

    if not worker_state:
        health = "offline"
    elif str(worker_state.get("worker_config_issue") or "").strip():
        health = "misconfigured"
    elif last_poll is None:
        health = "unknown"
    else:
        age_seconds = max((now - last_poll.replace(tzinfo=None)).total_seconds(), 0.0)
        health = "healthy" if age_seconds <= stale_after_seconds else "stalled"

    alerts_24h = 0
    last_alert_at = ""
    last_alert_dt: datetime | None = None
    last_alert_level = ""
    last_alert_type = ""
    last_alert_message = ""
    twenty_four_hours_ago = now - timedelta(hours=24)

    if alerts_path.exists():
        try:
            with open(alerts_path, encoding="utf-8") as f:
                for line in f:
                    text = line.strip()
                    if not text:
                        continue
                    try:
                        payload = json.loads(text)
                    except Exception:
                        continue
                    ts = _parse_dt(payload.get("timestamp", ""))
                    if ts and ts.replace(tzinfo=None) >= twenty_four_hours_ago:
                        alerts_24h += 1
                    if ts and (last_alert_dt is None or ts > last_alert_dt):
                        last_alert_dt = ts
                        last_alert_at = ts.strftime("%Y-%m-%d %H:%M")
                        last_alert_level = str(payload.get("level") or "")
                        last_alert_type = str(payload.get("event_type") or "")
                        last_alert_message = str(payload.get("message") or "")
        except Exception:
            pass

    return {
        "worker_health": health,
        "last_poll_at": last_poll.strftime("%Y-%m-%d %H:%M") if last_poll else "",
        "last_success_at": (
            _parse_dt(worker_state.get("last_success_at", "")).strftime("%Y-%m-%d %H:%M")
            if _parse_dt(worker_state.get("last_success_at", ""))
            else ""
        ),
        "last_error_at": (
            _parse_dt(worker_state.get("last_error_at", "")).strftime("%Y-%m-%d %H:%M")
            if _parse_dt(worker_state.get("last_error_at", ""))
            else ""
        ),
        "active_campaign_id": str(worker_state.get("active_campaign_id") or "").strip(),
        "last_idle_reason": str(worker_state.get("last_idle_reason") or "").strip(),
        "last_manifest_count": int(worker_state.get("last_manifest_count") or 0),
        "last_wait_campaign_id": str(worker_state.get("last_wait_campaign_id") or "").strip(),
        "last_wait_due_at": str(worker_state.get("last_wait_due_at") or "").strip(),
        "last_completed_campaign_id": str(worker_state.get("last_completed_campaign_id") or "").strip(),
        "last_failed_campaign_id": str(worker_state.get("last_failed_campaign_id") or "").strip(),
        "last_processed_manifest_uri": str(worker_state.get("last_processed_manifest_uri") or "").strip(),
        "last_poll_result": str(worker_state.get("last_poll_result") or "").strip(),
        "last_candidate_count": int(worker_state.get("last_candidate_count") or 0),
        "last_manifest_sample": worker_state.get("last_manifest_sample") or [],
        "last_sync_campaign_id": str(worker_state.get("last_sync_campaign_id") or "").strip(),
        "last_reconciled_campaign_id": str(worker_state.get("last_reconciled_campaign_id") or "").strip(),
        "worker_config_ok": bool(worker_state.get("worker_config_ok", True)),
        "worker_config_issue": str(worker_state.get("worker_config_issue") or "").strip(),
        "worker_bucket": str(worker_state.get("worker_bucket") or "").strip(),
        "worker_manifests_prefix": str(worker_state.get("worker_manifests_prefix") or "").strip(),
        "alerts_24h": alerts_24h,
        "last_alert_at": last_alert_at,
        "last_alert_level": last_alert_level,
        "last_alert_type": last_alert_type,
        "last_alert_message": last_alert_message,
        "release_branch": str(release.get("git_branch") or "").strip(),
        "release_commit_short": str(release.get("git_commit_short") or "").strip(),
        "release_updated_at": str(release.get("updated_at_utc") or "").strip(),
    }


def load_ready_cloud_deploys(limit: int = 20) -> list[dict]:
    """
    Return recently completed runs that are ready for cloud deploy but have not
    yet been handed off (or are not currently in-progress) according to
    cloud_deploy_status.json.
    """
    rows: list[dict] = []
    if not RUNS_DIR.exists():
        return rows

    blocked_statuses = {"pending", "started", "completed"}
    for run_dir in sorted(RUNS_DIR.iterdir(), reverse=True):
        if not run_dir.is_dir():
            continue

        campaign_id = run_dir.name
        final_queue = run_dir / "final_send_queue.csv"
        if not final_queue.exists():
            continue

        cfg = _load_run_config(run_dir)
        send_mode = str(cfg.get("send_mode") or "").strip().lower()
        dry_run = str(cfg.get("dry_run") or "").strip().lower() == "true"
        if send_mode == "dry_run" or dry_run:
            continue

        deploy = _read_json(run_dir / "cloud_deploy_status.json")
        deploy_status = str(deploy.get("cloud_deploy_status") or "").strip().lower()
        if deploy_status in blocked_statuses:
            continue

        queue_count = _count_csv(final_queue)
        state = _read_json(run_dir / "campaign_run_state.json")
        run_status = str(state.get("status") or "").strip().lower()
        if run_status != "completed":
            continue

        rows.append({
            "campaign_id": campaign_id,
            "location": str(cfg.get("base_city") or cfg.get("city") or "").strip(),
            "country": str(cfg.get("country") or "").strip(),
            "send_mode": send_mode or "unknown",
            "run_until": str(cfg.get("run_until") or "").strip(),
            "queue_count": queue_count,
            "deploy_status": deploy_status or "not_started",
            "modified": _mtime(final_queue),
        })

    rows.sort(key=lambda row: (row.get("modified", ""), row.get("campaign_id", "")), reverse=True)
    return rows[:limit]


def load_manual_review_queue(limit: int = 200) -> list[dict]:
    """Load the current run's manual_review_queue.csv for operator review."""
    _activate_display_context()
    rows = _read_csv(MANUAL_REVIEW_QUEUE_FILE)
    return rows[:limit]


def load_multi_run_comparison(limit: int = 8) -> list[dict]:
    """
    Build a recent completed multi-run comparison table from queue jobs + run artifacts.

    Each row summarizes one completed queue job so operators can compare city-level
    funnel efficiency, dedup loss, and contact quality without opening each run
    folder manually.
    """
    if not CAMPAIGN_QUEUE_FILE.exists():
        return []

    try:
        with open(CAMPAIGN_QUEUE_FILE, encoding="utf-8") as f:
            jobs = json.load(f)
    except Exception as exc:
        log.error("Failed to load campaign_queue.json for comparison view", exc=exc)
        return []

    if not isinstance(jobs, list):
        return []

    completed_jobs = [
        j for j in jobs
        if j.get("status") == "completed" and j.get("campaign_id")
    ]
    completed_jobs.sort(key=lambda j: j.get("completed_at", ""), reverse=True)

    rows: list[dict] = []
    for job in completed_jobs[:limit]:
        campaign_id = str(job.get("campaign_id", "")).strip()
        run_dir = RUNS_DIR / campaign_id
        if not run_dir.exists():
            continue

        raw_leads       = _count_csv(run_dir / "raw_leads.csv")
        dedup_skipped   = _count_csv(run_dir / "dedup_skipped.csv")
        qualified       = _count_csv(run_dir / "qualified_leads.csv")
        contacts        = _count_csv(run_dir / "enriched_contacts.csv")
        generated       = _count_csv(run_dir / "generated_emails.csv")
        initial_queue   = _count_csv(run_dir / "send_queue.csv")
        final_queue     = _count_csv(run_dir / "final_send_queue.csv")
        final_rejected  = _count_csv(run_dir / "final_rejected_emails.csv")

        send_summary = _read_json(run_dir / "send_batch_summary.json")
        send_total = int(send_summary.get("total") or 0)
        generic_only = int(send_summary.get("policy_generic_only") or 0)
        review_required = int(send_summary.get("review_required") or 0)
        hard_blocked = int(send_summary.get("blocked") or 0)
        delivery_ready = int(send_summary.get("sent") or 0) + int(send_summary.get("dry_run") or 0)

        dedup_rate = round(dedup_skipped / (raw_leads + dedup_skipped) * 100, 1) if (raw_leads + dedup_skipped) else 0.0
        qualification_rate = round(qualified / raw_leads * 100, 1) if raw_leads else 0.0
        final_queue_rate = round(final_queue / qualified * 100, 1) if qualified else 0.0
        generic_only_rate = round(generic_only / send_total * 100, 1) if send_total else 0.0
        review_required_rate = round(review_required / send_total * 100, 1) if send_total else 0.0
        delivery_ready_rate = round(delivery_ready / send_total * 100, 1) if send_total else 0.0
        repair_lift = max(final_queue - initial_queue, 0)

        rows.append({
            "completed_at":       job.get("completed_at", ""),
            "location":           f"{job.get('location', '')}, {job.get('country', '')}".strip(", "),
            "campaign_id":        campaign_id,
            "raw_leads":          raw_leads,
            "dedup_skipped":      dedup_skipped,
            "dedup_rate_pct":     dedup_rate,
            "qualified":          qualified,
            "qualification_pct":  qualification_rate,
            "contacts":           contacts,
            "generated_emails":   generated,
            "send_queue_initial": initial_queue,
            "final_send_queue":   final_queue,
            "final_queue_rate":   final_queue_rate,
            "delivery_ready":     delivery_ready,
            "delivery_ready_rate": delivery_ready_rate,
            "review_required":    review_required,
            "review_required_pct": review_required_rate,
            "hard_blocked":       hard_blocked,
            "repair_lift":        repair_lift,
            "final_rejected":     final_rejected,
            "generic_only_pct":   generic_only_rate,
        })

    return rows


# ---------------------------------------------------------------------------
# High-priority leads
# ---------------------------------------------------------------------------

def load_high_priority_leads() -> list[dict]:
    """
    Return rows from campaign_status.csv that are high-priority.

    A row is included if ANY of these are true:
    - priority_flag == "high"
    - lifecycle_status == "clicked_no_reply"
    - lifecycle_status == "followup_queued"
    - open_count >= 2  (engaged but not yet replied)
    - lead_score >= 70 (high-value even before sends)
    """
    rows = load_campaign_status()
    result = []
    for r in rows:
        pf = r.get("priority_flag", "").strip().lower()
        ls = r.get("lifecycle_status", "").strip().lower()
        try:
            open_count = int(r.get("open_count") or 0)
        except (ValueError, TypeError):
            open_count = 0
        try:
            lead_score = float(r.get("lead_score") or 0)
        except (ValueError, TypeError):
            lead_score = 0.0

        if (
            pf == "high"
            or ls in ("clicked_no_reply", "followup_queued")
            or open_count >= 2
            or lead_score >= 70
        ):
            result.append(r)
    return result


# ---------------------------------------------------------------------------
# Company detail
# ---------------------------------------------------------------------------

def get_company_detail(company_name: str) -> dict | None:
    """
    Return the campaign_status row for a single company, or None if not found.
    Also enriches with generated email subject/body from generated_emails.csv.
    """
    rows = load_campaign_status()
    match = next(
        (r for r in rows if r.get("company_name", "").strip().lower()
         == company_name.strip().lower()),
        None,
    )
    if not match:
        return None

    detail = dict(match)

    # Attach latest email subject/body if available
    kp_email = detail.get("kp_email", "")
    if kp_email:
        email_rows = _read_csv(GENERATED_EMAILS_FILE)
        email_match = next(
            (e for e in email_rows
             if e.get("kp_email", "").strip().lower() == kp_email.strip().lower()),
            None,
        )
        if email_match:
            detail["latest_subject"] = email_match.get("subject", "")
            detail["latest_body"]    = email_match.get("body", "") or email_match.get("email_body", "")

    return detail


def load_company_names() -> list[str]:
    """Return sorted unique company_name values from campaign_status.csv."""
    rows = load_campaign_status()
    names = sorted({r.get("company_name", "").strip() for r in rows if r.get("company_name")})
    return names


# ---------------------------------------------------------------------------
# Followup queue
# ---------------------------------------------------------------------------

def load_followup_queue() -> list[dict]:
    """Load data/followup_queue.csv.  Returns [] if missing."""
    return _read_csv(FOLLOWUP_QUEUE_FILE)


def load_followup_1_candidates() -> list[dict]:
    """Return followup_queue rows with followup_stage == 'followup_1'."""
    return [
        r for r in load_followup_queue()
        if r.get("followup_stage", "").strip().lower() == "followup_1"
    ]


# ---------------------------------------------------------------------------
# Enhanced file status (with row counts + modification times)
# ---------------------------------------------------------------------------

# All tracked pipeline files shown in the enhanced status view (in pipeline order)
KEY_FILES: list[tuple[str, Path]] = [
    ("search_tasks.json",        SEARCH_TASKS_FILE),
    ("raw_leads.csv",            RAW_LEADS_FILE),
    ("company_pages.json",       COMPANY_PAGES_FILE),
    ("company_text.json",        COMPANY_TEXT_FILE),
    ("company_analysis.json",    COMPANY_ANALYSIS_FILE),
    ("qualified_leads.csv",      QUALIFIED_LEADS_FILE),
    ("enriched_leads.csv",       ENRICHED_LEADS_FILE),
    ("generated_emails.csv",     GENERATED_EMAILS_FILE),
    ("manual_review_queue.csv",  MANUAL_REVIEW_QUEUE_FILE),
    ("send_logs.csv",            SEND_LOGS_FILE),
    ("engagement_summary.csv",   ENGAGEMENT_SUMMARY_FILE),
    ("followup_queue.csv",       FOLLOWUP_QUEUE_FILE),
    ("campaign_status.csv",      CAMPAIGN_STATUS_FILE),
    ("cloud_deploy_status.json", CLOUD_DEPLOY_STATUS_FILE),
    ("cloud_send_status.json",   CLOUD_SEND_STATUS_FILE),
    ("campaign_run_state.json",  CAMPAIGN_RUN_STATE_FILE),
    ("campaign_runner_logs.csv", CAMPAIGN_RUNNER_LOGS_FILE),
]

# TRACKED_FILES mirrors KEY_FILES for load_file_status() compatibility
TRACKED_FILES: list[tuple[str, Path]] = KEY_FILES


def _row_count(name: str, path: Path) -> int | str:
    """Return row count for CSV files; '-' for non-CSV."""
    if not path.exists():
        return ""
    if name.endswith(".csv"):
        return _count_csv(path)
    return "—"


def load_file_status() -> list[dict]:
    """Return {file, exists, size_kb} for all tracked pipeline files."""
    _activate_display_context()
    results = []
    for name, path in TRACKED_FILES:
        exists  = path.exists()
        size_kb = round(path.stat().st_size / 1024, 1) if exists else 0
        results.append({"file": name, "exists": exists, "size_kb": size_kb})
    return results


def load_enhanced_file_status() -> list[dict]:
    """
    Return {file, exists, rows, size_kb, modified} for key operational files.
    """
    _activate_display_context()
    results = []
    for name, path in KEY_FILES:
        exists   = path.exists()
        size_kb  = round(path.stat().st_size / 1024, 1) if exists else 0
        rows     = _row_count(name, path) if exists else ""
        modified = _mtime(path)
        results.append({
            "file":     name,
            "exists":   exists,
            "rows":     rows,
            "size_kb":  size_kb,
            "modified": modified,
        })
    return results


# ---------------------------------------------------------------------------
# City-level crawl stats (for Smart Location UI)
# ---------------------------------------------------------------------------

def get_city_crawl_stats() -> dict[str, dict]:
    log.data("get_city_crawl_stats()")
    _activate_display_context()
    """
    Return per-city statistics to power the city status display in the UI.

    Attempts reads in this order:
    1. SQLite companies table (source_location column) — most accurate
    2. raw_leads.csv fallback (source_location column)

    Infers status from campaign_run_state.json:
    - completed : city == active campaign city AND last_step == campaign_status
    - running   : city == active campaign city AND status == running
    - partial   : city has data but pipeline appears incomplete
    - new       : no records found for city

    Returns a dict keyed by city name:
    {
        "Vancouver": {"lead_count": 77, "status": "partial", "last_updated": "2026-01-15"},
        "Burnaby":   {"lead_count": 0,  "status": "new",     "last_updated": ""},
    }

    Tolerates missing database or CSV gracefully — returns {} on total failure.
    """
    stats: dict[str, dict] = {}

    # ---- 1. Try SQLite ---------------------------------------------------
    try:
        from config.settings import DATABASE_FILE
        if DATABASE_FILE.exists():
            from src.database.db_connection import get_db_connection
            conn = get_db_connection()
            try:
                cursor = conn.execute(
                    "SELECT source_location, COUNT(*) AS lead_count "
                    "FROM companies GROUP BY source_location"
                )
                for row in cursor.fetchall():
                    city = (row["source_location"] or "").strip()
                    if city:
                        stats[city] = {
                            "lead_count":   int(row["lead_count"]),
                            "status":       "partial",
                            "last_updated": "",
                        }
            finally:
                conn.close()
    except Exception as exc:
        log.warn("City crawl stats: SQLite read failed, falling back to CSV", exc=exc)

    # ---- 2. CSV fallback if DB gave nothing ------------------------------
    if not stats:
        try:
            rows = _read_csv(RAW_LEADS_FILE)
            city_counts: dict[str, int] = {}
            for r in rows:
                city = (r.get("source_location") or "").strip()
                if city:
                    city_counts[city] = city_counts.get(city, 0) + 1
            for city, count in city_counts.items():
                stats[city] = {
                    "lead_count":   count,
                    "status":       "partial",
                    "last_updated": "",
                }
        except Exception as exc:
            log.warn("City crawl stats: CSV fallback also failed", exc=exc)

    # ---- 3. Infer status from campaign state ----------------------------
    try:
        state     = load_current_campaign_state()
        run_status = state.get("status", "")
        last_step  = state.get("last_completed_step") or ""
        cfg        = state.get("config") or {}
        active_city = (cfg.get("base_city") or cfg.get("city") or "").strip()

        if active_city:
            if run_status == "running":
                if active_city not in stats:
                    stats[active_city] = {"lead_count": 0, "status": "new", "last_updated": ""}
                stats[active_city]["status"] = "running"
            elif run_status == "completed" and last_step == "campaign_status":
                if active_city not in stats:
                    stats[active_city] = {"lead_count": 0, "status": "new", "last_updated": ""}
                stats[active_city]["status"] = "completed"
            elif run_status == "failed":
                if active_city in stats:
                    stats[active_city]["status"] = "partial"
    except Exception as exc:
        log.warn("City crawl stats: failed to infer status from campaign state", exc=exc)

    log.data("City crawl stats result", cities=len(stats))
    return stats
