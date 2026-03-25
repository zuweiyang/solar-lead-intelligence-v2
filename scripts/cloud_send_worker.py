"""
Google Cloud VM worker for market-local auto-send.

This worker:
  1. polls GCS for run manifests
  2. downloads any new run folder into the local workspace
  3. waits until target-market local send time
  4. runs Gmail API send + campaign status refresh
  5. uploads updated run outputs and marks the manifest processed
"""
from __future__ import annotations

import json
import os
import shutil
import smtplib
import subprocess
import sys
import tempfile
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from email.utils import formataddr
from pathlib import Path
from zoneinfo import ZoneInfo

os.environ.setdefault("PYTHONUTF8", "1")
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.run_context import clear_active_run, set_active_run
from config.settings import (
    CLOUD_WORKER_ALERT_EMAIL_FROM,
    CLOUD_WORKER_ALERT_EMAIL_MODE,
    CLOUD_WORKER_ALERT_EMAIL_TO,
    CLOUD_WORKER_ALERT_SUBJECT_PREFIX,
    CLOUD_WORKER_ALERT_WEBHOOK,
    CLOUD_SEND_CAP_TIMEZONE,
    CLOUD_SEND_INBOX_DAILY_LIMIT,
    CLOUD_SEND_INBOX_HOURLY_LIMIT,
    CLOUD_SEND_SKIP_WEEKENDS,
    CLOUD_WORKER_POLL_SECONDS,
    DATA_DIR,
    GCS_BUCKET,
    GCS_FAILED_PREFIX,
    GCS_INFLIGHT_PREFIX,
    GCS_MANIFESTS_PREFIX,
    GCS_PROCESSED_PREFIX,
    GCS_RUNS_PREFIX,
    GCS_STATUS_PREFIX,
    RUNS_DIR,
    SMTP_FROM_EMAIL,
    SMTP_FROM_NAME,
    SMTP_HOST,
    SMTP_PASSWORD,
    SMTP_PORT,
    SMTP_USERNAME,
    SMTP_USE_TLS,
    SEND_LOGS_FILE,
)
from src.workflow_9_campaign_runner.campaign_state import (
    CLOUD_DEPLOY_COMPLETED,
    CLOUD_SEND_COMPLETED,
    CLOUD_SEND_FAILED,
    CLOUD_SEND_SENDING,
    CLOUD_SEND_SYNCED,
    CLOUD_SEND_WAITING_WINDOW,
    load_cloud_deploy_status,
    sync_cloud_deploy_status,
    load_cloud_send_status,
    sync_cloud_send_status,
)
from scripts.auto_send_runs import _campaign_next_due, _run_campaign_send

STATE_FILE = DATA_DIR / "cloud_send_worker_state.json"
ALERTS_FILE = DATA_DIR / "cloud_worker_alerts.jsonl"


def _resolve_gcloud_bin() -> str:
    configured = os.getenv("GCLOUD_BIN", "").strip()
    candidates = [configured, "gcloud.cmd", "gcloud.exe", "gcloud"]
    for candidate in candidates:
        if not candidate:
            continue
        resolved = shutil.which(candidate) or (candidate if Path(candidate).exists() else "")
        if resolved:
            return resolved
    raise RuntimeError(
        "Could not find gcloud executable. Set GCLOUD_BIN or add gcloud.cmd to PATH."
    )


GCLOUD_BIN = _resolve_gcloud_bin()


def _now_utc() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _bucket_uri(*parts: str) -> str:
    if not GCS_BUCKET:
        raise RuntimeError("GCS_BUCKET is not configured in .env / settings")
    clean = [part.strip("/").replace("\\", "/") for part in parts if part]
    suffix = "/".join(clean)
    return f"gs://{GCS_BUCKET}/{suffix}" if suffix else f"gs://{GCS_BUCKET}"


def _run_cmd(args: list[str], capture_output: bool = False) -> subprocess.CompletedProcess[str]:
    cmd = [GCLOUD_BIN, *args]
    print(f"[CloudWorker] RUN: {' '.join(cmd)}")
    return subprocess.run(
        cmd,
        check=True,
        text=True,
        capture_output=capture_output,
    )


def _status_uri(filename: str) -> str:
    return _bucket_uri(GCS_STATUS_PREFIX, filename)


def _upload_status_file(local_path: Path, filename: str) -> None:
    if not GCS_BUCKET:
        return
    if not local_path.exists():
        return
    _run_cmd(["storage", "cp", str(local_path), _status_uri(filename)])


def _directory_upload_stats(local_dir: Path) -> dict[str, int]:
    file_count = 0
    total_bytes = 0
    for path in local_dir.rglob("*"):
        if not path.is_file():
            continue
        file_count += 1
        try:
            total_bytes += path.stat().st_size
        except OSError:
            pass
    return {
        "file_count": file_count,
        "total_bytes": total_bytes,
    }


def _upload_directory(local_dir: Path, remote_dir_uri: str) -> dict[str, float | int]:
    stats = _directory_upload_stats(local_dir)
    started = time.perf_counter()
    _run_cmd(["storage", "cp", "--recursive", f"{local_dir}{os.sep}*", remote_dir_uri.rstrip("/") + "/"])
    elapsed = round(time.perf_counter() - started, 2)
    print(
        f"[CloudWorker] Uploaded directory in one recursive pass: "
        f"{stats['file_count']} files, {stats['total_bytes']} bytes, {elapsed}s"
    )
    return {
        "file_count": stats["file_count"],
        "total_bytes": stats["total_bytes"],
        "elapsed_seconds": elapsed,
    }


def _upload_file(local_path: Path, remote_uri: str) -> None:
    _run_cmd(["storage", "cp", str(local_path), remote_uri])


def _load_state() -> dict:
    if not STATE_FILE.exists():
        return {"completed_campaigns": [], "failed_campaigns": [], "synced_manifests": {}}
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            state = json.load(f)
            state.setdefault("completed_campaigns", [])
            state.setdefault("failed_campaigns", [])
            state.setdefault("synced_manifests", {})
            state.setdefault("last_poll_at", "")
            state.setdefault("last_success_at", "")
            state.setdefault("last_error_at", "")
            state.setdefault("active_campaign_id", "")
            state.setdefault("last_idle_reason", "")
            state.setdefault("last_manifest_count", 0)
            state.setdefault("last_inflight_count", 0)
            state.setdefault("last_wait_campaign_id", "")
            state.setdefault("last_wait_due_at", "")
            state.setdefault("last_completed_campaign_id", "")
            state.setdefault("last_failed_campaign_id", "")
            state.setdefault("last_processed_manifest_uri", "")
            state.setdefault("last_poll_result", "")
            state.setdefault("last_candidate_count", 0)
            state.setdefault("last_manifest_sample", [])
            state.setdefault("last_inflight_sample", [])
            state.setdefault("last_candidate_campaign_ids", [])
            state.setdefault("last_sync_campaign_id", "")
            state.setdefault("last_reconciled_campaign_id", "")
            state.setdefault("last_selected_campaign_id", "")
            state.setdefault("last_selected_due_at", "")
            state.setdefault("claimed_campaign_id", "")
            state.setdefault("claimed_manifest_uri", "")
            state.setdefault("worker_config_ok", True)
            state.setdefault("worker_config_issue", "")
            state.setdefault("worker_bucket", GCS_BUCKET)
            state.setdefault("worker_manifests_prefix", GCS_MANIFESTS_PREFIX)
            state.setdefault("inbox_cap_timezone", CLOUD_SEND_CAP_TIMEZONE or "UTC")
            state.setdefault("inbox_daily_cap", int(CLOUD_SEND_INBOX_DAILY_LIMIT or 0))
            state.setdefault("inbox_hourly_cap", int(CLOUD_SEND_INBOX_HOURLY_LIMIT or 0))
            state.setdefault("inbox_sent_today", 0)
            state.setdefault("inbox_remaining_today", 0)
            state.setdefault("inbox_sent_last_hour", 0)
            state.setdefault("inbox_remaining_this_hour", 0)
            state.setdefault("weekend_sending_enabled", not CLOUD_SEND_SKIP_WEEKENDS)
            state.setdefault("next_capacity_due_at", "")
            state.setdefault("next_capacity_reason", "")
            state.setdefault("last_live_email_count", 0)
            state.setdefault("last_manifest_email_count", 0)
            state.setdefault("last_inflight_email_count", 0)
            state.setdefault("last_carryover_email_count", 0)
            return state
    except Exception:
        return {"completed_campaigns": [], "failed_campaigns": [], "synced_manifests": {}}


def _save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    try:
        _upload_status_file(STATE_FILE, STATE_FILE.name)
    except Exception as exc:
        print(f"[CloudWorker] Failed to upload worker state mirror: {exc}")


def _post_alert(payload: dict) -> None:
    if not CLOUD_WORKER_ALERT_WEBHOOK:
        return
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        CLOUD_WORKER_ALERT_WEBHOOK,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310
        response.read()


def _alert_email_sender() -> str:
    return CLOUD_WORKER_ALERT_EMAIL_FROM or SMTP_FROM_EMAIL or SMTP_USERNAME


def _build_alert_email(payload: dict) -> EmailMessage:
    sender_email = _alert_email_sender()
    sender_name = SMTP_FROM_NAME or "Cloud Worker"
    recipient = CLOUD_WORKER_ALERT_EMAIL_TO
    if not recipient:
        raise RuntimeError("CLOUD_WORKER_ALERT_EMAIL_TO is not configured")
    if not sender_email:
        raise RuntimeError("No sender email configured for cloud worker alerts")

    subject_prefix = CLOUD_WORKER_ALERT_SUBJECT_PREFIX or "[CloudWorker]"
    level = str(payload.get("level") or "info").upper()
    event_type = str(payload.get("event_type") or "event")
    campaign_id = str(payload.get("campaign_id") or "")
    message = str(payload.get("message") or "")
    manifest_uri = str(payload.get("manifest_uri") or "")
    timestamp = str(payload.get("timestamp") or "")
    details = payload.get("details") or {}

    subject = f"{subject_prefix} {level} {event_type}"
    if campaign_id:
        subject = f"{subject} {campaign_id}"

    lines = [
        f"Timestamp: {timestamp}",
        f"Level: {level}",
        f"Event: {event_type}",
    ]
    if campaign_id:
        lines.append(f"Campaign: {campaign_id}")
    if manifest_uri:
        lines.append(f"Manifest: {manifest_uri}")
    lines.extend(["", message])
    if details:
        lines.extend(["", "Details:", json.dumps(details, ensure_ascii=False, indent=2)])

    msg = EmailMessage()
    msg["From"] = formataddr((sender_name, sender_email))
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.set_content("\n".join(lines))
    return msg


def _send_alert_email_via_smtp(msg: EmailMessage) -> None:
    sender_email = _alert_email_sender()
    if not SMTP_HOST or not SMTP_PORT or not SMTP_USERNAME or not SMTP_PASSWORD:
        raise RuntimeError("SMTP settings are incomplete for cloud worker alert email")
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        if SMTP_USE_TLS:
            server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.send_message(msg, from_addr=sender_email, to_addrs=[CLOUD_WORKER_ALERT_EMAIL_TO])


def _send_alert_email_via_gmail_api(msg: EmailMessage) -> None:
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    import base64

    token_file = Path("config") / "gmail_token.json"
    if not token_file.exists():
        from config.settings import GMAIL_TOKEN_FILE

        token_file = GMAIL_TOKEN_FILE

    creds = None
    if token_file.exists():
        creds = Credentials.from_authorized_user_file(
            str(token_file),
            scopes=["https://www.googleapis.com/auth/gmail.send"],
        )
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            raise RuntimeError("Gmail API token missing or invalid for alert email")

    service = build("gmail", "v1", credentials=creds)
    raw_bytes = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service.users().messages().send(userId="me", body={"raw": raw_bytes}).execute()


def _post_alert_email(payload: dict) -> None:
    if not CLOUD_WORKER_ALERT_EMAIL_TO:
        return
    msg = _build_alert_email(payload)
    mode = CLOUD_WORKER_ALERT_EMAIL_MODE or "gmail_api"
    if mode == "smtp":
        _send_alert_email_via_smtp(msg)
        return
    if mode == "gmail_api":
        _send_alert_email_via_gmail_api(msg)
        return
    raise RuntimeError(
        f"Unsupported CLOUD_WORKER_ALERT_EMAIL_MODE: {CLOUD_WORKER_ALERT_EMAIL_MODE!r}"
    )


def _record_alert(
    *,
    level: str,
    event_type: str,
    message: str,
    campaign_id: str = "",
    manifest_uri: str = "",
    details: dict | None = None,
) -> None:
    payload = {
        "timestamp": _now_utc(),
        "level": level,
        "event_type": event_type,
        "message": message,
        "campaign_id": campaign_id,
        "manifest_uri": manifest_uri,
        "details": details or {},
    }
    ALERTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(ALERTS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    try:
        _upload_status_file(ALERTS_FILE, ALERTS_FILE.name)
    except Exception as exc:
        print(f"[CloudWorker] Failed to upload alert log mirror: {exc}")
    try:
        _post_alert(payload)
    except Exception as exc:
        print(f"[CloudWorker] Alert delivery failed: {exc}")
    try:
        _post_alert_email(payload)
    except Exception as exc:
        print(f"[CloudWorker] Alert email delivery failed: {exc}")


def _update_worker_state(state: dict, **updates: object) -> None:
    state.update(updates)
    _save_state(state)


def _parse_dt(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _send_log_rows() -> list[dict]:
    if not SEND_LOGS_FILE.exists():
        return []
    try:
        import csv

        with open(SEND_LOGS_FILE, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def _next_weekday_start(local_now: datetime) -> datetime:
    cursor = local_now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    while cursor.weekday() >= 5:
        cursor += timedelta(days=1)
    return cursor


def _build_inbox_capacity_snapshot(now_utc: datetime) -> dict[str, object]:
    tz_name = CLOUD_SEND_CAP_TIMEZONE or "UTC"
    cap_tz = ZoneInfo(tz_name)
    local_now = now_utc.astimezone(cap_tz)
    rows = _send_log_rows()

    sent_rows: list[datetime] = []
    for row in rows:
        if str(row.get("send_status") or "").strip().lower() != "sent":
            continue
        ts = _parse_dt(row.get("timestamp") or "")
        if not ts:
            continue
        sent_rows.append(ts.astimezone(cap_tz))

    today_rows = [ts for ts in sent_rows if ts.date() == local_now.date()]
    one_hour_ago = local_now - timedelta(hours=1)
    hour_rows = [ts for ts in sent_rows if ts >= one_hour_ago]

    sent_today = len(today_rows)
    sent_last_hour = len(hour_rows)
    daily_cap = max(int(CLOUD_SEND_INBOX_DAILY_LIMIT or 0), 0)
    hourly_cap = max(int(CLOUD_SEND_INBOX_HOURLY_LIMIT or 0), 0)
    remaining_today = max(daily_cap - sent_today, 0) if daily_cap > 0 else 999999
    remaining_this_hour = max(hourly_cap - sent_last_hour, 0) if hourly_cap > 0 else 999999

    next_capacity_local: datetime | None = None
    capacity_reason = ""

    if CLOUD_SEND_SKIP_WEEKENDS and local_now.weekday() >= 5:
        next_capacity_local = _next_weekday_start(local_now)
        capacity_reason = "weekend_hold"
    elif daily_cap > 0 and sent_today >= daily_cap:
        next_capacity_local = _next_weekday_start(local_now) if CLOUD_SEND_SKIP_WEEKENDS else (
            local_now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        )
        capacity_reason = "daily_cap_reached"
    elif hourly_cap > 0 and sent_last_hour >= hourly_cap:
        oldest_hour_send = min(hour_rows) if hour_rows else local_now
        next_capacity_local = oldest_hour_send + timedelta(hours=1)
        if CLOUD_SEND_SKIP_WEEKENDS and next_capacity_local.weekday() >= 5:
            next_capacity_local = _next_weekday_start(next_capacity_local)
        capacity_reason = "hourly_cap_reached"

    return {
        "cap_timezone": tz_name,
        "local_now": local_now,
        "daily_cap": daily_cap,
        "hourly_cap": hourly_cap,
        "sent_today": sent_today,
        "sent_last_hour": sent_last_hour,
        "remaining_today": remaining_today if daily_cap > 0 else None,
        "remaining_this_hour": remaining_this_hour if hourly_cap > 0 else None,
        "weekend_hold": bool(CLOUD_SEND_SKIP_WEEKENDS),
        "next_capacity_utc": next_capacity_local.astimezone(timezone.utc) if next_capacity_local else None,
        "next_capacity_local": next_capacity_local,
        "capacity_reason": capacity_reason,
    }


def _send_summary_processed_count(summary: dict) -> int:
    keys = (
        "sent",
        "dry_run",
        "failed",
        "blocked",
        "review_required",
        "held",
        "deferred",
    )
    return sum(int(summary.get(key) or 0) for key in keys)


def _config_issue_message() -> str:
    bucket = str(GCS_BUCKET or "").strip()
    if not bucket:
        return "GCS_BUCKET is empty; cloud worker cannot see the manifest queue."
    if bucket in {"your-gcs-bucket-name", "example-bucket"} or "your-gcs-bucket" in bucket:
        return f"GCS_BUCKET is still a placeholder ({bucket}); worker is polling the wrong queue."
    return ""


def _reconcile_manifest_with_run_state(
    campaign_id: str,
    manifest_uri: str,
    state: dict,
    completed: set[str],
    failed: set[str],
) -> bool:
    """
    Return True when the manifest has been fully handled and should not continue
    through normal candidate preparation.
    """
    run_state = load_cloud_send_status(campaign_id)
    send_state = str(run_state.get("cloud_send_status") or "").strip().lower()

    if send_state == CLOUD_SEND_COMPLETED:
        print(f"[CloudWorker] Campaign already marked completed in run state, reconciling manifest: {campaign_id}")
        processed_uri = _mark_manifest_processed(manifest_uri, campaign_id)
        completed.add(campaign_id)
        sync_cloud_send_status(
            campaign_id,
            CLOUD_SEND_COMPLETED,
            details={
                "cloud_send_manifest_uri": manifest_uri,
                "cloud_send_processed_manifest_uri": processed_uri,
                "cloud_send_reconciled_at": _now_utc(),
            },
        )
        _update_worker_state(
            state,
            completed_campaigns=sorted(completed),
            last_success_at=_now_utc(),
            last_completed_campaign_id=campaign_id,
            last_processed_manifest_uri=processed_uri,
            last_reconciled_campaign_id=campaign_id,
            last_idle_reason="reconciled_completed_manifest",
            last_poll_result="reconciled_completed",
        )
        return True

    if send_state == CLOUD_SEND_FAILED:
        print(f"[CloudWorker] Campaign already marked failed in run state, skipping manifest: {campaign_id}")
        failed_uri = _mark_manifest_failed(manifest_uri, campaign_id)
        failed.add(campaign_id)
        sync_cloud_send_status(
            campaign_id,
            CLOUD_SEND_FAILED,
            error_message=run_state.get("cloud_send_error"),
            details={
                "cloud_send_manifest_uri": manifest_uri,
                "cloud_send_failed_manifest_uri": failed_uri,
                "cloud_send_reconciled_at": _now_utc(),
            },
        )
        _update_worker_state(
            state,
            failed_campaigns=sorted(failed),
            last_failed_campaign_id=campaign_id,
            last_idle_reason="awaiting_manual_recovery",
            last_reconciled_campaign_id=campaign_id,
            last_poll_result="reconciled_failed",
        )
        return True

    return False


def _list_json_uris(prefix_name: str) -> list[str]:
    prefix = _bucket_uri(prefix_name)
    try:
        result = _run_cmd(["storage", "ls", "--recursive", prefix], capture_output=True)
    except subprocess.CalledProcessError:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip().endswith(".json")]


def _list_manifest_uris() -> list[str]:
    return _list_json_uris(GCS_MANIFESTS_PREFIX)


def _list_inflight_manifest_uris() -> list[str]:
    return _list_json_uris(GCS_INFLIGHT_PREFIX)


def _download_manifest(manifest_uri: str) -> dict:
    with tempfile.TemporaryDirectory() as tmp:
        local_path = Path(tmp) / "manifest.json"
        _run_cmd(["storage", "cp", manifest_uri, str(local_path)])
        with open(local_path, encoding="utf-8") as f:
            return json.load(f)


def _sync_run_to_local(campaign_id: str) -> Path:
    target_dir = RUNS_DIR / campaign_id
    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    run_uri = _bucket_uri(GCS_RUNS_PREFIX, campaign_id)
    _run_cmd(["storage", "cp", "--recursive", f"{run_uri}/**", str(target_dir)])
    return target_dir


def _manifest_sync_key(manifest_uri: str, manifest: dict) -> str:
    uploaded_at = (manifest.get("uploaded_at") or "").strip()
    return f"{manifest_uri}|{uploaded_at}"


def _ensure_run_synced(
    campaign_id: str,
    manifest_uri: str,
    manifest: dict,
    state: dict,
) -> tuple[Path, bool]:
    sync_key = _manifest_sync_key(manifest_uri, manifest)
    synced = state.setdefault("synced_manifests", {})
    target_dir = RUNS_DIR / campaign_id
    if synced.get(campaign_id) == sync_key and target_dir.exists():
        return target_dir, False
    _sync_run_to_local(campaign_id)
    synced[campaign_id] = sync_key
    return target_dir, True


def _ensure_deploy_status_completed(campaign_id: str, manifest_uri: str) -> None:
    deploy_state = load_cloud_deploy_status(campaign_id)
    status = str(deploy_state.get("cloud_deploy_status") or "").strip().lower()
    if status == CLOUD_DEPLOY_COMPLETED:
        return

    run_uri = str(deploy_state.get("cloud_deploy_run_uri") or _bucket_uri(GCS_RUNS_PREFIX, campaign_id))
    sync_cloud_deploy_status(
        campaign_id,
        CLOUD_DEPLOY_COMPLETED,
        details={
            "cloud_deploy_run_uri": run_uri,
            "cloud_deploy_manifest_uri": manifest_uri,
            "cloud_deploy_reconciled_at": _now_utc(),
        },
    )


def _reconcile_local_deploy_statuses() -> None:
    if not RUNS_DIR.exists():
        return

    for run_dir in RUNS_DIR.iterdir():
        if not run_dir.is_dir():
            continue
        campaign_id = run_dir.name
        deploy_state = load_cloud_deploy_status(campaign_id)
        status = str(deploy_state.get("cloud_deploy_status") or "").strip().lower()
        if status == CLOUD_DEPLOY_COMPLETED:
            continue

        send_state = load_cloud_send_status(campaign_id)
        send_status = str(send_state.get("cloud_send_status") or "").strip().lower()
        if send_status not in {
            CLOUD_SEND_SYNCED,
            CLOUD_SEND_WAITING_WINDOW,
            CLOUD_SEND_SENDING,
            CLOUD_SEND_COMPLETED,
            CLOUD_SEND_FAILED,
        }:
            continue

        manifest_uri = str(
            send_state.get("cloud_send_manifest_uri")
            or deploy_state.get("cloud_deploy_manifest_uri")
            or ""
        ).strip()
        _ensure_deploy_status_completed(campaign_id, manifest_uri)


def _upload_run_outputs(campaign_id: str) -> dict[str, float | int]:
    run_dir = RUNS_DIR / campaign_id
    run_uri = _bucket_uri(GCS_RUNS_PREFIX, campaign_id)
    stats = _upload_directory(run_dir, run_uri)
    print(f"[CloudWorker] Uploaded updated outputs for {campaign_id} -> {run_uri}")
    return stats


def _claim_manifest(manifest_uri: str, campaign_id: str) -> str:
    claimed_uri = _bucket_uri(GCS_INFLIGHT_PREFIX, f"{campaign_id}.json")
    _run_cmd(["storage", "mv", manifest_uri, claimed_uri])
    print(f"[CloudWorker] Claimed manifest into inflight: {claimed_uri}")
    return claimed_uri


def _mark_manifest_processed(manifest_uri: str, campaign_id: str) -> str:
    processed_uri = _bucket_uri(GCS_PROCESSED_PREFIX, f"{campaign_id}-{int(time.time())}.json")
    _run_cmd(["storage", "mv", manifest_uri, processed_uri])
    print(f"[CloudWorker] Moved manifest to processed: {processed_uri}")
    return processed_uri


def _mark_manifest_failed(manifest_uri: str, campaign_id: str) -> str:
    failed_uri = _bucket_uri(GCS_FAILED_PREFIX, f"{campaign_id}-{int(time.time())}.json")
    _run_cmd(["storage", "mv", manifest_uri, failed_uri])
    print(f"[CloudWorker] Moved manifest to failed: {failed_uri}")
    return failed_uri


def _write_cloud_result(
    campaign_id: str,
    *,
    status: str,
    processed_manifest_uri: str = "",
    error_message: str | None = None,
) -> Path:
    run_dir = RUNS_DIR / campaign_id
    result_path = run_dir / "cloud_send_result.json"
    payload = {
        "campaign_id": campaign_id,
        "status": status,
        "completed_at": _now_utc(),
        "environment": "gcp_vm",
        "processed_manifest_uri": processed_manifest_uri,
        "error_message": error_message,
    }
    with open(result_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    return result_path


def _process_campaign(
    campaign_id: str,
    manifest_uri: str,
    ctx: dict,
    *,
    daily_limit_override: int | None = None,
    hourly_limit_override: int | None = None,
) -> dict[str, object]:
    print(f"[CloudWorker] Preparing campaign {campaign_id}")
    sync_cloud_send_status(
        campaign_id,
        CLOUD_SEND_SENDING,
        details={
            "cloud_send_started_at": _now_utc(),
            "cloud_send_manifest_uri": manifest_uri,
            "cloud_send_market": f"{ctx.get('city')}, {ctx.get('country')}".strip(", "),
            "cloud_send_timezone": ctx.get("timezone", ""),
        },
    )
    set_active_run(campaign_id)
    try:
        send_summary, _status_summary = _run_campaign_send(
            campaign_id,
            daily_limit_override=daily_limit_override,
            hourly_limit_override=hourly_limit_override,
        )
        upload_stats = _upload_run_outputs(campaign_id)
        processed_count = _send_summary_processed_count(send_summary)
        remaining_unprocessed = max(
            int(send_summary.get("remaining_unprocessed") or 0),
            max(int(send_summary.get("total") or 0) - processed_count, 0),
        )
        stopped_daily_limit = bool(int(send_summary.get("stopped_daily_limit") or 0))
        stopped_hourly_limit = bool(int(send_summary.get("stopped_hourly_limit") or 0))

        if remaining_unprocessed > 0 and (stopped_daily_limit or stopped_hourly_limit):
            result_path = _write_cloud_result(
                campaign_id,
                status="partial",
                error_message=(
                    "daily_cap_reached"
                    if stopped_daily_limit
                    else "hourly_cap_reached"
                ),
            )
            _upload_file(
                result_path,
                f"{_bucket_uri(GCS_RUNS_PREFIX, campaign_id).rstrip('/')}/cloud_send_result.json",
            )
            return {
                "completed": False,
                "processed_manifest_uri": "",
                "upload_stats": upload_stats,
                "send_summary": send_summary,
                "wait_reason": "daily_cap_reached" if stopped_daily_limit else "hourly_cap_reached",
                "remaining_unprocessed": remaining_unprocessed,
            }

        processed_uri = _mark_manifest_processed(manifest_uri, campaign_id)
        result_path = _write_cloud_result(
            campaign_id,
            status=CLOUD_SEND_COMPLETED,
            processed_manifest_uri=processed_uri,
        )
        _upload_file(result_path, f"{_bucket_uri(GCS_RUNS_PREFIX, campaign_id).rstrip('/')}/cloud_send_result.json")
        sync_cloud_send_status(
            campaign_id,
            CLOUD_SEND_COMPLETED,
            details={
                "cloud_send_completed_at": _now_utc(),
                "cloud_send_manifest_uri": manifest_uri,
                "cloud_send_processed_manifest_uri": processed_uri,
                "cloud_send_market": f"{ctx.get('city')}, {ctx.get('country')}".strip(", "),
                "cloud_send_timezone": ctx.get("timezone", ""),
                "cloud_send_upload_mode": "recursive_directory_cp",
                "cloud_send_uploaded_file_count": upload_stats["file_count"],
                "cloud_send_uploaded_bytes": upload_stats["total_bytes"],
                "cloud_send_upload_elapsed_seconds": upload_stats["elapsed_seconds"],
            },
        )
        return {
            "completed": True,
            "processed_manifest_uri": processed_uri,
            "upload_stats": upload_stats,
            "send_summary": send_summary,
            "wait_reason": "",
            "remaining_unprocessed": 0,
        }
    finally:
        clear_active_run()


def run_worker(poll_seconds: float = CLOUD_WORKER_POLL_SECONDS) -> None:
    state = _load_state()
    completed = set(state.get("completed_campaigns", []))
    failed = set(state.get("failed_campaigns", []))
    print(f"[CloudWorker] Started. Poll={poll_seconds}s")

    while True:
        now_utc = datetime.now(tz=timezone.utc)
        capacity = _build_inbox_capacity_snapshot(now_utc)
        _reconcile_local_deploy_statuses()
        config_issue = _config_issue_message()
        _update_worker_state(
            state,
            last_poll_at=_now_utc(),
            worker_bucket=GCS_BUCKET,
            worker_manifests_prefix=GCS_MANIFESTS_PREFIX,
            worker_config_ok=(config_issue == ""),
            worker_config_issue=config_issue,
            inbox_cap_timezone=str(capacity.get("cap_timezone") or "UTC"),
            inbox_daily_cap=int(capacity.get("daily_cap") or 0),
            inbox_hourly_cap=int(capacity.get("hourly_cap") or 0),
            inbox_sent_today=int(capacity.get("sent_today") or 0),
            inbox_remaining_today=(
                int(capacity.get("remaining_today") or 0)
                if capacity.get("remaining_today") is not None else -1
            ),
            inbox_sent_last_hour=int(capacity.get("sent_last_hour") or 0),
            inbox_remaining_this_hour=(
                int(capacity.get("remaining_this_hour") or 0)
                if capacity.get("remaining_this_hour") is not None else -1
            ),
            weekend_sending_enabled=not bool(CLOUD_SEND_SKIP_WEEKENDS),
            next_capacity_due_at=(
                capacity["next_capacity_utc"].isoformat()
                if capacity.get("next_capacity_utc") else ""
            ),
            next_capacity_reason=str(capacity.get("capacity_reason") or ""),
        )
        if config_issue:
            _update_worker_state(
                state,
                last_error_at=_now_utc(),
                last_idle_reason="config_error",
                last_poll_result="config_error",
                last_manifest_count=0,
                last_inflight_count=0,
                last_candidate_count=0,
                last_manifest_sample=[],
                last_inflight_sample=[],
                last_candidate_campaign_ids=[],
                last_selected_campaign_id="",
                last_selected_due_at="",
                claimed_campaign_id="",
                claimed_manifest_uri="",
            )
            _record_alert(
                level="error",
                event_type="worker_config_invalid",
                message=config_issue,
                details={
                    "bucket": GCS_BUCKET,
                    "manifests_prefix": GCS_MANIFESTS_PREFIX,
                },
            )
            time.sleep(poll_seconds)
            continue
        inflight_manifest_uris = _list_inflight_manifest_uris()
        manifest_uris = _list_manifest_uris()
        _update_worker_state(
            state,
            last_manifest_count=len(manifest_uris),
            last_manifest_sample=manifest_uris[:3],
            last_inflight_count=len(inflight_manifest_uris),
            last_inflight_sample=inflight_manifest_uris[:3],
        )
        if not inflight_manifest_uris and not manifest_uris:
            print("[CloudWorker] No manifests found. Sleeping.")
            _update_worker_state(
                state,
                last_idle_reason="no_manifests",
                last_poll_result="no_manifests",
                last_candidate_count=0,
                last_candidate_campaign_ids=[],
                last_wait_campaign_id="",
                last_wait_due_at="",
                last_selected_campaign_id="",
                last_selected_due_at="",
                claimed_campaign_id="",
                claimed_manifest_uri="",
            )
            time.sleep(poll_seconds)
            continue

        inflight_candidates: list[tuple[datetime, str, str, dict]] = []
        visible_candidates: list[tuple[datetime, str, str, dict]] = []
        inflight_email_count = 0
        manifest_email_count = 0
        source_manifest_uris = [*inflight_manifest_uris, *manifest_uris]

        for manifest_uri in source_manifest_uris:
            try:
                manifest = _download_manifest(manifest_uri)
            except Exception as exc:
                print(f"[CloudWorker] Failed to load manifest {manifest_uri}: {exc}")
                _update_worker_state(state, last_error_at=_now_utc())
                _record_alert(
                    level="error",
                    event_type="manifest_load_failed",
                    message=str(exc),
                    manifest_uri=manifest_uri,
                )
                continue

            campaign_id = (manifest.get("campaign_id") or "").strip()
            if not campaign_id:
                print(f"[CloudWorker] Invalid manifest, missing campaign_id: {manifest_uri}")
                _update_worker_state(state, last_error_at=_now_utc())
                _record_alert(
                    level="error",
                    event_type="manifest_invalid",
                    message="Manifest missing campaign_id",
                    manifest_uri=manifest_uri,
                )
                continue
            try:
                if _reconcile_manifest_with_run_state(campaign_id, manifest_uri, state, completed, failed):
                    continue
            except Exception as exc:
                print(f"[CloudWorker] Failed to reconcile manifest {campaign_id}: {exc}")
                _update_worker_state(
                    state,
                    last_error_at=_now_utc(),
                    last_failed_campaign_id=campaign_id,
                    last_poll_result="manifest_reconcile_failed",
                )
                _record_alert(
                    level="error",
                    event_type="manifest_reconcile_failed",
                    message=str(exc),
                    campaign_id=campaign_id,
                    manifest_uri=manifest_uri,
                )
                continue
            # Do not short-circuit purely on the worker's historical memory of
            # completed/failed campaigns. A campaign may be re-deployed with the
            # same campaign_id after operator recovery, which creates a fresh
            # manifest and resets run-scoped cloud_send_status to queued.
            #
            # True stale completed/failed manifests are already handled by
            # _reconcile_manifest_with_run_state() above; if that returned False,
            # the current run state is not terminal and this manifest should be
            # allowed back through candidate preparation.

            try:
                _, did_sync = _ensure_run_synced(campaign_id, manifest_uri, manifest, state)
                _ensure_deploy_status_completed(campaign_id, manifest_uri)
                if did_sync:
                    sync_cloud_send_status(
                        campaign_id,
                        CLOUD_SEND_SYNCED,
                        details={
                            "cloud_send_manifest_uri": manifest_uri,
                            "cloud_send_synced_at": _now_utc(),
                        },
                    )
                    _update_worker_state(
                        state,
                        synced_manifests=state.get("synced_manifests", {}),
                        last_sync_campaign_id=campaign_id,
                    )
                due, ctx = _campaign_next_due(campaign_id)
            except Exception as exc:
                print(f"[CloudWorker] Failed to prepare campaign {campaign_id}: {exc}")
                sync_cloud_send_status(
                    campaign_id,
                    CLOUD_SEND_FAILED,
                    error_message=str(exc),
                    details={
                        "cloud_send_manifest_uri": manifest_uri,
                        "cloud_send_failed_at": _now_utc(),
                        "cloud_send_failed_stage": "prepare",
                    },
                )
                failed_uri = ""
                try:
                    failed_uri = _mark_manifest_failed(manifest_uri, campaign_id)
                except Exception as move_exc:
                    print(f"[CloudWorker] Failed to move prepare-failed manifest for {campaign_id}: {move_exc}")
                if failed_uri:
                    sync_cloud_send_status(
                        campaign_id,
                        CLOUD_SEND_FAILED,
                        error_message=str(exc),
                        details={
                            "cloud_send_manifest_uri": manifest_uri,
                            "cloud_send_failed_manifest_uri": failed_uri,
                            "cloud_send_failed_at": _now_utc(),
                            "cloud_send_failed_stage": "prepare",
                        },
                    )
                failed.add(campaign_id)
                state.setdefault("synced_manifests", {}).pop(campaign_id, None)
                _update_worker_state(state, failed_campaigns=sorted(failed))
                _update_worker_state(
                    state,
                    last_error_at=_now_utc(),
                    last_poll_result="prepare_failed",
                )
                _record_alert(
                    level="error",
                    event_type="campaign_prepare_failed",
                    message=str(exc),
                    campaign_id=campaign_id,
                    manifest_uri=manifest_uri,
                )
                continue

            queue_count = int(ctx.get("queue_count") or 0)
            if manifest_uri in inflight_manifest_uris:
                inflight_email_count += queue_count
                inflight_candidates.append((due, campaign_id, manifest_uri, ctx))
            else:
                manifest_email_count += queue_count
                visible_candidates.append((due, campaign_id, manifest_uri, ctx))

        _update_worker_state(
            state,
            last_inflight_email_count=inflight_email_count,
            last_manifest_email_count=manifest_email_count,
            last_live_email_count=inflight_email_count + manifest_email_count,
            last_carryover_email_count=inflight_email_count + manifest_email_count,
        )

        if inflight_manifest_uris and not inflight_candidates:
            print("[CloudWorker] No actionable inflight manifests found. Sleeping.")
            _update_worker_state(
                state,
                last_idle_reason="no_actionable_inflight_manifests",
                last_poll_result="no_actionable_inflight_manifests",
                last_candidate_count=0,
                last_candidate_campaign_ids=[],
                last_wait_campaign_id="",
                last_wait_due_at="",
                last_selected_campaign_id="",
                last_selected_due_at="",
                claimed_campaign_id="",
                claimed_manifest_uri="",
            )
            time.sleep(poll_seconds)
            continue

        if not inflight_manifest_uris and not visible_candidates:
            print("[CloudWorker] No actionable manifests found. Sleeping.")
            _update_worker_state(
                state,
                last_idle_reason="no_actionable_manifests",
                last_poll_result="no_actionable_manifests",
                last_candidate_count=0,
                last_candidate_campaign_ids=[],
                last_wait_campaign_id="",
                last_wait_due_at="",
                last_selected_campaign_id="",
                last_selected_due_at="",
                claimed_campaign_id="",
                claimed_manifest_uri="",
            )
            time.sleep(poll_seconds)
            continue

        candidates = inflight_candidates if inflight_candidates else visible_candidates
        candidates.sort(key=lambda item: item[0])
        due, campaign_id, manifest_uri, ctx = candidates[0]

        if not inflight_candidates:
            try:
                manifest_uri = _claim_manifest(manifest_uri, campaign_id)
            except Exception as exc:
                print(f"[CloudWorker] Failed to claim manifest {campaign_id}: {exc}")
                _update_worker_state(
                    state,
                    last_error_at=_now_utc(),
                    last_failed_campaign_id=campaign_id,
                    last_poll_result="manifest_claim_failed",
                )
                _record_alert(
                    level="error",
                    event_type="manifest_claim_failed",
                    message=str(exc),
                    campaign_id=campaign_id,
                    manifest_uri=manifest_uri,
                )
                time.sleep(poll_seconds)
                continue

        _update_worker_state(
            state,
            last_candidate_count=len(candidates),
            last_candidate_campaign_ids=[campaign for _, campaign, _, _ in candidates[:5]],
            claimed_campaign_id=campaign_id,
            claimed_manifest_uri=manifest_uri,
        )
        selection_now = datetime.now(tz=timezone.utc)
        capacity_due = capacity.get("next_capacity_utc")
        effective_due = due
        wait_reason = "market_window"
        if isinstance(capacity_due, datetime) and capacity_due > effective_due:
            effective_due = capacity_due
            wait_reason = str(capacity.get("capacity_reason") or "capacity_hold")

        if effective_due > selection_now:
            wait_seconds = max((effective_due - selection_now).total_seconds(), 0.0)
            capped_wait = min(wait_seconds, poll_seconds)
            sync_cloud_send_status(
                campaign_id,
                CLOUD_SEND_WAITING_WINDOW,
                details={
                    "cloud_send_manifest_uri": manifest_uri,
                    "cloud_send_due_at": effective_due.isoformat(),
                    "cloud_send_wait_seconds": round(wait_seconds, 1),
                    "cloud_send_market": f"{ctx.get('city')}, {ctx.get('country')}".strip(", "),
                    "cloud_send_timezone": ctx.get("timezone", ""),
                    "cloud_send_wait_reason": wait_reason,
                    "cloud_send_inbox_daily_cap": int(capacity.get("daily_cap") or 0),
                    "cloud_send_inbox_sent_today": int(capacity.get("sent_today") or 0),
                    "cloud_send_inbox_remaining_today": int(capacity.get("remaining_today") or 0)
                    if capacity.get("remaining_today") is not None else None,
                    "cloud_send_inbox_hourly_cap": int(capacity.get("hourly_cap") or 0),
                    "cloud_send_inbox_sent_last_hour": int(capacity.get("sent_last_hour") or 0),
                    "cloud_send_inbox_remaining_this_hour": int(capacity.get("remaining_this_hour") or 0)
                    if capacity.get("remaining_this_hour") is not None else None,
                },
            )
            _update_worker_state(
                state,
                last_idle_reason=wait_reason if wait_reason != "market_window" else "waiting_window",
                last_wait_campaign_id=campaign_id,
                last_wait_due_at=effective_due.isoformat(),
                last_poll_result=wait_reason if wait_reason != "market_window" else "waiting_window",
                last_selected_campaign_id=campaign_id,
                last_selected_due_at=effective_due.isoformat(),
                claimed_campaign_id=campaign_id,
                claimed_manifest_uri=manifest_uri,
            )
            print(
                f"[CloudWorker] Next campaign window: {campaign_id} at {effective_due.isoformat()} UTC | "
                f"market={ctx.get('city')}, {ctx.get('country')} | tz={ctx.get('timezone')} | "
                f"reason={wait_reason} | "
                f"sleeping {capped_wait:.0f}s"
            )
            time.sleep(capped_wait)
            continue

        try:
            _update_worker_state(
                state,
                active_campaign_id=campaign_id,
                last_idle_reason="sending",
                last_wait_campaign_id="",
                last_wait_due_at="",
                last_poll_result="sending",
                last_selected_campaign_id=campaign_id,
                last_selected_due_at=effective_due.isoformat(),
                claimed_campaign_id=campaign_id,
                claimed_manifest_uri=manifest_uri,
            )
            process_result = _process_campaign(
                campaign_id,
                manifest_uri,
                ctx,
                daily_limit_override=(
                    int(capacity.get("remaining_today") or 0)
                    if capacity.get("remaining_today") is not None else None
                ),
                hourly_limit_override=(
                    int(capacity.get("remaining_this_hour") or 0)
                    if capacity.get("remaining_this_hour") is not None else None
                ),
            )
            if bool(process_result.get("completed")):
                processed_uri = str(process_result.get("processed_manifest_uri") or "")
                completed.add(campaign_id)
                failed.discard(campaign_id)
                state.setdefault("synced_manifests", {}).pop(campaign_id, None)
                _update_worker_state(
                    state,
                    completed_campaigns=sorted(completed),
                    failed_campaigns=sorted(failed),
                    last_success_at=_now_utc(),
                    active_campaign_id="",
                    last_completed_campaign_id=campaign_id,
                    last_processed_manifest_uri=processed_uri,
                    last_idle_reason="",
                    last_poll_result="completed_campaign",
                    last_selected_campaign_id=campaign_id,
                    last_selected_due_at=effective_due.isoformat(),
                    claimed_campaign_id="",
                    claimed_manifest_uri="",
                )
            else:
                capacity_after = _build_inbox_capacity_snapshot(datetime.now(tz=timezone.utc))
                next_due_after = capacity_after.get("next_capacity_utc") or datetime.now(tz=timezone.utc)
                wait_reason_after = str(process_result.get("wait_reason") or capacity_after.get("capacity_reason") or "capacity_hold")
                remaining_after = int(process_result.get("remaining_unprocessed") or 0)
                sync_cloud_send_status(
                    campaign_id,
                    CLOUD_SEND_WAITING_WINDOW,
                    details={
                        "cloud_send_manifest_uri": manifest_uri,
                        "cloud_send_due_at": next_due_after.isoformat(),
                        "cloud_send_wait_seconds": max(
                            round((next_due_after - datetime.now(tz=timezone.utc)).total_seconds(), 1),
                            0.0,
                        ),
                        "cloud_send_market": f"{ctx.get('city')}, {ctx.get('country')}".strip(", "),
                        "cloud_send_timezone": ctx.get("timezone", ""),
                        "cloud_send_wait_reason": wait_reason_after,
                        "cloud_send_remaining_records": remaining_after,
                    },
                )
                _update_worker_state(
                    state,
                    last_success_at=_now_utc(),
                    active_campaign_id="",
                    last_idle_reason=wait_reason_after,
                    last_poll_result=wait_reason_after,
                    last_wait_campaign_id=campaign_id,
                    last_wait_due_at=next_due_after.isoformat(),
                    last_selected_campaign_id=campaign_id,
                    last_selected_due_at=next_due_after.isoformat(),
                    claimed_campaign_id=campaign_id,
                    claimed_manifest_uri=manifest_uri,
                    last_carryover_email_count=remaining_after + manifest_email_count + inflight_email_count,
                )
        except Exception as exc:
            print(f"[CloudWorker] Campaign failed: {campaign_id}: {exc}")
            result_path = _write_cloud_result(campaign_id, status=CLOUD_SEND_FAILED, error_message=str(exc))
            try:
                upload_stats = _upload_run_outputs(campaign_id)
                _upload_file(result_path, f"{_bucket_uri(GCS_RUNS_PREFIX, campaign_id).rstrip('/')}/cloud_send_result.json")
            except Exception as upload_exc:
                print(f"[CloudWorker] Failed to upload failure artifacts for {campaign_id}: {upload_exc}")
                upload_stats = None
            sync_cloud_send_status(
                campaign_id,
                CLOUD_SEND_FAILED,
                error_message=str(exc),
                details={
                    "cloud_send_manifest_uri": manifest_uri,
                    "cloud_send_failed_at": _now_utc(),
                    "cloud_send_failed_stage": "send",
                    "cloud_send_market": f"{ctx.get('city')}, {ctx.get('country')}".strip(", "),
                    "cloud_send_timezone": ctx.get("timezone", ""),
                    "cloud_send_upload_mode": "recursive_directory_cp",
                    "cloud_send_uploaded_file_count": int(upload_stats["file_count"]) if upload_stats else 0,
                    "cloud_send_uploaded_bytes": int(upload_stats["total_bytes"]) if upload_stats else 0,
                    "cloud_send_upload_elapsed_seconds": float(upload_stats["elapsed_seconds"]) if upload_stats else 0.0,
                },
            )
            failed_uri = ""
            try:
                failed_uri = _mark_manifest_failed(manifest_uri, campaign_id)
            except Exception as move_exc:
                print(f"[CloudWorker] Failed to move failed manifest for {campaign_id}: {move_exc}")
            if failed_uri:
                sync_cloud_send_status(
                    campaign_id,
                    CLOUD_SEND_FAILED,
                    error_message=str(exc),
                    details={
                        "cloud_send_manifest_uri": manifest_uri,
                        "cloud_send_failed_manifest_uri": failed_uri,
                        "cloud_send_failed_at": _now_utc(),
                        "cloud_send_failed_stage": "send",
                    },
                )
            failed.add(campaign_id)
            state.setdefault("synced_manifests", {}).pop(campaign_id, None)
            _update_worker_state(
                state,
                last_error_at=_now_utc(),
                active_campaign_id="",
                failed_campaigns=sorted(failed),
                last_failed_campaign_id=campaign_id,
                last_idle_reason="awaiting_manual_recovery",
                last_poll_result="send_failed",
                last_selected_campaign_id=campaign_id,
                last_selected_due_at=due.isoformat(),
                claimed_campaign_id="",
                claimed_manifest_uri="",
            )
            _record_alert(
                level="error",
                event_type="campaign_send_failed",
                message=str(exc),
                campaign_id=campaign_id,
                manifest_uri=manifest_uri,
                details={
                    "market": f"{ctx.get('city')}, {ctx.get('country')}".strip(", "),
                    "timezone": ctx.get("timezone", ""),
                },
            )


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Google Cloud VM send worker")
    parser.add_argument(
        "--test-alert",
        action="store_true",
        help="Send a test cloud worker alert through configured alert channels and exit",
    )
    parser.add_argument(
        "--poll",
        type=float,
        default=CLOUD_WORKER_POLL_SECONDS,
        help="Idle poll interval in seconds",
    )
    args = parser.parse_args()
    if args.test_alert:
        _record_alert(
            level="info",
            event_type="test_alert",
            message="Manual cloud worker alert test.",
            details={
                "source": "cloud_send_worker.py",
                "alert_email_to": CLOUD_WORKER_ALERT_EMAIL_TO,
                "alert_email_mode": CLOUD_WORKER_ALERT_EMAIL_MODE,
                "alert_webhook_enabled": bool(CLOUD_WORKER_ALERT_WEBHOOK),
            },
        )
        print("[CloudWorker] Test alert recorded and delivery attempted.")
        return
    run_worker(poll_seconds=args.poll)


if __name__ == "__main__":
    main()
