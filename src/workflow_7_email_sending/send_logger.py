# Workflow 7: Email Sending — Send Logger
# Persistent append-only log for all send attempts (sent, failed, blocked, deferred).

import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config.settings import SEND_LOGS_FILE

LOG_FIELDS = [
    "timestamp",
    "campaign_id",
    "send_mode",
    "company_name",
    "place_id",
    "city",
    "region",
    "country",
    "source_location",
    "kp_name",
    "kp_title",
    "kp_email",
    "contact_name",
    "contact_title",
    "contact_email",
    "send_target_type",
    "contact_source",
    "contact_quality",
    "subject",
    "send_decision",
    "send_status",
    "decision_reason",
    "provider",
    "provider_message_id",
    "error_message",
    "tracking_id",
    "message_id",
    # P1-3B — queue policy fields (empty for pre-P1-3B log rows)
    "send_policy_action",
    "send_policy_reason",
]

DEDUP_WINDOW_HOURS = 24


def _now_utc() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _cutoff_utc(hours: int = DEDUP_WINDOW_HOURS) -> datetime:
    return datetime.now(tz=timezone.utc) - timedelta(hours=hours)


def _parse_ts(raw: str) -> datetime | None:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.strip())
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Core I/O
# ---------------------------------------------------------------------------

def load_send_logs(path: Path = SEND_LOGS_FILE) -> list[dict]:
    """Load all rows from send_logs.csv. Returns [] if file missing."""
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _rewrite_with_current_header(path: Path) -> None:
    """Upgrade legacy send_logs.csv files to the current LOG_FIELDS header."""
    rows = load_send_logs(path)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=LOG_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            normalized = {field: row.get(field, "") for field in LOG_FIELDS}
            writer.writerow(normalized)


def append_send_log(row: dict, path: Path = SEND_LOGS_FILE) -> None:
    """Append one row to send_logs.csv. Creates file with header if missing."""
    file_exists = path.exists()
    if file_exists:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            existing_fields = list(reader.fieldnames or [])
        if existing_fields != LOG_FIELDS:
            _rewrite_with_current_header(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=LOG_FIELDS, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def build_log_row(
    record: dict,
    send_decision: str,
    send_status: str,
    decision_reason: str = "",
    provider: str = "",
    provider_message_id: str = "",
    error_message: str = "",
    tracking_id: str = "",
    message_id: str = "",
    campaign_id: str = "",
    send_mode: str = "",
    send_policy_action: str = "",
    send_policy_reason: str = "",
) -> dict:
    """Build a complete log row dict from a send record and outcome."""
    return {
        "timestamp":           _now_utc(),
        "campaign_id":         campaign_id,
        "send_mode":           send_mode,
        "company_name":        record.get("company_name", ""),
        "place_id":            record.get("place_id", ""),
        "city":                record.get("city", ""),
        "region":              record.get("region", ""),
        "country":             record.get("country", ""),
        "source_location":     record.get("source_location", ""),
        "kp_name":             record.get("kp_name", ""),
        "kp_title":            record.get("kp_title", ""),
        "kp_email":            record.get("kp_email", ""),
        "contact_name":        record.get("contact_name", record.get("kp_name", "")),
        "contact_title":       record.get("contact_title", record.get("kp_title", "")),
        "contact_email":       record.get("contact_email", record.get("kp_email", "")),
        "send_target_type":    record.get("send_target_type", ""),
        "contact_source":      record.get("contact_source", ""),
        "contact_quality":     record.get("contact_quality", ""),
        "subject":             record.get("subject", ""),
        "send_decision":       send_decision,
        "send_status":         send_status,
        "decision_reason":     decision_reason,
        "provider":            provider,
        "provider_message_id": provider_message_id,
        "error_message":       error_message,
        "tracking_id":         tracking_id,
        "message_id":          message_id,
        "send_policy_action":  send_policy_action,
        "send_policy_reason":  send_policy_reason,
    }


# ---------------------------------------------------------------------------
# Dedup helpers (used by send_guard and pipeline)
# ---------------------------------------------------------------------------

def load_recent_logs(hours: int = DEDUP_WINDOW_HOURS,
                     path: Path = SEND_LOGS_FILE) -> list[dict]:
    """Return log rows whose timestamp falls within the last `hours` hours."""
    cutoff = _cutoff_utc(hours)
    recent: list[dict] = []
    for row in load_send_logs(path):
        ts = _parse_ts(row.get("timestamp", ""))
        if ts and ts >= cutoff:
            recent.append(row)
    return recent


def sent_recently(kp_email: str, subject: str,
                  hours: int = DEDUP_WINDOW_HOURS,
                  path: Path = SEND_LOGS_FILE) -> bool:
    """Return True if same kp_email+subject was sent/dry-run within `hours`."""
    em  = kp_email.lower().strip()
    sub = subject.lower().strip()
    for row in load_recent_logs(hours, path):
        if row.get("send_status") not in ("sent", "dry_run"):
            continue
        if (row.get("kp_email") or "").lower().strip() == em and \
           (row.get("subject")  or "").lower().strip() == sub:
            return True
    return False


def company_sent_recently(place_id: str, company_name: str,
                          hours: int = DEDUP_WINDOW_HOURS,
                          path: Path = SEND_LOGS_FILE) -> bool:
    """Return True if same company (place_id or name) was contacted within `hours`."""
    pid  = place_id.strip()
    name = company_name.lower().strip()
    for row in load_recent_logs(hours, path):
        if row.get("send_status") not in ("sent", "dry_run"):
            continue
        log_pid  = (row.get("place_id")      or "").strip()
        log_name = (row.get("company_name")  or "").lower().strip()
        if pid and log_pid and pid == log_pid:
            return True
        if not pid and name and log_name == name:
            return True
    return False
