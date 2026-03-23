# Workflow 7.5: Engagement Tracking — Engagement Logger
# Append-only event log for email open and click events.

import csv
from datetime import datetime, timezone
from pathlib import Path

from config.settings import ENGAGEMENT_LOGS_FILE

LOG_FIELDS = [
    "timestamp",
    "tracking_id",
    "message_id",
    "company_name",
    "kp_email",
    "event_type",    # open / click
    "target_url",
    "ip",
    "user_agent",
]


def _now_utc() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def load_engagement_logs(path: Path = ENGAGEMENT_LOGS_FILE) -> list[dict]:
    """Load all engagement event rows. Returns [] if file missing."""
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def append_engagement_event(row: dict, path: Path = ENGAGEMENT_LOGS_FILE) -> None:
    """Append one event row. Creates file with header if missing."""
    file_exists = path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=LOG_FIELDS, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def build_event_row(
    tracking_id: str,
    event_type: str,
    message_id: str = "",
    company_name: str = "",
    kp_email: str = "",
    target_url: str = "",
    ip: str = "",
    user_agent: str = "",
) -> dict:
    return {
        "timestamp":    _now_utc(),
        "tracking_id":  tracking_id,
        "message_id":   message_id,
        "company_name": company_name,
        "kp_email":     kp_email,
        "event_type":   event_type,
        "target_url":   target_url,
        "ip":           ip,
        "user_agent":   user_agent,
    }
