"""
Workflow 9 — Campaign Runner: Step Logger

Appends human-readable step events to data/campaign_runner_logs.csv.
"""
from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path

from config.settings import CAMPAIGN_RUNNER_LOGS_FILE

_FIELDS = ["timestamp", "campaign_id", "step_name", "status", "message"]

# Valid status values
LOG_STARTED   = "started"
LOG_COMPLETED = "completed"
LOG_SKIPPED   = "skipped"
LOG_FAILED    = "failed"


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def append_campaign_log(
    campaign_id: str,
    step_name: str,
    status: str,
    message: str = "",
    path: Path = CAMPAIGN_RUNNER_LOGS_FILE,
) -> None:
    """
    Append one log row to campaign_runner_logs.csv.
    Creates the file with a header row if it does not exist.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            "timestamp":   _now(),
            "campaign_id": campaign_id,
            "step_name":   step_name,
            "status":      status,
            "message":     message,
        })


def load_campaign_logs(
    campaign_id: str | None = None,
    path: Path = CAMPAIGN_RUNNER_LOGS_FILE,
) -> list[dict]:
    """
    Load campaign log rows, optionally filtered by campaign_id.
    Returns an empty list if the file does not exist.
    """
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if campaign_id:
        rows = [r for r in rows if r.get("campaign_id") == campaign_id]
    return rows
