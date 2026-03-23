# Workflow 8: Follow-up Automation — Follow-up Scheduler
# Computes due dates and determines which candidates are ready to queue now.

from datetime import datetime, timedelta, timezone
from pathlib import Path

from config.settings import (
    FOLLOWUP_1_DELAY_DAYS,
    FOLLOWUP_2_DELAY_DAYS,
    FOLLOWUP_3_DELAY_DAYS,
)

_DELAY_MAP: dict[str, int] = {
    "followup_1": FOLLOWUP_1_DELAY_DAYS,
    "followup_2": FOLLOWUP_2_DELAY_DAYS,
    "followup_3": FOLLOWUP_3_DELAY_DAYS,
}


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


def get_delay_days(followup_stage: str) -> int:
    """Return the configured delay in days for this follow-up stage."""
    return _DELAY_MAP.get(followup_stage, FOLLOWUP_1_DELAY_DAYS)


def compute_due_date(last_send_time: str, followup_stage: str) -> datetime | None:
    """
    Compute the datetime when this follow-up becomes due.

    Returns None if last_send_time cannot be parsed.
    """
    ts = _parse_ts(last_send_time)
    if ts is None:
        return None
    delay = get_delay_days(followup_stage)
    return ts + timedelta(days=delay)


def is_due(due_date: datetime, now: datetime | None = None) -> bool:
    """Return True if due_date is at or before now."""
    now = now or datetime.now(tz=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return now >= due_date


def build_followup_schedule(
    candidate: dict,
    now: datetime | None = None,
) -> dict:
    """
    Compute schedule fields for one follow-up candidate.

    Returns a dict with:
        due_date         — ISO string of when follow-up is due
        is_due           — bool: is the follow-up due right now?
        scheduled_action — "queue_now" / "wait" / "blocked"
        schedule_reason  — human-readable explanation
    """
    now = now or datetime.now(tz=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    stage          = candidate.get("followup_stage", "")
    last_send_time = candidate.get("last_send_time", "")

    due_date = compute_due_date(last_send_time, stage)

    if due_date is None:
        return {
            "due_date":         "",
            "is_due":           False,
            "scheduled_action": "blocked",
            "schedule_reason":  "Cannot parse last_send_time",
        }

    due_iso = due_date.isoformat()
    due     = is_due(due_date, now)
    delay   = get_delay_days(stage)

    if due:
        return {
            "due_date":         due_iso,
            "is_due":           True,
            "scheduled_action": "queue_now",
            "schedule_reason":  f"{stage} due {delay}d after last send",
        }
    else:
        days_remaining = (due_date - now).days
        return {
            "due_date":         due_iso,
            "is_due":           False,
            "scheduled_action": "wait",
            "schedule_reason":  f"Due in ~{days_remaining}d ({due_iso[:10]})",
        }
