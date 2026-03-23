# Workflow 8.5: Campaign Status Aggregator — Data Loader
# Loads and normalises all campaign input files into per-contact record dicts.
#
# Primary join key: place_id → kp_email → company_name (normalised lowercase)

import csv
import json
from pathlib import Path

from config.settings import (
    SEND_LOGS_FILE, ENGAGEMENT_SUMMARY_FILE,
    FOLLOWUP_LOGS_FILE, FOLLOWUP_QUEUE_FILE, FOLLOWUP_BLOCKED_FILE,
    FINAL_SEND_QUEUE_FILE, ENRICHED_LEADS_FILE,
)


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _norm(v: str) -> str:
    return (v or "").strip().lower()


def _key(record: dict) -> str:
    """Return the best available join key for a record."""
    pid = (record.get("place_id") or "").strip()
    if pid:
        return f"pid:{pid}"
    email = _norm(record.get("kp_email") or record.get("email") or "")
    if email:
        return f"email:{email}"
    name = _norm(record.get("company_name") or "")
    return f"name:{name}" if name else "unknown"


# ---------------------------------------------------------------------------
# Individual loaders
# ---------------------------------------------------------------------------

def load_send_logs(
    path: Path = SEND_LOGS_FILE,
    campaign_id: str = "",
) -> dict[str, dict]:
    """
    Load send_logs.csv.
    Returns {key → most-recent send_log row} (one per contact; keeps latest timestamp).

    campaign_id: when non-empty, only include rows stamped with that campaign_id.
    Rows without a campaign_id field (legacy rows) are excluded when filtering.
    Pass campaign_id="" to load all rows regardless of campaign (global CRM mode).
    """
    rows = _read_csv(path)
    result: dict[str, dict] = {}
    for row in rows:
        status = (row.get("send_status") or "").strip()
        if status not in {"sent", "dry_run"}:
            continue
        # Campaign-scoped filter: only include rows that belong to this campaign
        if campaign_id:
            row_cid = (row.get("campaign_id") or "").strip()
            if row_cid != campaign_id:
                continue
        k = _key(row)
        existing = result.get(k)
        if existing is None or row.get("timestamp", "") > existing.get("timestamp", ""):
            result[k] = row
    return result


def load_engagement(path: Path = ENGAGEMENT_SUMMARY_FILE) -> dict[str, dict]:
    """
    Load engagement_summary.csv.
    Returns {tracking_id → engagement row}.
    """
    rows = _read_csv(path)
    return {r.get("tracking_id", ""): r for r in rows if r.get("tracking_id")}


def load_followup_logs(
    path: Path = FOLLOWUP_LOGS_FILE,
    campaign_id: str = "",
) -> dict[str, dict]:
    """
    Load followup_logs.csv.
    Returns {key → latest followup_log row with decision=="followup"}.

    campaign_id: when non-empty, only rows stamped with that campaign_id are
    included.  Rows with an empty campaign_id field (written before this fix
    was applied) are excluded when filtering — they cannot be attributed to
    any specific campaign and must not contaminate per-campaign status reports.

    Pass campaign_id="" to load all rows regardless of campaign (global mode).
    """
    rows = _read_csv(path)
    result: dict[str, dict] = {}
    for row in rows:
        if (row.get("decision") or "").strip() != "followup":
            continue
        # Campaign-scoped filter: exclude rows that belong to a different campaign
        # or that have no campaign_id (pre-fix / legacy entries).
        if campaign_id:
            row_cid = (row.get("campaign_id") or "").strip()
            if row_cid != campaign_id:
                continue
        k = f"email:{_norm(row.get('kp_email', ''))}"
        existing = result.get(k)
        if existing is None or row.get("timestamp", "") > existing.get("timestamp", ""):
            result[k] = row
    return result


def load_followup_queue(path: Path = FOLLOWUP_QUEUE_FILE) -> dict[str, dict]:
    """
    Load followup_queue.csv.
    Returns {key → queue row} (most recently added per contact).
    """
    rows = _read_csv(path)
    result: dict[str, dict] = {}
    for row in rows:
        k = _key(row)
        result[k] = row          # last writer wins; queue is re-generated each run
    return result


def load_followup_blocked(path: Path = FOLLOWUP_BLOCKED_FILE) -> dict[str, dict]:
    """
    Load followup_blocked.csv.
    Returns {key → blocked row}.
    """
    rows = _read_csv(path)
    result: dict[str, dict] = {}
    for row in rows:
        k = f"email:{_norm(row.get('kp_email', ''))}"
        result[k] = row
    return result


def load_final_send_queue(path: Path = FINAL_SEND_QUEUE_FILE) -> dict[str, dict]:
    """
    Load final_send_queue.csv.
    Returns {key → final_send_queue row}.
    """
    rows = _read_csv(path)
    result: dict[str, dict] = {}
    for row in rows:
        k = _key(row)
        result[k] = row
    return result


def load_enriched_leads(path: Path = ENRICHED_LEADS_FILE) -> dict[str, dict]:
    """
    Load enriched_leads.csv.
    Returns {key → enriched_lead row}.
    """
    rows = _read_csv(path)
    result: dict[str, dict] = {}
    for row in rows:
        k = _key(row)
        result[k] = row
    return result


# ---------------------------------------------------------------------------
# Combined load
# ---------------------------------------------------------------------------

def load_all(
    send_logs_path:     Path = SEND_LOGS_FILE,
    engagement_path:    Path = ENGAGEMENT_SUMMARY_FILE,
    followup_logs_path: Path = FOLLOWUP_LOGS_FILE,
    followup_queue_path: Path = FOLLOWUP_QUEUE_FILE,
    followup_blocked_path: Path = FOLLOWUP_BLOCKED_FILE,
    final_send_queue_path: Path = FINAL_SEND_QUEUE_FILE,
    enriched_leads_path: Path = ENRICHED_LEADS_FILE,
    campaign_id: str = "",
) -> dict[str, dict]:
    """
    Returns a dict of all input tables keyed by table name.
    Caller (status_merger) decides how to merge them.

    campaign_id: when non-empty, send_logs is filtered to this campaign only.
    """
    return {
        "send_logs":        load_send_logs(send_logs_path, campaign_id=campaign_id),
        "engagement":       load_engagement(engagement_path),
        "followup_logs":    load_followup_logs(followup_logs_path, campaign_id=campaign_id),
        "followup_queue":   load_followup_queue(followup_queue_path),
        "followup_blocked": load_followup_blocked(followup_blocked_path),
        "final_send_queue": load_final_send_queue(final_send_queue_path),
        "enriched_leads":   load_enriched_leads(enriched_leads_path),
    }
