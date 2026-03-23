# Workflow 7.5: Engagement Tracking — Engagement Aggregator
# Aggregates event-level logs into per-email engagement summaries.

import csv
from pathlib import Path

from config.settings import ENGAGEMENT_LOGS_FILE, ENGAGEMENT_SUMMARY_FILE
from src.workflow_7_5_engagement_tracking.engagement_logger import load_engagement_logs

SUMMARY_FIELDS = [
    "tracking_id",
    "message_id",
    "company_name",
    "kp_email",
    "open_count",
    "first_open_time",
    "last_open_time",
    "click_count",
    "first_click_time",
    "last_click_time",
]


def aggregate_engagement_logs(
    logs_path: Path = ENGAGEMENT_LOGS_FILE,
) -> list[dict]:
    """
    Aggregate event rows into one summary row per tracking_id.
    Returns list of summary dicts sorted by tracking_id.
    """
    rows = load_engagement_logs(logs_path)
    if not rows:
        return []

    groups: dict[str, dict] = {}
    for row in rows:
        tid = row.get("tracking_id", "").strip()
        if not tid:
            continue
        if tid not in groups:
            groups[tid] = {
                "tracking_id":  tid,
                "message_id":   row.get("message_id", ""),
                "company_name": row.get("company_name", ""),
                "kp_email":     row.get("kp_email", ""),
                "opens":        [],
                "clicks":       [],
            }
        etype = row.get("event_type", "").lower()
        ts    = row.get("timestamp", "")
        if etype == "open":
            groups[tid]["opens"].append(ts)
        elif etype == "click":
            groups[tid]["clicks"].append(ts)

    summaries: list[dict] = []
    for tid, g in sorted(groups.items()):
        opens  = sorted(g["opens"])
        clicks = sorted(g["clicks"])
        summaries.append({
            "tracking_id":      tid,
            "message_id":       g["message_id"],
            "company_name":     g["company_name"],
            "kp_email":         g["kp_email"],
            "open_count":       len(opens),
            "first_open_time":  opens[0]  if opens  else "",
            "last_open_time":   opens[-1] if opens  else "",
            "click_count":      len(clicks),
            "first_click_time": clicks[0]  if clicks else "",
            "last_click_time":  clicks[-1] if clicks else "",
        })
    return summaries


def save_engagement_summary(
    summary_rows: list[dict],
    path: Path = ENGAGEMENT_SUMMARY_FILE,
) -> None:
    """Write full engagement summary (overwrites — this is derived data)."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SUMMARY_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(summary_rows)


def run(logs_path: Path = ENGAGEMENT_LOGS_FILE,
        summary_path: Path = ENGAGEMENT_SUMMARY_FILE) -> list[dict]:
    """Aggregate logs and write summary. Returns summary rows."""
    summaries = aggregate_engagement_logs(logs_path)
    save_engagement_summary(summaries, summary_path)
    print(
        f"[Workflow 7.5] Aggregated {len(summaries)} tracking IDs "
        f"→ {summary_path.name}"
    )
    return summaries
