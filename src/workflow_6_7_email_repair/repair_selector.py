# Workflow 6.7: Email Repair Loop — Repair Selector
# Loads scored_emails.csv and identifies emails eligible for repair.

import csv
import re
from pathlib import Path

from config.settings import SCORED_EMAILS_FILE

_PLACEHOLDER_RE = re.compile(r"\{[a-zA-Z_][a-zA-Z0-9_]*\}")

MIN_SCORE_FOR_REPAIR = 45   # emails below this are too broken to repair
REPAIRABLE_STATUSES  = {"manual_review", "rejected"}


def _has_placeholder(text: str) -> bool:
    return bool(_PLACEHOLDER_RE.search(text))


def _is_hard_broken(record: dict) -> bool:
    """Return True if the email has structural failures that repair cannot fix."""
    kp_email = record.get("kp_email", "")
    subject  = record.get("subject", "")
    body     = record.get("email_body", "")
    if not kp_email or "@" not in kp_email:
        return True
    if not subject.strip():
        return True
    if not body.strip():
        return True
    if _has_placeholder(subject + " " + body):
        return True
    return False


def load_repairable(path: Path = SCORED_EMAILS_FILE) -> tuple[list[dict], list[dict]]:
    """Return (repairable, skip_list).

    repairable — manual_review or rejected with overall_score >= MIN_SCORE_FOR_REPAIR
                 and no hard structural failures.
    skip_list  — approved records (already pass, no repair needed).
    """
    if not path.exists():
        print(f"[Workflow 6.7] scored_emails.csv not found: {path}")
        return [], []

    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    repairable: list[dict] = []
    skipped:    list[dict] = []

    for r in rows:
        status = r.get("approval_status", "")
        if status == "approved":
            skipped.append(r)
            continue
        if status not in REPAIRABLE_STATUSES:
            continue
        try:
            score = int(float(r.get("overall_score", 0)))
        except (ValueError, TypeError):
            score = 0
        if score < MIN_SCORE_FOR_REPAIR:
            print(
                f"[Workflow 6.7]   SKIP (score too low): "
                f"{r.get('company_name', '?')} — {score}"
            )
            continue
        if _is_hard_broken(r):
            print(
                f"[Workflow 6.7]   SKIP (hard failure): "
                f"{r.get('company_name', '?')}"
            )
            continue
        repairable.append(r)

    print(
        f"[Workflow 6.7] Repairable: {len(repairable)} | "
        f"Skipped (approved): {len(skipped)}"
    )
    return repairable, skipped
