# Workflow 8: Follow-up Automation
# Decides which leads are due for follow-up and dispatches the next email in sequence.

import csv
import json
from datetime import date, timedelta
from config.settings import CRM_DATABASE_FILE, EMAIL_TEMPLATES_FILE, EMAIL_LOGS_FILE

# Days after initial send to trigger each follow-up
FOLLOWUP_SCHEDULE = {
    1: 3,   # sequence step 1 → send 3 days after step 0
    2: 7,   # sequence step 2 → send 7 days after step 0
    3: 14,  # sequence step 3 → send 14 days after step 0
}

MAX_FOLLOWUPS = 3


def load_crm() -> list[dict]:
    try:
        with open(CRM_DATABASE_FILE, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        return []


def load_email_templates() -> dict[str, dict]:
    """Return templates keyed by website."""
    with open(EMAIL_TEMPLATES_FILE, encoding="utf-8") as f:
        templates = json.load(f)
    return {t["website"]: t for t in templates}


def get_followup_content(template: dict, step: int) -> tuple[str, str]:
    """Return (subject, body) for the given follow-up step."""
    if step == 1:
        return template.get("follow_up_subject", ""), template.get("follow_up_body", "")
    if step >= 2:
        return (
            template.get("final_follow_up_subject", ""),
            template.get("final_follow_up_body", ""),
        )
    return "", ""


def leads_due_for_followup(crm_records: list[dict]) -> list[dict]:
    """Return CRM records that are due for the next follow-up today."""
    today = date.today()
    due: list[dict] = []

    for record in crm_records:
        status = record.get("status", "")
        if status in ("replied", "interested", "meeting_scheduled", "unsubscribed"):
            continue  # Do not follow up with engaged / opted-out leads

        step = int(record.get("followup_step", 0))
        if step >= MAX_FOLLOWUPS:
            continue

        first_sent_str = record.get("first_sent_date", "")
        if not first_sent_str:
            continue

        try:
            first_sent = date.fromisoformat(first_sent_str)
        except ValueError:
            continue

        days_offset = FOLLOWUP_SCHEDULE.get(step + 1)
        if days_offset and today >= first_sent + timedelta(days=days_offset):
            due.append(record)

    return due


def run() -> list[dict]:
    """Identify and return leads that need a follow-up email today."""
    crm = load_crm()
    due = leads_due_for_followup(crm)
    print(f"[Workflow 8] {len(due)} leads due for follow-up today.")
    return due


if __name__ == "__main__":
    run()
