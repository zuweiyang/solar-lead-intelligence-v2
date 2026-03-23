# Workflow 7: Email Sending - Send Loader
# Loads final_send_queue.csv, validates columns, normalises records.

import csv
from pathlib import Path

from config.settings import FINAL_SEND_QUEUE_FILE

REQUIRED_COLUMNS = ["company_name", "kp_email", "subject", "email_body"]

APPROVED_STATUSES = {"approved", "approved_after_repair"}


def load_send_queue(path: Path = FINAL_SEND_QUEUE_FILE, limit: int = 0) -> list[dict]:
    """
    Load and validate final_send_queue.csv.

    Returns normalised records where approval_status is in APPROVED_STATUSES.
    Exits with a clear message if the file is missing or columns are wrong.
    """
    if not path.exists():
        print(f"[Workflow 7] final_send_queue not found - nothing to send.")
        return []

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            print(f"[Workflow 7] {path.name} is empty - nothing to send.")
            return []

        missing = [c for c in REQUIRED_COLUMNS if c not in reader.fieldnames]
        if missing:
            raise RuntimeError(
                f"[Workflow 7] {path.name} is missing required columns: {missing}"
            )

        rows = list(reader)

    records: list[dict] = []
    skipped_blank = 0
    skipped_status = 0

    for row in rows:
        record = {k: (v or "").strip() for k, v in row.items()}

        # Keep send-time compatibility with older kp_* readers while allowing
        # the final queue to expose clearer contact_* fields for audit.
        if not record.get("kp_email") and record.get("contact_email"):
            record["kp_email"] = record["contact_email"]
        if not record.get("kp_name") and record.get("contact_name"):
            record["kp_name"] = record["contact_name"]
        if not record.get("kp_title") and record.get("contact_title"):
            record["kp_title"] = record["contact_title"]

        if not any(record.get(c) for c in REQUIRED_COLUMNS):
            skipped_blank += 1
            continue

        status = record.get("approval_status", "")
        if status not in APPROVED_STATUSES:
            skipped_status += 1
            continue

        records.append(record)

    print(
        f"[Workflow 7] Loaded {len(records)} approved records "
        f"({skipped_blank} blank, {skipped_status} non-approved skipped)"
    )

    if limit > 0:
        records = records[:limit]
        print(f"[Workflow 7] Capped to {len(records)} records (limit={limit})")

    return records
