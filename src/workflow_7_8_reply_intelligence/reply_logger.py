# Workflow 7.8 — Reply Intelligence: Reply Logger
#
# Persists ReplyRecord instances to:
#   - data/crm/reply_logs.csv  (append-only, human-readable audit trail)
#   - solar_leads.db           (reply_events table, for querying and dedup)
#
# Dedup guarantee: already_logged() checks the CSV for the gmail_message_id
# before any insert; the DB has a UNIQUE constraint as a second guard.

import csv
from datetime import datetime, timezone
from pathlib import Path

from config.settings import REPLY_LOGS_FILE
from src.workflow_7_8_reply_intelligence.reply_models import CSV_FIELDS


# ---------------------------------------------------------------------------
# CSV persistence
# ---------------------------------------------------------------------------

def append_reply_log(reply, path=None) -> None:
    """
    Append one ReplyRecord to reply_logs.csv.
    Creates the file with a header row if it doesn't exist yet.
    Sets reply.logged_at to the current UTC time before writing.
    Modifies reply.logged_at in-place.
    """
    p = Path(str(path or REPLY_LOGS_FILE))
    p.parent.mkdir(parents=True, exist_ok=True)

    reply.logged_at = datetime.now(tz=timezone.utc).isoformat()

    file_exists = p.exists() and p.stat().st_size > 0
    with open(str(p), "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerow(reply.to_csv_row())


def load_reply_logs(path=None) -> list[dict]:
    """
    Load all rows from reply_logs.csv as a list of dicts.
    Returns [] if the file is missing or empty.
    """
    p = Path(str(path or REPLY_LOGS_FILE))
    if not p.exists() or p.stat().st_size == 0:
        return []
    try:
        with open(str(p), newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception as exc:
        print(f"[Workflow 7.8]   Could not load reply_logs: {exc}")
        return []


def already_logged(gmail_message_id: str, path=None) -> bool:
    """
    Return True if a row with this gmail_message_id already exists in
    reply_logs.csv.  Used to skip duplicate fetches in re-runs.
    O(n) scan — acceptable given expected log sizes (<10k rows).
    """
    if not gmail_message_id:
        return False
    for row in load_reply_logs(path):
        if row.get("gmail_message_id", "") == gmail_message_id:
            return True
    return False


# ---------------------------------------------------------------------------
# Database persistence
# ---------------------------------------------------------------------------

def log_reply_to_db(conn, reply) -> int:
    """
    Insert a ReplyRecord into the reply_events table.
    Uses INSERT OR IGNORE so duplicate gmail_message_ids are silently skipped
    (the UNIQUE constraint is the authoritative dedup guard in the DB).

    Returns the new row id, or 0 if the row was ignored (duplicate).
    """
    from src.database.db_utils import insert_reply_event
    return insert_reply_event(conn, reply)
