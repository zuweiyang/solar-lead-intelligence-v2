# Workflow 6.5: Email Quality Scoring — Data Loader
# Loads and normalises records from generated_emails.csv.

import csv

from config.settings import GENERATED_EMAILS_FILE

# Only fields that are truly unrecoverable are required
REQUIRED_COLUMNS = ["company_name", "kp_email", "subject"]

# Column aliases: if the canonical name is absent, try these alternatives
_ALIASES: dict[str, list[str]] = {
    "email_body":      ["body", "email_body"],
    "opening_line":    ["opening_line", "email_opening"],
    "generation_mode": ["generation_mode", "generation_source"],
    "generation_source": ["generation_source", "generation_mode"],
}

_DEFAULTS: dict = {
    "company_name": "", "website": "", "place_id": "",
    "city": "", "region": "", "country": "", "source_location": "",
    "kp_name": "", "kp_title": "", "kp_email": "",
    "contact_name": "", "contact_title": "", "contact_email": "",
    "send_target_type": "", "contact_source": "",
    "named_contact_available": "false", "generic_contact_available": "false",
    "contact_quality": "none", "generic_only": "false",
    "company_type": "", "market_focus": "", "lead_score": 0,
    "subject": "", "opening_line": "", "email_body": "",
    "email_angle": "", "generation_mode": "", "generation_source": "",
}


def _get(row: dict, key: str) -> str:
    """Return row value for key, trying known aliases when the canonical name is absent."""
    for alt in _ALIASES.get(key, [key]):
        if alt in row and row[alt] != "":
            val = row[alt]
            return val.strip() if isinstance(val, str) else val
    val = row.get(key, "")
    return val.strip() if isinstance(val, str) else val


def _normalise(row: dict) -> dict:
    record = dict(_DEFAULTS)
    for key in record:
        record[key] = _get(row, key)
    try:
        record["lead_score"] = int(record["lead_score"])
    except (ValueError, TypeError):
        record["lead_score"] = 0
    return record


def load_generated_emails(limit: int = 0) -> list[dict]:
    if not GENERATED_EMAILS_FILE.exists():
        raise FileNotFoundError(
            f"[Workflow 6.5] generated_emails.csv not found at {GENERATED_EMAILS_FILE}. "
            "Run Workflow 6 first."
        )

    with open(GENERATED_EMAILS_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        columns = set(reader.fieldnames or [])
        missing = [c for c in REQUIRED_COLUMNS if c not in columns]
        if missing:
            raise ValueError(
                f"[Workflow 6.5] generated_emails.csv is missing required columns: {missing}"
            )
        rows = [_normalise(row) for row in reader]

    return rows[:limit] if limit else rows
