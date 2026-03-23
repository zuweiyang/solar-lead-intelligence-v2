# Database: Manager
# Thin helpers for reading / writing the flat-file data store (CSV + JSON).
# Replace with a real database (PostgreSQL, SQLite) when scaling up.

import csv
import json
from pathlib import Path
from config.settings import (
    SEARCH_TASKS_FILE, RAW_LEADS_FILE, COMPANY_CONTENT_FILE,
    COMPANY_PROFILES_FILE, QUALIFIED_LEADS_FILE,
    EMAIL_TEMPLATES_FILE, EMAIL_LOGS_FILE, CRM_DATABASE_FILE,
)


# --- Generic helpers ---

def read_json(path: Path) -> list | dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, data: list | dict) -> None:
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def read_csv(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, records: list[dict], fieldnames: list[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)


def append_csv(path: Path, record: dict, fieldnames: list[str]) -> None:
    file_exists = path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerow(record)


# --- Domain-specific accessors ---

def get_search_tasks() -> list[dict]:
    return read_json(SEARCH_TASKS_FILE)

def save_search_tasks(tasks: list[dict]) -> None:
    write_json(SEARCH_TASKS_FILE, tasks)

def get_raw_leads() -> list[dict]:
    return read_csv(RAW_LEADS_FILE)

def get_company_content() -> list[dict]:
    return read_json(COMPANY_CONTENT_FILE)

def get_company_profiles() -> list[dict]:
    return read_json(COMPANY_PROFILES_FILE)

def get_qualified_leads() -> list[dict]:
    return read_csv(QUALIFIED_LEADS_FILE)

def get_email_templates() -> list[dict]:
    return read_json(EMAIL_TEMPLATES_FILE)

def get_email_logs() -> list[dict]:
    return read_csv(EMAIL_LOGS_FILE)

def get_crm_records() -> list[dict]:
    return read_csv(CRM_DATABASE_FILE)

def upsert_crm_record(record: dict) -> None:
    """Insert or update a CRM record matched by website."""
    CRM_FIELDS = [
        "company_name", "website", "email", "status",
        "first_sent_date", "last_contact_date", "followup_step", "notes",
    ]
    try:
        records = get_crm_records()
    except FileNotFoundError:
        records = []

    website = record.get("website", "").strip().lower()
    for i, r in enumerate(records):
        if r.get("website", "").strip().lower() == website:
            records[i] = {**r, **record}
            write_csv(CRM_DATABASE_FILE, records, CRM_FIELDS)
            return

    append_csv(CRM_DATABASE_FILE, record, CRM_FIELDS)
