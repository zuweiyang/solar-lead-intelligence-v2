"""
CSV → SQLite sync layer.

Reads existing CSV / JSON output files and imports them into the database.
Safe to run multiple times — uses INSERT OR IGNORE / upsert patterns to avoid duplicates.
"""
from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

from config.settings import (
    RAW_LEADS_FILE,
    ENRICHED_LEADS_FILE,
    ENRICHED_CONTACTS_FILE,
    GENERATED_EMAILS_FILE,
    SEND_LOGS_FILE,
    ENGAGEMENT_LOGS_FILE,
    FOLLOWUP_LOGS_FILE,
    CAMPAIGN_STATUS_FILE,
)
from src.database.db_utils import (
    insert_company,
    insert_contact,
    insert_email,
    log_email_send,
    log_engagement_event,
    get_company_id_by_name,
    get_company_by_place_id,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _read_json(path: Path) -> list:
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _company_id(conn: sqlite3.Connection, record: dict) -> int | None:
    """Try to resolve a company_id from place_id or company_name."""
    place_id = (record.get("place_id") or "").strip()
    if place_id:
        row = get_company_by_place_id(conn, place_id)
        if row:
            return row["id"]
    name = (record.get("company_name") or "").strip()
    if name:
        return get_company_id_by_name(conn, name)
    return None


def _contact_id(conn: sqlite3.Connection, record: dict) -> int | None:
    """Look up contact by kp_email (best available key)."""
    email = (record.get("kp_email") or "").strip()
    if not email:
        return None
    row = conn.execute(
        "SELECT id FROM contacts WHERE lower(email) = lower(?)", (email,)
    ).fetchone()
    return row["id"] if row else None


# ---------------------------------------------------------------------------
# Individual sync functions
# ---------------------------------------------------------------------------

def sync_raw_leads(conn: sqlite3.Connection) -> int:
    """raw_leads.csv → companies table."""
    rows = _read_csv(RAW_LEADS_FILE)
    count = 0
    with conn:
        for r in rows:
            insert_company(conn, r)
            count += 1
    print(f"[csv_sync] raw_leads → companies: {count} rows processed")
    return count


def sync_enriched_leads(conn: sqlite3.Connection) -> int:
    """
    enriched_leads.csv → companies (upsert) + contacts (insert if new email).
    """
    rows = _read_csv(ENRICHED_LEADS_FILE)
    companies_updated = 0
    contacts_inserted = 0
    with conn:
        for r in rows:
            cid = insert_company(conn, r)
            companies_updated += 1

            email = (r.get("kp_email") or "").strip()
            if email:
                existing = conn.execute(
                    "SELECT id FROM contacts WHERE lower(email) = lower(?) AND company_id = ?",
                    (email, cid),
                ).fetchone()
                if not existing:
                    insert_contact(conn, {**r, "company_id": cid})
                    contacts_inserted += 1

    print(
        f"[csv_sync] enriched_leads → companies: {companies_updated}, "
        f"contacts inserted: {contacts_inserted}"
    )
    return contacts_inserted


def sync_enriched_contacts(conn: sqlite3.Connection) -> int:
    """
    enriched_contacts.csv → companies (upsert) + contacts (multi-contact, deduped by email+company).
    Stores contact_rank and is_generic_mailbox for each row.
    """
    rows = _read_csv(ENRICHED_CONTACTS_FILE)
    if not rows:
        return 0
    companies_updated = 0
    contacts_inserted = 0
    with conn:
        for r in rows:
            cid = insert_company(conn, r)
            companies_updated += 1

            email = (r.get("kp_email") or "").strip()
            if email:
                existing = conn.execute(
                    "SELECT id FROM contacts WHERE lower(email) = lower(?) AND company_id = ?",
                    (email, cid),
                ).fetchone()
                if not existing:
                    insert_contact(conn, {
                        **r,
                        "company_id":        cid,
                        "contact_rank":      int(r.get("contact_rank") or 1),
                        "is_generic_mailbox": r.get("is_generic_mailbox", "false"),
                    })
                    contacts_inserted += 1

    print(
        f"[csv_sync] enriched_contacts → companies: {companies_updated}, "
        f"contacts inserted: {contacts_inserted}"
    )
    return contacts_inserted


def sync_generated_emails(conn: sqlite3.Connection) -> int:
    """generated_emails.csv → emails table."""
    rows = _read_csv(GENERATED_EMAILS_FILE)
    count = 0
    with conn:
        for r in rows:
            cid = _company_id(conn, r)
            kid = _contact_id(conn, r)
            # Skip if same subject+contact already exists
            subject = r.get("subject", "")
            kp_email = r.get("kp_email", "")
            existing = conn.execute(
                """
                SELECT e.id FROM emails e
                  JOIN contacts c ON c.id = e.contact_id
                 WHERE lower(c.email) = lower(?) AND e.subject = ?
                """,
                (kp_email, subject),
            ).fetchone()
            if existing:
                continue
            insert_email(conn, {**r, "company_id": cid, "contact_id": kid})
            count += 1
    print(f"[csv_sync] generated_emails → emails: {count} new rows")
    return count


def sync_send_logs(conn: sqlite3.Connection) -> int:
    """
    send_logs.csv → email_sends table (attempt-level logging).

    email_sends stores ALL send attempts: sent, dry_run, failed, blocked, deferred.
    Use send_status = 'sent' to count actually-delivered emails.
    """
    rows = _read_csv(SEND_LOGS_FILE)
    count = 0
    status_counts: dict[str, int] = {}
    with conn:
        for r in rows:
            kid = _contact_id(conn, r)
            # Find email_id by matching kp_email + subject
            email_id = None
            kp_email = r.get("kp_email", "")
            subject  = r.get("subject", "")
            if kp_email and subject:
                row = conn.execute(
                    """
                    SELECT e.id FROM emails e
                      JOIN contacts c ON c.id = e.contact_id
                     WHERE lower(c.email) = lower(?) AND e.subject = ?
                    """,
                    (kp_email, subject),
                ).fetchone()
                if row:
                    email_id = row["id"]

            log_email_send(conn, {
                **r,
                "contact_id": kid,
                "email_id":   email_id,
                "sent_time":  r.get("timestamp", ""),
            })
            count += 1
            s = (r.get("send_status") or "unknown").strip()
            status_counts[s] = status_counts.get(s, 0) + 1

    breakdown = ", ".join(f"{s}={n}" for s, n in sorted(status_counts.items()))
    print(
        f"[csv_sync] send_logs → email_sends: {count} attempt rows "
        f"({breakdown or 'none'})"
    )
    return count


def sync_engagement_logs(conn: sqlite3.Connection) -> int:
    """engagement_logs.csv → engagement table."""
    rows = _read_csv(ENGAGEMENT_LOGS_FILE)
    count = 0
    with conn:
        for r in rows:
            kid = _contact_id(conn, r)
            log_engagement_event(conn, {
                **r,
                "contact_id": kid,
                "event_time": r.get("timestamp", ""),
            })
            count += 1
    print(f"[csv_sync] engagement_logs → engagement: {count} rows")
    return count


def sync_followup_logs(conn: sqlite3.Connection) -> int:
    """followup_logs.csv → followups table."""
    rows = _read_csv(FOLLOWUP_LOGS_FILE)
    count = 0
    with conn:
        for r in rows:
            kid = _contact_id(conn, r)
            conn.execute(
                """
                INSERT INTO followups (contact_id, stage, sent_time, status)
                VALUES (?,?,?,?)
                """,
                (
                    kid,
                    r.get("followup_stage") or r.get("stage"),
                    r.get("sent_time") or r.get("timestamp", ""),
                    r.get("status", ""),
                ),
            )
            count += 1
    print(f"[csv_sync] followup_logs → followups: {count} rows")
    return count


# ---------------------------------------------------------------------------
# Master sync
# ---------------------------------------------------------------------------

def sync_all(conn: sqlite3.Connection) -> dict[str, int]:
    """
    Run all sync functions in dependency order.
    Returns a dict of {table: rows_synced}.
    """
    print("[csv_sync] Starting full CSV → database sync...")
    results = {
        "companies (raw_leads)":           sync_raw_leads(conn),
        "contacts (enriched_leads)":       sync_enriched_leads(conn),
        "contacts (enriched_contacts)":    sync_enriched_contacts(conn),
        "emails":                          sync_generated_emails(conn),
        "email_sends":                sync_send_logs(conn),
        "engagement":                 sync_engagement_logs(conn),
        "followups":                  sync_followup_logs(conn),
    }
    print("[csv_sync] Sync complete.")
    return results
