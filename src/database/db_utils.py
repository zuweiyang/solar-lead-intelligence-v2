"""
Database utility helpers for common insert / lookup operations.
All functions accept an open sqlite3.Connection.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# companies
# ---------------------------------------------------------------------------

def insert_company(conn: sqlite3.Connection, record: dict) -> int:
    """
    Insert a company row. If a row with the same place_id already exists,
    update mutable fields and return the existing id.

    Returns the company id.
    """
    place_id = (record.get("place_id") or "").strip() or None

    # Try to find existing record
    if place_id:
        row = conn.execute(
            "SELECT id FROM companies WHERE place_id = ?", (place_id,)
        ).fetchone()
        if row:
            conn.execute(
                """
                UPDATE companies
                   SET company_name    = ?,
                       website         = ?,
                       phone           = ?,
                       address         = ?,
                       city            = ?,
                       province        = ?,
                       country         = ?,
                       google_rating   = ?,
                       google_category = ?,
                       source_keyword  = ?,
                       source_location = ?,
                       updated_at      = ?
                 WHERE id = ?
                """,
                (
                    record.get("company_name", ""),
                    record.get("website", ""),
                    record.get("phone", ""),
                    record.get("address", ""),
                    record.get("city", ""),
                    record.get("province", ""),
                    record.get("country", ""),
                    record.get("google_rating"),
                    record.get("google_category", ""),
                    record.get("source_keyword", ""),
                    record.get("source_location", ""),
                    _now(),
                    row["id"],
                ),
            )
            return row["id"]

    cursor = conn.execute(
        """
        INSERT INTO companies
            (place_id, company_name, website, phone, address,
             city, province, country, google_rating, google_category,
             source_keyword, source_location, created_at, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            place_id,
            record.get("company_name", ""),
            record.get("website", ""),
            record.get("phone", ""),
            record.get("address", ""),
            record.get("city", ""),
            record.get("province", ""),
            record.get("country", ""),
            record.get("google_rating"),
            record.get("google_category", ""),
            record.get("source_keyword", ""),
            record.get("source_location", ""),
            _now(),
            _now(),
        ),
    )
    return cursor.lastrowid


def get_company_by_place_id(conn: sqlite3.Connection, place_id: str) -> dict | None:
    """Return company row as dict, or None if not found."""
    row = conn.execute(
        "SELECT * FROM companies WHERE place_id = ?", (place_id,)
    ).fetchone()
    return dict(row) if row else None


def get_company_id_by_name(conn: sqlite3.Connection, company_name: str) -> int | None:
    """Fuzzy-match by exact lower-cased name. Returns id or None."""
    row = conn.execute(
        "SELECT id FROM companies WHERE lower(company_name) = lower(?)",
        (company_name,),
    ).fetchone()
    return row["id"] if row else None


# ---------------------------------------------------------------------------
# contacts
# ---------------------------------------------------------------------------

def insert_contact(conn: sqlite3.Connection, record: dict) -> int:
    """
    Insert a contact row.
    Does NOT deduplicate — callers should check before inserting if needed.
    Returns the new contact id.
    Accepts optional P1-2A fields: contact_rank (int, default 1),
    is_generic_mailbox ("true"/"false" string or int 0/1, default 0).
    """
    is_generic_raw = record.get("is_generic_mailbox", 0)
    is_generic_int = (
        1 if str(is_generic_raw).lower() in ("true", "1") else 0
    )
    cursor = conn.execute(
        """
        INSERT INTO contacts
            (company_id, contact_name, contact_title, email, phone,
             source, confidence, contact_rank, is_generic_mailbox, created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (
            record.get("company_id"),
            record.get("contact_name", "") or record.get("kp_name", ""),
            record.get("contact_title", "") or record.get("kp_title", ""),
            record.get("email", "") or record.get("kp_email", ""),
            record.get("phone", ""),
            record.get("source", "") or record.get("enrichment_source", ""),
            record.get("confidence"),
            int(record.get("contact_rank") or 1),
            is_generic_int,
            _now(),
        ),
    )
    return cursor.lastrowid


# ---------------------------------------------------------------------------
# emails
# ---------------------------------------------------------------------------

def insert_email(conn: sqlite3.Connection, record: dict) -> int:
    """
    Insert a generated email draft.
    Returns the new email id.
    """
    cursor = conn.execute(
        """
        INSERT INTO emails
            (company_id, contact_id, subject, body,
             email_angle, quality_score, quality_status,
             generation_source, created_at)
        VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            record.get("company_id"),
            record.get("contact_id"),
            record.get("subject", ""),
            record.get("body", "") or record.get("email_body", ""),
            record.get("email_angle", ""),
            record.get("quality_score"),
            record.get("quality_status", ""),
            record.get("generation_source", ""),
            _now(),
        ),
    )
    return cursor.lastrowid


# ---------------------------------------------------------------------------
# email_sends
# ---------------------------------------------------------------------------

def log_email_send(conn: sqlite3.Connection, record: dict) -> int:
    """
    Log a send-attempt event (sent, dry_run, failed, blocked, deferred).
    email_sends stores attempt-level data — not just successfully sent emails.
    Returns the new email_sends id.
    """
    cursor = conn.execute(
        """
        INSERT INTO email_sends
            (email_id, contact_id, campaign_id, send_mode,
             sent_time, send_status, send_decision, message_id)
        VALUES (?,?,?,?,?,?,?,?)
        """,
        (
            record.get("email_id"),
            record.get("contact_id"),
            record.get("campaign_id", ""),
            record.get("send_mode", ""),
            record.get("sent_time") or record.get("timestamp") or _now(),
            record.get("send_status", ""),
            record.get("send_decision", ""),
            record.get("message_id") or record.get("provider_message_id", ""),
        ),
    )
    return cursor.lastrowid


# ---------------------------------------------------------------------------
# engagement
# ---------------------------------------------------------------------------

def log_engagement_event(conn: sqlite3.Connection, record: dict) -> int:
    """
    Log an engagement event (open / click / reply / bounce / unsubscribe).
    Returns the new engagement id.
    """
    metadata = record.get("metadata")
    if isinstance(metadata, dict):
        metadata = json.dumps(metadata)

    cursor = conn.execute(
        """
        INSERT INTO engagement
            (email_id, contact_id, event_type, event_time, metadata)
        VALUES (?,?,?,?,?)
        """,
        (
            record.get("email_id"),
            record.get("contact_id"),
            record.get("event_type", "open"),
            record.get("event_time") or record.get("timestamp") or _now(),
            metadata,
        ),
    )
    return cursor.lastrowid


# ---------------------------------------------------------------------------
# reply_events
# ---------------------------------------------------------------------------

def insert_reply_event(conn: sqlite3.Connection, reply) -> int:
    """
    Insert a reply event into the reply_events table.
    Uses INSERT OR IGNORE to safely skip duplicates (gmail_message_id is UNIQUE).
    Accepts a ReplyRecord dataclass instance or a dict.
    Returns the new row id (0 if duplicate was ignored).
    """
    def _get(field: str, default=""):
        if hasattr(reply, field):
            return getattr(reply, field, default)
        return reply.get(field, default) if isinstance(reply, dict) else default  # type: ignore

    cursor = conn.execute(
        """
        INSERT OR IGNORE INTO reply_events (
            logged_at, gmail_message_id, gmail_thread_id,
            from_email, from_name, to_email, subject,
            snippet, body_text, message_date, in_reply_to, "references",
            matched, match_method,
            matched_send_log_row_id, matched_tracking_id, matched_campaign_id,
            matched_company_name, matched_kp_email, matched_place_id,
            manual_review_required, match_error
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            _get("logged_at") or _now(),
            _get("gmail_message_id"),
            _get("gmail_thread_id"),
            _get("from_email"),
            _get("from_name"),
            _get("to_email"),
            _get("subject"),
            _get("snippet"),
            _get("body_text"),
            _get("message_date"),
            _get("in_reply_to"),
            _get("references"),
            1 if _get("matched") else 0,
            _get("match_method"),
            _get("matched_send_log_row_id"),
            _get("matched_tracking_id"),
            _get("matched_campaign_id"),
            _get("matched_company_name"),
            _get("matched_kp_email"),
            _get("matched_place_id"),
            1 if _get("manual_review_required") else 0,
            _get("match_error"),
        ),
    )
    conn.commit()
    return cursor.lastrowid or 0


def update_reply_classification(conn: sqlite3.Connection, reply) -> None:
    """
    Update the Ticket 2 classification and operational-state columns on an
    existing reply_events row identified by gmail_message_id.

    Called after insert_reply_event() when classification results are available.
    No-ops silently if the row doesn't exist (safe to call speculatively).
    """
    def _get(field: str, default=""):
        if hasattr(reply, field):
            return getattr(reply, field, default)
        return reply.get(field, default) if isinstance(reply, dict) else default  # type: ignore

    mid = _get("gmail_message_id")
    if not mid:
        return

    conn.execute(
        """
        UPDATE reply_events
           SET reply_type                        = ?,
               classification_method             = ?,
               classification_confidence         = ?,
               classification_reason             = ?,
               suppression_status                = ?,
               followup_paused                   = ?,
               alternate_contact_review_required = ?,
               manual_review_required            = ?
         WHERE gmail_message_id = ?
        """,
        (
            _get("reply_type"),
            _get("classification_method"),
            _get("classification_confidence", 0.0),
            _get("classification_reason"),
            _get("suppression_status"),
            1 if _get("followup_paused") else 0,
            1 if _get("alternate_contact_review_required") else 0,
            1 if _get("manual_review_required") else 0,
            mid,
        ),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# email_verification
# ---------------------------------------------------------------------------

def upsert_email_verification(conn: sqlite3.Connection, result) -> None:
    """
    Insert or update an email_verification row keyed on kp_email.

    Accepts a VerificationResult dataclass instance or a dict.
    Uses INSERT OR REPLACE so that re-verifying an address always reflects
    the latest result.
    """
    def _get(field: str, default=""):
        if hasattr(result, field):
            return getattr(result, field, default)
        return result.get(field, default) if isinstance(result, dict) else default  # type: ignore

    conn.execute(
        """
        INSERT INTO email_verification
            (kp_email, email_confidence_tier, send_eligibility, send_pool,
             is_generic_mailbox, provider_result, provider_name,
             verified_at, source_mode, verification_error,
             created_at, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,
                COALESCE((SELECT created_at FROM email_verification WHERE kp_email = ?), datetime('now')),
                datetime('now'))
        ON CONFLICT(kp_email) DO UPDATE SET
            email_confidence_tier = excluded.email_confidence_tier,
            send_eligibility      = excluded.send_eligibility,
            send_pool             = excluded.send_pool,
            is_generic_mailbox    = excluded.is_generic_mailbox,
            provider_result       = excluded.provider_result,
            provider_name         = excluded.provider_name,
            verified_at           = excluded.verified_at,
            source_mode           = excluded.source_mode,
            verification_error    = excluded.verification_error,
            updated_at            = datetime('now')
        """,
        (
            (_get("kp_email") or "").lower().strip(),
            _get("email_confidence_tier"),
            _get("send_eligibility"),
            _get("send_pool"),
            1 if _get("is_generic_mailbox") else 0,
            _get("provider_result"),
            _get("provider_name"),
            _get("verified_at"),
            _get("source_mode"),
            _get("error") or _get("verification_error"),
            (_get("kp_email") or "").lower().strip(),  # for COALESCE subquery
        ),
    )
    conn.commit()


def get_verification_by_email(conn: sqlite3.Connection, email: str) -> dict | None:
    """Return email_verification row as dict, or None if not found."""
    row = conn.execute(
        "SELECT * FROM email_verification WHERE kp_email = ?",
        (email.lower().strip(),),
    ).fetchone()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# sender_health  (Ticket 4)
# ---------------------------------------------------------------------------

def upsert_sender_health(conn: sqlite3.Connection, health) -> None:
    """
    Insert or update a sender_health row keyed on sender_email.

    Accepts a SenderHealth dataclass instance or a dict.
    Preserves created_at on updates.
    """
    def _get(field: str, default=None):
        if hasattr(health, field):
            return getattr(health, field, default)
        return health.get(field, default) if isinstance(health, dict) else default  # type: ignore

    sender_email = (_get("sender_email") or "").lower().strip()
    if not sender_email:
        return

    conn.execute(
        """
        INSERT INTO sender_health
            (sender_email, sending_domain, provider, active,
             hard_bounce_rate, invalid_rate, provider_send_failure_rate,
             unsubscribe_rate, spam_rate,
             last_health_updated_at, health_source, health_note,
             sender_breaker_active, sender_breaker_reason,
             created_at, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                COALESCE((SELECT created_at FROM sender_health WHERE sender_email = ?), datetime('now')),
                datetime('now'))
        ON CONFLICT(sender_email) DO UPDATE SET
            sending_domain             = excluded.sending_domain,
            provider                   = excluded.provider,
            active                     = excluded.active,
            hard_bounce_rate           = excluded.hard_bounce_rate,
            invalid_rate               = excluded.invalid_rate,
            provider_send_failure_rate = excluded.provider_send_failure_rate,
            unsubscribe_rate           = excluded.unsubscribe_rate,
            spam_rate                  = excluded.spam_rate,
            last_health_updated_at     = excluded.last_health_updated_at,
            health_source              = excluded.health_source,
            health_note                = excluded.health_note,
            sender_breaker_active      = excluded.sender_breaker_active,
            sender_breaker_reason      = excluded.sender_breaker_reason,
            updated_at                 = datetime('now')
        """,
        (
            sender_email,
            _get("sending_domain", ""),
            _get("provider", ""),
            1 if _get("active", True) else 0,
            _get("hard_bounce_rate", 0.0),
            _get("invalid_rate", 0.0),
            _get("provider_send_failure_rate", 0.0),
            _get("unsubscribe_rate", 0.0),
            _get("spam_rate", 0.0),
            _get("last_health_updated_at", ""),
            _get("health_source", ""),
            _get("health_note", ""),
            1 if _get("sender_breaker_active", False) else 0,
            _get("sender_breaker_reason", ""),
            sender_email,  # for COALESCE subquery
        ),
    )
    conn.commit()


def get_sender_health(conn: sqlite3.Connection, sender_email: str) -> dict | None:
    """Return sender_health row as dict, or None if not found."""
    row = conn.execute(
        "SELECT * FROM sender_health WHERE sender_email = ?",
        (sender_email.lower().strip(),),
    ).fetchone()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# campaign_breakers  (Ticket 4)
# ---------------------------------------------------------------------------

def upsert_campaign_breaker(
    conn: sqlite3.Connection,
    scope: str,
    scope_key: str,
    active: bool,
    reason: str,
) -> None:
    """
    Insert or update a campaign_breakers row keyed on (scope, scope_key).
    activated_at is set when transitioning from inactive → active; cleared on reset.
    """
    conn.execute(
        """
        INSERT INTO campaign_breakers
            (scope, scope_key, breaker_active, breaker_reason, activated_at, updated_at)
        VALUES (?, ?, ?, ?,
                CASE WHEN ? THEN datetime('now') ELSE NULL END,
                datetime('now'))
        ON CONFLICT(scope, scope_key) DO UPDATE SET
            breaker_active = excluded.breaker_active,
            breaker_reason = excluded.breaker_reason,
            activated_at   = CASE
                               WHEN excluded.breaker_active AND NOT campaign_breakers.breaker_active
                               THEN datetime('now')
                               WHEN NOT excluded.breaker_active THEN NULL
                               ELSE campaign_breakers.activated_at
                             END,
            updated_at     = excluded.updated_at
        """,
        (scope, scope_key, 1 if active else 0, reason, 1 if active else 0),
    )
    conn.commit()


def get_campaign_breaker_row(
    conn: sqlite3.Connection,
    scope: str,
    scope_key: str,
) -> dict | None:
    """Return campaign_breakers row as dict, or None if not found."""
    row = conn.execute(
        "SELECT * FROM campaign_breakers WHERE scope = ? AND scope_key = ?",
        (scope, scope_key),
    ).fetchone()
    return dict(row) if row else None


def get_reply_suppression_index(conn: sqlite3.Connection) -> dict[str, str]:
    """
    Load the worst (most restrictive) suppression_status per from_email
    from the reply_events table.

    Returns {from_email_lower: suppression_status}.
    Used by Workflow 8 stop-rules to block/defer follow-up for replied contacts.
    """
    from src.workflow_7_8_reply_intelligence.reply_state_manager import (
        worst_suppression,
    )
    index: dict[str, str] = {}
    try:
        rows = conn.execute(
            "SELECT from_email, suppression_status FROM reply_events "
            "WHERE suppression_status IS NOT NULL AND suppression_status != ''"
        ).fetchall()
        for from_email, sup_status in rows:
            if not from_email:
                continue
            email_lower = from_email.lower().strip()
            if email_lower:
                index[email_lower] = worst_suppression(
                    index.get(email_lower, ""), sup_status
                )
    except Exception as exc:
        print(f"[db_utils] get_reply_suppression_index failed: {exc}")
    return index
