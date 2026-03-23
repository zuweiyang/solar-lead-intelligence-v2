# Workflow 7.4 — Deliverability Breakers: Four-Scope State Layer
#
# Provides query and update functions for all four breaker scopes:
#   sender   — per sending address (sender_health table)
#   domain   — per sending domain  (campaign_breakers table, scope='domain')
#   campaign — per campaign_id     (campaign_breakers table, scope='campaign')
#   global   — kill-switch         (campaign_breakers table, scope='global', key='global')
#
# All functions accept an open sqlite3.Connection and are safe to call
# when the relevant row does not yet exist (get_* returns inactive/empty,
# set_* upserts as needed).
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Sender-scope breaker  (sender_health table)
# ---------------------------------------------------------------------------

def get_sender_breaker(conn: sqlite3.Connection, sender_email: str) -> tuple[bool, str]:
    """Return (active, reason) for the sender-scope breaker."""
    row = conn.execute(
        "SELECT sender_breaker_active, sender_breaker_reason "
        "FROM sender_health WHERE sender_email = ?",
        (sender_email.lower().strip(),),
    ).fetchone()
    if row is None:
        return False, ""
    return bool(row[0]), (row[1] or "")


def set_sender_breaker(
    conn: sqlite3.Connection,
    sender_email: str,
    active: bool,
    reason: str,
    sending_domain: str = "",
) -> None:
    """
    Activate or clear the sender-scope breaker for sender_email.
    Upserts the sender_health row if it does not yet exist.
    """
    email = sender_email.lower().strip()
    domain = sending_domain or (email.split("@", 1)[1] if "@" in email else "")
    conn.execute(
        """
        INSERT INTO sender_health
            (sender_email, sending_domain,
             sender_breaker_active, sender_breaker_reason,
             created_at, updated_at)
        VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
        ON CONFLICT(sender_email) DO UPDATE SET
            sender_breaker_active = excluded.sender_breaker_active,
            sender_breaker_reason = excluded.sender_breaker_reason,
            updated_at            = excluded.updated_at
        """,
        (email, domain, 1 if active else 0, reason),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Domain-scope breaker  (campaign_breakers table, scope='domain')
# ---------------------------------------------------------------------------

def get_domain_breaker(conn: sqlite3.Connection, sending_domain: str) -> tuple[bool, str]:
    """Return (active, reason) for the domain-scope breaker."""
    row = conn.execute(
        "SELECT breaker_active, breaker_reason "
        "FROM campaign_breakers WHERE scope = 'domain' AND scope_key = ?",
        (sending_domain.lower().strip(),),
    ).fetchone()
    if row is None:
        return False, ""
    return bool(row[0]), (row[1] or "")


def set_domain_breaker(
    conn: sqlite3.Connection,
    sending_domain: str,
    active: bool,
    reason: str,
) -> None:
    """Activate or clear the domain-scope breaker for sending_domain."""
    _upsert_breaker(conn, "domain", sending_domain.lower().strip(), active, reason)


# ---------------------------------------------------------------------------
# Campaign-scope breaker  (campaign_breakers table, scope='campaign')
# ---------------------------------------------------------------------------

def get_campaign_breaker(conn: sqlite3.Connection, campaign_id: str) -> tuple[bool, str]:
    """Return (active, reason) for the campaign-scope breaker."""
    row = conn.execute(
        "SELECT breaker_active, breaker_reason "
        "FROM campaign_breakers WHERE scope = 'campaign' AND scope_key = ?",
        (campaign_id,),
    ).fetchone()
    if row is None:
        return False, ""
    return bool(row[0]), (row[1] or "")


def set_campaign_breaker(
    conn: sqlite3.Connection,
    campaign_id: str,
    active: bool,
    reason: str,
) -> None:
    """Activate or clear the campaign-scope breaker for campaign_id."""
    _upsert_breaker(conn, "campaign", campaign_id, active, reason)


# ---------------------------------------------------------------------------
# Global breaker  (campaign_breakers table, scope='global', key='global')
# ---------------------------------------------------------------------------

_GLOBAL_KEY = "global"


def get_global_breaker(conn: sqlite3.Connection) -> tuple[bool, str]:
    """Return (active, reason) for the global kill-switch breaker."""
    row = conn.execute(
        "SELECT breaker_active, breaker_reason "
        "FROM campaign_breakers WHERE scope = 'global' AND scope_key = ?",
        (_GLOBAL_KEY,),
    ).fetchone()
    if row is None:
        return False, ""
    return bool(row[0]), (row[1] or "")


def set_global_breaker(
    conn: sqlite3.Connection,
    active: bool,
    reason: str,
) -> None:
    """Activate or clear the global kill-switch breaker."""
    _upsert_breaker(conn, "global", _GLOBAL_KEY, active, reason)


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

def _upsert_breaker(
    conn: sqlite3.Connection,
    scope: str,
    scope_key: str,
    active: bool,
    reason: str,
) -> None:
    conn.execute(
        """
        INSERT INTO campaign_breakers
            (scope, scope_key, breaker_active, breaker_reason,
             activated_at, updated_at)
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
