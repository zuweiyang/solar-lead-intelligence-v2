"""
Database schema — creates all tables if they do not already exist.
Call create_all_tables(conn) once during database initialisation.
"""
import sqlite3


# ---------------------------------------------------------------------------
# DDL statements — one per table
# ---------------------------------------------------------------------------

_DDL_COMPANIES = """
CREATE TABLE IF NOT EXISTS companies (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    place_id        TEXT    UNIQUE,
    company_name    TEXT    NOT NULL,
    website         TEXT,
    phone           TEXT,
    address         TEXT,
    city            TEXT,
    province        TEXT,
    country         TEXT,
    google_rating   REAL,
    google_category TEXT,
    source_keyword  TEXT,
    source_location TEXT,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at      TEXT    NOT NULL DEFAULT (datetime('now'))
);
"""

_DDL_COMPANY_ANALYSIS = """
CREATE TABLE IF NOT EXISTS company_analysis (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id       INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    company_type     TEXT,
    market_focus     TEXT,
    lead_score       INTEGER,
    confidence_score REAL,
    services_detected TEXT,
    analysis_source  TEXT,
    created_at       TEXT    NOT NULL DEFAULT (datetime('now'))
);
"""

_DDL_CONTACTS = """
CREATE TABLE IF NOT EXISTS contacts (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id    INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    contact_name  TEXT,
    contact_title TEXT,
    email         TEXT,
    phone         TEXT,
    site_phone    TEXT,
    whatsapp_phone TEXT,
    contact_channel TEXT,
    alt_outreach_possible INTEGER NOT NULL DEFAULT 0,
    manual_outreach_channel TEXT,
    manual_outreach_highlight INTEGER NOT NULL DEFAULT 0,
    source        TEXT,
    confidence    REAL,
    created_at    TEXT    NOT NULL DEFAULT (datetime('now'))
);
"""

_DDL_COMPANY_SIGNALS = """
CREATE TABLE IF NOT EXISTS company_signals (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id  INTEGER NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    signal_text TEXT,
    signal_type TEXT,
    source      TEXT,
    signal_date TEXT,
    created_at  TEXT    NOT NULL DEFAULT (datetime('now'))
);
"""

_DDL_EMAILS = """
CREATE TABLE IF NOT EXISTS emails (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id        INTEGER REFERENCES companies(id) ON DELETE SET NULL,
    contact_id        INTEGER REFERENCES contacts(id)  ON DELETE SET NULL,
    subject           TEXT,
    body              TEXT,
    email_angle       TEXT,
    quality_score     INTEGER,
    quality_status    TEXT,
    generation_source TEXT,
    created_at        TEXT    NOT NULL DEFAULT (datetime('now'))
);
"""

_DDL_EMAIL_SENDS = """
CREATE TABLE IF NOT EXISTS email_sends (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    email_id    INTEGER REFERENCES emails(id)   ON DELETE SET NULL,
    contact_id  INTEGER REFERENCES contacts(id) ON DELETE SET NULL,
    campaign_id TEXT,
    send_mode   TEXT,
    sent_time   TEXT,
    send_status TEXT,
    send_decision TEXT,
    message_id  TEXT
);
"""

# Migration: columns added after initial release — applied at startup via migrate_schema()
_MIGRATIONS_EMAIL_SENDS = [
    ("campaign_id",   "TEXT"),
    ("send_mode",     "TEXT"),
    ("send_decision", "TEXT"),
]

_DDL_ENGAGEMENT = """
CREATE TABLE IF NOT EXISTS engagement (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    email_id    INTEGER REFERENCES emails(id)   ON DELETE SET NULL,
    contact_id  INTEGER REFERENCES contacts(id) ON DELETE SET NULL,
    event_type  TEXT    NOT NULL,
    event_time  TEXT,
    metadata    TEXT
);
"""
# event_type values: open | click | reply | bounce | unsubscribe

_DDL_FOLLOWUPS = """
CREATE TABLE IF NOT EXISTS followups (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_id INTEGER REFERENCES contacts(id) ON DELETE SET NULL,
    stage      INTEGER NOT NULL,
    sent_time  TEXT,
    status     TEXT
);
"""
# stage values: 1 | 2 | 3

_DDL_REPLY_EVENTS = """
CREATE TABLE IF NOT EXISTS reply_events (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    logged_at               TEXT    NOT NULL,
    gmail_message_id        TEXT    UNIQUE,
    gmail_thread_id         TEXT,
    from_email              TEXT,
    from_name               TEXT,
    to_email                TEXT,
    subject                 TEXT,
    snippet                 TEXT,
    body_text               TEXT,
    message_date            TEXT,
    in_reply_to             TEXT,
    "references"            TEXT,
    matched                 INTEGER NOT NULL DEFAULT 0,
    match_method            TEXT,
    matched_send_log_row_id TEXT,
    matched_tracking_id     TEXT,
    matched_campaign_id     TEXT,
    matched_company_name    TEXT,
    matched_kp_email        TEXT,
    matched_place_id        TEXT,
    manual_review_required  INTEGER NOT NULL DEFAULT 0,
    match_error             TEXT,
    reply_type                         TEXT,
    classification_method              TEXT,
    classification_confidence          REAL,
    classification_reason              TEXT,
    suppression_status                 TEXT,
    followup_paused                    INTEGER NOT NULL DEFAULT 0,
    alternate_contact_review_required  INTEGER NOT NULL DEFAULT 0
);
"""
# match_method: thread_id | in_reply_to | references | email_subject | email_recent | ""
# reply_type: unsubscribe | hard_no | wrong_person | out_of_office | auto_reply_other |
#             request_quote | request_info | forwarded | positive_interest | soft_no | unknown
# suppression_status: none | paused | suppressed | handoff_to_human

_DDL_SENDER_HEALTH = """
CREATE TABLE IF NOT EXISTS sender_health (
    id                         INTEGER PRIMARY KEY AUTOINCREMENT,
    sender_email               TEXT    NOT NULL UNIQUE,
    sending_domain             TEXT,
    provider                   TEXT,
    active                     INTEGER NOT NULL DEFAULT 1,
    hard_bounce_rate           REAL    NOT NULL DEFAULT 0.0,
    invalid_rate               REAL    NOT NULL DEFAULT 0.0,
    provider_send_failure_rate REAL    NOT NULL DEFAULT 0.0,
    unsubscribe_rate           REAL    NOT NULL DEFAULT 0.0,
    spam_rate                  REAL    NOT NULL DEFAULT 0.0,
    last_health_updated_at     TEXT,
    health_source              TEXT,
    health_note                TEXT,
    sender_breaker_active      INTEGER NOT NULL DEFAULT 0,
    sender_breaker_reason      TEXT,
    created_at                 TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at                 TEXT    NOT NULL DEFAULT (datetime('now'))
);
"""
# sender_breaker_active: 0 = clear, 1 = tripped
# health_source values: send_logs | postmaster | manual

_DDL_CAMPAIGN_BREAKERS = """
CREATE TABLE IF NOT EXISTS campaign_breakers (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    scope          TEXT    NOT NULL,
    scope_key      TEXT    NOT NULL,
    breaker_active INTEGER NOT NULL DEFAULT 0,
    breaker_reason TEXT,
    activated_at   TEXT,
    updated_at     TEXT    NOT NULL DEFAULT (datetime('now')),
    UNIQUE(scope, scope_key)
);
"""
# scope values:     domain | campaign | global
# scope_key values: <sending_domain> | <campaign_id> | 'global'

_MIGRATIONS_SENDER_HEALTH:    list[tuple[str, str]] = []  # all columns in initial DDL
_MIGRATIONS_CAMPAIGN_BREAKERS: list[tuple[str, str]] = []  # all columns in initial DDL

# Migration: P1-2A multi-contact enrichment columns (for databases that already exist)
_MIGRATIONS_CONTACTS: list[tuple[str, str]] = [
    ("site_phone",                         "TEXT"),
    ("whatsapp_phone",                     "TEXT"),
    ("contact_channel",                    "TEXT"),
    ("alt_outreach_possible",              "INTEGER NOT NULL DEFAULT 0"),
    ("manual_outreach_channel",            "TEXT"),
    ("manual_outreach_highlight",          "INTEGER NOT NULL DEFAULT 0"),
    ("contact_rank",                       "INTEGER NOT NULL DEFAULT 1"),
    ("is_generic_mailbox",                 "INTEGER NOT NULL DEFAULT 0"),
    # P1-2B — contact scoring fields
    ("contact_fit_score",                  "INTEGER NOT NULL DEFAULT 0"),
    ("contact_priority_rank",              "INTEGER NOT NULL DEFAULT 0"),
    ("is_primary_contact",                 "INTEGER NOT NULL DEFAULT 0"),
    ("alternate_contact_review_candidate", "INTEGER NOT NULL DEFAULT 0"),
]

_DDL_EMAIL_VERIFICATION = """
CREATE TABLE IF NOT EXISTS email_verification (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    kp_email              TEXT    NOT NULL UNIQUE,
    email_confidence_tier TEXT,
    send_eligibility      TEXT,
    send_pool             TEXT,
    is_generic_mailbox    INTEGER NOT NULL DEFAULT 0,
    provider_result       TEXT,
    provider_name         TEXT,
    verified_at           TEXT,
    source_mode           TEXT,
    verification_error    TEXT,
    created_at            TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at            TEXT    NOT NULL DEFAULT (datetime('now'))
);
"""
# email_confidence_tier values: E0 | E1 | E2 | E3 | E4
# send_eligibility values: allow | allow_limited | hold | generic_pool_only | block
# send_pool values: primary_pool | limited_pool | risk_pool | generic_pool | blocked_pool
# source_mode values: live | mock | cached | skipped

# Migration: Ticket 3 email_verification table columns (for databases that already exist)
_MIGRATIONS_EMAIL_VERIFICATION: list[tuple[str, str]] = []  # all columns in initial DDL

# Migration: Ticket 2 classification columns added after Ticket 1 release.
# Applied at startup via migrate_schema() — idempotent (columns already added are skipped).
_MIGRATIONS_REPLY_EVENTS = [
    ("reply_type",                        "TEXT"),
    ("classification_method",             "TEXT"),
    ("classification_confidence",         "REAL"),
    ("classification_reason",             "TEXT"),
    ("suppression_status",                "TEXT"),
    ("followup_paused",                   "INTEGER NOT NULL DEFAULT 0"),
    ("alternate_contact_review_required", "INTEGER NOT NULL DEFAULT 0"),
]

# ---------------------------------------------------------------------------
# Index suggestions to speed up common queries
# ---------------------------------------------------------------------------

_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_companies_place_id      ON companies(place_id);",
    "CREATE INDEX IF NOT EXISTS idx_contacts_company_id     ON contacts(company_id);",
    "CREATE INDEX IF NOT EXISTS idx_contacts_email          ON contacts(email);",
    "CREATE INDEX IF NOT EXISTS idx_emails_company_id       ON emails(company_id);",
    "CREATE INDEX IF NOT EXISTS idx_email_sends_contact_id  ON email_sends(contact_id);",
    "CREATE INDEX IF NOT EXISTS idx_engagement_email_id     ON engagement(email_id);",
    "CREATE INDEX IF NOT EXISTS idx_followups_contact_id    ON followups(contact_id);",
    "CREATE INDEX IF NOT EXISTS idx_reply_events_thread_id        ON reply_events(gmail_thread_id);",
    "CREATE INDEX IF NOT EXISTS idx_reply_events_from_email       ON reply_events(from_email);",
    "CREATE INDEX IF NOT EXISTS idx_reply_events_matched_kp       ON reply_events(matched_kp_email);",
    "CREATE INDEX IF NOT EXISTS idx_email_verification_kp_email   ON email_verification(kp_email);",
    "CREATE INDEX IF NOT EXISTS idx_email_verification_tier       ON email_verification(email_confidence_tier);",
    "CREATE INDEX IF NOT EXISTS idx_sender_health_sender_email    ON sender_health(sender_email);",
    "CREATE INDEX IF NOT EXISTS idx_sender_health_sending_domain  ON sender_health(sending_domain);",
    "CREATE INDEX IF NOT EXISTS idx_sender_health_breaker_active  ON sender_health(sender_breaker_active);",
    "CREATE INDEX IF NOT EXISTS idx_campaign_breakers_scope_key   ON campaign_breakers(scope, scope_key);",
    "CREATE INDEX IF NOT EXISTS idx_campaign_breakers_active      ON campaign_breakers(breaker_active);",
]


def create_all_tables(conn: sqlite3.Connection) -> None:
    """
    Create all tables and indexes.  Safe to call multiple times
    (uses IF NOT EXISTS on every statement).
    """
    statements = [
        _DDL_COMPANIES,
        _DDL_COMPANY_ANALYSIS,
        _DDL_CONTACTS,
        _DDL_COMPANY_SIGNALS,
        _DDL_EMAILS,
        _DDL_EMAIL_SENDS,
        _DDL_ENGAGEMENT,
        _DDL_FOLLOWUPS,
        _DDL_REPLY_EVENTS,
        _DDL_EMAIL_VERIFICATION,
        _DDL_SENDER_HEALTH,
        _DDL_CAMPAIGN_BREAKERS,
        *_INDEXES,
    ]
    with conn:
        for stmt in statements:
            conn.execute(stmt)
    print(f"[db_schema] All tables and indexes ready.")


def migrate_schema(conn: sqlite3.Connection) -> None:
    """
    Apply incremental schema migrations for existing databases.
    Each migration is idempotent — adding a column that already exists is silently ignored.
    Call this after create_all_tables() when initialising the database connection.
    """
    def _add_columns(table: str, migrations: list) -> None:
        for col_name, col_def in migrations:
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_def}")
                conn.commit()
                print(f"[db_schema] Migration applied: {table}.{col_name} {col_def}")
            except sqlite3.OperationalError:
                pass  # column already exists — expected on subsequent startups

    _add_columns("contacts",           _MIGRATIONS_CONTACTS)
    _add_columns("email_sends",        _MIGRATIONS_EMAIL_SENDS)
    _add_columns("reply_events",       _MIGRATIONS_REPLY_EVENTS)
    _add_columns("email_verification", _MIGRATIONS_EMAIL_VERIFICATION)
    _add_columns("sender_health",      _MIGRATIONS_SENDER_HEALTH)
    _add_columns("campaign_breakers",  _MIGRATIONS_CAMPAIGN_BREAKERS)
