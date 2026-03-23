"""
Test suite — P1-2A: Multi-Contact Enrichment Output
=====================================================

Groups:
  A  generic mailbox detection
  B  enrich_lead_multi() returns a list with rank=1 as primary
  C  Apollo multi-contact mock (returns up to 3)
  D  Hunter multi-contact mock (returns up to 3)
  E  Slot-filling: Apollo + Hunter combined
  F  Website multi-contact mock
  G  Mock mode (no API keys)
  H  Guessed-email fallback (live run, real strategies failed)
  I  enriched_contacts.csv output: all ENRICHED_CONTACTS_FIELDS present, one row per contact
  J  enriched_leads.csv backward compat: ENRICHED_FIELDS only, one row per company
  K  DB migration: contacts table has contact_rank and is_generic_mailbox columns
  L  csv_sync.sync_enriched_contacts inserts multi-contact rows with correct metadata
  M  is_generic_mailbox flag correct in saved contacts CSV
"""
from __future__ import annotations

import csv
import io
import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

# Ensure repo root is on sys.path when run directly
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.workflow_5_5_lead_enrichment.enricher import (
    ENRICHED_CONTACTS_FIELDS,
    ENRICHED_FIELDS,
    _is_generic_mailbox,
    _make_contact_row,
    enrich_lead_multi,
    save_enriched_contacts,
    save_enriched_leads,
)
from src.database.db_schema import _MIGRATIONS_CONTACTS
from src.database.db_utils import insert_contact, insert_company


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAILURES: list[str] = []


def _assert(condition: bool, message: str) -> None:
    if condition:
        print(f"  [PASS] {message}")
    else:
        print(f"  [FAIL] {message}")
        _FAILURES.append(message)


def _make_lead(**kwargs) -> dict:
    """Return a minimal lead dict for enrichment tests."""
    base = {
        "company_name":      "Test Solar Co",
        "website":           "https://testsolar.com",
        "place_id":          "test_place_001",
        "company_type":      "solar_installer",
        "market_focus":      "commercial",
        "services_detected": "solar_installation",
        "confidence_score":  "0.85",
        "classification_method": "llm",
        "lead_score":        "72",
        "score_breakdown":   "base:40,market:10",
        "target_tier":       "A",
    }
    base.update(kwargs)
    return base


def _make_kp(name: str, title: str, email: str) -> dict:
    return {"kp_name": name, "kp_title": title, "kp_email": email}


_DDL_COMPANIES_SIMPLE = """
CREATE TABLE IF NOT EXISTS companies (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    place_id     TEXT    UNIQUE,
    company_name TEXT    NOT NULL,
    website      TEXT,
    phone        TEXT,
    address      TEXT,
    city         TEXT,
    province     TEXT,
    country      TEXT,
    google_rating   REAL,
    google_category TEXT,
    source_keyword  TEXT,
    source_location TEXT,
    created_at   TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at   TEXT    NOT NULL DEFAULT (datetime('now'))
);
"""

_DDL_CONTACTS_SIMPLE = """
CREATE TABLE IF NOT EXISTS contacts (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id         INTEGER NOT NULL,
    contact_name       TEXT,
    contact_title      TEXT,
    email              TEXT,
    phone              TEXT,
    source             TEXT,
    confidence         REAL,
    contact_rank       INTEGER NOT NULL DEFAULT 1,
    is_generic_mailbox INTEGER NOT NULL DEFAULT 0,
    created_at         TEXT    NOT NULL DEFAULT (datetime('now'))
);
"""


def _in_memory_db() -> sqlite3.Connection:
    """Create a minimal in-memory DB with just companies + contacts tables."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(_DDL_COMPANIES_SIMPLE)
    conn.execute(_DDL_CONTACTS_SIMPLE)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Group A — Generic mailbox detection
# ---------------------------------------------------------------------------

def test_group_a_generic_mailbox():
    print("\n[Group A] Generic mailbox detection")

    _assert(_is_generic_mailbox("info@example.com"),    "info@ is generic")
    _assert(_is_generic_mailbox("sales@example.com"),   "sales@ is generic")
    _assert(_is_generic_mailbox("contact@example.com"), "contact@ is generic")
    _assert(_is_generic_mailbox("admin@example.com"),   "admin@ is generic")
    _assert(_is_generic_mailbox("support@example.com"), "support@ is generic")
    _assert(_is_generic_mailbox("hello@example.com"),   "hello@ is generic")
    _assert(_is_generic_mailbox("enquiries@example.com"), "enquiries@ is generic")
    _assert(_is_generic_mailbox("noreply@example.com"), "noreply@ is generic")

    _assert(not _is_generic_mailbox("john.smith@example.com"),  "named email is NOT generic")
    _assert(not _is_generic_mailbox("j.doe@example.com"),       "j.doe@ is NOT generic")
    _assert(not _is_generic_mailbox("sarah.johnson@solar.ca"),  "sarah.johnson@ is NOT generic")
    _assert(not _is_generic_mailbox("ceo@example.com"),         "ceo@ is NOT in generic list")
    _assert(not _is_generic_mailbox(""),                         "empty string is NOT generic")
    _assert(not _is_generic_mailbox("notanemail"),               "non-email is NOT generic")

    # Case insensitive
    _assert(_is_generic_mailbox("INFO@EXAMPLE.COM"), "INFO@ (uppercase) is generic")
    _assert(_is_generic_mailbox("Sales@Domain.ca"),  "Sales@ (mixed case) is generic")


# ---------------------------------------------------------------------------
# Group B — enrich_lead_multi basic structure
# ---------------------------------------------------------------------------

def test_group_b_multi_returns_list():
    print("\n[Group B] enrich_lead_multi() returns list with correct structure")
    lead = _make_lead()

    kp_list = [
        _make_kp("Alice Chan", "CEO", "alice@testsolar.com"),
        _make_kp("Bob Lee",    "Director", "bob@testsolar.com"),
        _make_kp("", "",       "info@testsolar.com"),
    ]

    with (
        patch("src.workflow_5_5_lead_enrichment.enricher.APOLLO_API_KEY", "test_key"),
        patch("src.workflow_5_5_lead_enrichment.enricher.HUNTER_API_KEY", ""),
        patch("src.workflow_5_5_lead_enrichment.enricher._apollo_org_enrich"),
        patch("src.workflow_5_5_lead_enrichment.enricher._apollo_people_search_multi",
              return_value=kp_list),
        patch("src.workflow_5_5_lead_enrichment.enricher.time.sleep"),
    ):
        contacts = enrich_lead_multi(lead, index=0, max_contacts=3)

    _assert(isinstance(contacts, list),      "result is a list")
    _assert(len(contacts) == 3,              "3 contacts returned")
    _assert(contacts[0]["contact_rank"] == 1, "first contact has rank=1")
    _assert(contacts[1]["contact_rank"] == 2, "second contact has rank=2")
    _assert(contacts[2]["contact_rank"] == 3, "third contact has rank=3")

    # rank=1 is the primary; company fields must be present
    primary = contacts[0]
    _assert(primary["kp_email"] == "alice@testsolar.com",  "rank=1 email is alice@")
    _assert(primary["company_name"] == "Test Solar Co",    "company_name propagated")
    _assert(primary["enrichment_source"] == "apollo",      "source=apollo")

    # is_generic_mailbox flags
    _assert(contacts[0]["is_generic_mailbox"] == "false", "alice@ not generic")
    _assert(contacts[1]["is_generic_mailbox"] == "false", "bob@ not generic")
    _assert(contacts[2]["is_generic_mailbox"] == "true",  "info@ is generic")


# ---------------------------------------------------------------------------
# Group C — Apollo multi-contact (mock)
# ---------------------------------------------------------------------------

def test_group_c_apollo_multi():
    print("\n[Group C] Apollo multi-contact: returns up to max_contacts")
    lead = _make_lead()

    # Apollo returns 5 people but we cap at max_contacts=2
    apollo_people = [
        _make_kp("Alice Chan",  "CEO",      "alice@testsolar.com"),
        _make_kp("Bob Lee",     "Director", "bob@testsolar.com"),
        _make_kp("Carol Wang",  "Manager",  "carol@testsolar.com"),
        _make_kp("Dave Singh",  "Founder",  "dave@testsolar.com"),
        _make_kp("Eve Martinez","Owner",    "eve@testsolar.com"),
    ]

    with (
        patch("src.workflow_5_5_lead_enrichment.enricher.APOLLO_API_KEY", "key"),
        patch("src.workflow_5_5_lead_enrichment.enricher.HUNTER_API_KEY", ""),
        patch("src.workflow_5_5_lead_enrichment.enricher._apollo_org_enrich"),
        patch("src.workflow_5_5_lead_enrichment.enricher._apollo_people_search_multi",
              return_value=apollo_people[:2]),  # simulated max_results=2 applied inside
        patch("src.workflow_5_5_lead_enrichment.enricher.time.sleep"),
    ):
        contacts = enrich_lead_multi(lead, max_contacts=2)

    _assert(len(contacts) == 2,        "capped at max_contacts=2")
    _assert(contacts[0]["contact_rank"] == 1, "first is rank 1")
    _assert(contacts[1]["contact_rank"] == 2, "second is rank 2")
    emails = [c["kp_email"] for c in contacts]
    _assert(len(set(emails)) == len(emails), "no duplicate emails")

    # All apollo source
    for c in contacts:
        _assert(c["enrichment_source"] == "apollo", f"{c['kp_email']} source=apollo")


# ---------------------------------------------------------------------------
# Group D — Hunter multi-contact (mock)
# ---------------------------------------------------------------------------

def test_group_d_hunter_multi():
    print("\n[Group D] Hunter multi-contact: returns up to max_contacts")
    lead = _make_lead()

    hunter_people = [
        _make_kp("Frank Tan",  "CEO",      "frank@testsolar.com"),
        _make_kp("Grace Kim",  "Director", "grace@testsolar.com"),
        _make_kp("", "",       "info@testsolar.com"),
    ]

    with (
        patch("src.workflow_5_5_lead_enrichment.enricher.APOLLO_API_KEY", ""),
        patch("src.workflow_5_5_lead_enrichment.enricher.HUNTER_API_KEY", "hunter_key"),
        patch("src.workflow_5_5_lead_enrichment.enricher._query_hunter_multi",
              return_value=hunter_people),
        patch("src.workflow_5_5_lead_enrichment.enricher.time.sleep"),
    ):
        contacts = enrich_lead_multi(lead, max_contacts=3)

    _assert(len(contacts) == 3, "3 contacts from Hunter")
    for c in contacts:
        _assert(c["enrichment_source"] == "hunter", f"{c['kp_email']} source=hunter")

    _assert(contacts[0]["kp_email"] == "frank@testsolar.com",  "rank=1 is frank@")
    _assert(contacts[2]["is_generic_mailbox"] == "true",        "info@ is generic (rank=3)")


# ---------------------------------------------------------------------------
# Group E — Slot-filling: Apollo + Hunter combined
# ---------------------------------------------------------------------------

def test_group_e_slot_filling():
    print("\n[Group E] Slot-filling: Apollo fills 1, Hunter fills remaining 2")
    lead = _make_lead()

    apollo_result = [_make_kp("Alice", "CEO", "alice@testsolar.com")]
    hunter_results = [
        _make_kp("Bob", "Director", "bob@testsolar.com"),
        _make_kp("", "", "sales@testsolar.com"),
    ]

    with (
        patch("src.workflow_5_5_lead_enrichment.enricher.APOLLO_API_KEY", "key"),
        patch("src.workflow_5_5_lead_enrichment.enricher.HUNTER_API_KEY", "hkey"),
        patch("src.workflow_5_5_lead_enrichment.enricher._apollo_org_enrich"),
        patch("src.workflow_5_5_lead_enrichment.enricher._apollo_people_search_multi",
              return_value=apollo_result),
        patch("src.workflow_5_5_lead_enrichment.enricher._query_hunter_multi",
              return_value=hunter_results),
        patch("src.workflow_5_5_lead_enrichment.enricher.time.sleep"),
    ):
        contacts = enrich_lead_multi(lead, max_contacts=3)

    _assert(len(contacts) == 3, "3 contacts total")
    _assert(contacts[0]["enrichment_source"] == "apollo",  "rank=1 from apollo")
    _assert(contacts[1]["enrichment_source"] == "hunter",  "rank=2 from hunter")
    _assert(contacts[2]["enrichment_source"] == "hunter",  "rank=3 from hunter")

    # No duplicates
    emails = [c["kp_email"] for c in contacts]
    _assert(len(set(emails)) == 3, "all emails are distinct")

    # is_generic_mailbox correctly set
    _assert(contacts[0]["is_generic_mailbox"] == "false", "alice@ not generic")
    _assert(contacts[2]["is_generic_mailbox"] == "true",  "sales@ is generic")

    # Hunter not called if Apollo fills all slots
    with (
        patch("src.workflow_5_5_lead_enrichment.enricher.APOLLO_API_KEY", "key"),
        patch("src.workflow_5_5_lead_enrichment.enricher.HUNTER_API_KEY", "hkey"),
        patch("src.workflow_5_5_lead_enrichment.enricher._apollo_org_enrich"),
        patch("src.workflow_5_5_lead_enrichment.enricher._apollo_people_search_multi",
              return_value=[
                  _make_kp("A", "CEO",   "a@testsolar.com"),
                  _make_kp("B", "Dir",   "b@testsolar.com"),
                  _make_kp("C", "Owner", "c@testsolar.com"),
              ]),
        patch("src.workflow_5_5_lead_enrichment.enricher._query_hunter_multi") as mock_hunter,
        patch("src.workflow_5_5_lead_enrichment.enricher.time.sleep"),
    ):
        contacts2 = enrich_lead_multi(lead, max_contacts=3)

    _assert(len(contacts2) == 3,               "3 contacts all from Apollo")
    _assert(not mock_hunter.called,            "Hunter not called when Apollo fills all slots")


# ---------------------------------------------------------------------------
# Group F — Website multi-contact
# ---------------------------------------------------------------------------

def test_group_f_website_multi():
    print("\n[Group F] Website multi-contact: returns site_emails as separate contacts")
    lead = _make_lead()

    site_entry = {
        "site_emails": ["info@testsolar.com", "contact@testsolar.com"],
        "site_phones": ["+1-604-555-0101"],
    }

    with (
        patch("src.workflow_5_5_lead_enrichment.enricher.APOLLO_API_KEY", "key"),
        patch("src.workflow_5_5_lead_enrichment.enricher.HUNTER_API_KEY", ""),
        patch("src.workflow_5_5_lead_enrichment.enricher._apollo_org_enrich"),
        patch("src.workflow_5_5_lead_enrichment.enricher._apollo_people_search_multi",
              return_value=[]),
        patch("src.workflow_5_5_lead_enrichment.enricher._load_site_contacts",
              return_value={"testsolar.com": site_entry}),
        patch("src.workflow_5_5_lead_enrichment.enricher.time.sleep"),
    ):
        contacts = enrich_lead_multi(lead, max_contacts=3)

    site_contacts = [c for c in contacts if c["enrichment_source"] == "website"]
    _assert(len(site_contacts) >= 1, "at least 1 website contact returned")

    site_emails = [c["kp_email"] for c in site_contacts]
    _assert("info@testsolar.com" in site_emails, "info@ from site_emails[0]")

    for c in site_contacts:
        _assert(c["is_generic_mailbox"] == "true",
                f"website email {c['kp_email']} is generic")


# ---------------------------------------------------------------------------
# Group G — Mock mode (no API keys)
# ---------------------------------------------------------------------------

def test_group_g_mock_mode():
    print("\n[Group G] Mock mode: no API keys → deterministic fake contacts")
    lead = _make_lead()

    with (
        patch("src.workflow_5_5_lead_enrichment.enricher.APOLLO_API_KEY", ""),
        patch("src.workflow_5_5_lead_enrichment.enricher.HUNTER_API_KEY", ""),
        patch("src.workflow_5_5_lead_enrichment.enricher._load_site_contacts",
              return_value={}),
    ):
        contacts = enrich_lead_multi(lead, max_contacts=3)

    _assert(len(contacts) >= 1, "at least 1 mock contact returned")
    for c in contacts:
        _assert(c["enrichment_source"] == "mock", f"rank={c['contact_rank']} source=mock")
        _assert(c["contact_rank"] >= 1, "contact_rank >= 1")
        _assert("@" in c["kp_email"],    "mock email contains @")


# ---------------------------------------------------------------------------
# Group H — Guessed-email fallback
# ---------------------------------------------------------------------------

def test_group_h_guessed_fallback():
    print("\n[Group H] Guessed fallback for live run with no real contacts")
    lead = _make_lead()

    with (
        patch("src.workflow_5_5_lead_enrichment.enricher.APOLLO_API_KEY", "key"),
        patch("src.workflow_5_5_lead_enrichment.enricher.HUNTER_API_KEY", ""),
        patch("src.workflow_5_5_lead_enrichment.enricher._apollo_org_enrich"),
        patch("src.workflow_5_5_lead_enrichment.enricher._apollo_people_search_multi",
              return_value=[]),
        patch("src.workflow_5_5_lead_enrichment.enricher._load_site_contacts",
              return_value={}),
        patch("src.workflow_5_5_lead_enrichment.enricher.time.sleep"),
    ):
        contacts = enrich_lead_multi(lead, max_contacts=3)

    guessed = [c for c in contacts if c["enrichment_source"] == "guessed"]
    _assert(len(guessed) >= 1, "at least 1 guessed contact")
    for c in guessed:
        _assert("@testsolar.com" in c["kp_email"], "guessed email uses company domain")
        _assert(c["is_generic_mailbox"] == "true",  "guessed email is generic mailbox")


# ---------------------------------------------------------------------------
# Group I — enriched_contacts.csv output
# ---------------------------------------------------------------------------

def test_group_i_contacts_csv_output():
    print("\n[Group I] enriched_contacts.csv: correct fields, one row per contact")
    lead = _make_lead()

    kps = [
        _make_kp("Alice", "CEO",   "alice@testsolar.com"),
        _make_kp("Bob",   "Dir",   "bob@testsolar.com"),
        _make_kp("", "",           "info@testsolar.com"),
    ]

    with (
        patch("src.workflow_5_5_lead_enrichment.enricher.APOLLO_API_KEY", "key"),
        patch("src.workflow_5_5_lead_enrichment.enricher.HUNTER_API_KEY", ""),
        patch("src.workflow_5_5_lead_enrichment.enricher._apollo_org_enrich"),
        patch("src.workflow_5_5_lead_enrichment.enricher._apollo_people_search_multi",
              return_value=kps),
        patch("src.workflow_5_5_lead_enrichment.enricher.time.sleep"),
    ):
        contacts = enrich_lead_multi(lead, max_contacts=3)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False,
                                     newline="", encoding="utf-8") as f:
        tmp_path = Path(f.name)

    try:
        with patch("src.workflow_5_5_lead_enrichment.enricher.ENRICHED_CONTACTS_FILE",
                   tmp_path):
            save_enriched_contacts([contacts])

        with open(tmp_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = reader.fieldnames or []

        _assert(len(rows) == 3, "3 rows written (one per contact)")

        # All ENRICHED_CONTACTS_FIELDS present
        for field in ENRICHED_CONTACTS_FIELDS:
            _assert(field in fieldnames, f"field '{field}' in CSV header")

        # contact_rank values are 1, 2, 3
        ranks = [int(r["contact_rank"]) for r in rows]
        _assert(ranks == [1, 2, 3], "contact_rank values are [1, 2, 3]")

        # is_generic_mailbox flags
        _assert(rows[0]["is_generic_mailbox"] == "false", "alice@ not generic in CSV")
        _assert(rows[2]["is_generic_mailbox"] == "true",  "info@ generic in CSV")

        # company_name propagated to all rows
        for r in rows:
            _assert(r["company_name"] == "Test Solar Co",
                    f"rank={r['contact_rank']} has company_name")
    finally:
        tmp_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Group J — enriched_leads.csv backward compat
# ---------------------------------------------------------------------------

def test_group_j_leads_csv_backward_compat():
    print("\n[Group J] enriched_leads.csv backward compat: ENRICHED_FIELDS only, 1 row/company")

    leads = [_make_lead(company_name=f"Company {i}", website=f"https://company{i}.com")
             for i in range(3)]
    kps_per_lead = [
        [_make_kp("Alice", "CEO", f"alice@company{i}.com"),
         _make_kp("Bob",   "Dir", f"bob@company{i}.com")]
        for i in range(3)
    ]
    contacts_per_lead = []
    for i, (lead, kps) in enumerate(zip(leads, kps_per_lead)):
        rows = []
        for rank, kp in enumerate(kps, start=1):
            row = {**lead, **kp, "enrichment_source": "apollo",
                   "site_phone": "", "email_sendable": "true",
                   "contact_channel": "email", "alt_outreach_possible": "false",
                   "contact_trust": "trusted", "skip_reason": ""}
            rows.append(_make_contact_row(row, rank=rank))
        contacts_per_lead.append(rows)

    primary_contacts = [rows[0] for rows in contacts_per_lead]

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False,
                                     newline="", encoding="utf-8") as f:
        tmp_leads = Path(f.name)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False,
                                     newline="", encoding="utf-8") as f:
        tmp_contacts = Path(f.name)

    try:
        with (
            patch("src.workflow_5_5_lead_enrichment.enricher.ENRICHED_LEADS_FILE",    tmp_leads),
            patch("src.workflow_5_5_lead_enrichment.enricher.ENRICHED_CONTACTS_FILE", tmp_contacts),
        ):
            save_enriched_leads(primary_contacts)
            save_enriched_contacts(contacts_per_lead)

        # enriched_leads.csv: only ENRICHED_FIELDS, 3 rows (one per company)
        with open(tmp_leads, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            lead_rows = list(reader)
            lead_fields = reader.fieldnames or []

        _assert(len(lead_rows) == 3,                         "3 rows in enriched_leads.csv")
        _assert("contact_rank"       not in lead_fields,     "contact_rank NOT in enriched_leads.csv")
        _assert("is_generic_mailbox" not in lead_fields,     "is_generic_mailbox NOT in enriched_leads.csv")
        for field in ENRICHED_FIELDS:
            _assert(field in lead_fields, f"ENRICHED_FIELDS field '{field}' present")

        # enriched_contacts.csv: 6 rows (2 contacts × 3 companies)
        with open(tmp_contacts, newline="", encoding="utf-8") as f:
            contact_rows = list(csv.DictReader(f))
        _assert(len(contact_rows) == 6,                      "6 rows in enriched_contacts.csv")

        # Primary contacts in enriched_leads.csv match rank=1 in enriched_contacts.csv
        leads_emails    = {r["kp_email"] for r in lead_rows}
        rank1_emails    = {r["kp_email"] for r in contact_rows if r["contact_rank"] == "1"}
        _assert(leads_emails == rank1_emails,
                "emails in enriched_leads.csv match rank=1 emails in enriched_contacts.csv")
    finally:
        tmp_leads.unlink(missing_ok=True)
        tmp_contacts.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Group K — DB migration: contacts table gains new columns
# ---------------------------------------------------------------------------

def test_group_k_db_migration():
    print("\n[Group K] DB migration: contacts table gets contact_rank and is_generic_mailbox")

    # Check migration list content
    col_names = [col for col, _ in _MIGRATIONS_CONTACTS]
    _assert("contact_rank"       in col_names, "_MIGRATIONS_CONTACTS includes contact_rank")
    _assert("is_generic_mailbox" in col_names, "_MIGRATIONS_CONTACTS includes is_generic_mailbox")

    # Create a fresh in-memory DB and verify columns exist
    conn = _in_memory_db()
    try:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(contacts)").fetchall()}
        _assert("contact_rank"       in cols, "contact_rank column in contacts table")
        _assert("is_generic_mailbox" in cols, "is_generic_mailbox column in contacts table")
    finally:
        conn.close()

    # Test that insert_contact accepts the new fields
    conn2 = _in_memory_db()
    try:
        company_id = insert_company(conn2, {
            "company_name": "Migration Test Co",
            "place_id":     "mig_001",
            "website":      "https://migtest.com",
        })
        cid = insert_contact(conn2, {
            "company_id":        company_id,
            "kp_name":           "Jane Doe",
            "kp_email":          "jane@migtest.com",
            "enrichment_source": "apollo",
            "contact_rank":      2,
            "is_generic_mailbox": "false",
        })
        row = conn2.execute(
            "SELECT contact_rank, is_generic_mailbox FROM contacts WHERE id = ?", (cid,)
        ).fetchone()
        _assert(row["contact_rank"]       == 2, "contact_rank=2 stored correctly")
        _assert(row["is_generic_mailbox"] == 0, "is_generic_mailbox=0 stored correctly")

        # Test generic mailbox storage
        cid2 = insert_contact(conn2, {
            "company_id":        company_id,
            "kp_name":           "",
            "kp_email":          "info@migtest.com",
            "enrichment_source": "guessed",
            "contact_rank":      3,
            "is_generic_mailbox": "true",
        })
        row2 = conn2.execute(
            "SELECT contact_rank, is_generic_mailbox FROM contacts WHERE id = ?", (cid2,)
        ).fetchone()
        _assert(row2["contact_rank"]       == 3, "contact_rank=3 stored correctly")
        _assert(row2["is_generic_mailbox"] == 1, "is_generic_mailbox=1 for generic mailbox")
    finally:
        conn2.close()


# ---------------------------------------------------------------------------
# Group L — csv_sync.sync_enriched_contacts
# ---------------------------------------------------------------------------

def test_group_l_csv_sync_multi_contact():
    print("\n[Group L] csv_sync.sync_enriched_contacts inserts multi-contact rows")

    from src.database.csv_sync import sync_enriched_contacts

    # Build a contacts CSV with 2 companies × 2 contacts each
    companies = [
        {"company_name": "Solar A", "website": "https://solar-a.com", "place_id": "pa_001"},
        {"company_name": "Solar B", "website": "https://solar-b.com", "place_id": "pb_001"},
    ]
    contacts_data = []
    for c in companies:
        contacts_data.append({
            **c,
            "kp_name": "Alice", "kp_title": "CEO", "kp_email": f"alice@{c['website'].split('//')[1]}",
            "enrichment_source": "apollo", "contact_rank": "1", "is_generic_mailbox": "false",
            "email_sendable": "true", "contact_channel": "email",
            "alt_outreach_possible": "false", "contact_trust": "trusted", "skip_reason": "",
            "site_phone": "", "company_type": "", "market_focus": "",
            "services_detected": "", "confidence_score": "", "classification_method": "",
            "lead_score": "", "score_breakdown": "", "target_tier": "", "kp_email": f"alice@{c['website'].split('//')[1]}",
        })
        contacts_data.append({
            **c,
            "kp_name": "", "kp_title": "", "kp_email": f"info@{c['website'].split('//')[1]}",
            "enrichment_source": "guessed", "contact_rank": "2", "is_generic_mailbox": "true",
            "email_sendable": "true", "contact_channel": "email",
            "alt_outreach_possible": "false", "contact_trust": "trusted", "skip_reason": "",
            "site_phone": "", "company_type": "", "market_focus": "",
            "services_detected": "", "confidence_score": "", "classification_method": "",
            "lead_score": "", "score_breakdown": "", "target_tier": "",
        })

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False,
                                     newline="", encoding="utf-8") as f:
        tmp_path = Path(f.name)
        writer = csv.DictWriter(f, fieldnames=ENRICHED_CONTACTS_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in contacts_data:
            writer.writerow({k: row.get(k, "") for k in ENRICHED_CONTACTS_FIELDS})

    conn = _in_memory_db()
    try:
        from config import settings as _settings
        orig = getattr(_settings, "ENRICHED_CONTACTS_FILE", None)
        import src.database.csv_sync as csv_sync_mod
        orig_path = csv_sync_mod.ENRICHED_CONTACTS_FILE
        csv_sync_mod.ENRICHED_CONTACTS_FILE = tmp_path

        count = sync_enriched_contacts(conn)

        csv_sync_mod.ENRICHED_CONTACTS_FILE = orig_path

        _assert(count == 4, "4 contacts inserted (2 per company × 2 companies)")

        rows = conn.execute("SELECT * FROM contacts ORDER BY company_id, contact_rank").fetchall()
        _assert(len(rows) == 4, "4 contacts rows in DB")

        # Verify contact_rank and is_generic_mailbox stored correctly
        rank1_rows = [r for r in rows if r["contact_rank"] == 1]
        rank2_rows = [r for r in rows if r["contact_rank"] == 2]
        _assert(len(rank1_rows) == 2,           "2 rank=1 contacts (one per company)")
        _assert(len(rank2_rows) == 2,           "2 rank=2 contacts (one per company)")
        _assert(all(r["is_generic_mailbox"] == 0 for r in rank1_rows),
                "rank=1 contacts are NOT generic")
        _assert(all(r["is_generic_mailbox"] == 1 for r in rank2_rows),
                "rank=2 contacts ARE generic")
    finally:
        conn.close()
        tmp_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Group M — is_generic_mailbox end-to-end in saved CSV
# ---------------------------------------------------------------------------

def test_group_m_generic_flag_end_to_end():
    print("\n[Group M] is_generic_mailbox flag correct end-to-end via enrich_lead_multi")
    lead = _make_lead()

    # Apollo returns one named + Hunter returns one generic
    apollo_results = [_make_kp("Alice Chan", "CEO", "alice@testsolar.com")]
    hunter_results = [_make_kp("", "", "sales@testsolar.com")]

    with (
        patch("src.workflow_5_5_lead_enrichment.enricher.APOLLO_API_KEY", "key"),
        patch("src.workflow_5_5_lead_enrichment.enricher.HUNTER_API_KEY", "hkey"),
        patch("src.workflow_5_5_lead_enrichment.enricher._apollo_org_enrich"),
        patch("src.workflow_5_5_lead_enrichment.enricher._apollo_people_search_multi",
              return_value=apollo_results),
        patch("src.workflow_5_5_lead_enrichment.enricher._query_hunter_multi",
              return_value=hunter_results),
        patch("src.workflow_5_5_lead_enrichment.enricher.time.sleep"),
    ):
        contacts = enrich_lead_multi(lead, max_contacts=2)

    _assert(contacts[0]["is_generic_mailbox"] == "false", "alice@ marked not-generic")
    _assert(contacts[1]["is_generic_mailbox"] == "true",  "sales@ marked generic")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False,
                                     newline="", encoding="utf-8") as f:
        tmp = Path(f.name)
    try:
        with patch("src.workflow_5_5_lead_enrichment.enricher.ENRICHED_CONTACTS_FILE", tmp):
            save_enriched_contacts([contacts])
        with open(tmp, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        _assert(rows[0]["is_generic_mailbox"] == "false", "CSV row 1: not generic")
        _assert(rows[1]["is_generic_mailbox"] == "true",  "CSV row 2: generic")
    finally:
        tmp.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_all():
    test_group_a_generic_mailbox()
    test_group_b_multi_returns_list()
    test_group_c_apollo_multi()
    test_group_d_hunter_multi()
    test_group_e_slot_filling()
    test_group_f_website_multi()
    test_group_g_mock_mode()
    test_group_h_guessed_fallback()
    test_group_i_contacts_csv_output()
    test_group_j_leads_csv_backward_compat()
    test_group_k_db_migration()
    test_group_l_csv_sync_multi_contact()
    test_group_m_generic_flag_end_to_end()

    print(f"\n{'='*60}")
    if _FAILURES:
        print(f"RESULT: {len(_FAILURES)} FAILURE(S)")
        for f in _FAILURES:
            print(f"  - {f}")
        sys.exit(1)
    else:
        print("RESULT: ALL TESTS PASSED")
    print("="*60)


if __name__ == "__main__":
    run_all()
