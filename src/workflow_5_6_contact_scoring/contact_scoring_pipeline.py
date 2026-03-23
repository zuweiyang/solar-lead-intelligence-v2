# Workflow 5.6 — Contact Scoring + Priority Selection (P1-2B)
# Pipeline orchestrator: reads enriched_contacts.csv, scores and ranks contacts,
# writes scored_contacts.csv, optionally persists to DB.
#
# Input:  enriched_contacts.csv  (Workflow 5.5 / P1-2A)
#         verified_enriched_leads.csv (optional — adds email_confidence_tier if available)
# Output: scored_contacts.csv   (run-scoped)

from __future__ import annotations

import csv
import json
from pathlib import Path

from config.run_paths import RunPaths
from src.workflow_5_6_contact_scoring.contact_scoring_models import (
    CONTACT_SCORING_VERSION,
    SCORED_CONTACTS_FIELDS,
    ContactScoringStats,
    ScoredContact,
)
from src.workflow_5_6_contact_scoring.contact_scoring_rules import (
    assign_priority,
    compute_contact_fit_score,
    title_bucket,
)


# ---------------------------------------------------------------------------
# Verification tier lookup (optional enrichment from Ticket 3)
# ---------------------------------------------------------------------------

def _build_verification_index(verified_path: Path) -> dict[str, dict]:
    """
    Load verified_enriched_leads.csv and build {kp_email_lower: row} index.
    Returns {} if file is absent or unreadable (graceful degradation).
    """
    if not verified_path or not verified_path.exists():
        return {}
    try:
        with open(verified_path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        index: dict[str, dict] = {}
        for row in rows:
            email = (row.get("kp_email") or "").strip().lower()
            if email:
                index[email] = row
        return index
    except Exception as exc:
        print(f"[Workflow 5.6]   Could not load verification index: {exc}")
        return {}


def _apply_verification(contact: ScoredContact, index: dict[str, dict]) -> None:
    """Populate verification fields on contact from the index (in-place, non-fatal)."""
    if not index:
        return
    email_key = (contact.kp_email or "").strip().lower()
    if not email_key:
        return
    row = index.get(email_key)
    if not row:
        return
    contact.email_confidence_tier = row.get("email_confidence_tier", "")
    contact.send_eligibility       = row.get("send_eligibility", "")
    contact.send_pool              = row.get("send_pool", "")


# ---------------------------------------------------------------------------
# Row → ScoredContact
# ---------------------------------------------------------------------------

def _row_to_scored_contact(row: dict) -> ScoredContact:
    """Build a ScoredContact from a csv.DictReader row (all values are strings)."""
    sc = ScoredContact()
    # Copy every field that exists on ScoredContact from the CSV row
    for key in vars(sc):
        if key in row:
            setattr(sc, key, row[key])
    # Ensure booleans stored as string don't confuse downstream
    # (is_primary_contact etc. will be set by assign_priority later)
    sc.contact_scoring_version = CONTACT_SCORING_VERSION
    return sc


# ---------------------------------------------------------------------------
# Group contacts by company
# ---------------------------------------------------------------------------

def _group_by_company(contacts: list[ScoredContact]) -> dict[str, list[ScoredContact]]:
    """
    Group contacts by (place_id or website) — stable company key.
    Preserves original order within each group.
    """
    groups: dict[str, list[ScoredContact]] = {}
    for c in contacts:
        # Prefer place_id as the stable key; fall back to website, then company_name
        key = c.place_id or c.website or c.company_name or "__unknown__"
        groups.setdefault(key, []).append(c)
    return groups


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def _save_scored_contacts(contacts: list[ScoredContact], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SCORED_CONTACTS_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(c.to_csv_row() for c in contacts)
    print(f"[Workflow 5.6] Saved {len(contacts)} scored contacts → {out_path}")


def _persist_to_db(contacts: list[ScoredContact]) -> None:
    """
    Update contact_fit_score, contact_priority_rank, is_primary_contact, and
    alternate_contact_review_candidate in the DB contacts table.

    Non-fatal: if DB is unavailable or schema is not yet migrated, logs and continues.
    TODO (future ticket): expand to full contact scoring DB schema.
    """
    try:
        from src.database.db_connection import get_db_connection
        conn = get_db_connection()
        cursor = conn.cursor()
        updated = 0
        for c in contacts:
            if not c.kp_email:
                continue
            try:
                cursor.execute(
                    """
                    UPDATE contacts
                       SET contact_fit_score                   = ?,
                           contact_priority_rank               = ?,
                           is_primary_contact                  = ?,
                           alternate_contact_review_candidate  = ?
                     WHERE LOWER(email) = LOWER(?)
                    """,
                    (
                        c.contact_fit_score,
                        c.contact_priority_rank,
                        1 if c.is_primary_contact else 0,
                        1 if c.alternate_contact_review_candidate else 0,
                        c.kp_email,
                    ),
                )
                updated += cursor.rowcount
            except Exception as exc:
                print(f"[Workflow 5.6]   DB update skipped for {c.kp_email!r}: {exc}")
        conn.commit()
        conn.close()
        print(f"[Workflow 5.6] DB: updated {updated} contact rows")
    except Exception as exc:
        print(f"[Workflow 5.6] DB unavailable (non-fatal): {exc}")


# ---------------------------------------------------------------------------
# Public run()
# ---------------------------------------------------------------------------

def run(paths: RunPaths | None = None) -> list[ScoredContact]:
    """
    Score and rank all contacts produced by Workflow 5.5 (P1-2A).

    Steps:
      1. Load enriched_contacts.csv
      2. Optionally enrich with verification tiers from verified_enriched_leads.csv
      3. Compute contact_fit_score for each contact
      4. Group by company; sort and assign priority rank / selection flags
      5. Write scored_contacts.csv
      6. Persist key fields to DB (non-fatal)
      7. Print summary

    Args:
        paths: explicit RunPaths; if None, fetched from the active global.

    Returns:
        Flat list of ScoredContact objects (best-first within each company).
    """
    if paths is None:
        from config.run_paths import require_active_run_paths
        paths = require_active_run_paths()

    enriched_contacts_path = paths.enriched_contacts_file
    scored_contacts_path   = paths.scored_contacts_file

    # ── 0. Guard: no input file ──────────────────────────────────────────────
    if not enriched_contacts_path.exists():
        print(
            f"[Workflow 5.6] No enriched_contacts.csv found at {enriched_contacts_path} "
            "— writing empty scored_contacts.csv."
        )
        _save_scored_contacts([], scored_contacts_path)
        return []

    # ── 1. Load enriched_contacts ────────────────────────────────────────────
    with open(enriched_contacts_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print("[Workflow 5.6] enriched_contacts.csv is empty — writing empty output.")
        _save_scored_contacts([], scored_contacts_path)
        return []

    print(f"[Workflow 5.6] Scoring {len(rows)} contacts from {enriched_contacts_path}")

    # ── 2. Build optional verification index ────────────────────────────────
    ver_index = _build_verification_index(paths.verified_enriched_leads_file)
    if ver_index:
        print(f"[Workflow 5.6] Verification index loaded: {len(ver_index)} entries")
    else:
        print("[Workflow 5.6] No verification data — email_quality_score uses defaults")

    # ── 3. Convert rows → ScoredContact, apply verification, score ───────────
    stats = ContactScoringStats()
    scored: list[ScoredContact] = []

    for row in rows:
        try:
            sc = _row_to_scored_contact(row)
            _apply_verification(sc, ver_index)
            compute_contact_fit_score(sc)
            scored.append(sc)
            stats.total_contacts += 1
        except Exception as exc:
            name = row.get("company_name") or row.get("kp_email", "?")
            print(f"[Workflow 5.6]   ERROR scoring contact for {name!r}: {exc}")
            stats.errors += 1

    # ── 4. Group by company, assign priority ────────────────────────────────
    groups = _group_by_company(scored)
    stats.total_companies = len(groups)
    all_ranked: list[ScoredContact] = []

    for company_key, group in groups.items():
        try:
            ranked = assign_priority(group)
            all_ranked.extend(ranked)

            # Update stats
            if ranked:
                primary = ranked[0]
                stats.primary_selected += 1
                is_gen = str(primary.is_generic_mailbox).lower().strip() == "true"
                if is_gen:
                    stats.generic_as_primary += 1
                else:
                    stats.named_as_primary += 1
                stats.record_title(title_bucket(primary.kp_title))

            stats.fallback_contacts += sum(
                1 for c in ranked if c.is_fallback_contact
            )

            if not ranked:
                stats.zero_contact_companies += 1

        except Exception as exc:
            print(f"[Workflow 5.6]   ERROR assigning priority for {company_key!r}: {exc}")
            stats.errors += 1
            all_ranked.extend(group)  # preserve without selection fields

    # ── 5. Write output ──────────────────────────────────────────────────────
    _save_scored_contacts(all_ranked, scored_contacts_path)

    # ── 6. DB persistence (non-fatal) ────────────────────────────────────────
    _persist_to_db(all_ranked)

    # ── 7. Summary ───────────────────────────────────────────────────────────
    stats.print_summary()

    return all_ranked


# ---------------------------------------------------------------------------
# Backward-compat helper: get primary contact per company from scored output
# ---------------------------------------------------------------------------

def load_primary_contacts(scored_path: Path) -> dict[str, dict]:
    """
    Load scored_contacts.csv and return {place_id_or_website: primary_contact_row}
    for downstream consumers that need the selected primary contact.

    Returns {} if file is absent.
    """
    if not scored_path or not scored_path.exists():
        return {}
    try:
        with open(scored_path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        index: dict[str, dict] = {}
        for row in rows:
            if str(row.get("is_primary_contact", "")).lower() != "true":
                continue
            key = row.get("place_id") or row.get("website") or row.get("company_name", "")
            if key and key not in index:
                index[key] = row
        return index
    except Exception as exc:
        print(f"[Workflow 5.6] Could not load primary contacts: {exc}")
        return {}


if __name__ == "__main__":
    run()
