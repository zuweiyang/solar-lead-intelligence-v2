# Workflow 5.9 — Email Verification Gateway: Pipeline Orchestrator
#
# run() loads enriched_leads.csv, verifies each kp_email, writes
# verified_enriched_leads.csv, and persists results to the DB.
#
# Input:  ENRICHED_LEADS_FILE          (enriched_leads.csv)
# Output: VERIFIED_ENRICHED_LEADS_FILE (verified_enriched_leads.csv)
# DB:     email_verification table (upsert per kp_email)
from __future__ import annotations

import csv
from pathlib import Path

from config.settings import ENRICHED_LEADS_FILE, VERIFIED_ENRICHED_LEADS_FILE
from config.run_paths import RunPaths
from src.workflow_5_5_lead_enrichment.enricher import ENRICHED_FIELDS
from src.workflow_5_9_email_verification.email_verifier import verify_email
from src.workflow_5_9_email_verification.verification_models import (
    TIER_E0,
    VERIFICATION_EXTRA_FIELDS,
    VerificationResult,
)
from src.workflow_5_9_email_verification.verification_provider import get_provider


def run(
    limit: int = 0,
    provider_name: str = "",
    live: bool = False,
    paths: RunPaths | None = None,
) -> dict:
    """
    Verify emails for all contacts in enriched_leads.csv.

    Args:
        limit:          Max records to process (0 = all).
        provider_name:  Override provider name (default from EMAIL_VERIFIER_PROVIDER setting).
        live:           When True, use real provider API calls; False = mock.
        paths:          Explicit RunPaths from campaign_runner; if None, fetched from
                        the active global (standalone / backward-compat invocation).

    Returns a summary dict with counts.
    """
    if paths is None:
        from config.run_paths import require_active_run_paths
        paths = require_active_run_paths()

    from config.settings import EMAIL_VERIFIER_LIVE, EMAIL_VERIFIER_PROVIDER

    pname    = provider_name or EMAIL_VERIFIER_PROVIDER
    use_live = live or EMAIL_VERIFIER_LIVE
    source_mode = "live" if use_live else "mock"

    provider = get_provider(provider_name=pname, live=use_live)

    # Load enriched leads — use explicit path from RunPaths
    enriched_path = paths.enriched_leads_file
    if not enriched_path.exists():
        print(f"[Workflow 5.9] No enriched leads file found at {enriched_path}. Skipping.")
        return {"verified": 0, "e0_blocked": 0, "error": "no_input_file"}

    with open(str(enriched_path), newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print(f"[Workflow 5.9] enriched_leads.csv is empty at {enriched_path}. Skipping.")
        return {"verified": 0, "e0_blocked": 0, "error": "empty_input"}

    if limit and limit > 0:
        rows = rows[:limit]

    # Determine output CSV fields.
    # Start from the actual input field order; ensure all ENRICHED_FIELDS are present.
    input_fields: list[str] = list(rows[0].keys())
    for fname in ENRICHED_FIELDS:
        if fname not in input_fields:
            input_fields.append(fname)
    output_fields = input_fields + [f for f in VERIFICATION_EXTRA_FIELDS if f not in input_fields]

    # Acquire DB connection — get_db_connection() raises RuntimeError if schema init fails.
    # This step is non-fatal: if DB is unavailable, CSV output is still written.
    conn = None
    db_write_count = 0
    try:
        from src.database.db_connection import get_db_connection
        conn = get_db_connection()
        print("[Workflow 5.9] DB ready — email_verification table confirmed.")
    except RuntimeError as exc:
        print(f"[Workflow 5.9] DB unavailable — schema init failed: {exc}")
        print("[Workflow 5.9] WARNING: verification results will NOT be persisted to DB this run.")
    except Exception as exc:
        print(f"[Workflow 5.9] DB unavailable (non-fatal): {exc}")

    counters: dict[str, int] = {
        "total":      len(rows),
        "verified":   0,
        "e0_blocked": 0,
        "e1":         0,
        "e2":         0,
        "e3":         0,
        "e4":         0,
        "no_email":   0,
        "errors":     0,
    }

    output_rows: list[dict] = []

    for row in rows:
        email = (row.get("kp_email") or "").strip()

        if not email or "@" not in email:
            # No verifiable email — pass through with E0/block defaults
            result = VerificationResult(
                kp_email              = email,
                email_confidence_tier = TIER_E0,
                send_eligibility      = "block",
                send_pool             = "blocked_pool",
                is_generic_mailbox    = False,
                provider_result       = "no_email",
                provider_name         = "none",
                verified_at           = "",
                source_mode           = "skipped",
                error                 = "missing_or_invalid_email",
            )
            counters["no_email"] += 1
        else:
            result = verify_email(email, provider, source_mode=source_mode)
            counters["verified"] += 1
            if result.error:
                counters["errors"] += 1

        tier_key = result.email_confidence_tier.lower()
        if tier_key in counters:
            counters[tier_key] += 1
        if result.email_confidence_tier == TIER_E0:
            counters["e0_blocked"] += 1

        # Persist to DB
        if conn:
            try:
                from src.database.db_utils import upsert_email_verification
                upsert_email_verification(conn, result)
                db_write_count += 1
            except Exception as exc:
                print(f"[Workflow 5.9] DB upsert failed for {email!r}: {exc}")

        # Build output row
        out_row = {**row}
        out_row["email_confidence_tier"] = result.email_confidence_tier
        out_row["send_eligibility"]      = result.send_eligibility
        out_row["send_pool"]             = result.send_pool
        out_row["is_generic_mailbox"]    = "true" if result.is_generic_mailbox else "false"
        out_row["provider_result"]       = result.provider_result
        out_row["provider_name"]         = result.provider_name
        out_row["verified_at"]           = result.verified_at
        out_row["source_mode"]           = result.source_mode
        out_row["verification_error"]    = result.error
        output_rows.append(out_row)

    if conn:
        conn.close()

    # Write verified_enriched_leads.csv — use explicit path from RunPaths
    out_path = paths.verified_enriched_leads_file
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(str(out_path), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=output_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(output_rows)

    db_status = (
        f"DB persisted: {db_write_count} rows"
        if db_write_count > 0
        else "DB persisted: 0 rows (DB unavailable)"
    )
    print(
        f"[Workflow 5.9] Verification complete: "
        f"{counters['verified']} verified, "
        f"{counters['e0_blocked']} blocked (E0), "
        f"{counters['errors']} errors | {db_status} → {out_path}"
    )
    return counters
