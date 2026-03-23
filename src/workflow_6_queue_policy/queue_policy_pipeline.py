# Workflow 6 — Queue Policy Enforcement (P1-3A)
# Pipeline orchestrator.
#
# Input:  scored_contacts.csv   (P1-2B — required)
#         verified_enriched_leads.csv (Ticket 3 — optional)
# Output: queue_policy.csv  (run-scoped via RunPaths)
#
# One record per company: the P1-2B-selected primary contact, enriched with
# verification data and stamped with a deterministic policy action.
#
# Backward compatibility: Workflow 6 (email_generation) continues to read
# enriched_leads.csv unchanged.  This file is purely additive for P1-3B.

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from config.run_paths import RunPaths
from src.workflow_6_queue_policy.queue_policy_models import (
    QUEUE_POLICY_FIELDS,
    QUEUE_POLICY_VERSION,
    SOURCE_FALLBACK,
    SOURCE_SCORED_CONTACTS,
    SOURCE_VERIFIED_LEADS,
    QueuePolicyRecord,
    QueuePolicyStats,
)
from src.workflow_6_queue_policy.queue_policy_rules import apply_policy


# ---------------------------------------------------------------------------
# Load primary contacts from scored_contacts.csv
# ---------------------------------------------------------------------------

def _load_scored_primaries(scored_path: Path) -> list[dict]:
    """
    Load scored_contacts.csv and return only primary-contact rows.

    Returns [] if file is absent, unreadable, or empty.
    Each returned dict is a raw CSV row with all columns from P1-2B.
    """
    if not scored_path or not scored_path.exists():
        return []
    try:
        with open(scored_path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    except Exception as exc:
        print(f"[Workflow 6 — Queue Policy]  Could not read scored_contacts.csv: {exc}")
        return []

    primaries = [r for r in rows if r.get("is_primary_contact", "").strip() == "true"]
    return primaries


# ---------------------------------------------------------------------------
# Build verification index from verified_enriched_leads.csv
# ---------------------------------------------------------------------------

def _build_verification_index(verified_path: Path | None) -> dict[str, dict]:
    """
    Load verified_enriched_leads.csv → {kp_email_lower: row}.
    Returns {} when file is absent or unreadable (graceful degradation).
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
        print(f"[Workflow 6 — Queue Policy]  Could not load verification index: {exc}")
        return {}


# ---------------------------------------------------------------------------
# Build one QueuePolicyRecord from a scored_contacts row + optional verification
# ---------------------------------------------------------------------------

def _build_record(
    scored_row: dict,
    verification_index: dict[str, dict],
) -> QueuePolicyRecord:
    """
    Construct a QueuePolicyRecord from one scored_contacts primary row.

    Verification data is resolved in priority order:
    1. Fields already on the scored_contacts row (populated by P1-2B from
       verified_enriched_leads.csv when Step 5.9 ran before Step 5.6)
    2. Look up in verification_index by primary contact email
    3. Fallback: no verification data; conservative policy rules apply
    """
    rec = QueuePolicyRecord()

    # ── Company identity ──────────────────────────────────────────────────
    rec.company_name          = scored_row.get("company_name", "")
    rec.website               = scored_row.get("website", "")
    rec.place_id              = scored_row.get("place_id", "")
    rec.lead_score            = scored_row.get("lead_score", "")
    rec.qualification_status  = scored_row.get("qualification_status", "")
    rec.target_tier           = scored_row.get("target_tier", "")
    rec.company_type          = scored_row.get("company_type", "")
    rec.market_focus          = scored_row.get("market_focus", "")

    # ── Selected primary contact ──────────────────────────────────────────
    rec.selected_contact_email       = scored_row.get("kp_email", "")
    rec.selected_contact_name        = scored_row.get("kp_name", "")
    rec.selected_contact_title       = scored_row.get("kp_title", "")
    rec.selected_contact_rank        = scored_row.get("contact_priority_rank", "")
    rec.selected_contact_is_generic  = scored_row.get("is_generic_mailbox", "false")
    rec.selected_contact_source      = scored_row.get("enrichment_source", "")
    rec.contact_fit_score            = scored_row.get("contact_fit_score", "")
    rec.contact_selection_reason     = scored_row.get("contact_selection_reason", "")

    # ── Resolve verification data ─────────────────────────────────────────
    # Priority 1: fields already on scored_contacts row from P1-2B
    elig_from_scored  = (scored_row.get("send_eligibility") or "").strip()
    pool_from_scored  = (scored_row.get("send_pool") or "").strip()
    tier_from_scored  = (scored_row.get("email_confidence_tier") or "").strip()

    if elig_from_scored:
        rec.selected_send_eligibility       = elig_from_scored
        rec.selected_send_pool              = pool_from_scored
        rec.selected_email_confidence_tier  = tier_from_scored
        rec.verification_source             = SOURCE_SCORED_CONTACTS

    else:
        # Priority 2: look up in verification_index by primary email
        email_key = rec.selected_contact_email.strip().lower()
        ver_row   = verification_index.get(email_key) if email_key else None

        if ver_row:
            rec.selected_send_eligibility       = ver_row.get("send_eligibility", "")
            rec.selected_send_pool              = ver_row.get("send_pool", "")
            rec.selected_email_confidence_tier  = ver_row.get("email_confidence_tier", "")
            rec.verification_source             = SOURCE_VERIFIED_LEADS
        else:
            # Priority 3: no verification data
            rec.selected_send_eligibility       = ""
            rec.selected_send_pool              = ""
            rec.selected_email_confidence_tier  = ""
            rec.verification_source             = SOURCE_FALLBACK

    rec.policy_version = QUEUE_POLICY_VERSION
    return rec


# ---------------------------------------------------------------------------
# Save queue_policy.csv and policy_summary.json
# ---------------------------------------------------------------------------

def _save_queue_policy(records: list[QueuePolicyRecord], out_path: Path) -> None:
    """Write all QueuePolicyRecord objects to queue_policy.csv."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=QUEUE_POLICY_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for rec in records:
            writer.writerow(rec.to_csv_row())
    print(f"[Workflow 6 — Queue Policy]  Wrote {len(records)} records → {out_path}")


def _save_policy_summary(stats: "QueuePolicyStats", out_path: Path) -> None:
    """
    Write queue-stage policy counts to policy_summary.json (P1-3C).

    This persists the queue-time policy distribution so it can later be
    compared against send-time policy counts in campaign_status_summary.json.
    """
    summary = {
        "generated_at":   datetime.now(tz=timezone.utc).isoformat(),
        "policy_version": QUEUE_POLICY_VERSION,
        "queue_stage": {
            "total":          stats.total_evaluated,
            "queue_normal":   stats.queue_normal_count,
            "queue_limited":  stats.queue_limited_count,
            "hold":           stats.hold_count,
            "generic_only":   stats.generic_only_count,
            "block":          stats.block_count,
            "named_primary":  stats.named_primary_count,
            "generic_primary": stats.generic_primary_count,
            "errors":         stats.error_count,
        },
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"[Workflow 6 — Queue Policy]  Policy summary → {out_path}")


# ---------------------------------------------------------------------------
# Main pipeline entry point
# ---------------------------------------------------------------------------

def run(paths: RunPaths | None = None) -> dict:
    """
    Queue policy enforcement pipeline.

    Args:
        paths: Explicit RunPaths from campaign_runner; fetched from active
               global when None (standalone / backward-compat invocation).

    Returns a summary dict:
      {total, queue_normal, queue_limited, hold, generic_only, block,
       named_primary, generic_primary, errors, output_file}
    """
    if paths is None:
        from config.run_paths import require_active_run_paths
        paths = require_active_run_paths()

    scored_path   = paths.scored_contacts_file
    verified_path = paths.verified_enriched_leads_file
    out_path      = paths.queue_policy_file

    print(f"[Workflow 6 — Queue Policy]  Input: {scored_path}")

    # ── Load scored primary contacts ──────────────────────────────────────
    primaries = _load_scored_primaries(scored_path)
    if not primaries:
        print(
            f"[Workflow 6 — Queue Policy]  No primary contacts found in "
            f"scored_contacts.csv — writing empty queue_policy.csv."
        )
        _save_queue_policy([], out_path)
        _save_policy_summary(QueuePolicyStats(), paths.policy_summary_file)
        return {
            "total": 0, "queue_normal": 0, "queue_limited": 0,
            "hold": 0, "generic_only": 0, "block": 0,
            "named_primary": 0, "generic_primary": 0,
            "errors": 0, "output_file": str(out_path),
        }

    # ── Build verification index (optional) ───────────────────────────────
    verification_index = _build_verification_index(verified_path)
    if verification_index:
        print(
            f"[Workflow 6 — Queue Policy]  Verification index: "
            f"{len(verification_index)} entries from {verified_path.name}"
        )
    else:
        print(
            f"[Workflow 6 — Queue Policy]  No verification data found — "
            f"applying conservative fallback rules."
        )

    # ── Process each primary contact ──────────────────────────────────────
    stats   = QueuePolicyStats()
    records: list[QueuePolicyRecord] = []

    for scored_row in primaries:
        try:
            rec = _build_record(scored_row, verification_index)
            apply_policy(rec)
            records.append(rec)
            stats.record(rec)
        except Exception as exc:
            stats.error_count += 1
            cname = scored_row.get("company_name", "<unknown>")
            print(f"[Workflow 6 — Queue Policy]  ERROR processing {cname!r}: {exc}")
            continue

    # ── Write output ──────────────────────────────────────────────────────
    _save_queue_policy(records, out_path)
    _save_policy_summary(stats, paths.policy_summary_file)
    stats.print_summary()

    return {
        "total":          stats.total_evaluated,
        "queue_normal":   stats.queue_normal_count,
        "queue_limited":  stats.queue_limited_count,
        "hold":           stats.hold_count,
        "generic_only":   stats.generic_only_count,
        "block":          stats.block_count,
        "named_primary":  stats.named_primary_count,
        "generic_primary":stats.generic_primary_count,
        "errors":         stats.error_count,
        "output_file":    str(out_path),
    }


# ---------------------------------------------------------------------------
# Downstream helper: load normal-queue candidates
# ---------------------------------------------------------------------------

def load_queued_normal(policy_path: Path) -> list[dict]:
    """
    Return rows from queue_policy.csv where send_policy_action == "queue_normal".

    Returns [] when the file is missing or unreadable.
    Intended for P1-3B send-time enforcement or downstream analytics.
    """
    if not policy_path or not policy_path.exists():
        return []
    try:
        with open(policy_path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        return [r for r in rows if r.get("send_policy_action") == "queue_normal"]
    except Exception:
        return []


def load_queue_policy(policy_path: Path) -> list[dict]:
    """
    Return all rows from queue_policy.csv.

    Returns [] when the file is missing or unreadable.
    """
    if not policy_path or not policy_path.exists():
        return []
    try:
        with open(policy_path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []
