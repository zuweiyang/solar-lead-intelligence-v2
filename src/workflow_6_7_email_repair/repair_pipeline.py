# Workflow 6.7: Email Repair Loop — Orchestrator
# Selects repairable emails → rewrites → rescores → produces final send queue.

import csv
import traceback
from pathlib import Path

from config.settings import (
    REPAIRED_EMAILS_FILE, RESCORED_EMAILS_FILE,
    FINAL_SEND_QUEUE_FILE, FINAL_REJECTED_FILE,
    EMAIL_REPAIR_ERRORS_FILE, COMPANY_OPENINGS_FILE,
)
from src.workflow_6_7_email_repair.repair_selector import load_repairable
from src.workflow_6_7_email_repair.email_rewriter  import rewrite_email, _get_provider
from src.workflow_6_5_email_quality.email_quality_scorer import score_email

# Threshold for final approval after repair (slightly lower than initial 75)
REPAIR_APPROVE_THRESHOLD = 72
REPAIR_SPAM_MAX          = 35

OUTPUT_FIELDS = [
    "company_name", "website", "place_id",
    "city", "region", "country", "source_location",
    "kp_name", "kp_title", "kp_email",
    "contact_name", "contact_title", "contact_email",
    "send_target_type", "contact_source",
    "named_contact_available", "generic_contact_available",
    "contact_quality", "generic_only",
    "company_type", "market_focus", "lead_score",
    "subject", "opening_line", "email_body",
    "email_angle", "generation_mode", "generation_source",
    "personalization_score", "relevance_score", "spam_risk_score",
    "overall_score", "approval_status", "review_notes",
    "scoring_mode", "scoring_source",
    "repair_mode", "repair_source",
    "original_score", "original_status",
]

_ERROR_FIELDS = [
    "company_name", "kp_email", "subject",
    "original_status", "original_score",
    "exception_message", "traceback_snippet", "repair_mode_attempted",
]

# Numeric fields that must be int (not empty string) for downstream processing
_NUMERIC_FIELDS = (
    "lead_score", "overall_score",
    "personalization_score", "relevance_score", "spam_risk_score",
)

# Text fields that must be str (not None)
_TEXT_FIELDS = (
    "company_name", "city", "region", "country", "source_location",
    "kp_email", "kp_name", "kp_title",
    "contact_name", "contact_title", "contact_email",
    "send_target_type", "contact_source",
    "named_contact_available", "generic_contact_available",
    "contact_quality", "generic_only",
    "subject", "opening_line", "email_body",
    "review_notes", "email_angle", "company_type", "market_focus",
    "approval_status", "generation_source", "generation_mode",
)


def _normalize_record(record: dict) -> dict:
    """Normalize field types before repair processing."""
    out = dict(record)
    for f in _NUMERIC_FIELDS:
        val = out.get(f, "")
        try:
            out[f] = int(float(str(val).strip())) if str(val).strip() else 0
        except (ValueError, TypeError):
            out[f] = 0
    for f in _TEXT_FIELDS:
        out[f] = str(out.get(f) or "").strip()
    return out


def _determine_final_status(overall: int, spam: int) -> str:
    if overall >= REPAIR_APPROVE_THRESHOLD and spam <= REPAIR_SPAM_MAX:
        return "approved_after_repair"
    if overall >= 60:
        return "manual_review_after_repair"
    return "rejected_final"


def _save_csv(records: list[dict], path: Path) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)


def _log_repair_error(
    record: dict,
    exc: Exception,
    tb: str,
    mode_attempted: str,
) -> None:
    """Append one row to email_repair_errors.csv for each failed repair."""
    write_header = not EMAIL_REPAIR_ERRORS_FILE.exists()
    row = {
        "company_name":          record.get("company_name", ""),
        "kp_email":              record.get("kp_email", ""),
        "subject":               record.get("subject", ""),
        "original_status":       record.get("approval_status", ""),
        "original_score":        record.get("overall_score", ""),
        "exception_message":     str(exc)[:300],
        "traceback_snippet":     tb[:600],
        "repair_mode_attempted": mode_attempted,
    }
    try:
        with open(EMAIL_REPAIR_ERRORS_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=_ERROR_FIELDS)
            if write_header:
                writer.writeheader()
            writer.writerow(row)
    except Exception:
        pass  # never crash the pipeline due to error logging


def _load_signal_lookup() -> dict[str, dict]:
    """
    Load best_signal and signal_facts per company from company_openings.json.
    Returns {normalized_company_name → {"best_signal": ..., "signal_facts": ...}}.
    signal_facts is the structured whitelist used by the rewriter to prevent hallucination.
    """
    if not COMPANY_OPENINGS_FILE.exists():
        return {}
    try:
        import json
        with open(COMPANY_OPENINGS_FILE, encoding="utf-8") as f:
            records = json.load(f)
        return {
            (r.get("company_name") or "").strip().lower(): {
                "best_signal":  r.get("best_signal",  ""),
                "signal_facts": r.get("signal_facts",  {}),
            }
            for r in records
            if r.get("best_signal")
        }
    except Exception:
        return {}


def run(limit: int = 0) -> dict:
    """Run the repair loop. Returns summary dict."""
    repairable, already_approved = load_repairable()

    # Pre-load signal context; augmented into each record before rewriting so
    # the rewriter can produce company-specific opening lines.
    signal_lookup = _load_signal_lookup()

    if not repairable:
        print("[Workflow 6.7] No emails to repair.")
        _save_csv([], REPAIRED_EMAILS_FILE)
        _save_csv([], RESCORED_EMAILS_FILE)
        _save_csv(already_approved, FINAL_SEND_QUEUE_FILE)
        _save_csv([], FINAL_REJECTED_FILE)
        return {"repaired": 0, "approved_after_repair": 0, "rejected_final": 0}

    if limit > 0:
        repairable = repairable[:limit]

    provider = _get_provider()
    mode_str = f"AI ({provider[0]}/{provider[2]})" if provider else "rule-based fallback"
    print(f"[Workflow 6.7] Repairable: {len(repairable)} | Skipped (approved): {len(already_approved)}")
    print(f"[Workflow 6.7] Rewriting {len(repairable)} emails — mode: {mode_str}")

    repaired_records:  list[dict] = []
    rescored_records:  list[dict] = []
    final_approved:    list[dict] = []
    final_rejected:    list[dict] = []

    ai_repairs        = 0
    rule_repairs      = 0
    ai_rewrite_errors = 0   # AI attempted but failed; fell back to rule-based
    error_count       = 0   # entire record processing failed (outer except)

    for i, raw_record in enumerate(repairable, 1):
        # Normalize inputs before any processing
        record = _normalize_record(raw_record)

        # Augment with best_signal + signal_facts for grounded opening repair
        name_key = (record.get("company_name") or "").strip().lower()
        if name_key in signal_lookup:
            record = {**record, **signal_lookup[name_key]}

        # Use kp_email as primary identifier in log messages; fall back to company_name
        identifier = (
            record.get("kp_email")
            or record.get("company_name")
            or f"record {i}"
        )
        print(f"[Workflow 6.7] ({i}/{len(repairable)})")

        mode_attempted = "ai" if provider else "rule"

        try:
            original_score  = record.get("overall_score", 0)
            original_status = record.get("approval_status", "")

            draft, repair_mode, repair_source, ai_error = rewrite_email(record)

            # Track AI rewrite failures (fell back to rule — not a hard record error)
            if ai_error:
                ai_rewrite_errors += 1
                _log_repair_error(
                    record,
                    Exception(ai_error),
                    "",          # no traceback — rewriter already handled it
                    "ai",
                )

            if not draft.get("subject", "").strip() or not draft.get("email_body", "").strip():
                print(f"[Workflow 6.7]   WARN: empty repair draft for {identifier}, skipping")
                continue

            if repair_mode == "ai":
                ai_repairs += 1
            else:
                rule_repairs += 1

            # Build repaired record — merge original fields with new content
            repaired = {k: record.get(k, "") for k in OUTPUT_FIELDS}
            repaired.update({
                "subject":         draft.get("subject", ""),
                "opening_line":    draft.get("opening_line", ""),
                "email_body":      draft.get("email_body", ""),
                "repair_mode":     repair_mode,
                "repair_source":   repair_source,
                "original_score":  original_score,
                "original_status": original_status,
            })
            repaired_records.append(repaired)

            print(f"[Workflow 6.7]   → [{repair_source}] {draft.get('subject', '')[:60]}")

            # Rescore repaired email
            rescored = score_email(repaired)
            overall  = rescored.get("overall_score", 0)
            spam     = rescored.get("spam_risk_score", 0)
            final_status = _determine_final_status(overall, spam)
            rescored["approval_status"] = final_status
            rescored["repair_mode"]     = repair_mode
            rescored["repair_source"]   = repair_source
            rescored["original_score"]  = original_score
            rescored["original_status"] = original_status
            rescored_records.append(rescored)

            print(
                f"[Workflow 6.7]   Rescore: {original_score} → {overall} | "
                f"Status: {original_status} → {final_status}"
            )

            if final_status == "approved_after_repair":
                final_approved.append(rescored)
            else:
                final_rejected.append(rescored)

        except Exception as exc:
            tb = traceback.format_exc()
            error_count += 1
            print(f"[Workflow 6.7]   ERROR ({identifier}): {exc}")
            print(f"[Workflow 6.7]   TRACEBACK:\n{tb}")
            _log_repair_error(record, exc, tb, mode_attempted)
            continue

    # Final send queue = originally approved + newly approved after repair
    combined_send_queue = list(already_approved) + final_approved

    _save_csv(repaired_records,    REPAIRED_EMAILS_FILE)
    _save_csv(rescored_records,    RESCORED_EMAILS_FILE)
    _save_csv(combined_send_queue, FINAL_SEND_QUEUE_FILE)
    _save_csv(final_rejected,      FINAL_REJECTED_FILE)

    total_errors_logged = ai_rewrite_errors + error_count
    print(
        f"\n[Workflow 6.7] Repair complete:\n"
        f"  AI rewrites       : {ai_repairs}\n"
        f"  Rule rewrites     : {rule_repairs}\n"
        f"  AI rewrite errors : {ai_rewrite_errors}  (fell back to rule)\n"
        f"  Record failures   : {error_count}  (record skipped entirely)\n"
        f"  Errors logged     : {total_errors_logged}  → {EMAIL_REPAIR_ERRORS_FILE.name}\n"
        f"  Approved (repair) : {len(final_approved)}\n"
        f"  Rejected (final)  : {len(final_rejected)}\n"
        f"  Final send queue  : {len(combined_send_queue)} total "
        f"({len(already_approved)} original + {len(final_approved)} repaired)"
    )
    print(f"[Workflow 6.7] → {REPAIRED_EMAILS_FILE}")
    print(f"[Workflow 6.7] → {RESCORED_EMAILS_FILE}")
    print(f"[Workflow 6.7] → {FINAL_SEND_QUEUE_FILE}")
    print(f"[Workflow 6.7] → {FINAL_REJECTED_FILE}")
    if total_errors_logged:
        print(f"[Workflow 6.7] → {EMAIL_REPAIR_ERRORS_FILE}  ({total_errors_logged} error rows)")

    return {
        "repaired":              len(repaired_records),
        "ai_repairs":            ai_repairs,
        "rule_repairs":          rule_repairs,
        "ai_rewrite_errors":     ai_rewrite_errors,
        "error_count":           error_count,
        "errors_logged":         total_errors_logged,
        "approved_after_repair": len(final_approved),
        "rejected_final":        len(final_rejected),
        "final_send_queue":      len(combined_send_queue),
    }


if __name__ == "__main__":
    run()
