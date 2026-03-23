"""
Smoke test for Workflow 6.7 — Email Repair Loop.

Run from the project root:
    py scripts/test_email_repair.py

Loads scored_emails.csv, repairs manual_review/rejected emails,
rescores them, and writes final_send_queue.csv.
"""

import sys
import csv
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.workflow_6_7_email_repair.repair_selector import load_repairable
from src.workflow_6_7_email_repair.email_rewriter  import _get_provider
from src.workflow_6_7_email_repair.repair_pipeline  import run
from config.settings import (
    REPAIRED_EMAILS_FILE, RESCORED_EMAILS_FILE,
    FINAL_SEND_QUEUE_FILE, FINAL_REJECTED_FILE,
)

REQUIRED_COLUMNS = [
    "company_name", "website", "place_id",
    "kp_name", "kp_title", "kp_email",
    "subject", "opening_line", "email_body",
    "email_angle", "repair_mode", "repair_source",
    "original_score", "original_status",
    "overall_score", "approval_status",
]


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def main():
    print("=" * 60)
    print("Workflow 6.7 Smoke Test — Email Repair Loop")
    print("=" * 60)

    provider = _get_provider()
    mode = f"AI ({provider[0]}/{provider[2]})" if provider else "Rule-based fallback"
    print(f"\n  Repair mode: {mode}")

    # Step 1 — Preview what's repairable
    print("\n[1] Scanning scored_emails.csv for repairable records...")
    repairable, already_approved = load_repairable()
    print(f"    Repairable emails  : {len(repairable)}")
    print(f"    Already approved   : {len(already_approved)}")
    if not repairable:
        print("\n    INFO: No repairable emails found.")
        print("    Run Workflow 6.5 first to generate scored_emails.csv.")
        print("\n" + "=" * 60)
        print("Workflow 6.7 smoke test completed (nothing to repair).")
        print("=" * 60)
        return

    for r in repairable:
        print(
            f"    → {r.get('company_name', '?')} | "
            f"status={r.get('approval_status')} | "
            f"score={r.get('overall_score')}"
        )

    # Step 2 — Run repair loop
    print(f"\n[2] Running repair pipeline on {len(repairable)} emails...")
    summary = run()

    # Step 3 — Validate output files
    print("\n[3] Validating output files...")
    errors = 0

    for label, path in [
        ("repaired_emails.csv",    REPAIRED_EMAILS_FILE),
        ("rescored_emails.csv",    RESCORED_EMAILS_FILE),
        ("final_send_queue.csv",   FINAL_SEND_QUEUE_FILE),
        ("final_rejected_emails.csv", FINAL_REJECTED_FILE),
    ]:
        if not path.exists():
            print(f"    FAIL: {label} not created.")
            errors += 1
        else:
            rows = _read_csv(path)
            print(f"    OK — {label} written with {len(rows)} rows.")

    # Step 4 — Field validation on repaired emails
    print("\n[4] Field validation on repaired_emails.csv...")
    repaired_rows = _read_csv(REPAIRED_EMAILS_FILE)
    for r in repaired_rows:
        missing = [c for c in REQUIRED_COLUMNS if c not in r]
        if missing:
            print(f"    FAIL: {r.get('company_name', '?')} missing columns: {missing}")
            errors += 1
        if not r.get("subject", "").strip():
            print(f"    FAIL: {r.get('company_name', '?')} has empty subject")
            errors += 1
        if not r.get("email_body", "").strip():
            print(f"    FAIL: {r.get('company_name', '?')} has empty email_body")
            errors += 1
    if repaired_rows and errors == 0:
        print(f"    OK — all {len(repaired_rows)} repaired drafts are valid.")

    # Step 5 — Print sample repaired outputs
    rescored_rows = _read_csv(RESCORED_EMAILS_FILE)
    if rescored_rows:
        print(f"\n[5] Sample rescored outputs (up to 3):")
        for r in rescored_rows[:3]:
            print(f"\n    {'─' * 56}")
            print(f"    Company     : {r.get('company_name', '')}")
            print(f"    Repair mode : {r.get('repair_mode', '')} / {r.get('repair_source', '')}")
            print(f"    Score       : {r.get('original_score', '?')} → {r.get('overall_score', '?')}")
            print(f"    Status      : {r.get('original_status', '?')} → {r.get('approval_status', '?')}")
            print(f"    Subject     : {r.get('subject', '')}")

    # Step 6 — Summary
    print(f"\n[6] Final send queue: {FINAL_SEND_QUEUE_FILE.name}")
    final_queue = _read_csv(FINAL_SEND_QUEUE_FILE)
    print(f"    Total emails ready to send: {len(final_queue)}")

    if errors:
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Workflow 6.7 smoke test completed successfully.")
    print(f"  Repaired           : {summary.get('repaired', 0)}")
    print(f"  Approved (repair)  : {summary.get('approved_after_repair', 0)}")
    print(f"  Final send queue   : {summary.get('final_send_queue', 0)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
