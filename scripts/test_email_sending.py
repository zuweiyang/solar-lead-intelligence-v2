"""
Smoke test for Workflow 7.0 — Email Sending + Send Logging.

Run from the project root:
    py scripts/test_email_sending.py

Runs in dry-run mode by default (EMAIL_SEND_MODE=dry_run in .env or not set).
Does NOT send any real emails.
"""

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import EMAIL_SEND_MODE, SEND_LOGS_FILE, SEND_BATCH_SUMMARY, FINAL_SEND_QUEUE_FILE
from src.workflow_7_email_sending.send_loader import load_send_queue
from src.workflow_7_email_sending.send_guard  import (
    check_required_fields, check_email_format,
    check_approval_status, check_business_hours, run_checks,
)
from src.workflow_7_email_sending.send_logger import load_send_logs
from src.workflow_7_email_sending.send_pipeline import run


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def main():
    print("=" * 60)
    print("Workflow 7.0 Smoke Test — Email Sending")
    print("=" * 60)

    # Confirm dry-run
    mode = EMAIL_SEND_MODE
    print(f"\n  Mode: {mode.upper()}")
    if mode.lower() != "dry_run":
        print(
            "\n  WARNING: EMAIL_SEND_MODE is not 'dry_run'.\n"
            "  Set EMAIL_SEND_MODE=dry_run in .env to test safely."
        )

    # Step 1 — Confirm input file exists
    print(f"\n[1] Checking final_send_queue.csv...")
    if not FINAL_SEND_QUEUE_FILE.exists():
        print(
            f"    FAIL: {FINAL_SEND_QUEUE_FILE} not found.\n"
            f"    Run Workflow 6.7 first."
        )
        sys.exit(1)
    queue_rows = _read_csv(FINAL_SEND_QUEUE_FILE)
    print(f"    OK — {len(queue_rows)} rows in final_send_queue.csv")

    # Step 2 — Unit test send_guard
    print("\n[2] Unit testing send_guard...")
    errors = 0

    # Required fields check — missing email
    bad = {"kp_email": "", "subject": "Test", "email_body": "Body"}
    result = check_required_fields(bad)
    assert result is not None and result["decision"] == "blocked", "FAIL: should block missing kp_email"

    # Email format check — malformed
    bad2 = {"kp_email": "notavalidemail"}
    result2 = check_email_format(bad2)
    assert result2 is not None and result2["decision"] == "blocked", "FAIL: should block bad email format"

    # Email format check — valid
    good = {"kp_email": "test@example.com"}
    result3 = check_email_format(good)
    assert result3 is None, "FAIL: valid email should pass"

    # Approval status check — wrong status
    bad3 = {"kp_email": "x@x.com", "subject": "s", "email_body": "b",
            "approval_status": "manual_review"}
    result4 = check_approval_status(bad3)
    assert result4 is not None and result4["decision"] == "blocked", "FAIL: non-approved should block"

    # Approval status check — approved_after_repair is allowed
    good2 = {"approval_status": "approved_after_repair"}
    result5 = check_approval_status(good2)
    assert result5 is None, "FAIL: approved_after_repair should pass"

    # Reply suppression / bounce should block future sends
    suppressed_record = {
        "kp_email": "bounced@example.com",
        "subject": "Test",
        "email_body": "Body",
        "approval_status": "approved",
    }
    suppressed_result = run_checks(
        suppressed_record,
        recent_logs=[],
        send_mode="gmail_api",
        reply_index={
            "bounced@example.com": {
                "suppression_status": "suppressed",
                "reply_type": "bounce",
            }
        },
    )
    assert suppressed_result["decision"] == "blocked", "FAIL: bounced email should be blocked by reply suppression"

    # Business hours — Saturday should defer
    from datetime import datetime
    saturday = datetime(2026, 3, 14, 10, 0)  # known Saturday
    result6 = check_business_hours(saturday)
    assert result6 is not None and result6["decision"] == "deferred", "FAIL: Saturday should defer"

    # Business hours — before-hours should defer
    early = datetime(2026, 3, 16, 7, 30)   # Monday 07:30
    result7 = check_business_hours(early)
    assert result7 is not None and result7["decision"] == "deferred", "FAIL: 07:30 should defer"

    # Business hours — within window should pass
    working = datetime(2026, 3, 16, 10, 0)  # Monday 10:00
    result8 = check_business_hours(working)
    assert result8 is None, "FAIL: Monday 10:00 should pass"

    print(f"    OK — all send_guard unit tests passed.")

    # Step 3 — Load queue and preview
    print(f"\n[3] Loading send queue...")
    records = load_send_queue()
    print(f"    Loaded {len(records)} approved records.")
    for r in records[:3]:
        print(
            f"    → {r.get('company_name', '?')} | "
            f"{r.get('kp_email', '')} | "
            f"{r.get('subject', '')[:50]}"
        )

    # Step 4 — Run pipeline in dry-run mode
    print(f"\n[4] Running send pipeline (dry-run)...")
    summary = run()

    print(f"\n    Results:")
    print(f"    Total loaded : {summary.get('total', 0)}")
    print(f"    Sent         : {summary.get('sent', 0)}")
    print(f"    Dry-run      : {summary.get('dry_run', 0)}")
    print(f"    Failed       : {summary.get('failed', 0)}")
    print(f"    Blocked      : {summary.get('blocked', 0)}")
    print(f"    Deferred     : {summary.get('deferred', 0)}")

    # Step 5 — Confirm send_logs.csv was written
    print(f"\n[5] Verifying send_logs.csv...")
    if not SEND_LOGS_FILE.exists():
        print(f"    FAIL: {SEND_LOGS_FILE} not created.")
        sys.exit(1)
    log_rows = load_send_logs()
    print(f"    OK — {SEND_LOGS_FILE.name} written with {len(log_rows)} rows.")

    if log_rows:
        print(f"\n    Sample log rows (up to 3):")
        for row in log_rows[-3:]:
            print(
                f"    → [{row.get('send_status','?')}] "
                f"{row.get('company_name','?')} | "
                f"{row.get('kp_email','')} | "
                f"{row.get('subject','')[:45]}"
            )
            if row.get("decision_reason"):
                print(f"      Reason: {row.get('decision_reason')}")

    # Step 6 — Confirm send_batch_summary.json written
    print(f"\n[6] Verifying send_batch_summary.json...")
    if not SEND_BATCH_SUMMARY.exists():
        print(f"    FAIL: {SEND_BATCH_SUMMARY} not created.")
        errors += 1
    else:
        print(f"    OK — {SEND_BATCH_SUMMARY.name} written.")

    if errors:
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Workflow 7.0 smoke test completed successfully.")
    print(f"  Mode         : {mode.upper()}")
    print(f"  Processed    : {summary.get('total', 0)}")
    print(f"  Dry-run sent : {summary.get('dry_run', 0)}")
    print(f"  send_logs.csv: {len(log_rows)} total rows")
    print("=" * 60)


if __name__ == "__main__":
    main()
