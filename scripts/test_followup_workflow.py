"""
Smoke test for Workflow 8 — Follow-up Automation.

Run from the project root:
    py scripts/test_followup_workflow.py

Uses synthetic "sent" records with past timestamps so follow-up_1 is
always due regardless of when the test runs.
"""

import csv
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import (
    FOLLOWUP_CANDIDATES_FILE, FOLLOWUP_QUEUE_FILE,
    FOLLOWUP_BLOCKED_FILE, FOLLOWUP_LOGS_FILE,
)
from src.workflow_8_followup.followup_selector   import select_candidates
from src.workflow_8_followup.followup_stop_rules  import classify_engagement, check_stop_rules
from src.workflow_8_followup.followup_scheduler   import compute_due_date, is_due, build_followup_schedule
from src.workflow_8_followup.followup_generator   import _get_provider
from src.workflow_8_followup.followup_pipeline    import run


# ---------------------------------------------------------------------------
# Synthetic test data — timestamps 4 days in the past so followup_1 is due
# ---------------------------------------------------------------------------

_NOW = datetime.now(tz=timezone.utc)
_4D_AGO = (_NOW - timedelta(days=4)).isoformat()
_8D_AGO = (_NOW - timedelta(days=8)).isoformat()
_1D_AGO = (_NOW - timedelta(days=1)).isoformat()

SYNTHETIC_SEND_LOGS = [
    # followup_1 due (4d ago, no prior followups)
    {
        "timestamp": _4D_AGO,
        "company_name": "Test Solar Co",
        "place_id": "ChIJtest_smoke001",
        "kp_name": "Alice Chen",
        "kp_email": "alice@testsolar-smoke.com",
        "subject": "Commercial solar install support",
        "send_decision": "send",
        "send_status": "dry_run",
        "decision_reason": "All checks passed",
        "provider": "dry_run",
        "provider_message_id": "dry-abc001",
        "error_message": "",
        "tracking_id": "ChIJtest_smoke001_tid001",
        "message_id": "msg_smoke001",
    },
    # followup_1 also due (8d ago)
    {
        "timestamp": _8D_AGO,
        "company_name": "Green Battery LLC",
        "place_id": "ChIJtest_smoke002",
        "kp_name": "Bob Martinez",
        "kp_email": "bob@greenbattery-smoke.io",
        "subject": "Storage support for solar installers",
        "send_decision": "send",
        "send_status": "dry_run",
        "decision_reason": "All checks passed",
        "provider": "dry_run",
        "provider_message_id": "dry-abc002",
        "error_message": "",
        "tracking_id": "ChIJtest_smoke002_tid001",
        "message_id": "msg_smoke002",
    },
    # NOT due yet (1d ago — followup_1 needs 3d)
    {
        "timestamp": _1D_AGO,
        "company_name": "Quick Install Inc",
        "place_id": "ChIJtest_smoke003",
        "kp_name": "Carol White",
        "kp_email": "carol@quickinstall-smoke.com",
        "subject": "Support for growing install teams",
        "send_decision": "send",
        "send_status": "dry_run",
        "decision_reason": "All checks passed",
        "provider": "dry_run",
        "provider_message_id": "dry-abc003",
        "error_message": "",
        "tracking_id": "ChIJtest_smoke003_tid001",
        "message_id": "msg_smoke003",
    },
]

SYNTHETIC_ENGAGEMENT = [
    # alice: 1 open, no click
    {
        "tracking_id": "ChIJtest_smoke001_tid001",
        "message_id": "msg_smoke001",
        "company_name": "Test Solar Co",
        "kp_email": "alice@testsolar-smoke.com",
        "open_count": "1",
        "first_open_time": _4D_AGO,
        "last_open_time": _4D_AGO,
        "click_count": "0",
        "first_click_time": "",
        "last_click_time": "",
    },
    # bob: 3 opens, 1 click
    {
        "tracking_id": "ChIJtest_smoke002_tid001",
        "message_id": "msg_smoke002",
        "company_name": "Green Battery LLC",
        "kp_email": "bob@greenbattery-smoke.io",
        "open_count": "3",
        "first_open_time": _8D_AGO,
        "last_open_time": _4D_AGO,
        "click_count": "1",
        "first_click_time": _4D_AGO,
        "last_click_time": _4D_AGO,
    },
    # carol: no opens
    {
        "tracking_id": "ChIJtest_smoke003_tid001",
        "message_id": "msg_smoke003",
        "company_name": "Quick Install Inc",
        "kp_email": "carol@quickinstall-smoke.com",
        "open_count": "0",
        "first_open_time": "",
        "last_open_time": "",
        "click_count": "0",
        "first_click_time": "",
        "last_click_time": "",
    },
]


def _write_temp_csv(rows: list[dict], suffix: str) -> Path:
    """Write rows to a temp CSV and return its path."""
    tmp = Path(tempfile.mktemp(suffix=suffix))
    if not rows:
        tmp.write_text("", encoding="utf-8")
        return tmp
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return tmp


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def main():
    print("=" * 60)
    print("Workflow 8 Smoke Test — Follow-up Automation")
    print("=" * 60)

    provider = _get_provider()
    mode = f"AI ({provider[0]}/{provider[2]})" if provider else "Rule-based fallback"
    print(f"\n  Generation mode : {mode}")

    errors = 0

    # ------------------------------------------------------------------
    # Step 1 — Unit test engagement classification
    # ------------------------------------------------------------------
    print("\n[1] Testing engagement classification...")
    assert classify_engagement(0, 0) == "no_open",            "FAIL: 0/0"
    assert classify_engagement(1, 0) == "opened_no_click",    "FAIL: 1/0"
    assert classify_engagement(3, 0) == "multi_open_no_click","FAIL: 3/0"
    assert classify_engagement(2, 1) == "clicked_no_reply",   "FAIL: 2/1"
    assert classify_engagement(0, 1) == "clicked_no_reply",   "FAIL: 0/1"
    print("    OK — all 5 engagement classifications correct.")

    # ------------------------------------------------------------------
    # Step 2 — Unit test scheduler
    # ------------------------------------------------------------------
    print("\n[2] Testing followup scheduler...")
    # 4 days ago → followup_1 (3d delay) should be due
    four_days_ago = (_NOW - timedelta(days=4)).isoformat()
    due = compute_due_date(four_days_ago, "followup_1")
    assert due is not None, "FAIL: compute_due_date returned None"
    assert is_due(due, _NOW), f"FAIL: followup_1 should be due now (due={due})"

    # 1 day ago → followup_1 should NOT be due
    one_day_ago = (_NOW - timedelta(days=1)).isoformat()
    due2 = compute_due_date(one_day_ago, "followup_1")
    assert not is_due(due2, _NOW), "FAIL: 1d-ago followup_1 should not be due"

    # Schedule result for a due candidate
    candidate_test = {"followup_stage": "followup_1", "last_send_time": four_days_ago}
    sched = build_followup_schedule(candidate_test, now=_NOW)
    assert sched["is_due"] is True,           "FAIL: is_due should be True"
    assert sched["scheduled_action"] == "queue_now", "FAIL: action should be queue_now"
    print("    OK — scheduler due-date logic correct.")

    # ------------------------------------------------------------------
    # Step 3 — Unit test stop rules
    # ------------------------------------------------------------------
    print("\n[3] Testing stop rules...")
    # Missing email → block
    r1 = check_stop_rules({"kp_email": "", "followup_stage": "followup_1"})
    assert r1["decision"] == "blocked", "FAIL: empty email should block"

    # Bad email format → block
    r2 = check_stop_rules({"kp_email": "notanemail", "followup_stage": "followup_1"})
    assert r2["decision"] == "blocked", "FAIL: bad email should block"

    # Stage exceeded → block
    r3 = check_stop_rules({"kp_email": "x@x.com", "followup_stage": "followup_99"})
    assert r3["decision"] == "blocked", "FAIL: invalid stage should block"

    # Valid → allow
    r4 = check_stop_rules({"kp_email": "x@x.com", "followup_stage": "followup_1"})
    assert r4["decision"] == "followup", "FAIL: valid candidate should be allowed"

    # Suppressed → block
    r5 = check_stop_rules({"kp_email": "x@x.com", "followup_stage": "followup_1", "suppressed": "true"})
    assert r5["decision"] == "blocked", "FAIL: suppressed should block"
    print("    OK — all 5 stop-rule checks correct.")

    # ------------------------------------------------------------------
    # Step 4 — Run full pipeline on synthetic data
    # ------------------------------------------------------------------
    print(f"\n[4] Running full pipeline on 3 synthetic contacts...")

    send_logs_path  = _write_temp_csv(SYNTHETIC_SEND_LOGS,  "_smoke_sendlogs.csv")
    engagement_path = _write_temp_csv(SYNTHETIC_ENGAGEMENT, "_smoke_engagement.csv")
    # Use a temp path for reading prior stage history (no prior followups in test)
    # but always write new log entries to the canonical FOLLOWUP_LOGS_FILE
    empty_history = Path(tempfile.mktemp(suffix="_smoke_history.csv"))

    summary = run(
        now                = _NOW,
        send_logs_path     = send_logs_path,
        engagement_path    = engagement_path,
        followup_logs_path = empty_history,          # read: empty history
        log_output_path    = FOLLOWUP_LOGS_FILE,     # write: canonical log
    )

    # Cleanup temp input files
    send_logs_path.unlink(missing_ok=True)
    engagement_path.unlink(missing_ok=True)
    empty_history.unlink(missing_ok=True)

    print(f"\n    Pipeline summary:")
    print(f"    Candidates   : {summary.get('candidates', 0)}")
    print(f"    Queued       : {summary.get('queued', 0)}")
    print(f"    Blocked      : {summary.get('blocked', 0)}")
    print(f"    Deferred     : {summary.get('deferred', 0)}")
    print(f"    followup_1   : {summary.get('followup_1', 0)}")
    print(f"    followup_2   : {summary.get('followup_2', 0)}")
    print(f"    followup_3   : {summary.get('followup_3', 0)}")

    # Expect: alice (due) + bob (due) → queued; carol (not due) → deferred
    assert summary.get("candidates", 0) == 3,    f"FAIL: expected 3 candidates, got {summary.get('candidates')}"
    assert summary.get("queued", 0)     >= 2,    f"FAIL: expected ≥2 queued, got {summary.get('queued')}"
    assert summary.get("deferred", 0)   >= 1,    f"FAIL: expected ≥1 deferred, got {summary.get('deferred')}"

    # ------------------------------------------------------------------
    # Step 5 — Verify output files
    # ------------------------------------------------------------------
    print("\n[5] Verifying output files...")
    for label, path in [
        ("followup_candidates.csv", FOLLOWUP_CANDIDATES_FILE),
        ("followup_queue.csv",      FOLLOWUP_QUEUE_FILE),
        ("followup_blocked.csv",    FOLLOWUP_BLOCKED_FILE),
        ("followup_logs.csv",       FOLLOWUP_LOGS_FILE),
    ]:
        if not path.exists():
            print(f"    FAIL: {label} not created.")
            errors += 1
        else:
            rows = _read_csv(path)
            print(f"    OK — {label}: {len(rows)} rows")

    # ------------------------------------------------------------------
    # Step 6 — Print sample follow-up drafts
    # ------------------------------------------------------------------
    queue_rows = _read_csv(FOLLOWUP_QUEUE_FILE)

    # Filter to our smoke test records
    smoke_rows = [r for r in queue_rows if "smoke" in r.get("kp_email", "")]

    if smoke_rows:
        print(f"\n[6] Sample follow-up drafts (up to 3 smoke-test records):")
        for r in smoke_rows[:3]:
            print(f"\n    {'─' * 54}")
            print(f"    Company    : {r.get('company_name', '')}")
            print(f"    Contact    : {r.get('kp_name', '')} <{r.get('kp_email', '')}>")
            print(f"    Stage      : {r.get('followup_stage', '')}")
            print(f"    Engagement : {r.get('engagement_status', '')}")
            print(f"    Source     : {r.get('generation_source', '')}")
            print(f"    Subject    : {r.get('followup_subject', '')}")
            body_lines = (r.get("followup_body") or "").splitlines()
            for line in body_lines[:5]:
                print(f"      {line}")
            if len(body_lines) > 5:
                print(f"      ... ({len(body_lines) - 5} more lines)")
    else:
        print("\n[6] No smoke-test queue rows to preview (may have been filtered).")

    if errors:
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Workflow 8 smoke test completed successfully.")
    print(f"  Candidates   : {summary.get('candidates', 0)}")
    print(f"  Queued       : {summary.get('queued', 0)}")
    print(f"  Deferred     : {summary.get('deferred', 0)}")
    print(f"  Blocked      : {summary.get('blocked', 0)}")
    print(f"  followup_1   : {summary.get('followup_1', 0)}")
    print(f"  followup_2   : {summary.get('followup_2', 0)}")
    print(f"  followup_3   : {summary.get('followup_3', 0)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
