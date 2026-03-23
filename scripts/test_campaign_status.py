"""
Smoke test for Workflow 8.5 — Campaign Status Aggregator.

Run from the project root:
    py scripts/test_campaign_status.py

Uses synthetic data covering all major lifecycle_status branches.
"""

import csv
import json
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.workflow_8_5_campaign_status.status_loader     import load_send_logs, load_engagement
from src.workflow_8_5_campaign_status.status_classifier import classify_status, build_summary
from src.workflow_8_5_campaign_status.status_pipeline   import run


# ---------------------------------------------------------------------------
# Synthetic data
# ---------------------------------------------------------------------------

_NOW    = datetime.now(tz=timezone.utc)
_10D    = (_NOW - timedelta(days=10)).isoformat()
_7D     = (_NOW - timedelta(days=7)).isoformat()
_4D     = (_NOW - timedelta(days=4)).isoformat()
_1D     = (_NOW - timedelta(days=1)).isoformat()

# send_logs.csv — 5 sent contacts
SEND_LOGS = [
    # alice: sent 10d ago, has engagement + followup sent
    {
        "timestamp": _10D, "company_name": "Test Solar Co",
        "place_id": "pid_smoke001", "kp_name": "Alice Chen",
        "kp_email": "alice@smoke.test", "subject": "Solar support",
        "send_decision": "send", "send_status": "dry_run",
        "decision_reason": "", "provider": "dry_run",
        "provider_message_id": "dry-001", "error_message": "",
        "tracking_id": "pid_smoke001_tid001", "message_id": "msg_001",
    },
    # bob: sent 7d ago, clicked — should be clicked_no_reply or followup_queued
    {
        "timestamp": _7D, "company_name": "Green Battery LLC",
        "place_id": "pid_smoke002", "kp_name": "Bob Martinez",
        "kp_email": "bob@smoke.test", "subject": "Battery storage support",
        "send_decision": "send", "send_status": "dry_run",
        "decision_reason": "", "provider": "dry_run",
        "provider_message_id": "dry-002", "error_message": "",
        "tracking_id": "pid_smoke002_tid001", "message_id": "msg_002",
    },
    # carol: sent 4d ago, opened only
    {
        "timestamp": _4D, "company_name": "Quick Install Inc",
        "place_id": "pid_smoke003", "kp_name": "Carol White",
        "kp_email": "carol@smoke.test", "subject": "Install support",
        "send_decision": "send", "send_status": "dry_run",
        "decision_reason": "", "provider": "dry_run",
        "provider_message_id": "dry-003", "error_message": "",
        "tracking_id": "pid_smoke003_tid001", "message_id": "msg_003",
    },
    # dave: sent 7d ago, no engagement
    {
        "timestamp": _7D, "company_name": "Dave Power Co",
        "place_id": "pid_smoke004", "kp_name": "Dave Lee",
        "kp_email": "dave@smoke.test", "subject": "Power systems",
        "send_decision": "send", "send_status": "dry_run",
        "decision_reason": "", "provider": "dry_run",
        "provider_message_id": "dry-004", "error_message": "",
        "tracking_id": "pid_smoke004_tid001", "message_id": "msg_004",
    },
    # eve: sent 10d ago, followup blocked
    {
        "timestamp": _10D, "company_name": "Eve Energy",
        "place_id": "pid_smoke005", "kp_name": "Eve Nguyen",
        "kp_email": "eve@smoke.test", "subject": "Energy solutions",
        "send_decision": "send", "send_status": "dry_run",
        "decision_reason": "", "provider": "dry_run",
        "provider_message_id": "dry-005", "error_message": "",
        "tracking_id": "pid_smoke005_tid001", "message_id": "msg_005",
    },
]

# engagement_summary.csv
ENGAGEMENT = [
    # alice: 2 opens, 0 clicks
    {
        "tracking_id": "pid_smoke001_tid001", "message_id": "msg_001",
        "company_name": "Test Solar Co", "kp_email": "alice@smoke.test",
        "open_count": "2", "first_open_time": _10D, "last_open_time": _7D,
        "click_count": "0", "first_click_time": "", "last_click_time": "",
    },
    # bob: 3 opens, 1 click
    {
        "tracking_id": "pid_smoke002_tid001", "message_id": "msg_002",
        "company_name": "Green Battery LLC", "kp_email": "bob@smoke.test",
        "open_count": "3", "first_open_time": _7D, "last_open_time": _4D,
        "click_count": "1", "first_click_time": _4D, "last_click_time": _4D,
    },
    # carol: 1 open, 0 clicks
    {
        "tracking_id": "pid_smoke003_tid001", "message_id": "msg_003",
        "company_name": "Quick Install Inc", "kp_email": "carol@smoke.test",
        "open_count": "1", "first_open_time": _4D, "last_open_time": _4D,
        "click_count": "0", "first_click_time": "", "last_click_time": "",
    },
    # dave: no engagement
    {
        "tracking_id": "pid_smoke004_tid001", "message_id": "msg_004",
        "company_name": "Dave Power Co", "kp_email": "dave@smoke.test",
        "open_count": "0", "first_open_time": "", "last_open_time": "",
        "click_count": "0", "first_click_time": "", "last_click_time": "",
    },
    # eve: 1 open
    {
        "tracking_id": "pid_smoke005_tid001", "message_id": "msg_005",
        "company_name": "Eve Energy", "kp_email": "eve@smoke.test",
        "open_count": "1", "first_open_time": _10D, "last_open_time": _10D,
        "click_count": "0", "first_click_time": "", "last_click_time": "",
    },
]

# followup_logs.csv — alice has had followup_1 sent
FOLLOWUP_LOGS = [
    {
        "timestamp": _4D, "company_name": "Test Solar Co",
        "place_id": "pid_smoke001", "kp_email": "alice@smoke.test",
        "followup_stage": "followup_1", "engagement_status": "opened_no_click",
        "decision": "followup", "reason": "Due and allowed",
        "due_date": _4D, "is_due": "True",
        "followup_subject": "Re: Solar support", "generation_mode": "rule",
        "generation_source": "fallback_template",
    },
]

# followup_queue.csv — bob is queued for followup_1
FOLLOWUP_QUEUE = [
    {
        "company_name": "Green Battery LLC", "place_id": "pid_smoke002",
        "kp_name": "Bob Martinez", "kp_email": "bob@smoke.test",
        "tracking_id": "pid_smoke002_tid001", "message_id": "msg_002",
        "followup_stage": "followup_1", "engagement_status": "clicked_no_reply",
        "due_date": _1D,
        "followup_subject": "Re: Battery storage support",
        "followup_body": "Hi Bob,\n\nFollowing up...",
        "followup_reason": "followup_1 due 3d after last send",
        "generation_mode": "rule", "generation_source": "fallback_template",
    },
]

# followup_blocked.csv — eve is blocked
FOLLOWUP_BLOCKED = [
    {
        "company_name": "Eve Energy", "kp_email": "eve@smoke.test",
        "followup_stage": "followup_1", "decision": "blocked",
        "reason": "Email format invalid",
    },
]


def _write_temp_csv(rows: list[dict], suffix: str) -> Path:
    tmp = Path(tempfile.mktemp(suffix=suffix))
    if not rows:
        tmp.write_text("", encoding="utf-8")
        return tmp
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return tmp


def _write_temp_json(data, suffix: str) -> Path:
    tmp = Path(tempfile.mktemp(suffix=suffix))
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return tmp


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def main():
    print("=" * 60)
    print("Workflow 8.5 Smoke Test — Campaign Status Aggregator")
    print("=" * 60)

    errors = 0

    # ------------------------------------------------------------------
    # Step 1 — Unit test classifier directly
    # ------------------------------------------------------------------
    print("\n[1] Testing status classifier directly...")

    # sent_no_open
    r1 = classify_status({
        "initial_send_status": "dry_run",
        "open_count": 0, "click_count": 0,
        "last_followup_stage": "", "queued_followup_stage": "",
        "followup_block_decision": "",
    })
    assert r1["lifecycle_status"] == "sent_no_open", f"FAIL: {r1['lifecycle_status']}"
    assert r1["priority_flag"] == "medium", f"FAIL priority: {r1['priority_flag']}"

    # clicked_no_reply
    r2 = classify_status({
        "initial_send_status": "dry_run",
        "open_count": 2, "click_count": 1,
        "last_followup_stage": "", "queued_followup_stage": "",
        "followup_block_decision": "",
    })
    assert r2["lifecycle_status"] == "clicked_no_reply", f"FAIL: {r2['lifecycle_status']}"
    assert r2["priority_flag"] == "high", f"FAIL priority: {r2['priority_flag']}"

    # followup_queued
    r3 = classify_status({
        "initial_send_status": "dry_run",
        "open_count": 1, "click_count": 0,
        "last_followup_stage": "", "queued_followup_stage": "followup_1",
        "followup_block_decision": "",
        "queued_followup_due": "2026-03-15",
    })
    assert r3["lifecycle_status"] == "followup_queued", f"FAIL: {r3['lifecycle_status']}"

    # followup_sent
    r4 = classify_status({
        "initial_send_status": "dry_run",
        "open_count": 2, "click_count": 0,
        "last_followup_stage": "followup_1", "queued_followup_stage": "",
        "followup_block_decision": "",
        "last_followup_time": "2026-03-10",
    })
    assert r4["lifecycle_status"] == "followup_sent", f"FAIL: {r4['lifecycle_status']}"
    assert r4["next_action"] == "send_followup_2", f"FAIL next: {r4['next_action']}"

    # completed
    r5 = classify_status({
        "initial_send_status": "dry_run",
        "open_count": 0, "click_count": 0,
        "last_followup_stage": "followup_3", "queued_followup_stage": "",
        "followup_block_decision": "",
    })
    assert r5["lifecycle_status"] == "completed", f"FAIL: {r5['lifecycle_status']}"

    # followup_blocked
    r6 = classify_status({
        "initial_send_status": "dry_run",
        "open_count": 1, "click_count": 0,
        "last_followup_stage": "", "queued_followup_stage": "",
        "followup_block_decision": "blocked",
        "followup_block_reason": "invalid email",
    })
    assert r6["lifecycle_status"] == "followup_blocked", f"FAIL: {r6['lifecycle_status']}"

    # not_sent
    r7 = classify_status({
        "initial_send_status": "",
        "open_count": 0, "click_count": 0,
        "last_followup_stage": "", "queued_followup_stage": "",
        "followup_block_decision": "",
    })
    assert r7["lifecycle_status"] == "not_sent", f"FAIL: {r7['lifecycle_status']}"

    print("    OK — all 7 classifier checks correct.")

    # ------------------------------------------------------------------
    # Step 2 — Run full pipeline on synthetic data
    # ------------------------------------------------------------------
    print("\n[2] Running full pipeline on 5 synthetic contacts...")

    send_p   = _write_temp_csv(SEND_LOGS,       "_smoke_status_send.csv")
    eng_p    = _write_temp_csv(ENGAGEMENT,       "_smoke_status_eng.csv")
    fl_p     = _write_temp_csv(FOLLOWUP_LOGS,    "_smoke_status_fl.csv")
    fq_p     = _write_temp_csv(FOLLOWUP_QUEUE,   "_smoke_status_fq.csv")
    fb_p     = _write_temp_csv(FOLLOWUP_BLOCKED, "_smoke_status_fb.csv")
    empty_p  = _write_temp_csv([],               "_smoke_status_empty.csv")

    # Use temp output files so we don't pollute real data
    status_out  = Path(tempfile.mktemp(suffix="_smoke_campaign_status.csv"))
    summary_out = Path(tempfile.mktemp(suffix="_smoke_campaign_summary.json"))

    summary = run(
        send_logs_path=send_p,
        engagement_path=eng_p,
        followup_logs_path=fl_p,
        followup_queue_path=fq_p,
        followup_blocked_path=fb_p,
        final_send_queue_path=empty_p,
        enriched_leads_path=empty_p,
        status_output_path=status_out,
        summary_output_path=summary_out,
    )

    # Cleanup temp inputs
    for p in [send_p, eng_p, fl_p, fq_p, fb_p, empty_p]:
        p.unlink(missing_ok=True)

    print(f"\n    Pipeline summary:")
    print(f"    Total contacts : {summary.get('total_contacts', 0)}")
    for k, v in summary.get("lifecycle_status", {}).items():
        print(f"    {k:<22}: {v}")

    assert summary.get("total_contacts", 0) == 5, \
        f"FAIL: expected 5 contacts, got {summary.get('total_contacts')}"

    ls = summary.get("lifecycle_status", {})
    assert ls.get("followup_sent", 0) >= 1,   "FAIL: expected ≥1 followup_sent (alice)"
    assert ls.get("followup_queued", 0) >= 1, "FAIL: expected ≥1 followup_queued (bob)"
    assert ls.get("followup_blocked", 0) >= 1, "FAIL: expected ≥1 followup_blocked (eve)"

    # ------------------------------------------------------------------
    # Step 3 — Verify output files
    # ------------------------------------------------------------------
    print("\n[3] Verifying output files...")

    if not status_out.exists():
        print("    FAIL: campaign_status.csv not created.")
        errors += 1
    else:
        rows = _read_csv(status_out)
        print(f"    OK — campaign_status.csv: {len(rows)} rows")
        assert len(rows) == 5, f"FAIL: expected 5 rows, got {len(rows)}"

    if not summary_out.exists():
        print("    FAIL: campaign_status_summary.json not created.")
        errors += 1
    else:
        data = _read_json(summary_out)
        print(f"    OK — campaign_status_summary.json: {list(data.keys())}")

    # ------------------------------------------------------------------
    # Step 4 — Print sample status records
    # ------------------------------------------------------------------
    if status_out.exists():
        rows = _read_csv(status_out)
        smoke_rows = [r for r in rows if "smoke.test" in r.get("kp_email", "")]
        if smoke_rows:
            print(f"\n[4] Sample status records ({len(smoke_rows)} smoke contacts):")
            for r in smoke_rows:
                print(
                    f"    {'─' * 54}\n"
                    f"    Company : {r.get('company_name', '')}\n"
                    f"    Email   : {r.get('kp_email', '')}\n"
                    f"    Status  : {r.get('lifecycle_status', '')}\n"
                    f"    Action  : {r.get('next_action', '')}\n"
                    f"    Priority: {r.get('priority_flag', '')}"
                )

    # Cleanup temp outputs
    status_out.unlink(missing_ok=True)
    summary_out.unlink(missing_ok=True)

    if errors:
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Workflow 8.5 smoke test completed successfully.")
    print(f"  Total contacts : {summary.get('total_contacts', 0)}")
    for k, v in summary.get("lifecycle_status", {}).items():
        print(f"  {k:<22}: {v}")
    print("=" * 60)


if __name__ == "__main__":
    main()
