"""
Smoke test for Workflow 7.5 — Open / Click Tracking.

Run from the project root:
    py scripts/test_engagement_tracking.py

Tests tracking ID generation, email injection, event logging, and aggregation.
Does NOT start the Flask server.
"""

import sys
import csv
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import ENGAGEMENT_LOGS_FILE, ENGAGEMENT_SUMMARY_FILE, TRACKING_BASE_URL
from src.workflow_7_5_engagement_tracking.tracking_id_manager import (
    generate_tracking_id, generate_message_id,
)
from src.workflow_7_5_engagement_tracking.email_tracking_injector import (
    prepare_tracked_email,
)
from src.workflow_7_5_engagement_tracking.engagement_logger import (
    load_engagement_logs, append_engagement_event, build_event_row,
)
from src.workflow_7_5_engagement_tracking.engagement_aggregator import (
    run as run_aggregator,
)


SAMPLE_RECORDS = [
    {
        "place_id":     "ChIJtest001",
        "company_name": "Test Solar Co",
        "kp_email":     "alice@testsolar.com",
        "subject":      "Commercial solar install support",
        "email_body": (
            "Hi Alice,\n\n"
            "Your team appears focused on commercial solar installation.\n\n"
            "We support commercial solar installers with equipment supply and "
            "project coordination. The goal is fewer supply chain delays.\n\n"
            "Open to a short intro if this is relevant?\n\n"
            "Best,\nSolar Supply Team"
        ),
    },
    {
        "place_id":     "ChIJtest002",
        "company_name": "Green Power LLC",
        "kp_email":     "bob@greenpower.io",
        "subject":      "Storage support for solar installers",
        "email_body": (
            "Hi Bob,\n\n"
            "It looks like Green Power handles both solar and battery storage work.\n\n"
            "We work with installers on battery storage integration — covering supply, "
            "system design support, and commissioning.\n\n"
            "Happy to share more if useful."
        ),
    },
]


def main():
    print("=" * 60)
    print("Workflow 7.5 Smoke Test — Open / Click Tracking")
    print("=" * 60)
    print(f"\n  Tracking base URL: {TRACKING_BASE_URL}")

    errors = 0

    # ------------------------------------------------------------------
    # Step 1 — Tracking ID generation
    # ------------------------------------------------------------------
    print("\n[1] Testing tracking ID generation...")
    ids: list[tuple[str, str]] = []
    for rec in SAMPLE_RECORDS:
        tid = generate_tracking_id(rec)
        mid = generate_message_id(rec)
        ids.append((tid, mid))
        print(f"    tracking_id : {tid}")
        print(f"    message_id  : {mid}")
        assert tid and "_" in tid, f"FAIL: bad tracking_id: {tid!r}"
        assert mid.startswith("msg_"), f"FAIL: bad message_id: {mid!r}"
        assert all(c.isalnum() or c in "_-" for c in tid), f"FAIL: unsafe chars in {tid!r}"
    print("    OK — tracking IDs generated and URL-safe.")

    # ------------------------------------------------------------------
    # Step 2 — Email HTML injection
    # ------------------------------------------------------------------
    print("\n[2] Testing email tracking injector...")
    for i, rec in enumerate(SAMPLE_RECORDS):
        tid, _ = ids[i]
        result = prepare_tracked_email(rec["email_body"], tid, TRACKING_BASE_URL)

        assert result["plain_text"] == rec["email_body"], "FAIL: plain text corrupted"
        assert result["tracking_id"] == tid, "FAIL: tracking_id mismatch"
        assert "track/open/" in result["html_body"], "FAIL: pixel not injected"
        assert tid in result["html_body"], "FAIL: tracking_id not in HTML"
        assert isinstance(result["tracked_links_count"], int), "FAIL: link count missing"

        print(
            f"    [{rec['company_name']}] HTML size: {len(result['html_body'])} chars | "
            f"links rewritten: {result['tracked_links_count']}"
        )
    print("    OK — HTML injection works, pixel present, plain text preserved.")

    # ------------------------------------------------------------------
    # Step 3 — Engagement event logging
    # ------------------------------------------------------------------
    print("\n[3] Testing engagement event logging...")
    initial_count = len(load_engagement_logs())

    events_to_log = [
        (ids[0][0], "open",  ids[0][1], SAMPLE_RECORDS[0]["company_name"], SAMPLE_RECORDS[0]["kp_email"], ""),
        (ids[0][0], "click", ids[0][1], SAMPLE_RECORDS[0]["company_name"], SAMPLE_RECORDS[0]["kp_email"], "https://example.com/product-a"),
        (ids[1][0], "open",  ids[1][1], SAMPLE_RECORDS[1]["company_name"], SAMPLE_RECORDS[1]["kp_email"], ""),
        (ids[1][0], "open",  ids[1][1], SAMPLE_RECORDS[1]["company_name"], SAMPLE_RECORDS[1]["kp_email"], ""),
        (ids[1][0], "click", ids[1][1], SAMPLE_RECORDS[1]["company_name"], SAMPLE_RECORDS[1]["kp_email"], "https://example.com/product-b"),
    ]

    for tid, etype, mid, cname, email, url in events_to_log:
        row = build_event_row(
            tracking_id=tid, event_type=etype, message_id=mid,
            company_name=cname, kp_email=email,
            target_url=url, ip="127.0.0.1", user_agent="smoke-test/1.0",
        )
        append_engagement_event(row)

    all_logs = load_engagement_logs()
    new_events = len(all_logs) - initial_count
    print(f"    Logged {new_events} new events (total rows: {len(all_logs)})")
    assert new_events == 5, f"FAIL: expected 5 new events, got {new_events}"

    if not ENGAGEMENT_LOGS_FILE.exists():
        print(f"    FAIL: {ENGAGEMENT_LOGS_FILE} not created.")
        errors += 1
    else:
        print(f"    OK — engagement_logs.csv written with {len(all_logs)} total rows.")

    # ------------------------------------------------------------------
    # Step 4 — Aggregation
    # ------------------------------------------------------------------
    print("\n[4] Testing engagement aggregation...")
    summaries = run_aggregator()

    if not ENGAGEMENT_SUMMARY_FILE.exists():
        print(f"    FAIL: {ENGAGEMENT_SUMMARY_FILE} not created.")
        errors += 1
    else:
        print(f"    OK — engagement_summary.csv written with {len(summaries)} rows.")

    summary_map = {s["tracking_id"]: s for s in summaries}

    s0 = summary_map.get(ids[0][0])
    assert s0 is not None, f"FAIL: tracking_id {ids[0][0]} not in summary"
    assert int(s0["open_count"])  >= 1, f"FAIL: expected >=1 open for record 0"
    assert int(s0["click_count"]) >= 1, f"FAIL: expected >=1 click for record 0"

    s1 = summary_map.get(ids[1][0])
    assert s1 is not None, f"FAIL: tracking_id {ids[1][0]} not in summary"
    assert int(s1["open_count"])  >= 2, f"FAIL: expected >=2 opens for record 1"
    assert int(s1["click_count"]) >= 1, f"FAIL: expected >=1 click for record 1"

    print(f"\n    Sample summary rows:")
    for s in summaries:
        if s["tracking_id"] in (ids[0][0], ids[1][0]):
            print(
                f"    -> {s['company_name']} | "
                f"opens={s['open_count']} | clicks={s['click_count']} | "
                f"first_open={s.get('first_open_time', '')[:19]}"
            )

    # ------------------------------------------------------------------
    # Step 5 — Flask server availability check
    # ------------------------------------------------------------------
    print("\n[5] Checking tracking server...")
    try:
        from src.workflow_7_5_engagement_tracking.tracking_server import app, _PIXEL_GIF
        if app is not None:
            print(f"    OK — Flask app created. Pixel GIF: {len(_PIXEL_GIF)} bytes.")
            print(f"    Routes: /health | /track/open/<id> | /track/click/<id>")
            print(f"    Run server: py -m src.workflow_7_5_engagement_tracking.tracking_server")
        else:
            print("    INFO: Flask not installed. pip install flask")
    except ImportError as e:
        print(f"    INFO: {e}")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    open_total  = sum(int(s.get("open_count",  0)) for s in summaries)
    click_total = sum(int(s.get("click_count", 0)) for s in summaries)

    print(f"\n    Engagement events logged : {len(all_logs)}")
    print(f"    Total opens              : {open_total}")
    print(f"    Total clicks             : {click_total}")
    print(f"    Summary rows             : {len(summaries)}")

    if errors:
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Workflow 7.5 smoke test completed successfully.")
    print(f"  engagement_logs.csv   : {len(all_logs)} rows")
    print(f"  engagement_summary.csv: {len(summaries)} rows")
    print(f"  Total opens           : {open_total}")
    print(f"  Total clicks          : {click_total}")
    print("=" * 60)


if __name__ == "__main__":
    main()
