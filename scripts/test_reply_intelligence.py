"""
Smoke tests for Workflow 7.8 — Reply Intelligence.

Run from the project root:
    py scripts/test_reply_intelligence.py

Covers:
    Scenario 1: Level 1a — thread_id match
    Scenario 2: Level 1b — In-Reply-To header match
    Scenario 3: Level 1c — References header match
    Scenario 4: Level 2  — email + normalized subject match
    Scenario 5: Level 3  — email-only match (manual_review=True)
    Scenario 6: No match (manual_review=True, matched=False)
    Scenario 7: Malformed send log rows are skipped gracefully
    Scenario 8: already_logged() dedup guard (CSV-based)
    Scenario 9: Pipeline run with no Gmail token exits cleanly

All tests use synthetic data and temp files — no real Gmail calls.
"""

import csv
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.workflow_7_8_reply_intelligence.reply_models  import ReplyRecord, CSV_FIELDS
from src.workflow_7_8_reply_intelligence.reply_matcher import (
    build_send_log_index,
    match_reply,
    _normalize_subject,
    _extract_local_parts,
)
from src.workflow_7_8_reply_intelligence.reply_logger import (
    append_reply_log,
    load_reply_logs,
    already_logged,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime.now(tz=timezone.utc).isoformat()

# Canonical send_log row used across most scenarios
_BASE_SEND_LOG = {
    "timestamp":           _NOW,
    "campaign_id":         "campaign_test_001",
    "send_mode":           "dry_run",
    "company_name":        "Sunrise Solar",
    "place_id":            "ChIJtest_place001",
    "kp_name":             "Bob Lee",
    "kp_email":            "bob@sunrisesolar.com",
    "subject":             "Solar mounting for your EPC projects",
    "send_decision":       "send",
    "send_status":         "sent",
    "decision_reason":     "",
    "provider":            "gmail_api",
    "provider_message_id": "19cff2dadb400b39",   # Gmail internal message ID
    "error_message":       "",
    "tracking_id":         "ChIJtest_place001_tid001",
    "message_id":          "msg_001",
}


def _make_index(rows: list[dict] | None = None) -> dict:
    return build_send_log_index(rows or [_BASE_SEND_LOG])


def _blank_reply(**kwargs) -> ReplyRecord:
    r = ReplyRecord(
        timestamp       = _NOW,
        gmail_message_id= "inbox_msg_001",
        gmail_thread_id = "thread_001",
        from_email      = "bob@sunrisesolar.com",
        subject         = "Solar mounting for your EPC projects",
    )
    for k, v in kwargs.items():
        setattr(r, k, v)
    return r


def _pass(name: str) -> None:
    print(f"  [PASS] {name}")


def _fail(name: str, detail: str) -> None:
    print(f"  [FAIL] {name} — {detail}")
    sys.exit(1)


def _assert(condition: bool, scenario: str, detail: str = "") -> None:
    if condition:
        _pass(scenario)
    else:
        _fail(scenario, detail or "assertion failed")


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def test_normalize_subject() -> None:
    print("\n[Normalisation helpers]")
    cases = [
        ("RE: Solar mounting for your EPC projects",    "solar mounting for your epc projects"),
        ("Re[2]: Solar mounting for your EPC projects", "solar mounting for your epc projects"),
        ("FWD: Some subject",                           "some subject"),
        ("FW: Some subject",                            "some subject"),
        ("Re: Re: Subject",                             "subject"),
        ("Fwd: Re: Subject",                            "subject"),
        ("No prefix here",                              "no prefix here"),
        ("  spaces   around  ",                         "spaces around"),
    ]
    for raw, expected in cases:
        result = _normalize_subject(raw)
        _assert(
            result == expected,
            f"normalize_subject({raw!r})",
            f"got {result!r}, expected {expected!r}",
        )


def test_extract_local_parts() -> None:
    print("\n[Local-part extraction]")
    _assert(
        _extract_local_parts("<19cff2dadb400b39@mail.gmail.com>") == ["19cff2dadb400b39"],
        "angle-bracket single",
    )
    _assert(
        set(_extract_local_parts(
            "<19cff2dadb400b39@mail.gmail.com> <aabbccdd11223344@mail.gmail.com>"
        )) == {"19cff2dadb400b39", "aabbccdd11223344"},
        "angle-bracket multiple",
    )
    _assert(
        _extract_local_parts("") == [],
        "empty string",
    )
    # Bare ID fallback (no angle brackets)
    _assert(
        _extract_local_parts("19cff2dadb400b39") == ["19cff2dadb400b39"],
        "bare ID fallback",
    )


# ---------------------------------------------------------------------------
# Scenario 1 — Level 1a: thread_id
# ---------------------------------------------------------------------------

def test_scenario_1_thread_id() -> None:
    print("\n[Scenario 1] Level 1a — thread_id match")
    reply = _blank_reply()
    index = _make_index()

    # outbound_thread_ids contains the provider_message_id of the sent email
    match_reply(reply, index, outbound_thread_ids=["19cff2dadb400b39"])

    _assert(reply.matched,                          "matched=True")
    _assert(reply.match_method == "thread_id",      f"match_method=thread_id (got {reply.match_method!r})")
    _assert(reply.matched_tracking_id == "ChIJtest_place001_tid001", "tracking_id propagated")
    _assert(not reply.manual_review_required,       "manual_review=False")


# ---------------------------------------------------------------------------
# Scenario 2 — Level 1b: In-Reply-To
# ---------------------------------------------------------------------------

def test_scenario_2_in_reply_to() -> None:
    print("\n[Scenario 2] Level 1b — In-Reply-To header match")
    reply = _blank_reply(
        in_reply_to="<19cff2dadb400b39@mail.gmail.com>"
    )
    index = _make_index()

    match_reply(reply, index, outbound_thread_ids=[])

    _assert(reply.matched,                           "matched=True")
    _assert(reply.match_method == "in_reply_to",     f"match_method=in_reply_to (got {reply.match_method!r})")
    _assert(reply.matched_place_id == "ChIJtest_place001", "place_id propagated")
    _assert(not reply.manual_review_required,        "manual_review=False")


# ---------------------------------------------------------------------------
# Scenario 3 — Level 1c: References
# ---------------------------------------------------------------------------

def test_scenario_3_references() -> None:
    print("\n[Scenario 3] Level 1c — References header match")
    reply = _blank_reply(
        references="<earlier_msg@mail.gmail.com> <19cff2dadb400b39@mail.gmail.com>"
    )
    index = _make_index()

    match_reply(reply, index, outbound_thread_ids=[])

    _assert(reply.matched,                          "matched=True")
    _assert(reply.match_method == "references",     f"match_method=references (got {reply.match_method!r})")
    _assert(reply.matched_company_name == "Sunrise Solar", "company_name propagated")


# ---------------------------------------------------------------------------
# Scenario 4 — Level 2: email + normalized subject
# ---------------------------------------------------------------------------

def test_scenario_4_email_subject() -> None:
    print("\n[Scenario 4] Level 2 — email + normalized subject match")
    reply = _blank_reply(
        subject="RE: Solar mounting for your EPC projects",
        # No In-Reply-To or References
        in_reply_to="",
        references="",
    )
    index = _make_index()

    match_reply(reply, index, outbound_thread_ids=[])

    _assert(reply.matched,                           "matched=True")
    _assert(reply.match_method == "email_subject",   f"match_method=email_subject (got {reply.match_method!r})")
    _assert(not reply.manual_review_required,        "manual_review=False")


# ---------------------------------------------------------------------------
# Scenario 5 — Level 3: email only (ambiguous → manual_review=True)
# ---------------------------------------------------------------------------

def test_scenario_5_email_only() -> None:
    print("\n[Scenario 5] Level 3 — email-only match (manual_review=True)")
    reply = _blank_reply(
        subject="Something unrelated",
        in_reply_to="",
        references="",
    )
    index = _make_index()

    match_reply(reply, index, outbound_thread_ids=[])

    _assert(reply.matched,                           "matched=True")
    _assert(reply.match_method == "email_recent",    f"match_method=email_recent (got {reply.match_method!r})")
    _assert(reply.manual_review_required,            "manual_review=True")


# ---------------------------------------------------------------------------
# Scenario 5b — Level 3: multiple recent candidates → ambiguous (no match)
# ---------------------------------------------------------------------------

def test_scenario_5b_email_only_ambiguous() -> None:
    print("\n[Scenario 5b] Level 3 — multiple recent candidates → ambiguous")
    ts_now = datetime.now(tz=timezone.utc).isoformat()
    rows = [
        {**_BASE_SEND_LOG, "provider_message_id": "pid_A", "tracking_id": "tid_A",
         "timestamp": ts_now},
        {**_BASE_SEND_LOG, "provider_message_id": "pid_B", "tracking_id": "tid_B",
         "timestamp": ts_now},
    ]
    index = build_send_log_index(rows)
    reply = _blank_reply(
        subject="Something unrelated",
        in_reply_to="",
        references="",
    )
    match_reply(reply, index, outbound_thread_ids=[])

    _assert(not reply.matched,                              "matched=False (ambiguous)")
    _assert(reply.match_method == "",                       f"match_method='' (got {reply.match_method!r})")
    _assert(reply.manual_review_required,                   "manual_review=True")
    _assert(reply.match_error == "email_only_ambiguous",    f"match_error=email_only_ambiguous (got {reply.match_error!r})")


# ---------------------------------------------------------------------------
# Scenario 5c — Level 3: all candidates outside 90-day window → no match
# ---------------------------------------------------------------------------

def test_scenario_5c_email_only_no_recent() -> None:
    print("\n[Scenario 5c] Level 3 — all sends older than 90 days → no match")
    old_ts = (datetime.now(tz=timezone.utc) - timedelta(days=100)).isoformat()
    old_row = {**_BASE_SEND_LOG, "timestamp": old_ts}
    index = build_send_log_index([old_row])
    reply = _blank_reply(
        subject="Something unrelated",
        in_reply_to="",
        references="",
    )
    match_reply(reply, index, outbound_thread_ids=[])

    _assert(not reply.matched,              "matched=False (no recent candidate)")
    _assert(reply.match_error == "no_match", f"match_error=no_match (got {reply.match_error!r})")


# ---------------------------------------------------------------------------
# Scenario 6 — No match
# ---------------------------------------------------------------------------

def test_scenario_6_no_match() -> None:
    print("\n[Scenario 6] No match")
    reply = _blank_reply(
        from_email="unknown@stranger.com",
        subject="Something completely different",
        in_reply_to="",
        references="",
    )
    index = _make_index()

    match_reply(reply, index, outbound_thread_ids=[])

    _assert(not reply.matched,                       "matched=False")
    _assert(reply.match_method == "",                f"match_method='' (got {reply.match_method!r})")
    _assert(reply.manual_review_required,            "manual_review=True")
    _assert(reply.match_error == "no_match",         "match_error=no_match")


# ---------------------------------------------------------------------------
# Scenario 7 — Malformed send log rows skipped gracefully
# ---------------------------------------------------------------------------

def test_scenario_7_malformed_send_logs() -> None:
    print("\n[Scenario 7] Malformed send log rows skipped gracefully")
    malformed_rows = [
        # Missing send_status — should be skipped (not in _SENDABLE_STATUSES)
        {"timestamp": _NOW, "provider_message_id": "bad_id_1",
         "kp_email": "x@x.com", "subject": "test"},
        # send_status=failed — should be skipped
        {"timestamp": _NOW, "provider_message_id": "bad_id_2",
         "kp_email": "x@x.com", "subject": "test", "send_status": "failed"},
        # Valid row mixed in
        _BASE_SEND_LOG,
    ]
    index = build_send_log_index(malformed_rows)

    # Only the valid row should be indexed
    _assert("19cff2dadb400b39" in index["by_provider_msg_id"], "valid row indexed")
    _assert("bad_id_1" not in index["by_provider_msg_id"],    "missing-status row skipped")
    _assert("bad_id_2" not in index["by_provider_msg_id"],    "failed-status row skipped")

    # Matching still works with mixed input
    reply = _blank_reply(in_reply_to="<19cff2dadb400b39@mail.gmail.com>")
    match_reply(reply, index, outbound_thread_ids=[])
    _assert(reply.matched, "match succeeds despite malformed rows")


# ---------------------------------------------------------------------------
# Scenario 8 — already_logged() dedup guard
# ---------------------------------------------------------------------------

def test_scenario_8_dedup_guard() -> None:
    print("\n[Scenario 8] already_logged() CSV dedup guard")
    with tempfile.TemporaryDirectory() as tmp:
        log_path = Path(tmp) / "reply_logs.csv"

        reply = _blank_reply(gmail_message_id="dedup_test_msg_001")

        _assert(not already_logged("dedup_test_msg_001", path=log_path),
                "not logged before first write")

        append_reply_log(reply, path=log_path)

        _assert(already_logged("dedup_test_msg_001", path=log_path),
                "logged after first write")

        # Second append of same message — already_logged() returns True,
        # allowing the caller to skip.  (The logger itself doesn't prevent
        # double-writes; the caller is responsible for checking first.)
        rows = load_reply_logs(path=log_path)
        _assert(len(rows) == 1, f"one row in CSV (got {len(rows)})")

        # Different message ID is NOT detected as duplicate
        _assert(not already_logged("different_msg_999", path=log_path),
                "different ID not detected as duplicate")


# ---------------------------------------------------------------------------
# Scenario 8b — Two different replies in the same thread → both preserved
# ---------------------------------------------------------------------------

def test_scenario_8b_two_replies_same_thread() -> None:
    print("\n[Scenario 8b] Two different replies in same thread → both rows preserved")
    with tempfile.TemporaryDirectory() as tmp:
        log_path = Path(tmp) / "reply_logs.csv"

        r1 = _blank_reply(gmail_message_id="thread_reply_001", gmail_thread_id="shared_thread")
        r2 = _blank_reply(gmail_message_id="thread_reply_002", gmail_thread_id="shared_thread")

        append_reply_log(r1, path=log_path)
        append_reply_log(r2, path=log_path)

        rows = load_reply_logs(path=log_path)
        _assert(len(rows) == 2, f"two distinct replies preserved (got {len(rows)})")
        ids = {r.get("gmail_message_id") for r in rows}
        _assert("thread_reply_001" in ids, "first reply ID present")
        _assert("thread_reply_002" in ids, "second reply ID present")


# ---------------------------------------------------------------------------
# Scenario 8c — Different gmail_message_ids, same subject → both preserved
# ---------------------------------------------------------------------------

def test_scenario_8c_same_subject_different_ids() -> None:
    print("\n[Scenario 8c] Different gmail_message_ids, same subject → both preserved")
    with tempfile.TemporaryDirectory() as tmp:
        log_path = Path(tmp) / "reply_logs.csv"

        r1 = _blank_reply(gmail_message_id="subj_reply_001",
                          subject="RE: Solar mounting for your EPC projects")
        r2 = _blank_reply(gmail_message_id="subj_reply_002",
                          subject="RE: Solar mounting for your EPC projects")

        append_reply_log(r1, path=log_path)
        append_reply_log(r2, path=log_path)

        rows = load_reply_logs(path=log_path)
        _assert(len(rows) == 2, f"two replies with same subject preserved (got {len(rows)})")


# ---------------------------------------------------------------------------
# CRM path check — REPLY_LOGS_FILE must be under data/crm/
# ---------------------------------------------------------------------------

def test_reply_logs_crm_scoped() -> None:
    print("\n[CRM path] REPLY_LOGS_FILE is CRM-scoped (not run-scoped)")
    from config.settings import REPLY_LOGS_FILE
    path_str = str(REPLY_LOGS_FILE).replace("\\", "/")
    _assert(
        "data/crm" in path_str,
        f"REPLY_LOGS_FILE under data/crm/ (got {path_str!r})",
    )


# ---------------------------------------------------------------------------
# Scenario 9 — Pipeline fails gracefully when token is missing
# ---------------------------------------------------------------------------

def test_scenario_9_pipeline_no_token() -> None:
    print("\n[Scenario 9] Pipeline exits cleanly when Gmail token is absent")
    import os
    from unittest.mock import patch

    # Patch GMAIL_TOKEN_FILE to a path that doesn't exist
    fake_token = Path("/nonexistent/gmail_token.json")

    with patch("config.settings.GMAIL_TOKEN_FILE", fake_token), \
         patch("src.workflow_7_8_reply_intelligence.reply_fetcher.GMAIL_TOKEN_FILE", fake_token):
        from src.workflow_7_8_reply_intelligence.reply_pipeline import run
        result = run(hours_back=1, max_results=5, our_email="test@example.com")

    _assert(result["fetched"] == 0,       f"fetched=0 (got {result['fetched']})")
    _assert(result["errors"]  == 0,       f"errors=0 (got {result['errors']})")


# ---------------------------------------------------------------------------
# to_csv_row / from_csv_row round-trip
# ---------------------------------------------------------------------------

def test_model_roundtrip() -> None:
    print("\n[Model] to_csv_row / from_csv_row round-trip")
    original = ReplyRecord(
        timestamp        = _NOW,
        gmail_message_id = "rtrip_001",
        from_email       = "alice@example.com",
        subject          = "Test subject",
        matched          = True,
        match_method     = "thread_id",
        manual_review_required = False,
        match_error      = "",
    )
    row    = original.to_csv_row()
    recon  = ReplyRecord.from_csv_row(row)

    _assert(recon.gmail_message_id == "rtrip_001", "message_id preserved")
    _assert(recon.matched is True,                  "matched bool preserved (True)")
    _assert(recon.match_method == "thread_id",      "match_method preserved")
    _assert(recon.manual_review_required is False,  "manual_review bool preserved (False)")

    # Serialise a False matched record
    r2 = ReplyRecord(matched=False, manual_review_required=True)
    r2r = ReplyRecord.from_csv_row(r2.to_csv_row())
    _assert(r2r.matched is False,                   "matched=False round-trips")
    _assert(r2r.manual_review_required is True,     "manual_review=True round-trips")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 64)
    print("Workflow 7.8 — Reply Intelligence smoke tests")
    print("=" * 64)

    test_normalize_subject()
    test_extract_local_parts()
    test_model_roundtrip()
    test_scenario_1_thread_id()
    test_scenario_2_in_reply_to()
    test_scenario_3_references()
    test_scenario_4_email_subject()
    test_scenario_5_email_only()
    test_scenario_5b_email_only_ambiguous()
    test_scenario_5c_email_only_no_recent()
    test_scenario_6_no_match()
    test_scenario_7_malformed_send_logs()
    test_scenario_8_dedup_guard()
    test_scenario_8b_two_replies_same_thread()
    test_scenario_8c_same_subject_different_ids()
    test_reply_logs_crm_scoped()
    test_scenario_9_pipeline_no_token()

    print("\n" + "=" * 64)
    print("All tests passed.")
    print("=" * 64)
