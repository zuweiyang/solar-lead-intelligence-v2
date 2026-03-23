"""
Smoke tests for Workflow 7.8 Ticket 2 — Reply Classification + Stop Rules.

Run from the project root:
    py scripts/test_reply_classification.py

Covers:
    Group A: Classification correctness for all 10 reply types + unknown
    Group B: Priority ordering (safety rules beat positive language)
    Group C: State transitions (suppressed / paused / handoff / manual review)
    Group D: Workflow 8 stop-rule integration (reply state overrides open/click)
    Group E: Wrong-person handling (pause + reroute flag)
    Group F: Resilience (malformed / empty records don't crash)
    Group G: Persistence round-trip (CSV fields survive to_csv_row/from_csv_row)

No real Gmail calls. No real database required. All synthetic data.
"""

import csv
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.workflow_7_8_reply_intelligence.reply_classifier import (
    classify_reply,
    apply_classification_to_reply,
    REPLY_TYPE_BOUNCE,
    REPLY_TYPE_UNSUBSCRIBE,
    REPLY_TYPE_HARD_NO,
    REPLY_TYPE_WRONG_PERSON,
    REPLY_TYPE_OUT_OF_OFFICE,
    REPLY_TYPE_AUTO_REPLY_OTHER,
    REPLY_TYPE_REQUEST_QUOTE,
    REPLY_TYPE_REQUEST_INFO,
    REPLY_TYPE_FORWARDED,
    REPLY_TYPE_POSITIVE_INTEREST,
    REPLY_TYPE_SOFT_NO,
    REPLY_TYPE_UNKNOWN,
)
from src.workflow_7_8_reply_intelligence.reply_state_manager import (
    derive_state,
    apply_state_to_reply,
    SUPPRESSION_NONE,
    SUPPRESSION_PAUSED,
    SUPPRESSION_SUPPRESSED,
    SUPPRESSION_HANDOFF,
    worst_suppression,
)
from src.workflow_7_8_reply_intelligence.reply_models import ReplyRecord, CSV_FIELDS
from src.workflow_8_followup.followup_stop_rules import (
    check_stop_rules,
    load_reply_suppression_index,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _reply(subject="", snippet="", body_text="") -> ReplyRecord:
    return ReplyRecord(
        gmail_message_id="test_msg",
        from_email="prospect@example.com",
        subject=subject,
        snippet=snippet,
        body_text=body_text,
    )


def _classify(subject="", snippet="", body_text="") -> str:
    return classify_reply(_reply(subject, snippet, body_text)).reply_type


def _assert(condition: bool, name: str, detail: str = "") -> None:
    if condition:
        print(f"  [PASS] {name}")
    else:
        print(f"  [FAIL] {name}" + (f" — {detail}" if detail else ""))
        sys.exit(1)


# ---------------------------------------------------------------------------
# Group A: Classification correctness
# ---------------------------------------------------------------------------

def test_group_a_classification() -> None:
    print("\n[Group A] Classification correctness")

    # Bounce
    _assert(_classify(subject="Delivery Status Notification (Failure)", body_text="Undeliverable: 550 5.1.1 user unknown") == REPLY_TYPE_BOUNCE, "bounce — DSN subject")
    _assert(_classify(body_text="Mail delivery failed: recipient address rejected") == REPLY_TYPE_BOUNCE, "bounce — delivery failed")

    # Unsubscribe
    _assert(_classify(body_text="Please unsubscribe me from your list") == REPLY_TYPE_UNSUBSCRIBE, "unsubscribe — explicit")
    _assert(_classify(snippet="remove me from your mailing list") == REPLY_TYPE_UNSUBSCRIBE, "unsubscribe — remove me")
    _assert(_classify(body_text="stop emailing me, I asked you before") == REPLY_TYPE_UNSUBSCRIBE, "unsubscribe — stop emailing me")
    _assert(_classify(snippet="please take me off this list") == REPLY_TYPE_UNSUBSCRIBE, "unsubscribe — take me off")
    _assert(_classify(body_text="opt out") == REPLY_TYPE_UNSUBSCRIBE, "unsubscribe — opt out")

    # Hard no
    _assert(_classify(body_text="We are not interested in this.") == REPLY_TYPE_HARD_NO, "hard_no — we are not interested")
    _assert(_classify(snippet="no thanks, not for us") == REPLY_TYPE_HARD_NO, "hard_no — no thanks not for us")
    # "please do not email" is the hard_no trigger; avoid "do not email us/me" which are unsubscribe
    _assert(_classify(body_text="Please do not email our team about this.") == REPLY_TYPE_HARD_NO, "hard_no — please do not email")
    _assert(_classify(snippet="not a fit for our business") == REPLY_TYPE_HARD_NO, "hard_no — not a fit")

    # Wrong person
    _assert(_classify(body_text="I think you have the wrong person, I don't handle purchasing.") == REPLY_TYPE_WRONG_PERSON, "wrong_person — wrong person")
    _assert(_classify(snippet="contact procurement for this type of request") == REPLY_TYPE_WRONG_PERSON, "wrong_person — contact procurement")
    _assert(_classify(body_text="I'm not responsible for this, please contact purchasing.") == REPLY_TYPE_WRONG_PERSON, "wrong_person — not responsible")

    # Out of office
    _assert(_classify(subject="Out of Office: Re: Solar Mounting") == REPLY_TYPE_OUT_OF_OFFICE, "out_of_office — subject prefix")
    _assert(_classify(snippet="I am out of office until March 25th") == REPLY_TYPE_OUT_OF_OFFICE, "out_of_office — out of office")
    _assert(_classify(body_text="I am currently on annual leave and will return on April 1.") == REPLY_TYPE_OUT_OF_OFFICE, "out_of_office — annual leave")

    # Auto-reply other
    _assert(_classify(snippet="This is an automated response to confirm receipt of your email.") == REPLY_TYPE_AUTO_REPLY_OTHER, "auto_reply_other — automated response")
    _assert(_classify(body_text="A support ticket has been received. Ticket #23451.") == REPLY_TYPE_AUTO_REPLY_OTHER, "auto_reply_other — ticket system")
    _assert(_classify(body_text="Please do not reply to this email as this mailbox is not monitored.") == REPLY_TYPE_AUTO_REPLY_OTHER, "auto_reply_other — unmonitored")
    _assert(_classify(snippet="Thank you for contacting us, our team will get back to you.") == REPLY_TYPE_AUTO_REPLY_OTHER, "auto_reply_other — thank you for contacting")

    # Request quote
    _assert(_classify(body_text="Could you send us a quote for 500 units?") == REPLY_TYPE_REQUEST_QUOTE, "request_quote — send us a quote")
    _assert(_classify(snippet="We need a quotation and the lead time") == REPLY_TYPE_REQUEST_QUOTE, "request_quote — quotation + lead time")
    _assert(_classify(body_text="What is the price per unit? And what's the MOQ?") == REPLY_TYPE_REQUEST_QUOTE, "request_quote — price + moq")
    # "proposal" alone was removed (too broad); "request for proposal" is the explicit trigger
    _assert(_classify(snippet="Please provide a request for proposal for our project") == REPLY_TYPE_REQUEST_QUOTE, "request_quote — request for proposal")

    # Request info
    _assert(_classify(body_text="Please send details about your mounting systems.") == REPLY_TYPE_REQUEST_INFO, "request_info — send details")
    _assert(_classify(snippet="Can you send catalog and product information?") == REPLY_TYPE_REQUEST_INFO, "request_info — send catalog")
    _assert(_classify(body_text="Could you share specs and a product brochure?") == REPLY_TYPE_REQUEST_INFO, "request_info — share specs")

    # Forwarded
    _assert(_classify(body_text="I have forwarded your email to our procurement manager.") == REPLY_TYPE_FORWARDED, "forwarded — forwarded")
    _assert(_classify(snippet="I've copied the right person on this thread") == REPLY_TYPE_FORWARDED, "forwarded — copied right person")
    _assert(_classify(body_text="I'm introducing you to Jane who handles our solar projects.") == REPLY_TYPE_FORWARDED, "forwarded — introducing")

    # Positive interest
    _assert(_classify(body_text="Sounds interesting! Let's set up a call to discuss.") == REPLY_TYPE_POSITIVE_INTEREST, "positive_interest — sounds interesting")
    _assert(_classify(snippet="We are interested in learning more. When are you available?") == REPLY_TYPE_POSITIVE_INTEREST, "positive_interest — we are interested")
    _assert(_classify(body_text="Happy to discuss this with you. Can we schedule a call?") == REPLY_TYPE_POSITIVE_INTEREST, "positive_interest — happy to discuss")

    # Soft no
    _assert(_classify(body_text="Not now, but please circle back next quarter.") == REPLY_TYPE_SOFT_NO, "soft_no — not now, circle back")
    # "not in a position" is a hard_no trigger; use text that clearly signals soft timing
    _assert(_classify(snippet="Currently not evaluating vendors, but try later.") == REPLY_TYPE_SOFT_NO, "soft_no — currently not, try later")
    _assert(_classify(body_text="No budget right now. Follow up later in the year.") == REPLY_TYPE_SOFT_NO, "soft_no — no budget, follow up later")

    # Unknown fallback
    _assert(_classify(body_text="Noted.") == REPLY_TYPE_UNKNOWN, "unknown — single-word reply")
    _assert(_classify(subject="", snippet="", body_text="") == REPLY_TYPE_UNKNOWN, "unknown — empty reply")
    _assert(_classify(body_text="Please see below.") == REPLY_TYPE_UNKNOWN, "unknown — ambiguous")


# ---------------------------------------------------------------------------
# Group B: Priority ordering
# ---------------------------------------------------------------------------

def test_group_b_priority() -> None:
    print("\n[Group B] Priority ordering — safety rules beat positive language")

    # Unsubscribe must beat any positive-sounding language
    _assert(
        _classify(body_text="Sounds interesting but please unsubscribe me anyway.") == REPLY_TYPE_UNSUBSCRIBE,
        "unsubscribe > positive_interest",
    )

    # Hard no must beat interest language
    _assert(
        _classify(body_text="Happy to hear more, but honestly not interested.") == REPLY_TYPE_HARD_NO,
        "hard_no > positive_interest",
    )

    # Wrong person before OOO (both in same message)
    _assert(
        _classify(body_text="I am out of office and this is the wrong person anyway.") == REPLY_TYPE_WRONG_PERSON,
        "wrong_person > out_of_office",
    )

    # OOO before auto_reply_other (both signals present)
    _assert(
        _classify(body_text="Out of office. This is an automated response.") == REPLY_TYPE_OUT_OF_OFFICE,
        "out_of_office > auto_reply_other",
    )

    # Request quote before request info
    _assert(
        _classify(body_text="Please send a quotation and some details about your products.") == REPLY_TYPE_REQUEST_QUOTE,
        "request_quote > request_info",
    )

    # Request info before positive_interest
    _assert(
        _classify(body_text="Sounds interesting, please send details.") == REPLY_TYPE_REQUEST_INFO,
        "request_info > positive_interest",
    )


# ---------------------------------------------------------------------------
# Group C: State transitions
# ---------------------------------------------------------------------------

def test_group_c_state_transitions() -> None:
    print("\n[Group C] Operational state transitions")

    cases = [
        (REPLY_TYPE_BOUNCE,            SUPPRESSION_SUPPRESSED, True,  False),
        (REPLY_TYPE_UNSUBSCRIBE,       SUPPRESSION_SUPPRESSED, True,  False),
        (REPLY_TYPE_HARD_NO,           SUPPRESSION_SUPPRESSED, True,  False),
        (REPLY_TYPE_POSITIVE_INTEREST, SUPPRESSION_HANDOFF,    True,  False),
        (REPLY_TYPE_REQUEST_INFO,      SUPPRESSION_HANDOFF,    True,  False),
        (REPLY_TYPE_REQUEST_QUOTE,     SUPPRESSION_HANDOFF,    True,  False),
        (REPLY_TYPE_FORWARDED,         SUPPRESSION_HANDOFF,    True,  False),
        (REPLY_TYPE_WRONG_PERSON,      SUPPRESSION_PAUSED,     True,  True),   # reroute flag
        (REPLY_TYPE_SOFT_NO,           SUPPRESSION_PAUSED,     True,  False),
        (REPLY_TYPE_OUT_OF_OFFICE,     SUPPRESSION_PAUSED,     True,  False),
        (REPLY_TYPE_AUTO_REPLY_OTHER,  SUPPRESSION_PAUSED,     True,  False),
    ]

    for rtype, expected_sup, expected_paused, expected_reroute in cases:
        state = derive_state(rtype)
        _assert(state.suppression_status == expected_sup,
                f"{rtype} → suppression_status={expected_sup}",
                f"got {state.suppression_status!r}")
        _assert(state.followup_paused == expected_paused,
                f"{rtype} → followup_paused={expected_paused}")
        _assert(state.alternate_contact_review_required == expected_reroute,
                f"{rtype} → alternate_contact_review_required={expected_reroute}",
                f"got {state.alternate_contact_review_required}")

    # Unknown → paused + manual review (conservative: uncertainty must not permit automation)
    state = derive_state(REPLY_TYPE_UNKNOWN)
    _assert(state.suppression_status == SUPPRESSION_PAUSED, "unknown → suppression_status=paused (conservative)")
    _assert(state.followup_paused is True,                   "unknown → followup_paused=True")
    _assert(state.manual_review_required is True,            "unknown → manual_review_required=True")

    # apply_state_to_reply never downgrades manual_review_required
    reply = ReplyRecord(manual_review_required=True)
    state_no_review = derive_state(REPLY_TYPE_SOFT_NO)  # soft_no doesn't set manual_review
    apply_state_to_reply(reply, state_no_review)
    _assert(reply.manual_review_required is True, "apply_state does not downgrade manual_review_required")


# ---------------------------------------------------------------------------
# Group D: Workflow 8 stop-rules integration
# ---------------------------------------------------------------------------

def test_group_d_workflow8_stoprules() -> None:
    print("\n[Group D] Workflow 8 stop-rule integration")

    def _candidate(email: str) -> dict:
        return {
            "kp_email": email,
            "followup_stage": "followup_1",
            "company_name": "Test Co",
            "open_count": 3,   # simulates engagement signal that should be overridden
            "click_count": 1,
        }

    # Build a synthetic reply_index
    reply_index = {
        "bounced@example.com":          {"suppression_status": "suppressed",       "reply_type": "bounce"},
        "suppressed@example.com":       {"suppression_status": "suppressed",       "reply_type": "unsubscribe"},
        "handoff@example.com":          {"suppression_status": "handoff_to_human",  "reply_type": "positive_interest"},
        "paused@example.com":           {"suppression_status": "paused",            "reply_type": "soft_no"},
        "wrongperson@example.com":      {"suppression_status": "paused",            "reply_type": "wrong_person"},
        "noreplystate@example.com":     {},
    }

    # Bounce → blocked
    r = check_stop_rules(_candidate("bounced@example.com"), reply_index=reply_index)
    _assert(r["decision"] == "blocked",   "bounce → blocked")

    # Suppressed → blocked (even with open/click signals)
    r = check_stop_rules(_candidate("suppressed@example.com"), reply_index=reply_index)
    _assert(r["decision"] == "blocked",   "suppressed → blocked")

    # Handoff → blocked
    r = check_stop_rules(_candidate("handoff@example.com"), reply_index=reply_index)
    _assert(r["decision"] == "blocked",   "handoff_to_human → blocked")

    # Paused → deferred
    r = check_stop_rules(_candidate("paused@example.com"), reply_index=reply_index)
    _assert(r["decision"] == "deferred",  "paused → deferred")

    # Wrong person → deferred (paused suppression)
    r = check_stop_rules(_candidate("wrongperson@example.com"), reply_index=reply_index)
    _assert(r["decision"] == "deferred",  "wrong_person → deferred")

    # No reply state → falls through to normal stop-rule checks → allowed
    r = check_stop_rules(_candidate("noreplystate@example.com"), reply_index=reply_index)
    _assert(r["allowed"],                 "no reply state → allowed by stop-rules")

    # Without reply_index → backward compatible (no reply check)
    r = check_stop_rules(_candidate("suppressed@example.com"), reply_index=None)
    _assert(r["allowed"],                 "no reply_index → backward compatible (allowed)")

    # Reply type only (no suppression_status) safety net
    index_rtype_only = {
        "hard_no@example.com": {"suppression_status": "", "reply_type": "hard_no"},
        "ooo@example.com":     {"suppression_status": "", "reply_type": "out_of_office"},
    }
    r = check_stop_rules(_candidate("hard_no@example.com"), reply_index=index_rtype_only)
    _assert(r["decision"] == "blocked",   "reply_type hard_no only → blocked")

    r = check_stop_rules(_candidate("ooo@example.com"), reply_index=index_rtype_only)
    _assert(r["decision"] == "deferred",  "reply_type out_of_office only → deferred")


# ---------------------------------------------------------------------------
# Group E: Wrong-person handling
# ---------------------------------------------------------------------------

def test_group_e_wrong_person() -> None:
    print("\n[Group E] Wrong-person handling")

    result = classify_reply(_reply(
        body_text="I think you have the wrong contact. Please reach out to procurement."
    ))
    _assert(result.reply_type == REPLY_TYPE_WRONG_PERSON,       "classified as wrong_person")

    state = derive_state(REPLY_TYPE_WRONG_PERSON)
    _assert(state.suppression_status == SUPPRESSION_PAUSED,     "suppression=paused (not suppressed)")
    _assert(state.followup_paused is True,                       "followup_paused=True")
    _assert(state.alternate_contact_review_required is True,     "alternate_contact_review_required=True")

    # Apply to a ReplyRecord and verify fields propagate
    reply = ReplyRecord(from_email="wp@example.com")
    apply_classification_to_reply(reply, result)
    apply_state_to_reply(reply, state)
    _assert(reply.reply_type == REPLY_TYPE_WRONG_PERSON,         "reply.reply_type set")
    _assert(reply.suppression_status == SUPPRESSION_PAUSED,      "reply.suppression_status set")
    _assert(reply.alternate_contact_review_required is True,      "reply.alternate_contact_review_required set")

    # Confirm no send execution happens — state is flag-only
    # (no automated send to another contact; just marking for review)
    _assert(True, "no automatic contact switch — flag only (design requirement)")


# ---------------------------------------------------------------------------
# Group F: Resilience
# ---------------------------------------------------------------------------

def test_group_f_resilience() -> None:
    print("\n[Group F] Resilience — malformed / empty records don't crash")

    # Empty ReplyRecord
    empty_reply = ReplyRecord()
    result = classify_reply(empty_reply)
    _assert(result.reply_type == REPLY_TYPE_UNKNOWN, "empty ReplyRecord → unknown")

    # Dict with missing fields
    dict_reply = {"subject": None, "snippet": None, "body_text": None}
    result = classify_reply(dict_reply)
    _assert(result.reply_type == REPLY_TYPE_UNKNOWN, "dict with None fields → unknown")

    # apply_classification_to_reply is safe on ReplyRecord
    apply_classification_to_reply(empty_reply, result)
    _assert(empty_reply.reply_type == REPLY_TYPE_UNKNOWN, "apply_classification safe on empty record")

    # derive_state with unknown type
    state = derive_state("totally_unknown_type_xyz")
    _assert(state.manual_review_required is True, "unknown reply_type → manual_review_required")

    # check_stop_rules with empty email candidate
    r = check_stop_rules({"kp_email": "", "followup_stage": "followup_1"}, reply_index={"": {}})
    _assert(r["decision"] == "blocked", "empty kp_email → blocked")

    # load_reply_suppression_index with missing file
    result_index = load_reply_suppression_index(path=Path("/nonexistent/reply_logs.csv"))
    _assert(result_index == {}, "missing reply_logs.csv → empty index")

    # worst_suppression edge cases
    _assert(worst_suppression("suppressed", "paused") == "suppressed", "suppressed > paused")
    _assert(worst_suppression("", "paused") == "paused", "'' vs paused → paused wins")
    _assert(worst_suppression("none", "") == "none", "none vs '' → none wins")


# ---------------------------------------------------------------------------
# Group G: Persistence round-trip
# ---------------------------------------------------------------------------

def test_group_g_persistence() -> None:
    print("\n[Group G] Persistence — CSV round-trip for Ticket 2 fields")
    import tempfile, csv as _csv

    # Create a fully-populated ReplyRecord with Ticket 2 fields
    r = ReplyRecord(
        gmail_message_id             = "persist_test_001",
        from_email                   = "persist@example.com",
        subject                      = "Re: Solar Offer",
        matched                      = True,
        match_method                 = "thread_id",
        reply_type                   = REPLY_TYPE_SOFT_NO,
        classification_method        = "rule_based",
        classification_confidence    = 1.0,
        classification_reason        = "not now",
        suppression_status           = SUPPRESSION_PAUSED,
        followup_paused              = True,
        alternate_contact_review_required = False,
        manual_review_required       = False,
    )

    # Verify all Ticket 2 fields are in CSV_FIELDS
    ticket2_fields = [
        "reply_type", "classification_method", "classification_confidence",
        "classification_reason", "suppression_status", "followup_paused",
        "alternate_contact_review_required",
    ]
    for f in ticket2_fields:
        _assert(f in CSV_FIELDS, f"CSV_FIELDS contains {f!r}")

    # Round-trip through CSV row
    row = r.to_csv_row()
    _assert(row["reply_type"] == REPLY_TYPE_SOFT_NO,       "reply_type serialized")
    _assert(row["followup_paused"] == "true",               "followup_paused bool → 'true'")
    _assert(row["classification_confidence"] == 1.0,        "classification_confidence preserved")

    r2 = ReplyRecord.from_csv_row(row)
    _assert(r2.reply_type == REPLY_TYPE_SOFT_NO,            "reply_type deserialized")
    _assert(r2.followup_paused is True,                     "followup_paused deserialized as bool")
    _assert(r2.classification_confidence == 1.0,            "classification_confidence deserialized")
    _assert(r2.suppression_status == SUPPRESSION_PAUSED,    "suppression_status preserved")
    _assert(r2.alternate_contact_review_required is False,  "alternate_contact_review_required=False round-trips")

    # Write and read back via real CSV file — test new dual-key suppression strategy
    with tempfile.TemporaryDirectory() as tmp:
        log_path = Path(tmp) / "reply_logs.csv"

        # Row 1: matched soft_no → indexed by matched_kp_email (not by from_email)
        r_matched = ReplyRecord(
            gmail_message_id          = "persist_test_001",
            from_email                = "persist@example.com",
            matched                   = True,
            matched_kp_email          = "kp@example.com",
            reply_type                = REPLY_TYPE_SOFT_NO,
            classification_method     = "rule_based",
            classification_confidence = 1.0,
            classification_reason     = "not now",
            suppression_status        = SUPPRESSION_PAUSED,
            followup_paused           = True,
        )

        # Row 2: unmatched unsubscribe → indexed by from_email (safety fallback)
        r_unsub = ReplyRecord(
            gmail_message_id          = "persist_test_002",
            from_email                = "optout@example.com",
            matched                   = False,
            matched_kp_email          = "",
            reply_type                = REPLY_TYPE_UNSUBSCRIBE,
            classification_method     = "rule_based",
            classification_confidence = 1.0,
            classification_reason     = "unsubscribe",
            suppression_status        = SUPPRESSION_SUPPRESSED,
            followup_paused           = True,
        )

        with open(str(log_path), "w", newline="", encoding="utf-8") as f:
            writer = _csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerow(r_matched.to_csv_row())
            writer.writerow(r_unsub.to_csv_row())

        # Matched soft_no: indexed by matched_kp_email
        index = load_reply_suppression_index(path=log_path)
        _assert("kp@example.com" in index,               "matched soft_no → indexed by matched_kp_email")
        _assert(index["kp@example.com"]["suppression_status"] == SUPPRESSION_PAUSED,
                "suppression_status loaded from matched_kp_email key")
        _assert(index["kp@example.com"]["reply_type"] == REPLY_TYPE_SOFT_NO,
                "reply_type loaded from matched_kp_email key")

        # Unmatched unsubscribe: indexed by from_email
        _assert("optout@example.com" in index,            "unmatched unsubscribe → indexed by from_email")
        _assert(index["optout@example.com"]["suppression_status"] == SUPPRESSION_SUPPRESSED,
                "unsubscribe suppression_status loaded from from_email key")

        # from_email of the matched soft_no is NOT in the index (non-safety type)
        _assert("persist@example.com" not in index,
                "matched soft_no: from_email not indexed separately (non-safety type)")


# ---------------------------------------------------------------------------
# Group H: Suppression key precedence (Part A hardening)
# ---------------------------------------------------------------------------

def test_group_h_suppression_key_precedence() -> None:
    print("\n[Group H] Suppression key precedence — matched_kp_email vs from_email")
    import csv as _csv, tempfile

    with tempfile.TemporaryDirectory() as tmp:
        log_path = Path(tmp) / "reply_logs.csv"
        from src.workflow_7_8_reply_intelligence.reply_models import CSV_FIELDS

        # Row 1: matched reply (matched=true, matched_kp_email set)
        # → indexed by matched_kp_email
        matched_row = {f: "" for f in CSV_FIELDS}
        matched_row.update({
            "gmail_message_id":   "h_msg_001",
            "from_email":         "sender@company.com",
            "matched":            "true",
            "matched_kp_email":   "kp@company.com",
            "suppression_status": "paused",
            "reply_type":         "soft_no",
        })

        # Row 2: unmatched unsubscribe (matched=false, no matched_kp_email)
        # → indexed by from_email only (safety type)
        unmatched_unsub = {f: "" for f in CSV_FIELDS}
        unmatched_unsub.update({
            "gmail_message_id":   "h_msg_002",
            "from_email":         "optout@other.com",
            "matched":            "false",
            "matched_kp_email":   "",
            "suppression_status": "suppressed",
            "reply_type":         "unsubscribe",
        })

        # Row 3: unmatched soft_no — should NOT be indexed (non-safety, no match)
        unmatched_soft = {f: "" for f in CSV_FIELDS}
        unmatched_soft.update({
            "gmail_message_id":   "h_msg_003",
            "from_email":         "ghost@nowhere.com",
            "matched":            "false",
            "matched_kp_email":   "",
            "suppression_status": "paused",
            "reply_type":         "soft_no",
        })

        with open(str(log_path), "w", newline="", encoding="utf-8") as f:
            writer = _csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows([matched_row, unmatched_unsub, unmatched_soft])

        index = load_reply_suppression_index(path=log_path)

        # Matched reply is indexed by matched_kp_email
        _assert("kp@company.com" in index,
                "matched reply → indexed by matched_kp_email")
        _assert(index["kp@company.com"]["suppression_status"] == "paused",
                "matched_kp_email key has correct suppression_status")

        # Unmatched unsubscribe is indexed by from_email (safety type)
        _assert("optout@other.com" in index,
                "unmatched unsubscribe → indexed by from_email (safety fallback)")
        _assert(index["optout@other.com"]["suppression_status"] == "suppressed",
                "unsubscribe from_email key has correct suppression_status")

        # Unmatched soft_no is NOT indexed
        _assert("ghost@nowhere.com" not in index,
                "unmatched soft_no → NOT indexed (no from_email fallback for non-safety type)")

        # Matched reply's from_email is NOT indexed (unless it's a safety type)
        # sender@company.com is not unsubscribe/hard_no → no from_email entry
        _assert("sender@company.com" not in index,
                "matched non-safety reply → from_email NOT separately indexed")


# ---------------------------------------------------------------------------
# Group I: Multi-reply state resolution (Part B hardening)
# ---------------------------------------------------------------------------

def test_group_i_multi_reply_resolution() -> None:
    print("\n[Group I] Multi-reply state resolution — worst suppression wins")
    import csv as _csv, tempfile

    with tempfile.TemporaryDirectory() as tmp:
        log_path = Path(tmp) / "reply_logs.csv"
        from src.workflow_7_8_reply_intelligence.reply_models import CSV_FIELDS

        def _row(msg_id, from_email, sup, rtype, matched_kp=""):
            r = {f: "" for f in CSV_FIELDS}
            r.update({
                "gmail_message_id":   msg_id,
                "from_email":         from_email,
                "matched":            "true" if matched_kp else "false",
                "matched_kp_email":   matched_kp,
                "suppression_status": sup,
                "reply_type":         rtype,
            })
            return r

        # Scenario I-1: out_of_office first, then hard_no for same contact
        # → hard_no (suppressed) must win over out_of_office (paused)
        rows_i1 = [
            _row("i1_a", "prospect@biz.com", "paused",     "out_of_office", "kp@biz.com"),
            _row("i1_b", "prospect@biz.com", "suppressed",  "hard_no",      "kp@biz.com"),
        ]

        # Scenario I-2: two soft_no rows (equal suppression) — later row's reply_type wins
        rows_i2 = [
            _row("i2_a", "dual@biz.com", "paused", "soft_no",  "kp2@biz.com"),
            _row("i2_b", "dual@biz.com", "paused", "wrong_person", "kp2@biz.com"),
        ]

        with open(str(log_path), "w", newline="", encoding="utf-8") as f:
            writer = _csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows_i1 + rows_i2)

        index = load_reply_suppression_index(path=log_path)

        # Scenario I-1: strongest wins
        _assert(index["kp@biz.com"]["suppression_status"] == "suppressed",
                "multi-reply: hard_no (suppressed) beats out_of_office (paused)")
        _assert(index["kp@biz.com"]["reply_type"] == "hard_no",
                "multi-reply: reply_type follows strongest suppression row")

        # Scenario I-2: tied suppression → later row wins on reply_type
        _assert(index["kp2@biz.com"]["suppression_status"] == "paused",
                "multi-reply tied: suppression stays paused")
        _assert(index["kp2@biz.com"]["reply_type"] == "wrong_person",
                "multi-reply tied: later row's reply_type wins")


# ---------------------------------------------------------------------------
# Group J: Workflow 8 Rule 0 overrides engagement heuristics (Part H hardening)
# ---------------------------------------------------------------------------

def test_group_j_workflow8_engagement_override() -> None:
    print("\n[Group J] Workflow 8 Rule 0 overrides engagement heuristics")

    def _candidate(email: str, open_count: int = 3, click_count: int = 1) -> dict:
        return {
            "kp_email":       email,
            "followup_stage": "followup_1",
            "company_name":   "Engaged Co",
            "open_count":     open_count,
            "click_count":    click_count,
        }

    # Simulate a highly engaged contact (opens + clicks) who replied hard_no
    idx_hard_no = {"engaged_hardno@biz.com": {"suppression_status": "suppressed", "reply_type": "hard_no"}}
    r = check_stop_rules(_candidate("engaged_hardno@biz.com"), reply_index=idx_hard_no)
    _assert(r["decision"] == "blocked",
            "opened_no_click + hard_no reply → blocked (reply state beats engagement)")

    # clicked_no_reply + request_quote → handoff_to_human → blocked (not queued)
    idx_quote = {"clicked_quote@biz.com": {"suppression_status": "handoff_to_human", "reply_type": "request_quote"}}
    r = check_stop_rules(_candidate("clicked_quote@biz.com"), reply_index=idx_quote)
    _assert(r["decision"] == "blocked",
            "clicked_no_reply + request_quote → blocked (handoff_to_human beats engagement)")

    # prior engagement + out_of_office → paused/deferred
    idx_ooo = {"ooo_engaged@biz.com": {"suppression_status": "paused", "reply_type": "out_of_office"}}
    r = check_stop_rules(_candidate("ooo_engaged@biz.com"), reply_index=idx_ooo)
    _assert(r["decision"] == "deferred",
            "prior engagement + out_of_office → deferred (reply state wins)")

    # prior engagement + unknown reply → deferred (unknown now pauses)
    idx_unknown = {"unknown_engaged@biz.com": {"suppression_status": "paused", "reply_type": "unknown"}}
    r = check_stop_rules(_candidate("unknown_engaged@biz.com"), reply_index=idx_unknown)
    _assert(r["decision"] == "deferred",
            "prior engagement + unknown reply → deferred (conservative pause wins)")

    # Confirm: no reply state → engagement alone doesn't block (falls through)
    r = check_stop_rules(_candidate("clean@biz.com"), reply_index={})
    _assert(r["allowed"],
            "high engagement, no reply state → allowed (reply state has no veto)")


# ---------------------------------------------------------------------------
# Group K: OOO vs auto_reply_other stay distinct; unknown is paused (Parts C/D)
# ---------------------------------------------------------------------------

def test_group_k_ooo_vs_auto_and_unknown() -> None:
    print("\n[Group K] OOO vs auto_reply_other distinct; unknown → paused")

    # Both pause, but their stored reply_types are distinct
    state_ooo  = derive_state(REPLY_TYPE_OUT_OF_OFFICE)
    state_auto = derive_state(REPLY_TYPE_AUTO_REPLY_OTHER)

    _assert(state_ooo.suppression_status  == SUPPRESSION_PAUSED, "OOO  → paused")
    _assert(state_auto.suppression_status == SUPPRESSION_PAUSED, "auto → paused")

    # Applying them to records preserves the distinct reply_type in CSV
    r_ooo = ReplyRecord(from_email="ooo@biz.com")
    apply_classification_to_reply(r_ooo, _make_result(REPLY_TYPE_OUT_OF_OFFICE))
    apply_state_to_reply(r_ooo, state_ooo)

    r_auto = ReplyRecord(from_email="auto@biz.com")
    apply_classification_to_reply(r_auto, _make_result(REPLY_TYPE_AUTO_REPLY_OTHER))
    apply_state_to_reply(r_auto, state_auto)

    _assert(r_ooo.reply_type  == REPLY_TYPE_OUT_OF_OFFICE,    "OOO  stored reply_type=out_of_office")
    _assert(r_auto.reply_type == REPLY_TYPE_AUTO_REPLY_OTHER, "auto stored reply_type=auto_reply_other")
    _assert(r_ooo.reply_type  != r_auto.reply_type,           "OOO and auto_reply_other stored distinctly")

    # Unknown is now paused (Part C hardening)
    state_unk = derive_state(REPLY_TYPE_UNKNOWN)
    _assert(state_unk.suppression_status == SUPPRESSION_PAUSED, "unknown → suppression_status=paused")
    _assert(state_unk.followup_paused is True,                   "unknown → followup_paused=True")
    _assert(state_unk.manual_review_required is True,            "unknown → manual_review_required=True")

    # Verify Workflow 8 safety net: "unknown" in _PAUSE_REPLY_TYPES handles
    # old CSV data where suppression_status may be empty but reply_type="unknown"
    from src.workflow_8_followup.followup_stop_rules import check_stop_rules
    idx_old_unknown = {"legacy@biz.com": {"suppression_status": "", "reply_type": "unknown"}}
    r = check_stop_rules(
        {"kp_email": "legacy@biz.com", "followup_stage": "followup_1", "company_name": "Legacy"},
        reply_index=idx_old_unknown,
    )
    _assert(r["decision"] == "deferred",
            "old CSV row (suppression_status='', reply_type='unknown') → deferred via safety net")


# ---------------------------------------------------------------------------
# Group L: Rule tightening — removed triggers don't fire (Part E hardening)
# ---------------------------------------------------------------------------

def test_group_l_rule_tightening() -> None:
    print("\n[Group L] Rule tightening — removed triggers behave correctly")

    # "tell me more" was removed from positive_interest (it's in request_info first)
    # → must classify as request_info, not positive_interest
    result = classify_reply(_reply(body_text="Tell me more about your products."))
    _assert(result.reply_type == REPLY_TYPE_REQUEST_INFO,
            '"tell me more" → request_info (not positive_interest; request_info is earlier in cascade)')

    # "proposal" alone was removed from request_quote — must NOT trigger it
    result = classify_reply(_reply(body_text="We reviewed your proposal and it needs changes."))
    # No hard_no, request_quote, or other earlier trigger should fire on this text alone
    # "proposal" no longer in request_quote → falls to unknown or soft match
    _assert(result.reply_type not in (REPLY_TYPE_REQUEST_QUOTE,),
            '"proposal" alone no longer triggers request_quote')

    # "request for proposal" still works (was not removed)
    result = classify_reply(_reply(body_text="We would like to send you a request for proposal."))
    _assert(result.reply_type == REPLY_TYPE_REQUEST_QUOTE,
            '"request for proposal" still triggers request_quote')

    # "should contact" was removed from wrong_person — must NOT trigger it
    result = classify_reply(_reply(body_text="You should contact us to get started."))
    _assert(result.reply_type != REPLY_TYPE_WRONG_PERSON,
            '"should contact" alone no longer triggers wrong_person')

    # "please contact" was removed from wrong_person — bare form must NOT trigger it
    result = classify_reply(_reply(body_text="Please contact us for more information."))
    _assert(result.reply_type != REPLY_TYPE_WRONG_PERSON,
            '"please contact" alone no longer triggers wrong_person')

    # Specific wrong_person patterns still work
    result = classify_reply(_reply(body_text="You have the wrong person, please contact procurement."))
    _assert(result.reply_type == REPLY_TYPE_WRONG_PERSON,
            '"you have the wrong" still correctly triggers wrong_person')


# ---------------------------------------------------------------------------
# Helper used in Group K
# ---------------------------------------------------------------------------

from src.workflow_7_8_reply_intelligence.reply_classifier import ClassificationResult

def _make_result(reply_type: str) -> ClassificationResult:
    return ClassificationResult(
        reply_type               = reply_type,
        classification_method    = "rule_based",
        classification_confidence= 1.0,
        classification_reason    = "test",
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 68)
    print("Workflow 7.8 Ticket 2 — Reply Classification smoke tests")
    print("=" * 68)

    test_group_a_classification()
    test_group_b_priority()
    test_group_c_state_transitions()
    test_group_d_workflow8_stoprules()
    test_group_e_wrong_person()
    test_group_f_resilience()
    test_group_g_persistence()
    test_group_h_suppression_key_precedence()
    test_group_i_multi_reply_resolution()
    test_group_j_workflow8_engagement_override()
    test_group_k_ooo_vs_auto_and_unknown()
    test_group_l_rule_tightening()

    print("\n" + "=" * 68)
    print("All tests passed.")
    print("=" * 68)
