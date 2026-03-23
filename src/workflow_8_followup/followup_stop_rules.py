# Workflow 8: Follow-up Automation — Stop Rules & Engagement Classification
# Determines when follow-up should be allowed, deferred, or permanently blocked.
#
# Ticket 2 addition: reply-state check (highest priority, Rule 0).
# A real human reply overrides open/click engagement heuristics.
# Reply state is loaded from reply_logs.csv (CSV-primary; always available).

import csv
import re
from pathlib import Path

from config.settings import FOLLOWUP_MAX_STAGE, REPLY_LOGS_FILE

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# Stages that are valid to send
VALID_STAGES = {f"followup_{i}" for i in range(1, FOLLOWUP_MAX_STAGE + 1)}

# send_status values that confirm an initial email was actually attempted
GOOD_SEND_STATUSES = {"sent", "dry_run"}

# Suppression statuses that block follow-up entirely
_BLOCK_SUPPRESSION_STATUSES = {"suppressed", "handoff_to_human"}

# Suppression statuses that defer (pause) follow-up
_PAUSE_SUPPRESSION_STATUSES = {"paused"}

# Reply types that block follow-up (human has engaged; must not receive automation)
_BLOCK_REPLY_TYPES = {
    "bounce",
    "positive_interest",
    "request_info",
    "request_quote",
    "forwarded",
    "hard_no",
    "unsubscribe",
}

# Reply types that pause follow-up (temporary hold)
# "unknown" is included so that contacts with an unclassified real reply are
# paused rather than allowed to continue receiving automated follow-up.
# This mirrors the conservative unknown state introduced in reply_state_manager.
_PAUSE_REPLY_TYPES = {
    "wrong_person",
    "soft_no",
    "out_of_office",
    "auto_reply_other",
    "unknown",
}

# Reply types that use from_email as an ADDITIONAL suppression key regardless
# of whether the reply was matched to a send_log row.  These safety-critical
# opt-out types must protect the sender address even if no CRM match exists.
_SAFETY_SUPPRESSION_TYPES = {"unsubscribe", "hard_no"}


# ---------------------------------------------------------------------------
# Reply suppression index (CSV-based loader)
# ---------------------------------------------------------------------------

def load_reply_suppression_index(path=None) -> dict:
    """
    Build a {email_lower: {"suppression_status": str, "reply_type": str}} index
    from reply_logs.csv.

    Suppression key strategy (Part A hardening):
      Primary key:  matched_kp_email when the reply was reliably matched to a
                    send_log row (matched == "true" and matched_kp_email non-empty).
                    This is the CRM-verified association and preferred for all types.
      Safety key:   from_email is ALWAYS ALSO indexed for unsubscribe and hard_no,
                    regardless of match status.  An unmatched opt-out must still
                    protect the sender's address.
      Non-safety types with no reliable match: NOT indexed by from_email.
                    This prevents an unmatched soft_no / OOO / positive_interest
                    from over-suppressing contacts that were never actually emailed.

    Multi-reply resolution policy (Part B hardening):
      When multiple reply rows exist for the same email key, the most restrictive
      suppression_status wins (worst_suppression: suppressed > handoff_to_human >
      paused > none > "").  When two rows share the same suppression level (a tie),
      the later row in the CSV (append-order = chronological) wins on reply_type.
      This means: strongest suppression always wins; for equal strength, most recent
      reply determines the stored reply_type.

    Returns {} if reply_logs.csv is missing or empty.
    This is the primary Workflow 8 reply-state source — it does not require DB access.
    """
    from src.workflow_7_8_reply_intelligence.reply_state_manager import worst_suppression

    p = Path(str(path or REPLY_LOGS_FILE))
    if not p.exists() or p.stat().st_size == 0:
        return {}

    index: dict[str, dict] = {}

    def _update(key: str, sup: str, rtype: str) -> None:
        """Apply worst-suppression merge for a given index key."""
        prev = index.get(key)
        if prev is None:
            index[key] = {"suppression_status": sup, "reply_type": rtype}
        else:
            # worst_suppression(a, b) returns a when rank(a) >= rank(b),
            # so for a tie the newer entry's data wins (chronological order).
            if worst_suppression(sup, prev["suppression_status"]) == sup:
                index[key] = {"suppression_status": sup, "reply_type": rtype}

    try:
        with open(str(p), newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                from_email = (row.get("from_email")       or "").lower().strip()
                matched_kp = (row.get("matched_kp_email") or "").lower().strip()
                matched    = (row.get("matched")           or "").lower() in ("true", "1", "yes")
                sup        = (row.get("suppression_status") or "").strip()
                rtype      = (row.get("reply_type")        or "").strip()

                if not from_email:
                    continue
                # Skip rows with no state signal at all
                if not sup and not rtype:
                    continue

                # Primary: matched_kp_email when reliably matched to a send_log row
                if matched and matched_kp:
                    _update(matched_kp, sup, rtype)

                # Safety key: unsubscribe/hard_no always also indexed by from_email
                # so that even unmatched opt-outs protect the sender address.
                if rtype in _SAFETY_SUPPRESSION_TYPES:
                    _update(from_email, sup, rtype)
                # Non-safety type with no reliable match: skip from_email indexing
                # to avoid broad suppression of contacts never actually emailed.

    except Exception as exc:
        print(f"[Workflow 8] Could not load reply suppression index: {exc}")

    return index


# ---------------------------------------------------------------------------
# Engagement classification
# ---------------------------------------------------------------------------

def classify_engagement(open_count: int, click_count: int) -> str:
    """
    Classify contact engagement based on open and click counts.

    Values:
        clicked_no_reply    — at least one link click recorded
        multi_open_no_click — opened 2+ times, no click
        opened_no_click     — opened once, no click
        no_open             — no opens recorded
    """
    if click_count > 0:
        return "clicked_no_reply"
    if open_count >= 2:
        return "multi_open_no_click"
    if open_count == 1:
        return "opened_no_click"
    return "no_open"


# ---------------------------------------------------------------------------
# Decision helpers
# ---------------------------------------------------------------------------

def _allow(reason: str = "") -> dict:
    return {"allowed": True, "decision": "followup", "reason": reason}

def _block(reason: str) -> dict:
    return {"allowed": False, "decision": "blocked", "reason": reason}

def _defer(reason: str) -> dict:
    return {"allowed": False, "decision": "deferred", "reason": reason}


# ---------------------------------------------------------------------------
# Main stop-rule check
# ---------------------------------------------------------------------------

def check_stop_rules(candidate: dict, reply_index: dict | None = None) -> dict:
    """
    Run all stop rules against a follow-up candidate.

    Rules checked in order:
    0.  Reply state (Ticket 2) — highest priority.
        A real human reply overrides open/click heuristics.
        suppressed/handoff → blocked; paused → deferred.
    1.  Missing kp_email — block
    2.  Malformed kp_email — block
    3.  Stage not in valid set — block
    4.  Stage exceeds max — block
    5.  Suppressed flag present — block

    Args:
        candidate:    follow-up candidate dict from followup_selector
        reply_index:  optional pre-loaded {email: {suppression_status, reply_type}}
                      from load_reply_suppression_index().  If None, reply-state
                      check is skipped (backward-compatible for existing callers).

    Returns {"allowed": bool, "decision": str, "reason": str}.
    """
    email = (candidate.get("kp_email") or "").strip()

    # --- Rule 0: Reply state check (Ticket 2, highest priority) ---
    if reply_index is not None and email:
        entry = reply_index.get(email.lower())
        if entry:
            sup   = entry.get("suppression_status", "")
            rtype = entry.get("reply_type", "")

            if sup in _BLOCK_SUPPRESSION_STATUSES:
                return _block(
                    f"Reply received: suppression_status={sup!r} reply_type={rtype!r}"
                )
            if sup in _PAUSE_SUPPRESSION_STATUSES:
                return _defer(
                    f"Reply received: suppression_status={sup!r} reply_type={rtype!r}"
                )
            # Safety net: reply_type present but suppression_status missing
            if rtype in _BLOCK_REPLY_TYPES:
                return _block(f"Reply received: reply_type={rtype!r}")
            if rtype in _PAUSE_REPLY_TYPES:
                return _defer(f"Reply received: reply_type={rtype!r}")

    # --- Rule 1: Missing kp_email ---
    if not email:
        return _block("Missing kp_email")

    # --- Rule 2: Malformed kp_email ---
    if not _EMAIL_RE.match(email):
        return _block(f"Malformed email address: {email!r}")

    # --- Rule 3: Stage not in valid set ---
    stage = (candidate.get("followup_stage") or "").strip()
    if stage not in VALID_STAGES:
        return _block(f"Invalid or exhausted follow-up stage: {stage!r}")

    # --- Rule 4: Stage exceeds max ---
    try:
        stage_num = int(stage.split("_")[-1])
    except (ValueError, IndexError):
        return _block(f"Cannot parse stage number from: {stage!r}")

    if stage_num > FOLLOWUP_MAX_STAGE:
        return _block(f"Maximum follow-up stage reached ({FOLLOWUP_MAX_STAGE})")

    # --- Rule 5: Suppression flag (manual override field) ---
    if candidate.get("suppressed", "").strip().lower() in ("1", "true", "yes"):
        return _block("Contact is suppressed")

    return _allow("Stop rules passed")
