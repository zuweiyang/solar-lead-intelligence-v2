# Workflow 7.8 — Reply Intelligence: Deterministic Reply Matcher
#
# Matches an inbound ReplyRecord to a prior outbound send_log row using a
# layered deterministic strategy.  No AI or probabilistic scoring is used.
#
# Priority order (explicit, must not be reordered):
#   Level 1a: Gmail thread_id    — outbound message ID in the same Gmail thread
#   Level 1b: In-Reply-To header — local-part matches a provider_message_id
#   Level 1c: References header  — newest local-part (reversed) matches a provider_message_id
#   Level 2:  email + subject    — kp_email + normalized subject match
#   Level 3:  email only         — ONLY when exactly one recent candidate exists
#                                  (within _EMAIL_ONLY_MAX_SEND_AGE_DAYS).
#                                  Multiple recent candidates → ambiguous (no match).
#   No match: manual_review=True, matched=False
#
# Safety rule: stronger header-based evidence ALWAYS wins over weaker fallbacks.
# Level 3 email-only match is conservative by design — it sets manual_review=True
# even on a successful single-candidate match, and refuses to match at all when
# multiple recent outbound sends exist for the same recipient.

import csv
import re
from datetime import datetime, timedelta, timezone

from config.settings import SEND_LOGS_FILE

# Only these statuses reached the recipient — used when filtering send_logs
_SENDABLE_STATUSES = {"sent", "dry_run"}

# Email-only fallback window: sends older than this are not considered for Level 3
_EMAIL_ONLY_MAX_SEND_AGE_DAYS = 90

# Compiled subject-prefix pattern (applied in a loop for nested prefixes)
# Matches: Re:  Re[N]:  Fwd:  FWD:  FW:  and variants
_SUBJECT_PREFIX_RE = re.compile(
    r'^(re|fwd?)\s*(\[\d+\])?\s*:\s*',
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

def _normalize_subject(s: str) -> str:
    """
    Strip Re:/Fwd:/FW:/Re[N]: prefixes (including nested), lowercase, collapse
    whitespace.

    Prefix stripping is applied in a loop so nested prefixes such as
    "Re: Re: Subject" and "Fwd: Re: Subject" are fully stripped.

    Examples:
        "RE: Solar mounting for your EPC projects"
            → "solar mounting for your epc projects"
        "Re[2]: Solar mounting for your EPC projects"
            → "solar mounting for your epc projects"
        "FWD: Some subject"  → "some subject"
        "FW: Some subject"   → "some subject"
        "Re: Re: Subject"    → "subject"
        "Fwd: Re: Subject"   → "subject"
    """
    s = (s or "").strip()
    while True:
        stripped = _SUBJECT_PREFIX_RE.sub("", s).strip()
        if stripped == s:
            break
        s = stripped
    return " ".join(s.lower().split())


def _extract_local_parts(header_val: str) -> list[str]:
    """
    Extract the local-part(s) from RFC 2822 Message-ID values in a header.

    Gmail's internal message IDs (e.g. "19cff2dadb400b39") appear as the
    local-part of the RFC Message-ID header: <19cff2dadb400b39@mail.gmail.com>.

    Also handles bare IDs (no angle brackets) as a fallback.
    """
    if not header_val:
        return []
    # Primary: extract <local@domain> patterns
    parts = re.findall(r'<([^@>\s]+)@[^>]*>', header_val)
    if parts:
        return [p.strip() for p in parts if p.strip()]
    # Fallback: bare IDs (no angle brackets)
    parts = [p.strip() for p in header_val.split() if p.strip()]
    return parts


def _parse_ts(raw: str) -> datetime | None:
    """Parse an ISO 8601 timestamp string. Returns None on failure."""
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.strip())
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Send log loading and indexing
# ---------------------------------------------------------------------------

def load_send_logs(path=None) -> list[dict]:
    """
    Load send_logs.csv from the global CRM path.
    Returns [] if file is missing, unreadable, or empty.
    Malformed individual rows are skipped silently.
    """
    p = path or SEND_LOGS_FILE
    try:
        if not p.exists():
            return []
        rows = []
        with open(str(p), newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    rows.append(dict(row))
                except Exception:
                    continue
        return rows
    except Exception as exc:
        print(f"[Workflow 7.8]   Could not load send_logs: {exc}")
        return []


def build_send_log_index(send_logs: list[dict]) -> dict:
    """
    Build O(1) lookup structures from send_log rows.

    Only rows with send_status in {sent, dry_run} are indexed
    (blocked/failed/deferred rows never reached the recipient).
    Rows with empty or malformed fields are skipped gracefully.

    Returns dict of:
        by_provider_msg_id  — {provider_message_id → row}
        by_message_id_local — {local-part of Message-ID → row}
        by_tracking_id      — {tracking_id → row}
        by_kp_email         — {kp_email_lower → [rows, newest-first]}
        by_email_subject    — {(kp_email_lower, norm_subject) → [rows, newest-first]}
    """
    by_pid   = {}   # provider_message_id
    by_local = {}   # local-part lookup (same content as pid for Gmail IDs)
    by_tid   = {}   # tracking_id
    by_email = {}   # kp_email → list[row]
    by_es    = {}   # (kp_email, norm_subject) → list[row]

    for row in send_logs:
        status = (row.get("send_status") or "").strip().lower()
        if status not in _SENDABLE_STATUSES:
            continue

        pid = (row.get("provider_message_id") or "").strip()
        if pid:
            by_pid[pid]   = row
            by_local[pid] = row   # Gmail IDs are already the local-part

        tid = (row.get("tracking_id") or "").strip()
        if tid:
            by_tid[tid] = row

        email = (row.get("kp_email") or "").strip().lower()
        if email:
            by_email.setdefault(email, []).append(row)
            norm_subj = _normalize_subject(row.get("subject", ""))
            if norm_subj:
                by_es.setdefault((email, norm_subj), []).append(row)

    # Sort each bucket by timestamp descending (most recent send wins)
    for rows in by_email.values():
        rows.sort(key=lambda r: r.get("timestamp", ""), reverse=True)
    for rows in by_es.values():
        rows.sort(key=lambda r: r.get("timestamp", ""), reverse=True)

    return {
        "by_provider_msg_id":  by_pid,
        "by_message_id_local": by_local,
        "by_tracking_id":      by_tid,
        "by_kp_email":         by_email,
        "by_email_subject":    by_es,
    }


# ---------------------------------------------------------------------------
# Match application helper
# ---------------------------------------------------------------------------

def _apply_match(reply, row: dict, method: str) -> None:
    """Populate reply match fields from a matched send_log row. Modifies reply in-place."""
    reply.matched                = True
    reply.match_method           = method
    # Use timestamp+kp_email as a stable audit reference (no integer row IDs in CSV)
    reply.matched_send_log_row_id = f"{row.get('timestamp', '')}|{row.get('kp_email', '')}"
    reply.matched_tracking_id    = row.get("tracking_id", "")
    reply.matched_campaign_id    = row.get("campaign_id", "")
    reply.matched_company_name   = row.get("company_name", "")
    reply.matched_kp_email       = row.get("kp_email", "")
    reply.matched_place_id       = row.get("place_id", "")
    reply.manual_review_required = False


# ---------------------------------------------------------------------------
# Core matching function
# ---------------------------------------------------------------------------

def match_reply(reply, index: dict, outbound_thread_ids: list[str] | None = None) -> None:
    """
    Attempt to match a ReplyRecord to a prior outbound send_log row.
    Modifies `reply` in-place.

    Matching priority (must follow this exact order — do not reorder):
        Level 1a: thread_id      — strongest; Gmail-level thread membership
        Level 1b: In-Reply-To   — strong; RFC header pointing at specific message
        Level 1c: References    — strong; chain of message IDs (newest checked first)
        Level 2:  email_subject — medium; email + normalized subject agreement
        Level 3:  email_recent  — weak; email only, single recent candidate only
        No match                — manual_review_required, matched=False

    Args:
        reply:               ReplyRecord with raw fields populated
        index:               dict from build_send_log_index()
        outbound_thread_ids: Gmail message IDs of our outbound messages in the
                             same thread (from get_thread_outbound_message_ids).
                             Pass [] or None to skip Level 1a.
    """
    by_pid   = index.get("by_provider_msg_id",  {})
    by_local = index.get("by_message_id_local",  {})
    by_email = index.get("by_kp_email",          {})
    by_es    = index.get("by_email_subject",      {})

    # --- Level 1a: thread_id ---
    # The outbound message's Gmail ID is provider_message_id in send_logs.
    # If the reply is in the same thread as one of our sent messages, it's a reply.
    if outbound_thread_ids:
        for oid in outbound_thread_ids:
            row = by_pid.get(oid)
            if row:
                _apply_match(reply, row, "thread_id")
                return

    # --- Level 1b: In-Reply-To ---
    # In-Reply-To contains the RFC Message-ID of the original email.
    # For Gmail-sent emails, the local-part is the Gmail internal ID.
    if reply.in_reply_to:
        for lp in _extract_local_parts(reply.in_reply_to):
            row = by_local.get(lp)
            if row:
                _apply_match(reply, row, "in_reply_to")
                return

    # --- Level 1c: References ---
    # References contains the full chain of Message-IDs ordered oldest → newest.
    # We iterate in REVERSE to match the most recent outbound message first,
    # which is the actual parent of this reply.
    if reply.references:
        for lp in reversed(_extract_local_parts(reply.references)):
            row = by_local.get(lp)
            if row:
                _apply_match(reply, row, "references")
                return

    # --- Level 2: email + normalized subject ---
    from_email_lower = (reply.from_email or "").lower()
    norm_subj        = _normalize_subject(reply.subject)

    if from_email_lower and norm_subj:
        candidates = by_es.get((from_email_lower, norm_subj), [])
        if candidates:
            _apply_match(reply, candidates[0], "email_subject")
            return

    # --- Level 3: email only (restricted fallback) ---
    #
    # Safety requirements for Level 3 to activate (ALL must be true):
    #   (a) from_email is present
    #   (b) exactly ONE recent outbound send exists within _EMAIL_ONLY_MAX_SEND_AGE_DAYS
    #   (c) no stronger header-based evidence was available (already guaranteed
    #       by reaching this point in the cascade)
    #
    # If (b) fails because multiple recent sends exist → set ambiguous error,
    # do NOT force a match.  The reply event is preserved for manual review.
    if from_email_lower:
        candidates = by_email.get(from_email_lower, [])
        if candidates:
            cutoff = datetime.now(tz=timezone.utc) - timedelta(days=_EMAIL_ONLY_MAX_SEND_AGE_DAYS)
            recent = [
                r for r in candidates
                if (ts := _parse_ts(r.get("timestamp"))) is not None and ts >= cutoff
            ]
            if len(recent) == 1:
                # Single unambiguous recent candidate — safe to use, but must be reviewed
                _apply_match(reply, recent[0], "email_recent")
                reply.manual_review_required = True   # override — weak evidence
                return
            elif len(recent) > 1:
                # Multiple recent sends → cannot safely pick one; flag for review
                reply.matched                = False
                reply.match_method           = ""
                reply.manual_review_required = True
                reply.match_error            = "email_only_ambiguous"
                return
            # len(recent) == 0: all sends are outside the time window → fall through

    # --- No match ---
    reply.matched                = False
    reply.match_method           = ""
    reply.manual_review_required = True
    reply.match_error            = "no_match"
