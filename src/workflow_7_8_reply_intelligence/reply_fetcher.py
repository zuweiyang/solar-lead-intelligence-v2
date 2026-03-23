# Workflow 7.8 — Reply Intelligence: Gmail Reply Fetcher
#
# Authenticates with Gmail API using the existing token (requires gmail.readonly scope),
# fetches recent inbound messages from INBOX, and parses them into ReplyRecord instances.
#
# Scope requirement:
#   gmail.readonly must be present in config/gmail_token.json.
#   Re-run scripts/authorize_gmail.py if the token was created before Workflow 7.8
#   was added (old tokens only had gmail.send).

import base64
import re
from datetime import datetime, timedelta, timezone

from config.settings import GMAIL_TOKEN_FILE

_GMAIL_READ_SCOPES = {
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
    "https://mail.google.com/",
}


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _get_gmail_read_service():
    """
    Load OAuth2 credentials and return a Gmail API service object for reading.

    Raises RuntimeError if:
    - token file is missing (user hasn't run authorize_gmail.py)
    - token doesn't include a read scope (user ran old authorize_gmail.py)
    - token is expired and cannot be refreshed
    """
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build

    if not GMAIL_TOKEN_FILE.exists():
        raise RuntimeError(
            "Gmail token not found. Run scripts/authorize_gmail.py to authorize."
        )

    creds = Credentials.from_authorized_user_file(str(GMAIL_TOKEN_FILE))

    # Verify read scope — fail early with a clear message
    token_scopes = set(getattr(creds, "scopes", None) or [])
    if token_scopes and not (token_scopes & _GMAIL_READ_SCOPES):
        raise RuntimeError(
            "Gmail token lacks a read scope (gmail.readonly). "
            "Delete config/gmail_token.json and re-run scripts/authorize_gmail.py "
            "to re-authorize with both gmail.send and gmail.readonly."
        )

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            with open(str(GMAIL_TOKEN_FILE), "w") as f:
                f.write(creds.to_json())
        except Exception as exc:
            raise RuntimeError(f"Gmail token refresh failed: {exc}") from exc

    if not creds or not creds.valid:
        raise RuntimeError(
            "Gmail credentials are invalid. Re-run scripts/authorize_gmail.py."
        )

    return build("gmail", "v1", credentials=creds)


# ---------------------------------------------------------------------------
# Header / body helpers
# ---------------------------------------------------------------------------

def _header_value(headers: list, name: str) -> str:
    """Extract one header value from the Gmail API payload.headers list."""
    name_lower = name.lower()
    for h in headers:
        if h.get("name", "").lower() == name_lower:
            return h.get("value", "")
    return ""


def _parse_from_header(raw: str) -> tuple[str, str]:
    """
    Parse 'Display Name <email@domain.com>' or bare 'email@domain.com'.
    Returns (name, email_lower).
    """
    m = re.match(r'^(.*?)\s*<([^>]+)>\s*$', (raw or "").strip())
    if m:
        name  = m.group(1).strip().strip('"')
        email = m.group(2).strip().lower()
        return name, email
    email = (raw or "").strip().lower()
    return "", email


def _extract_plain_text(payload: dict, depth: int = 0) -> str:
    """
    Recursively extract plain/text content from a MIME payload dict.
    Caps recursion at depth 10 and content at 3000 chars.
    Returns "" if no plain text part is found.
    """
    if depth > 10:
        return ""
    mime_type = (payload.get("mimeType") or "").lower()
    if mime_type == "text/plain":
        data = (payload.get("body") or {}).get("data", "")
        if data:
            try:
                return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")[:3000]
            except Exception:
                return ""
    if mime_type.startswith("multipart/"):
        for part in payload.get("parts") or []:
            text = _extract_plain_text(part, depth + 1)
            if text:
                return text
    return ""


# ---------------------------------------------------------------------------
# Message → ReplyRecord
# ---------------------------------------------------------------------------

def _message_to_reply(msg: dict) -> "ReplyRecord | None":
    """
    Convert a Gmail API message dict (format=full) to a partial ReplyRecord.
    Returns None if parsing fails (pipeline skips that message gracefully).
    """
    from src.workflow_7_8_reply_intelligence.reply_models import ReplyRecord

    try:
        msg_id    = msg.get("id", "")
        thread_id = msg.get("threadId", "")
        snippet   = (msg.get("snippet") or "")[:500]
        payload   = msg.get("payload") or {}
        headers   = payload.get("headers") or []

        from_raw    = _header_value(headers, "From")
        to_raw      = _header_value(headers, "To")
        subject     = _header_value(headers, "Subject")
        date_raw    = _header_value(headers, "Date")
        in_reply_to = _header_value(headers, "In-Reply-To").strip()
        references  = _header_value(headers, "References").strip()

        from_name, from_email = _parse_from_header(from_raw)
        _, to_email           = _parse_from_header(to_raw)
        body_text             = _extract_plain_text(payload)

        now_utc = datetime.now(tz=timezone.utc).isoformat()

        return ReplyRecord(
            timestamp       = now_utc,
            gmail_message_id= msg_id,
            gmail_thread_id = thread_id,
            from_email      = from_email,
            from_name       = from_name,
            to_email        = to_email,
            subject         = subject,
            snippet         = snippet,
            body_text       = body_text[:2000],
            message_date    = date_raw,
            in_reply_to     = in_reply_to,
            references      = references,
        )
    except Exception as exc:
        print(f"[Workflow 7.8]   Error parsing message {msg.get('id', '?')}: {exc}")
        return None


# ---------------------------------------------------------------------------
# Thread lookup (Level 1a matching support)
# ---------------------------------------------------------------------------

def get_thread_outbound_message_ids(
    thread_id: str,
    service,
    our_email: str,
) -> list[str]:
    """
    Fetch all messages in a Gmail thread and return the Gmail internal IDs
    of messages sent FROM our email address (outbound sends).

    Used by reply_pipeline to enable Level 1a (thread_id) matching.
    Returns [] on any error — caller falls back to weaker match levels.
    """
    if not thread_id or not our_email:
        return []
    try:
        thread = service.users().threads().get(
            userId="me",
            id=thread_id,
            format="metadata",
            metadataHeaders=["From"],
        ).execute()
    except Exception as exc:
        print(f"[Workflow 7.8]   Thread fetch failed ({thread_id}): {exc}")
        return []

    our_email_lower = our_email.lower()
    outbound_ids = []
    for msg in thread.get("messages") or []:
        msg_headers = (msg.get("payload") or {}).get("headers") or []
        from_val    = _header_value(msg_headers, "From").lower()
        if our_email_lower in from_val:
            mid = msg.get("id", "")
            if mid:
                outbound_ids.append(mid)
    return outbound_ids


# ---------------------------------------------------------------------------
# Main fetch function
# ---------------------------------------------------------------------------

def fetch_recent_replies(
    hours_back: int = 72,
    max_results: int = 100,
    our_email: str = "",
    service=None,
) -> list:
    """
    Fetch recent inbound reply messages from Gmail INBOX.

    Filters applied:
    - in:inbox  (excludes SENT and other folders)
    - after:{date}  (only messages within the last `hours_back` hours)
    - -from:me  (excludes messages sent from our own account)

    Args:
        hours_back:   look-back window in hours (default 72)
        max_results:  cap on messages fetched from Gmail (default 100)
        our_email:    our Gmail address — used for self-send exclusion
        service:      pre-built Gmail API service (avoids re-authenticating
                      when called from pipeline that already has a service)

    Returns list of ReplyRecord with raw fields populated.
    Raises RuntimeError if Gmail auth fails.
    """
    if service is None:
        service = _get_gmail_read_service()

    cutoff     = datetime.now(tz=timezone.utc) - timedelta(hours=hours_back)
    after_date = cutoff.strftime("%Y/%m/%d")

    sender_filter = f"-from:{our_email}" if our_email else "-from:me"
    query = f"in:inbox {sender_filter} after:{after_date}"

    print(f"[Workflow 7.8] Fetching replies — query: {query!r}")

    try:
        resp = service.users().messages().list(
            userId="me",
            q=query,
            maxResults=min(max_results, 500),   # Gmail API hard cap
        ).execute()
    except Exception as exc:
        raise RuntimeError(f"Gmail messages.list failed: {exc}") from exc

    messages_meta = resp.get("messages") or []
    if not messages_meta:
        print("[Workflow 7.8]   No reply messages found in the window.")
        return []

    print(f"[Workflow 7.8]   Found {len(messages_meta)} messages — fetching details...")

    replies = []
    for meta in messages_meta:
        msg_id = meta.get("id", "")
        if not msg_id:
            continue
        try:
            msg    = service.users().messages().get(
                userId="me",
                id=msg_id,
                format="full",
            ).execute()
            record = _message_to_reply(msg)
            if record:
                replies.append(record)
        except Exception as exc:
            print(f"[Workflow 7.8]   Error fetching message {msg_id}: {exc}")
            continue   # one bad message doesn't stop the rest

    print(f"[Workflow 7.8]   Parsed {len(replies)} reply records.")
    return replies
