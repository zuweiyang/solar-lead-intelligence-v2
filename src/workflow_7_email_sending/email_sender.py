# Workflow 7: Email Sending — Email Sender
# Sends one email per call. Supports dry_run, smtp, and gmail_api modes.
# Provider extensibility: add SendGrid / Mailgun in future by adding a new _send_* function.

import base64
import random
import smtplib
import time
import uuid
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from config.settings import (
    EMAIL_SEND_MODE,
    SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD,
    SMTP_USE_TLS, SMTP_FROM_EMAIL, SMTP_FROM_NAME,
    REPLY_TO_EMAIL,
    GMAIL_CLIENT_SECRET_FILE, GMAIL_TOKEN_FILE,
    GMAIL_API_MIN_SEND_INTERVAL_SECONDS,
    GMAIL_API_MAX_RETRIES,
    GMAIL_API_BACKOFF_BASE_SECONDS,
    GMAIL_API_BACKOFF_MAX_SECONDS,
    GMAIL_API_ENABLE_JITTER,
    SEND_PACING_MIN_SECONDS,
    SEND_PACING_MAX_SECONDS,
    SEND_HOURLY_LIMIT,
)

# Module-level pacing state — tracks when the last real send completed.
_last_real_send_time: float = 0.0

# ---------------------------------------------------------------------------
# Result builder helpers
# ---------------------------------------------------------------------------

def _result(send_status: str, provider: str,
            message_id: str = "", error: str = "") -> dict:
    return {
        "send_status":         send_status,
        "provider":            provider,
        "provider_message_id": message_id,
        "error_message":       error,
    }


def _target_real_send_gap(mode: str) -> float:
    """
    Return a randomized gap between real sends.

    The hourly cap defines the minimum average spacing, while the configured
    pacing range adds human-like variation so sends do not land on a rigid cadence.
    """
    hourly_floor = (3600.0 / SEND_HOURLY_LIMIT) if SEND_HOURLY_LIMIT > 0 else 0.0
    provider_floor = GMAIL_API_MIN_SEND_INTERVAL_SECONDS if mode == "gmail_api" else 0.0
    min_gap = max(SEND_PACING_MIN_SECONDS, hourly_floor, provider_floor)
    max_gap = max(SEND_PACING_MAX_SECONDS, min_gap)
    if max_gap <= min_gap:
        return min_gap
    return random.uniform(min_gap, max_gap)


def _apply_real_send_pacing(mode: str) -> None:
    """Sleep before a real send if the previous send was too recent."""
    global _last_real_send_time

    if mode not in {"smtp", "gmail_api"}:
        return

    target_gap = _target_real_send_gap(mode)
    elapsed = time.monotonic() - _last_real_send_time
    if elapsed >= target_gap:
        return

    wait = target_gap - elapsed
    print(
        f"[Send Pacing] Waiting {wait:.1f}s before next {mode} send "
        f"(target gap {target_gap:.1f}s)"
    )
    time.sleep(wait)


def _mark_real_send_completed() -> None:
    global _last_real_send_time
    _last_real_send_time = time.monotonic()


# ---------------------------------------------------------------------------
# Dry-run mode
# ---------------------------------------------------------------------------

def _send_dry_run(record: dict) -> dict:
    """Simulate a successful send without touching SMTP."""
    fake_id = f"dry-{uuid.uuid4().hex[:12]}"
    print(
        f"[Workflow 7]   [DRY-RUN] Would send → "
        f"{record.get('kp_email')} | {record.get('subject', '')[:50]}"
    )
    return _result("dry_run", "dry_run", message_id=fake_id)


# ---------------------------------------------------------------------------
# SMTP mode
# ---------------------------------------------------------------------------

def _build_mime(record: dict) -> MIMEMultipart:
    to_addr = record.get("kp_email", "")
    subject = record.get("subject", "")
    body    = record.get("email_body", "")

    from_header = (
        f"{SMTP_FROM_NAME} <{SMTP_FROM_EMAIL}>"
        if SMTP_FROM_NAME else SMTP_FROM_EMAIL
    )

    msg = MIMEMultipart("alternative")
    msg["From"]    = from_header
    msg["To"]      = to_addr
    msg["Subject"] = subject
    if REPLY_TO_EMAIL:
        msg["Reply-To"] = REPLY_TO_EMAIL
    msg.attach(MIMEText(body, "plain", "utf-8"))
    html_body = record.get("html_body", "")
    if html_body:
        msg.attach(MIMEText(html_body, "html", "utf-8"))
    return msg


def _send_smtp(record: dict) -> dict:
    to_addr = record.get("kp_email", "")
    msg     = _build_mime(record)
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            if SMTP_USE_TLS:
                server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(SMTP_FROM_EMAIL, to_addr, msg.as_string())
        _mark_real_send_completed()
        # smtplib doesn't return message IDs — generate a local reference
        local_id = f"smtp-{uuid.uuid4().hex[:12]}"
        return _result("sent", "smtp", message_id=local_id)
    except Exception as exc:
        return _result("failed", "smtp", error=str(exc))


# ---------------------------------------------------------------------------
# Gmail API mode — helpers
# ---------------------------------------------------------------------------

def _get_gmail_service():
    """Load (and refresh if needed) OAuth2 credentials, return Gmail service."""
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build

    creds = None
    if GMAIL_TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(
            str(GMAIL_TOKEN_FILE),
            scopes=["https://www.googleapis.com/auth/gmail.send"],
        )

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(str(GMAIL_TOKEN_FILE), "w") as f:
                f.write(creds.to_json())
        else:
            raise RuntimeError(
                "Gmail API token missing or invalid. "
                "Run scripts/authorize_gmail.py to obtain a token."
            )

    return build("gmail", "v1", credentials=creds)


def _is_retryable_gmail_exc(exc: Exception) -> bool:
    """
    Return True if the exception represents a transient Gmail API failure.

    Retryable:
      - HTTP 429 (Too Many Requests)
      - HTTP 500, 502, 503, 504 (server-side errors)
      - HTTP 403 with reason rateLimitExceeded / userRateLimitExceeded
      - Socket / transport errors (no HTTP status)

    Non-retryable:
      - HTTP 400 (bad request — bad MIME, bad recipient, etc.)
      - HTTP 401 (authentication failure)
      - HTTP 403 without a rate-limit reason (e.g. permission denied, policy block)
    """
    try:
        from googleapiclient.errors import HttpError
        if isinstance(exc, HttpError):
            status = exc.resp.status
            if status in (429, 500, 502, 503, 504):
                return True
            if status == 403:
                # Check for rate-limit sub-reason in the error body
                body = exc.error_details or []
                for detail in body:
                    reason = detail.get("reason", "")
                    if reason in ("rateLimitExceeded", "userRateLimitExceeded"):
                        return True
                return False  # 403 for auth/policy — non-retryable
            return False  # 400, 401, other 4xx — non-retryable
    except ImportError:
        pass
    # Network/socket errors (no HTTP status) — retryable
    exc_str = str(type(exc).__name__).lower()
    if any(t in exc_str for t in ("timeout", "connection", "socket", "transport")):
        return True
    return False


def _backoff_seconds(attempt: int) -> float:
    """
    Exponential backoff: min(base * 2^attempt + jitter, cap).
    attempt is 0-indexed (first retry = attempt 0).
    """
    wait = GMAIL_API_BACKOFF_BASE_SECONDS * (2 ** attempt)
    if GMAIL_API_ENABLE_JITTER:
        wait += random.uniform(0, GMAIL_API_BACKOFF_BASE_SECONDS)
    return min(wait, GMAIL_API_BACKOFF_MAX_SECONDS)


# ---------------------------------------------------------------------------
# Gmail API mode — sender with pacing + retry
# ---------------------------------------------------------------------------

def _send_gmail_api(record: dict) -> dict:
    to_addr = record.get("kp_email", "")
    msg     = _build_mime(record)

    raw_bytes = base64.urlsafe_b64encode(msg.as_bytes()).decode()

    last_exc: Exception = RuntimeError("No attempt made")
    for attempt in range(GMAIL_API_MAX_RETRIES + 1):
        try:
            service = _get_gmail_service()
            sent = (
                service.users()
                .messages()
                .send(userId="me", body={"raw": raw_bytes})
                .execute()
            )
            _mark_real_send_completed()
            message_id = sent.get("id", f"gmail-{uuid.uuid4().hex[:12]}")
            if attempt > 0:
                print(f"[Gmail API]   Succeeded on attempt {attempt + 1}")
            return _result("sent", "gmail_api", message_id=message_id)

        except Exception as exc:
            last_exc = exc
            if not _is_retryable_gmail_exc(exc):
                print(f"[Gmail API]   Non-retryable error: {exc}")
                break
            if attempt < GMAIL_API_MAX_RETRIES:
                wait = _backoff_seconds(attempt)
                print(
                    f"[Gmail API]   Retryable error (attempt {attempt + 1}/{GMAIL_API_MAX_RETRIES})"
                    f" — backoff {wait:.1f}s: {exc}"
                )
                time.sleep(wait)
            else:
                print(f"[Gmail API]   Max retries ({GMAIL_API_MAX_RETRIES}) exhausted: {exc}")

    return _result("failed", "gmail_api", error=str(last_exc))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def send_one(record: dict, mode: str = "") -> dict:
    """
    Send a single email. Returns a structured result dict.
    Never raises — catches all exceptions internally.

    mode: explicit send mode ("dry_run", "smtp", "gmail_api").
    Defaults to EMAIL_SEND_MODE from config if not provided.
    """
    resolved_mode = (mode or EMAIL_SEND_MODE).lower().strip()
    try:
        if resolved_mode == "dry_run":
            return _send_dry_run(record)
        elif resolved_mode == "smtp":
            _apply_real_send_pacing("smtp")
            return _send_smtp(record)
        elif resolved_mode == "gmail_api":
            _apply_real_send_pacing("gmail_api")
            return _send_gmail_api(record)
        else:
            return _result(
                "failed", resolved_mode,
                error=f"Unknown EMAIL_SEND_MODE: {resolved_mode!r}"
            )
    except Exception as exc:
        return _result("failed", resolved_mode, error=f"Unexpected error: {exc}")
