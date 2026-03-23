# Workflow 7.5: Engagement Tracking — Tracking ID Manager
# Generates unique, URL-safe tracking IDs for sent emails.

import hashlib
import uuid
from datetime import datetime, timezone


def _ts() -> str:
    """Compact UTC timestamp: 20260315T103501"""
    return datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")


def generate_tracking_id(record: dict) -> str:
    """
    Generate a unique, URL-safe tracking ID for one email send.

    Format: {place_id_or_prefix}_{timestamp}_{random4}
    Example: ChIJ123_20260315T103501_a8f2
    """
    prefix = (record.get("place_id") or "email").replace(" ", "_")[:16]
    prefix = "".join(c if c.isalnum() or c in "_-" else "" for c in prefix)
    prefix = prefix or "email"
    suffix = uuid.uuid4().hex[:4]
    return f"{prefix}_{_ts()}_{suffix}"


def generate_message_id(record: dict) -> str:
    """
    Generate a stable message ID for log correlation.

    Uses SHA-1 of kp_email + subject + timestamp.
    Format: msg_{hex12}
    """
    key = (
        (record.get("kp_email") or "")
        + (record.get("subject") or "")
        + _ts()
    )
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
    return f"msg_{digest}"
