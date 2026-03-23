# Workflow 7: Email Sending - Send Guard
# Enforces all safety rules before any email is dispatched.
#
# Guard check order:
#   required_fields -> email_format -> approval_status ->
#   email_eligibility (Ticket 3 E0) ->
#   global_breaker -> domain_breaker -> sender_breaker -> campaign_breaker ->
#   [business_hours - skipped in dry_run] ->
#   duplicate -> company_throttle

import json
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from config.settings import (
    CAMPAIGN_RUN_STATE_FILE,
    SEND_WINDOW_END,
    SEND_WINDOW_SLOTS,
    SEND_WINDOW_START,
)

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

APPROVED_STATUSES = {"approved", "approved_after_repair"}
DEDUP_WINDOW_HOURS = 24
CONTACT_SUPPRESS_HOURS = 72

_DEFAULT_BUSINESS_DAYS = {0, 1, 2, 3, 4}  # Monday-Friday
_SAUDI_BUSINESS_DAYS = {6, 0, 1, 2, 3}    # Sunday-Thursday

_COUNTRY_TIMEZONES = {
    "brazil": "America/Sao_Paulo",
    "saudi arabia": "Asia/Riyadh",
    "ksa": "Asia/Riyadh",
    "united arab emirates": "Asia/Dubai",
    "uae": "Asia/Dubai",
    "qatar": "Asia/Qatar",
    "kuwait": "Asia/Kuwait",
    "oman": "Asia/Muscat",
    "bahrain": "Asia/Bahrain",
    "egypt": "Africa/Cairo",
    "jordan": "Asia/Amman",
}

_CITY_TIMEZONES = {
    ("brazil", "belo horizonte"): "America/Sao_Paulo",
    ("brazil", "contagem"): "America/Sao_Paulo",
    ("brazil", "uberlandia"): "America/Sao_Paulo",
    ("brazil", "uberaba"): "America/Sao_Paulo",
    ("brazil", "juiz de fora"): "America/Sao_Paulo",
    ("brazil", "betim"): "America/Sao_Paulo",
    ("brazil", "montes claros"): "America/Sao_Paulo",
    ("saudi arabia", "riyadh"): "Asia/Riyadh",
    ("saudi arabia", "jeddah"): "Asia/Riyadh",
    ("saudi arabia", "dammam"): "Asia/Riyadh",
    ("saudi arabia", "khobar"): "Asia/Riyadh",
    ("saudi arabia", "al khobar"): "Asia/Riyadh",
    ("saudi arabia", "mecca"): "Asia/Riyadh",
    ("saudi arabia", "makkah"): "Asia/Riyadh",
    ("saudi arabia", "medina"): "Asia/Riyadh",
}

_COUNTRY_BUSINESS_DAYS = {
    "saudi arabia": _SAUDI_BUSINESS_DAYS,
    "ksa": _SAUDI_BUSINESS_DAYS,
}


def _normalize_text(value: str) -> str:
    return (value or "").strip().lower()


def _root_domain(email_or_website: str) -> str:
    """Return the root domain (last two labels) of an email address or URL."""
    value = _normalize_text(email_or_website)
    if not value:
        return ""
    for prefix in ("https://", "http://", "www."):
        if value.startswith(prefix):
            value = value[len(prefix):]
    if "@" in value:
        value = value.split("@", 1)[1]
    value = value.split("/")[0].strip()
    if not value:
        return ""
    parts = value.split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else value


def _normalize_company(name: str) -> str:
    """Strip bilingual suffixes and lowercase for matching."""
    value = (name or "").strip()
    if "|" in value:
        value = value.split("|")[0].strip()
    return value.lower()


def _parse_window_slots() -> list[tuple[int, int]]:
    slots: list[tuple[int, int]] = []
    for raw in (SEND_WINDOW_SLOTS or "").split(","):
        piece = raw.strip()
        if not piece or "-" not in piece:
            continue
        start_raw, end_raw = [part.strip() for part in piece.split("-", 1)]
        try:
            start_hour = int(start_raw.split(":", 1)[0])
            end_hour = int(end_raw.split(":", 1)[0])
        except ValueError:
            continue
        if 0 <= start_hour < end_hour <= 24:
            slots.append((start_hour, end_hour))
    if slots:
        return slots
    return [(SEND_WINDOW_START, SEND_WINDOW_END)]


_WINDOW_SLOTS = _parse_window_slots()


def _location_from_source(source_location: str) -> tuple[str, str]:
    parts = [part.strip() for part in (source_location or "").split(",") if part.strip()]
    if not parts:
        return "", ""
    city = parts[0]
    country = parts[-1] if len(parts) >= 2 else ""
    return city, country


def _load_campaign_state(campaign_id: str = "") -> dict:
    if not CAMPAIGN_RUN_STATE_FILE.exists():
        return {}
    try:
        with open(CAMPAIGN_RUN_STATE_FILE, encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        return {}
    saved_campaign_id = (state.get("campaign_id") or "").strip()
    if campaign_id and saved_campaign_id and campaign_id != saved_campaign_id:
        return {}
    return state


def _resolve_location(record: dict, campaign_id: str = "") -> tuple[str, str]:
    city = (record.get("city") or "").strip()
    country = (record.get("country") or "").strip()
    if city and country:
        return city, country

    src_city, src_country = _location_from_source(record.get("source_location", ""))
    city = city or src_city
    country = country or src_country
    if city and country:
        return city, country

    state = _load_campaign_state(campaign_id=campaign_id)
    if state:
        city = city or (state.get("base_city") or state.get("city") or "").strip()
        country = country or (state.get("country") or "").strip()
    return city, country


def _resolve_timezone(record: dict, campaign_id: str = "") -> tuple[str, str, str]:
    city, country = _resolve_location(record, campaign_id=campaign_id)
    city_key = _normalize_text(city)
    country_key = _normalize_text(country)

    tz_name = ""
    if city_key and country_key:
        tz_name = _CITY_TIMEZONES.get((country_key, city_key), "")
    if not tz_name and country_key:
        tz_name = _COUNTRY_TIMEZONES.get(country_key, "")
    if not tz_name:
        tz_name = "UTC"
    return city, country, tz_name


def _business_days_for_country(country: str) -> set[int]:
    return _COUNTRY_BUSINESS_DAYS.get(_normalize_text(country), _DEFAULT_BUSINESS_DAYS)


def _format_window_slots() -> str:
    return ", ".join(f"{start:02d}:00-{end:02d}:00" for start, end in _WINDOW_SLOTS)


def get_target_market_context(record: dict, campaign_id: str = "") -> dict:
    """Return resolved market context for send-window decisions."""
    city, country, tz_name = _resolve_timezone(record, campaign_id=campaign_id)
    return {
        "city": city,
        "country": country,
        "timezone": tz_name,
        "business_days": sorted(_business_days_for_country(country)),
        "window_slots": list(_WINDOW_SLOTS),
    }


def next_eligible_send_time(
    record: dict,
    now: datetime | None = None,
    campaign_id: str = "",
) -> datetime:
    """
    Return the next UTC datetime when this record becomes send-eligible.

    If already inside an allowed market-local window, returns `now`.
    """
    now_utc = now or datetime.now(tz=timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)

    city, country, tz_name = _resolve_timezone(record, campaign_id=campaign_id)
    local_tz = ZoneInfo(tz_name)
    local_now = now_utc.astimezone(local_tz)
    business_days = _business_days_for_country(country)

    for day_offset in range(0, 14):
        if day_offset == 0:
            candidate_date = local_now.date()
        else:
            candidate_date = local_now.date().fromordinal(local_now.date().toordinal() + day_offset)

        candidate_midnight = datetime(
            candidate_date.year,
            candidate_date.month,
            candidate_date.day,
            tzinfo=local_tz,
        )
        if candidate_midnight.weekday() not in business_days:
            continue

        for start_hour, end_hour in _WINDOW_SLOTS:
            slot_start = candidate_midnight.replace(hour=start_hour, minute=0, second=0, microsecond=0)
            slot_end = candidate_midnight.replace(hour=end_hour, minute=0, second=0, microsecond=0)
            if day_offset == 0 and slot_start <= local_now < slot_end:
                return now_utc
            if slot_start > local_now:
                return slot_start.astimezone(timezone.utc)

    return now_utc


_BREAKER_PREFIXES = (
    "blocked_e0_email",
    "blocked_global_breaker",
    "blocked_domain_breaker",
    "blocked_sender_breaker",
    "blocked_campaign_breaker",
)


def is_breaker_block(reason: str) -> bool:
    """Return True if a guard block was caused by a deliverability breaker or E0 eligibility."""
    return any(reason.startswith(prefix) for prefix in _BREAKER_PREFIXES)


def _allow(reason: str = "") -> dict:
    return {"allowed": True, "decision": "send", "reason": reason}


def _block(reason: str) -> dict:
    return {"allowed": False, "decision": "blocked", "reason": reason}


def _defer(reason: str) -> dict:
    return {"allowed": False, "decision": "deferred", "reason": reason}


def _review(reason: str) -> dict:
    return {"allowed": False, "decision": "review_required", "reason": reason}


def check_reply_suppression(record: dict, reply_index: dict | None = None) -> dict | None:
    """
    Block/defer sends for contacts already suppressed by inbound reply handling.

    This closes the loop for bounce / unsubscribe / hard-no events so a rerun of
    the send pipeline does not target the same address again.
    """
    email = (record.get("kp_email") or "").strip().lower()
    if not email:
        return None

    if reply_index is None:
        try:
            from src.workflow_8_followup.followup_stop_rules import load_reply_suppression_index
            reply_index = load_reply_suppression_index()
        except Exception:
            reply_index = {}

    entry = (reply_index or {}).get(email)
    if not entry:
        return None

    sup = (entry.get("suppression_status") or "").strip()
    rtype = (entry.get("reply_type") or "").strip()
    if sup in {"suppressed", "handoff_to_human"}:
        return _block(f"blocked_reply_suppression: suppression_status={sup!r} reply_type={rtype!r}")
    if sup == "paused":
        return _defer(f"deferred_reply_suppression: suppression_status={sup!r} reply_type={rtype!r}")
    if rtype in {"bounce", "unsubscribe", "hard_no"}:
        return _block(f"blocked_reply_suppression: reply_type={rtype!r}")
    return None


def check_required_fields(record: dict) -> dict | None:
    for field in ("kp_email", "subject", "email_body"):
        if not record.get(field, "").strip():
            return _block(f"Missing required field: {field}")
    return None


def check_email_format(record: dict) -> dict | None:
    email = record.get("kp_email", "").strip()
    if not _EMAIL_RE.match(email):
        return _block(f"Malformed email address: {email!r}")
    return None


def check_approval_status(record: dict) -> dict | None:
    status = record.get("approval_status", "").strip()
    if status not in APPROVED_STATUSES:
        return _block(f"Approval status not sendable: {status!r}")
    return None


def check_business_hours(
    record: dict,
    now: datetime | None = None,
    campaign_id: str = "",
) -> dict | None:
    now_utc = now or datetime.now(tz=timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)

    city, country, tz_name = _resolve_timezone(record, campaign_id=campaign_id)
    local_now = now_utc.astimezone(ZoneInfo(tz_name))
    business_days = _business_days_for_country(country)

    if local_now.weekday() not in business_days:
        return _defer(
            f"Outside business days for {city or country or tz_name} "
            f"({local_now.strftime('%A')} {tz_name})"
        )

    for start_hour, end_hour in _WINDOW_SLOTS:
        if start_hour <= local_now.hour < end_hour:
            return None

    return _defer(
        f"Outside target-market send window for {city or country or tz_name} "
        f"({local_now.strftime('%H:%M')} {tz_name} not in {_format_window_slots()})"
    )


def check_duplicate(record: dict, recent_logs: list[dict]) -> dict | None:
    """Defer if the same email+subject was sent or dry-run within 24 hours."""
    email = record.get("kp_email", "").lower().strip()
    subject = record.get("subject", "").lower().strip()
    for log in recent_logs:
        log_email = (log.get("kp_email") or "").lower().strip()
        log_subject = (log.get("subject") or "").lower().strip()
        log_status = log.get("send_status", "")
        if log_status in {"sent", "dry_run"} and log_email == email and log_subject == subject:
            return _defer("deferred_duplicate_email_subject_within_24h")
    return None


def check_company_throttle(
    record: dict,
    recent_logs: list[dict],
    send_mode: str = "",
) -> dict | None:
    """
    Defer if the same contact or company was already reached recently.

    Matching priority:
    1. same contact email
    2. same company place_id
    3. same root domain
    4. same normalized company name

    Dry-run is intentionally looser:
    - still suppresses same contact and same place_id
    - skips domain/name-wide suppression so operators can preview fresh
      first-touch capacity without historical cross-run overlap dominating
      the queue size

    There is no manual-review branch here: recent-contact suppression is an
    automatic defer/skip decision.
    """
    suppress_statuses = {"sent", "dry_run", "deferred"}
    relaxed_dry_run = (send_mode or "").strip().lower() == "dry_run"

    email = (record.get("kp_email") or "").lower().strip()
    place_id = (record.get("place_id") or "").strip()
    company_name = _normalize_company(record.get("company_name") or "")
    domain = _root_domain(email) or _root_domain(record.get("website") or "")

    for log in recent_logs:
        if log.get("send_status", "") not in suppress_statuses:
            continue

        log_email = (log.get("kp_email") or "").lower().strip()
        log_place = (log.get("place_id") or "").strip()
        log_company = _normalize_company(log.get("company_name") or "")
        log_domain = _root_domain(log_email)

        if email and log_email and email == log_email:
            return _defer("deferred_same_contact_in_suppress_window")
        if place_id and log_place and place_id == log_place:
            return _defer("deferred_same_company_place_id_in_suppress_window")
        if not relaxed_dry_run:
            if domain and log_domain and domain == log_domain:
                return _defer("deferred_same_company_domain_in_suppress_window")
            if company_name and log_company and company_name == log_company:
                return _defer("deferred_same_company_name_in_suppress_window")
    return None


def check_email_eligibility(record: dict) -> dict | None:
    """
    Block if send_eligibility == 'block' (Ticket 3 E0 contacts).
    """
    eligibility = record.get("send_eligibility", "").strip()
    if eligibility == "block":
        tier = record.get("email_confidence_tier", "E0")
        return _block(f"blocked_e0_email: send_eligibility=block tier={tier}")
    return None


def check_global_breaker(conn) -> dict | None:
    if conn is None:
        return None
    try:
        from src.workflow_7_4_deliverability.breaker_state import get_global_breaker

        active, reason = get_global_breaker(conn)
        if active:
            return _block(f"blocked_global_breaker: {reason}")
    except Exception:
        pass
    return None


def check_domain_breaker(conn, sending_domain: str) -> dict | None:
    if conn is None or not sending_domain:
        return None
    try:
        from src.workflow_7_4_deliverability.breaker_state import get_domain_breaker

        active, reason = get_domain_breaker(conn, sending_domain)
        if active:
            return _block(f"blocked_domain_breaker: {reason}")
    except Exception:
        pass
    return None


def check_sender_breaker(conn, sender_email: str) -> dict | None:
    if conn is None or not sender_email:
        return None
    try:
        from src.workflow_7_4_deliverability.breaker_state import get_sender_breaker

        active, reason = get_sender_breaker(conn, sender_email)
        if active:
            return _block(f"blocked_sender_breaker: {reason}")
    except Exception:
        pass
    return None


def check_campaign_breaker(conn, campaign_id: str) -> dict | None:
    if conn is None or not campaign_id:
        return None
    try:
        from src.workflow_7_4_deliverability.breaker_state import get_campaign_breaker

        active, reason = get_campaign_breaker(conn, campaign_id)
        if active:
            return _block(f"blocked_campaign_breaker: {reason}")
    except Exception:
        pass
    return None


def run_checks(
    record: dict,
    recent_logs: list[dict],
    now: datetime | None = None,
    send_mode: str = "",
    conn=None,
    campaign_id: str = "",
    reply_index: dict | None = None,
) -> dict:
    """
    Run all guards in order.

    In dry_run mode the business-hours check is skipped, and company-throttle
    only enforces exact-contact / exact-place suppression.
    """
    from config.settings import SMTP_FROM_EMAIL

    sender_email = SMTP_FROM_EMAIL.strip().lower()
    sender_domain = sender_email.split("@", 1)[1] if "@" in sender_email else ""

    checks = [
        lambda: check_required_fields(record),
        lambda: check_email_format(record),
        lambda: check_approval_status(record),
        lambda: check_email_eligibility(record),
        lambda: check_global_breaker(conn),
        lambda: check_domain_breaker(conn, sender_domain),
        lambda: check_sender_breaker(conn, sender_email),
        lambda: check_campaign_breaker(conn, campaign_id),
        lambda: check_reply_suppression(record, reply_index=reply_index),
        lambda: check_duplicate(record, recent_logs),
        lambda: check_company_throttle(record, recent_logs, send_mode=send_mode),
    ]
    if send_mode != "dry_run":
        checks.insert(8, lambda: check_business_hours(record, now, campaign_id=campaign_id))

    for check in checks:
        result = check()
        if result is not None:
            return result

    return _allow("All checks passed")
