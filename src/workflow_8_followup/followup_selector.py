# Workflow 8: Follow-up Automation — Candidate Selector
# Identifies contacts due for follow-up by joining send logs with engagement data.

import csv
from datetime import datetime, timezone
from pathlib import Path

from config.settings import (
    SEND_LOGS_FILE, ENGAGEMENT_SUMMARY_FILE, FOLLOWUP_LOGS_FILE,
    FOLLOWUP_MAX_STAGE, SCORED_CONTACTS_FILE,
)

SENT_STATUSES = {"sent", "dry_run"}

CANDIDATE_FIELDS = [
    "company_name", "place_id", "kp_name", "kp_email",
    "subject", "tracking_id", "message_id",
    "last_send_time", "followup_stage",
    "open_count", "click_count", "engagement_status",
    "followup_reason",
    "followup_route", "original_contact_email", "original_contact_name",
]


def _parse_ts(raw: str) -> datetime | None:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.strip())
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _load_sent_logs(path: Path) -> list[dict]:
    """Return rows from send_logs.csv that have a successful send status."""
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return [
            r for r in csv.DictReader(f)
            if r.get("send_status", "") in SENT_STATUSES
        ]


def _latest_send_per_email(rows: list[dict]) -> dict[str, dict]:
    """Return {kp_email: latest_row} keeping most recent timestamp per contact."""
    latest: dict[str, dict] = {}
    for row in rows:
        email = (row.get("kp_email") or "").lower().strip()
        if not email:
            continue
        ts = _parse_ts(row.get("timestamp", ""))
        if ts is None:
            continue
        prev = latest.get(email)
        if prev is None:
            latest[email] = row
        else:
            prev_ts = _parse_ts(prev.get("timestamp", ""))
            if prev_ts is None or ts > prev_ts:
                latest[email] = row
    return latest


def _load_engagement_by_email(path: Path) -> dict[str, dict]:
    """
    Return {kp_email: aggregated_engagement}.
    If multiple tracking IDs exist for the same email, sums counts.
    """
    if not path.exists():
        return {}
    agg: dict[str, dict] = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            email = (row.get("kp_email") or "").lower().strip()
            if not email:
                continue
            opens  = int(row.get("open_count",  0) or 0)
            clicks = int(row.get("click_count", 0) or 0)
            if email not in agg:
                agg[email] = {
                    "open_count":  opens,
                    "click_count": clicks,
                    "tracking_id": row.get("tracking_id", ""),
                    "message_id":  row.get("message_id", ""),
                }
            else:
                agg[email]["open_count"]  += opens
                agg[email]["click_count"] += clicks
    return agg


def _load_prior_followup_stages(path: Path) -> dict[str, int]:
    """
    Return {kp_email: number_of_prior_followups_sent}.
    Reads followup_logs.csv and counts successful followup entries per email.
    """
    if not path.exists():
        return {}
    counts: dict[str, int] = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("decision", "") != "followup":
                continue
            email = (row.get("kp_email") or "").lower().strip()
            if email:
                counts[email] = counts.get(email, 0) + 1
    return counts


def _company_key(row: dict) -> str:
    place_id = (row.get("place_id") or "").strip()
    if place_id:
        return f"pid:{place_id}"
    name = (row.get("company_name") or "").strip().lower()
    return f"name:{name}" if name else ""


def _load_generic_fallback_index(path: Path = SCORED_CONTACTS_FILE) -> dict[str, dict]:
    """
    Return one usable generic mailbox per company from scored_contacts.csv.

    This supports the routing rule:
    initial send -> named contact
    no reply after the first delay -> generic mailbox fallback
    """
    if not path.exists():
        return {}

    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    index: dict[str, dict] = {}
    for row in rows:
        key = _company_key(row)
        if not key:
            continue
        email = (row.get("kp_email") or "").strip()
        if not email:
            continue
        is_generic = (row.get("is_generic_mailbox") or "").strip().lower() == "true"
        if not is_generic:
            continue
        sendable = (row.get("email_sendable") or "").strip().lower() == "true"
        eligibility = (row.get("send_eligibility") or "").strip().lower()
        if not sendable and eligibility not in {"allow", "allow_limited", "generic_pool_only"}:
            continue
        current = index.get(key)
        if current is None:
            index[key] = row
            continue
        try:
            current_rank = int(current.get("contact_priority_rank") or current.get("contact_rank") or 999)
        except (TypeError, ValueError):
            current_rank = 999
        try:
            new_rank = int(row.get("contact_priority_rank") or row.get("contact_rank") or 999)
        except (TypeError, ValueError):
            new_rank = 999
        if new_rank < current_rank:
            index[key] = row
    return index


def _stage_name(n: int) -> str:
    """0 prior followups → followup_1, 1 → followup_2, etc."""
    stage_num = n + 1
    return f"followup_{min(stage_num, FOLLOWUP_MAX_STAGE)}"


def select_candidates(
    send_logs_path: Path     = SEND_LOGS_FILE,
    engagement_path: Path    = ENGAGEMENT_SUMMARY_FILE,
    followup_logs_path: Path = FOLLOWUP_LOGS_FILE,
    campaign_id: str         = "",
) -> list[dict]:
    """
    Identify all contacts eligible for follow-up consideration.

    campaign_id: when non-empty, restricts candidates to contacts from that
    campaign only (campaign-scoped mode).  Pass "" for global CRM mode (all
    campaigns — used for debugging or explicit global follow-up runs).

    Prior follow-up stage count is always computed globally (across all
    campaigns) to prevent re-sending follow-ups to contacts already reached
    from a different campaign.

    Returns a list of candidate dicts (before stop-rule filtering).
    """
    sent_rows = _load_sent_logs(send_logs_path)
    if not sent_rows:
        print("[Workflow 8] No sent/dry_run records found in send_logs.csv.")
        return []

    # Campaign-scope filter: restrict to contacts from the specified campaign.
    # Prior-stage history remains global (prevents cross-campaign duplicate follow-ups).
    if campaign_id:
        campaign_rows = [
            r for r in sent_rows
            if (r.get("campaign_id") or "").strip() == campaign_id
        ]
        scope_label = f"campaign {campaign_id}"
        print(
            f"[Workflow 8] Selector scope: {scope_label} — "
            f"{len(campaign_rows)} of {len(sent_rows)} send_log rows match."
        )
        sent_rows = campaign_rows
    else:
        scope_label = "GLOBAL (all campaigns)"
        print(
            f"[Workflow 8] Selector scope: {scope_label} — "
            f"{len(sent_rows)} send_log rows."
        )

    if not sent_rows:
        print(f"[Workflow 8] No send_log rows found for scope: {scope_label}.")
        return []

    latest_per_email = _latest_send_per_email(sent_rows)
    engagement_map   = _load_engagement_by_email(engagement_path)
    prior_stages     = _load_prior_followup_stages(followup_logs_path)
    generic_fallback = _load_generic_fallback_index()

    candidates: list[dict] = []

    for email, send_row in latest_per_email.items():
        prior_count = prior_stages.get(email, 0)

        # Already maxed out
        if prior_count >= FOLLOWUP_MAX_STAGE:
            continue

        stage = _stage_name(prior_count)

        eng = engagement_map.get(email, {})
        open_count  = eng.get("open_count",  0)
        click_count = eng.get("click_count", 0)

        # Determine tracking_id: prefer engagement record, fall back to send log
        tracking_id = eng.get("tracking_id", "") or send_row.get("tracking_id", "")
        message_id  = eng.get("message_id",  "") or send_row.get("message_id",  "")

        target_name = send_row.get("kp_name", "")
        target_email = email
        route = "same_contact"
        reason = f"Stage {stage} candidate"

        company_key = _company_key(send_row)
        generic_row = generic_fallback.get(company_key, {}) if company_key else {}
        should_use_generic_fallback = (
            prior_count == 0
            and (send_row.get("send_target_type") or "").strip().lower() == "named"
            and generic_row
            and (generic_row.get("kp_email") or "").strip().lower() != email
        )
        if should_use_generic_fallback:
            target_name = generic_row.get("kp_name", "")
            target_email = (generic_row.get("kp_email") or "").strip().lower()
            route = "generic_fallback_after_named"
            reason = "Stage followup_1 generic fallback after initial named contact"

        candidates.append({
            "company_name":     send_row.get("company_name", ""),
            "place_id":         send_row.get("place_id", ""),
            "kp_name":          target_name,
            "kp_email":         target_email,
            "subject":          send_row.get("subject", ""),
            "tracking_id":      tracking_id,
            "message_id":       message_id,
            "last_send_time":   send_row.get("timestamp", ""),
            "followup_stage":   stage,
            "open_count":       open_count,
            "click_count":      click_count,
            "engagement_status": "",    # filled by stop_rules
            "followup_reason":  reason,
            "followup_route":   route,
            "original_contact_email": email,
            "original_contact_name": send_row.get("kp_name", ""),
        })

    # dry_run contacts are eligible for follow-up by design — they were in-scope but not SMTP-sent.
    print(
        f"[Workflow 8] Selector: {len(candidates)} candidates from "
        f"{len(latest_per_email)} unique contacts — scope: {scope_label}."
    )
    return candidates
