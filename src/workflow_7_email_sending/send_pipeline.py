# Workflow 7: Email Sending - Send Pipeline
# Orchestrates: load -> queue policy -> guard -> send -> log -> summarise.

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from config.settings import (
    DAILY_EMAIL_LIMIT,
    EMAIL_SEND_MODE,
    MANUAL_REVIEW_QUEUE_FILE,
    QUEUE_POLICY_FILE,
    SEND_HOURLY_LIMIT,
    SEND_BATCH_SUMMARY,
    TRACKING_BASE_URL,
)
from src.workflow_6_queue_policy.queue_policy_models import (
    POLICY_BLOCK,
    POLICY_GENERIC_ONLY,
    POLICY_HOLD,
    POLICY_QUEUE_LIMITED,
    POLICY_QUEUE_NORMAL,
)
from src.workflow_7_email_sending.email_sender import send_one
from src.workflow_7_email_sending.send_guard import (
    CONTACT_SUPPRESS_HOURS,
    is_breaker_block,
    run_checks,
)
from src.workflow_7_email_sending.send_loader import load_send_queue
from src.workflow_7_email_sending.send_logger import (
    append_send_log,
    build_log_row,
    load_recent_logs,
)

try:
    from src.workflow_7_5_engagement_tracking.email_tracking_injector import (
        prepare_tracked_email,
    )
    from src.workflow_7_5_engagement_tracking.tracking_id_manager import (
        generate_message_id,
        generate_tracking_id,
    )

    _TRACKING_AVAILABLE = True
except ImportError:
    _TRACKING_AVAILABLE = False


_MANUAL_REVIEW_FIELDS = [
    "campaign_id",
    "company_name",
    "website",
    "place_id",
    "kp_name",
    "kp_title",
    "kp_email",
    "contact_name",
    "contact_title",
    "contact_email",
    "send_target_type",
    "contact_source",
    "contact_quality",
    "company_type",
    "market_focus",
    "lead_score",
    "subject",
    "opening_line",
    "email_body",
    "approval_status",
    "overall_score",
    "send_policy_action",
    "send_policy_reason",
    "review_reason",
    "review_tags",
]


def _count_hourly_send_slots(rows: list[dict], effective_mode: str) -> int:
    """
    Count sends already consumed inside the last hour.

    Real sending modes only count true sent rows; dry-run mode counts dry-run
    rows so preview runs still simulate pacing safely.
    """
    success_statuses = {"dry_run"} if effective_mode == "dry_run" else {"sent"}
    return sum(1 for row in rows if row.get("send_status") in success_statuses)


def _append_manual_review_row(
    record: dict,
    campaign_id: str,
    review_reason: str,
    policy_action: str,
    policy_reason: str,
) -> None:
    """Append one row to the per-run manual review queue."""
    MANUAL_REVIEW_QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
    file_exists = MANUAL_REVIEW_QUEUE_FILE.exists()
    row = {
        "campaign_id": campaign_id,
        "company_name": record.get("company_name", ""),
        "website": record.get("website", ""),
        "place_id": record.get("place_id", ""),
        "kp_name": record.get("kp_name", ""),
        "kp_title": record.get("kp_title", ""),
        "kp_email": record.get("kp_email", ""),
        "contact_name": record.get("contact_name", record.get("kp_name", "")),
        "contact_title": record.get("contact_title", record.get("kp_title", "")),
        "contact_email": record.get("contact_email", record.get("kp_email", "")),
        "send_target_type": record.get("send_target_type", ""),
        "contact_source": record.get("contact_source", ""),
        "contact_quality": record.get("contact_quality", ""),
        "company_type": record.get("company_type", ""),
        "market_focus": record.get("market_focus", ""),
        "lead_score": record.get("lead_score", ""),
        "subject": record.get("subject", ""),
        "opening_line": record.get("opening_line", ""),
        "email_body": record.get("email_body", ""),
        "approval_status": record.get("approval_status", ""),
        "overall_score": record.get("overall_score", ""),
        "send_policy_action": policy_action,
        "send_policy_reason": policy_reason,
        "review_reason": review_reason,
        "review_tags": "|".join(
            part for part in [policy_action or "", review_reason or ""] if part
        ),
    }
    with open(MANUAL_REVIEW_QUEUE_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=_MANUAL_REVIEW_FIELDS,
            extrasaction="ignore",
        )
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def _load_policy_indices(policy_path) -> tuple[dict, dict, dict, bool]:
    """
    Load queue_policy.csv and return (by_place_id, by_email, by_company, file_found).
    """
    path = Path(str(policy_path))
    if not path.exists():
        return {}, {}, {}, False
    try:
        with open(path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
    except Exception as exc:
        print(f"[Workflow 7] Could not read queue_policy.csv: {exc}")
        return {}, {}, {}, True

    by_place_id: dict[str, dict] = {}
    by_email: dict[str, dict] = {}
    by_company: dict[str, dict] = {}
    for row in rows:
        pid = (row.get("place_id") or "").strip()
        if pid:
            by_place_id[pid] = row
        email = (row.get("selected_contact_email") or "").strip().lower()
        if email:
            by_email[email] = row
        cname = (row.get("company_name") or "").strip().lower()
        if cname:
            by_company[cname] = row
    return by_place_id, by_email, by_company, True


def _lookup_policy(
    record: dict,
    by_place_id: dict,
    by_email: dict,
    by_company: dict | None = None,
) -> dict | None:
    """Return the policy row for a send-queue record, or None if not found."""
    pid = (record.get("place_id") or "").strip()
    if pid and pid in by_place_id:
        return by_place_id[pid]
    email = (record.get("kp_email") or "").strip().lower()
    if email and email in by_email:
        return by_email[email]
    if by_company:
        cname = (record.get("company_name") or "").strip().lower()
        if cname and cname in by_company:
            return by_company[cname]
    return None


def run(
    limit: int = 0,
    campaign_id: str = "",
    send_mode: str = "",
    daily_limit_override: int | None = None,
    hourly_limit_override: int | None = None,
) -> dict:
    """
    Run one send batch.

    Limit overrides let the cloud worker respect the remaining inbox capacity
    for the current day/hour instead of only the static process defaults.
    """
    records = load_send_queue(limit=limit)
    if not records:
        print("[Workflow 7] No records to process.")
        return _empty_summary()

    conn = None
    try:
        from src.database.db_connection import get_db_connection

        conn = get_db_connection()
    except Exception as exc:
        print(f"[Workflow 7] DB unavailable for breaker checks (non-fatal): {exc}")

    effective_mode = send_mode or EMAIL_SEND_MODE
    effective_daily_limit = (
        int(daily_limit_override)
        if daily_limit_override is not None and int(daily_limit_override) > 0
        else DAILY_EMAIL_LIMIT
    )
    effective_hourly_limit = (
        int(hourly_limit_override)
        if hourly_limit_override is not None and int(hourly_limit_override) > 0
        else SEND_HOURLY_LIMIT
    )
    mode_str = effective_mode.upper()
    print(
        f"[Workflow 7] Starting send batch - mode: {mode_str} | "
        f"records: {len(records)} | daily limit: {effective_daily_limit} | "
        f"hourly limit: {effective_hourly_limit} | "
        f"campaign: {campaign_id or 'unknown'}"
    )

    by_place_id, by_email, by_company, policy_file_found = _load_policy_indices(
        QUEUE_POLICY_FILE
    )
    if not policy_file_found:
        print(
            "[Workflow 7] WARNING: queue_policy.csv not found - "
            "records will be processed without queue policy enforcement. "
            "Run the queue_policy step before the send step for policy enforcement."
        )
    else:
        print(
            f"[Workflow 7] Queue policy loaded - "
            f"{len(by_place_id)} place_id entries, {len(by_email)} email entries, "
            f"{len(by_company)} company_name entries."
        )

    recent_logs = load_recent_logs(hours=CONTACT_SUPPRESS_HOURS)
    live_log: list[dict] = []
    try:
        from src.workflow_8_followup.followup_stop_rules import load_reply_suppression_index
        reply_index = load_reply_suppression_index()
    except Exception as exc:
        print(f"[Workflow 7] Could not load reply suppression index: {exc}")
        reply_index = {}

    counters = {
        "total": len(records),
        "sent": 0,
        "dry_run": 0,
        "failed": 0,
        "blocked": 0,
        "review_required": 0,
        "held": 0,
        "deferred": 0,
        "breaker_blocked": 0,
        "policy_blocked": 0,
        "policy_held": 0,
        "policy_queue_limited": 0,
        "policy_queue_normal": 0,
        "policy_missing": 0,
        "final_named_sends": 0,
        "final_generic_sends": 0,
        "policy_match_place_id": 0,
        "policy_match_email": 0,
        "policy_match_company": 0,
        "processed": 0,
        "remaining_unprocessed": 0,
        "stopped_daily_limit": 0,
        "stopped_hourly_limit": 0,
    }
    now = datetime.now(tz=timezone.utc)

    for i, record in enumerate(records, 1):
        name = record.get("company_name") or record.get("website", f"record {i}")

        slots_used = counters["sent"] + counters["dry_run"]
        if effective_daily_limit > 0 and slots_used >= effective_daily_limit:
            counters["stopped_daily_limit"] = 1
            print(f"[Workflow 7] Daily limit {effective_daily_limit} reached - stopping.")
            break

        if effective_hourly_limit > 0:
            recent_hour_logs = load_recent_logs(hours=1)
            hourly_used = _count_hourly_send_slots(
                recent_hour_logs + live_log,
                effective_mode=effective_mode,
            )
            if hourly_used >= effective_hourly_limit:
                counters["stopped_hourly_limit"] = 1
                print(
                    f"[Workflow 7] Hourly limit {effective_hourly_limit} reached - stopping batch "
                    f"to spread sending and reduce provider risk."
                )
                break

        print(f"[Workflow 7] ({i}/{len(records)}) {name}")

        if policy_file_found:
            policy_row = _lookup_policy(record, by_place_id, by_email, by_company)
            if policy_row is not None:
                record_email = (record.get("kp_email") or "").strip().lower()
                record_pid = (record.get("place_id") or "").strip()
                record_name = (record.get("company_name") or "").strip().lower()
                if record_pid and record_pid in by_place_id:
                    counters["policy_match_place_id"] += 1
                elif record_email and record_email in by_email:
                    counters["policy_match_email"] += 1
                elif record_name and record_name in by_company:
                    counters["policy_match_company"] += 1

            if policy_row is None:
                policy_action = "policy_missing"
                policy_reason = "not_in_queue_policy"
                counters["policy_missing"] += 1
                pid_debug = (record.get("place_id") or "")[:16] or "(empty)"
                email_debug = (record.get("kp_email") or "") or "(empty)"
                print(
                    f"[Workflow 7]   POLICY MISSING - {name} not in queue_policy.csv "
                    f"(place_id={pid_debug}, kp_email={email_debug}) - proceeding through guards"
                )
            else:
                policy_action = (policy_row.get("send_policy_action") or "").strip()
                policy_reason = (policy_row.get("send_policy_reason") or "").strip()
        else:
            policy_action = ""
            policy_reason = ""

        if policy_action == POLICY_BLOCK:
            counters["policy_blocked"] += 1
            counters["blocked"] += 1
            log_row = build_log_row(
                record,
                send_decision="policy_blocked",
                send_status="blocked",
                decision_reason="policy_blocked",
                campaign_id=campaign_id,
                send_mode=effective_mode,
                send_policy_action=policy_action,
                send_policy_reason=policy_reason,
            )
            append_send_log(log_row)
            counters["processed"] += 1
            print(f"[Workflow 7]   POLICY BLOCKED - {name} ({policy_reason})")
            continue

        if policy_action == POLICY_HOLD:
            counters["policy_held"] += 1
            counters["held"] += 1
            log_row = build_log_row(
                record,
                send_decision="policy_held",
                send_status="held",
                decision_reason="policy_held",
                campaign_id=campaign_id,
                send_mode=effective_mode,
                send_policy_action=policy_action,
                send_policy_reason=policy_reason,
            )
            append_send_log(log_row)
            counters["processed"] += 1
            print(f"[Workflow 7]   POLICY HELD - {name} ({policy_reason})")
            continue

        if policy_action == POLICY_GENERIC_ONLY:
            counters["policy_blocked"] += 1
            counters["blocked"] += 1
            log_row = build_log_row(
                record,
                send_decision="policy_blocked",
                send_status="blocked",
                decision_reason="legacy_generic_only_blocked",
                campaign_id=campaign_id,
                send_mode=effective_mode,
                send_policy_action=policy_action,
                send_policy_reason=policy_reason or "legacy_generic_only_blocked",
            )
            append_send_log(log_row)
            counters["processed"] += 1
            print(f"[Workflow 7]   LEGACY GENERIC BLOCKED - {name} ({policy_reason or policy_action})")
            continue
        elif policy_action == POLICY_QUEUE_LIMITED:
            counters["policy_queue_limited"] += 1
        elif policy_action == POLICY_QUEUE_NORMAL:
            counters["policy_queue_normal"] += 1

        if (record.get("send_target_type") or "").strip().lower() == "generic":
            counters["policy_blocked"] += 1
            counters["blocked"] += 1
            log_row = build_log_row(
                record,
                send_decision="policy_blocked",
                send_status="blocked",
                decision_reason="generic_targets_disabled",
                campaign_id=campaign_id,
                send_mode=effective_mode,
                send_policy_action=policy_action,
                send_policy_reason=policy_reason or "generic_targets_disabled",
            )
            append_send_log(log_row)
            counters["processed"] += 1
            print(f"[Workflow 7]   GENERIC TARGET BLOCKED - {name} (generic first-touch disabled)")
            continue

        combined_logs = recent_logs + live_log
        guard = run_checks(
            record,
            combined_logs,
            now=now,
            send_mode=effective_mode,
            conn=conn,
            campaign_id=campaign_id,
            reply_index=reply_index,
        )

        if not guard["allowed"]:
            decision = guard["decision"]
            reason = guard["reason"]
            print(f"[Workflow 7]   {decision.upper()} - {reason}")
            counters[decision] += 1
            if decision == "review_required":
                _append_manual_review_row(
                    record,
                    campaign_id=campaign_id,
                    review_reason=reason,
                    policy_action=policy_action,
                    policy_reason=policy_reason,
                )
            if decision == "blocked" and is_breaker_block(reason):
                counters["breaker_blocked"] += 1
            log_row = build_log_row(
                record,
                send_decision=decision,
                send_status=decision,
                decision_reason=reason,
                campaign_id=campaign_id,
                send_mode=effective_mode,
                send_policy_action=policy_action,
                send_policy_reason=policy_reason,
            )
            append_send_log(log_row)
            counters["processed"] += 1
            continue

        tracking_id = generate_tracking_id(record) if _TRACKING_AVAILABLE else ""
        message_id = generate_message_id(record) if _TRACKING_AVAILABLE else ""

        send_record = record
        if _TRACKING_AVAILABLE and tracking_id and TRACKING_BASE_URL.strip():
            tracked = prepare_tracked_email(
                record.get("email_body", ""),
                tracking_id,
                TRACKING_BASE_URL,
            )
            send_record = {**record, "html_body": tracked["html_body"]}
            print(
                f"[Workflow 7]   Tracking injected - "
                f"links: {tracked['tracked_links_count']} | tid: {tracking_id[:20]}"
            )

        result = send_one(send_record, mode=effective_mode)
        status = result["send_status"]
        provider = result["provider"]
        provider_message_id = result["provider_message_id"]
        error_message = result["error_message"]

        counters[status] = counters.get(status, 0) + 1
        if status in {"sent", "dry_run"}:
            if record.get("send_target_type") == "generic":
                counters["final_generic_sends"] += 1
            else:
                counters["final_named_sends"] += 1

        if status == "failed":
            print(f"[Workflow 7]   FAILED - {error_message}")
        else:
            print(
                f"[Workflow 7]   [{status.upper()}] -> {record.get('kp_email')} | "
                f"{record.get('subject', '')[:50]}"
            )

        log_row = build_log_row(
            record,
            send_decision="send",
            send_status=status,
            decision_reason=guard["reason"],
            provider=provider,
            provider_message_id=provider_message_id,
            error_message=error_message,
            tracking_id=tracking_id,
            message_id=message_id,
            campaign_id=campaign_id,
            send_mode=effective_mode,
            send_policy_action=policy_action,
            send_policy_reason=policy_reason,
        )
        append_send_log(log_row)
        counters["processed"] += 1

        if status in {"sent", "dry_run"}:
            live_log.append(log_row)

    if conn:
        conn.close()

    counters["remaining_unprocessed"] = max(counters["total"] - counters["processed"], 0)

    print(
        f"\n[Workflow 7] Batch complete:\n"
        f"  Total loaded    : {counters['total']}\n"
        f"  Sent            : {counters['sent']}\n"
        f"  Dry-run         : {counters['dry_run']}\n"
        f"  Failed          : {counters['failed']}\n"
        f"  Blocked         : {counters['blocked']}\n"
        f"  Review required : {counters['review_required']}\n"
        f"  Held            : {counters['held']}\n"
        f"  Deferred        : {counters['deferred']}\n"
        f"  Processed       : {counters['processed']}\n"
        f"  Remaining       : {counters['remaining_unprocessed']}\n"
        f"  Breaker-blocked : {counters['breaker_blocked']}\n"
        f"  Policy breakdown:\n"
        f"    queue_normal  : {counters['policy_queue_normal']}\n"
        f"    queue_limited : {counters['policy_queue_limited']}\n"
        f"    held          : {counters['policy_held']}\n"
        f"    blocked       : {counters['policy_blocked']}\n"
        f"    missing       : {counters['policy_missing']}\n"
        f"  Final targets:\n"
        f"    named sends   : {counters['final_named_sends']}\n"
        f"    generic sends : {counters['final_generic_sends']}\n"
        f"  Policy join method:\n"
        f"    by place_id   : {counters['policy_match_place_id']}\n"
        f"    by kp_email   : {counters['policy_match_email']}\n"
        f"    by company    : {counters['policy_match_company']}\n"
        f"  Daily cap stop  : {counters['stopped_daily_limit']}\n"
        f"  Hourly cap stop : {counters['stopped_hourly_limit']}"
    )

    _write_batch_summary(counters)
    return counters


def _empty_summary() -> dict:
    return {
        key: 0
        for key in (
            "total",
            "sent",
            "dry_run",
            "failed",
            "blocked",
            "review_required",
            "held",
            "deferred",
            "breaker_blocked",
            "policy_blocked",
            "policy_held",
            "policy_queue_limited",
            "policy_queue_normal",
            "policy_missing",
            "final_named_sends",
            "final_generic_sends",
            "policy_match_place_id",
            "policy_match_email",
            "policy_match_company",
            "processed",
            "remaining_unprocessed",
            "stopped_daily_limit",
            "stopped_hourly_limit",
        )
    }


def _write_batch_summary(counters: dict) -> None:
    summary = {
        "batch_time": datetime.now().isoformat(),
        **counters,
    }
    with open(SEND_BATCH_SUMMARY, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"[Workflow 7] Summary -> {SEND_BATCH_SUMMARY}")


if __name__ == "__main__":
    run()
