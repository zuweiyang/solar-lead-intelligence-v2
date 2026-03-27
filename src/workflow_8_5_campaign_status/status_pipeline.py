# Workflow 8.5: Campaign Status Aggregator - Pipeline Orchestrator
# Loads all campaign files -> merges per-contact -> classifies -> writes outputs.

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from config.settings import (
    CAMPAIGN_STATUS_FILE,
    CAMPAIGN_STATUS_SUMMARY,
    ENGAGEMENT_SUMMARY_FILE,
    ENRICHED_LEADS_FILE,
    FINAL_SEND_QUEUE_FILE,
    FOLLOWUP_BLOCKED_FILE,
    FOLLOWUP_LOGS_FILE,
    FOLLOWUP_QUEUE_FILE,
    POLICY_SUMMARY_FILE,
    RAW_LEADS_FILE,
    QUALIFIED_LEADS_FILE,
    SCORED_CONTACTS_FILE,
    SEND_BATCH_SUMMARY,
    SEND_LOGS_FILE,
)
from src.workflow_8_5_campaign_status.status_classifier import build_summary, classify_all
from src.workflow_8_5_campaign_status.status_loader import load_all
from src.workflow_8_5_campaign_status.status_merger import merge_contact_records


STATUS_FIELDS = [
    "place_id",
    "company_name",
    "kp_name",
    "kp_email",
    "initial_send_time",
    "initial_send_status",
    "initial_subject",
    "initial_provider",
    "send_policy_action",
    "send_policy_reason",
    "tracking_id",
    "message_id",
    "open_count",
    "click_count",
    "first_open_time",
    "last_open_time",
    "first_click_time",
    "last_click_time",
    "last_followup_stage",
    "last_followup_time",
    "last_followup_subject",
    "queued_followup_stage",
    "queued_followup_due",
    "queued_followup_subject",
    "followup_block_decision",
    "followup_block_reason",
    "approval_status",
    "overall_score",
    "industry",
    "company_size",
    "city",
    "lifecycle_status",
    "next_action",
    "priority_flag",
]


def _load_json_safe(path: Path) -> dict:
    try:
        if path.exists():
            with open(path, encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _read_csv_safe(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _write_csv(records: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=STATUS_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)


def _write_json(data: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def _build_policy_section(policy_summary: dict, send_batch: dict) -> dict:
    queue_stage = policy_summary.get("queue_stage", {})
    send_keys = (
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
    )
    send_stage = {k: send_batch.get(k, 0) for k in send_keys} if send_batch else {}
    return {
        "queue_stage": queue_stage,
        "send_stage": send_stage,
    }


def _as_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() == "true"


def _queue_contact_integrity(rows: list[dict]) -> tuple[int, int, int]:
    complete = 0
    named = 0
    generic = 0
    for row in rows:
        email = (row.get("contact_email") or row.get("kp_email") or "").strip()
        target_type = (row.get("send_target_type") or "").strip().lower()
        if email:
            if target_type == "named":
                if (row.get("contact_name") or row.get("kp_name") or "").strip():
                    complete += 1
            else:
                complete += 1
        if target_type == "named":
            named += 1
        elif target_type == "generic":
            generic += 1
    return complete, named, generic


def _build_quality_report(
    raw_leads_path: Path,
    qualified_leads_path: Path,
    scored_contacts_path: Path,
    final_send_queue_path: Path,
    send_batch_data: dict,
) -> dict:
    raw_rows = _read_csv_safe(raw_leads_path)
    qualified_rows = _read_csv_safe(qualified_leads_path)
    contact_rows = _read_csv_safe(scored_contacts_path)
    final_queue_rows = _read_csv_safe(final_send_queue_path)

    named_company_keys: set[str] = set()
    generic_company_keys: set[str] = set()
    companies_with_any_sendable: set[str] = set()

    for row in contact_rows:
        key = (
            (row.get("place_id") or "").strip()
            or (row.get("company_name") or "").strip().lower()
        )
        if not key:
            continue
        email = (row.get("kp_email") or "").strip()
        if not email:
            continue
        sendable = _as_bool(row.get("email_sendable", "")) or (
            (row.get("send_eligibility") or "").strip().lower()
            in {"allow", "allow_limited"}
        )
        if not sendable:
            continue
        companies_with_any_sendable.add(key)
        is_generic = _as_bool(row.get("is_generic_mailbox", ""))
        if not is_generic and (row.get("kp_name") or "").strip():
            named_company_keys.add(key)
        if is_generic:
            generic_company_keys.add(key)

    generic_mailbox_only_companies = generic_company_keys - named_company_keys
    queue_complete, queue_named, queue_generic = _queue_contact_integrity(final_queue_rows)
    final_queue_count = len(final_queue_rows)

    contact_name_complete = sum(
        1 for row in final_queue_rows if (row.get("contact_name") or row.get("kp_name") or "").strip()
    )
    contact_email_complete = sum(
        1 for row in final_queue_rows if (row.get("contact_email") or row.get("kp_email") or "").strip()
    )

    return {
        "raw_leads": len(raw_rows),
        "qualified_leads": len(qualified_rows),
        "named_contact_companies": len(named_company_keys),
        "generic_mailbox_only_companies": len(generic_mailbox_only_companies),
        "companies_with_any_sendable_contact": len(companies_with_any_sendable),
        "final_send_queue_count": final_queue_count,
        "final_named_sends_count": send_batch_data.get("final_named_sends", queue_named),
        "final_generic_sends_count": send_batch_data.get("final_generic_sends", queue_generic),
        "contact_field_completeness_pct": round(
            (contact_email_complete / final_queue_count) * 100, 1
        ) if final_queue_count else 0.0,
        "final_queue_contact_integrity_pct": round(
            (queue_complete / final_queue_count) * 100, 1
        ) if final_queue_count else 0.0,
        "final_queue_contact_name_completeness_pct": round(
            (contact_name_complete / final_queue_count) * 100, 1
        ) if final_queue_count else 0.0,
    }


def run(
    campaign_id: str = "",
    send_logs_path: Path = SEND_LOGS_FILE,
    engagement_path: Path = ENGAGEMENT_SUMMARY_FILE,
    followup_logs_path: Path = FOLLOWUP_LOGS_FILE,
    followup_queue_path: Path = FOLLOWUP_QUEUE_FILE,
    followup_blocked_path: Path = FOLLOWUP_BLOCKED_FILE,
    final_send_queue_path: Path = FINAL_SEND_QUEUE_FILE,
    enriched_leads_path: Path = ENRICHED_LEADS_FILE,
    status_output_path: Path = CAMPAIGN_STATUS_FILE,
    summary_output_path: Path = CAMPAIGN_STATUS_SUMMARY,
    policy_summary_path: Path = POLICY_SUMMARY_FILE,
    send_batch_summary_path: Path = SEND_BATCH_SUMMARY,
) -> dict:
    """
    Run Workflow 8.5 - Campaign Status Aggregator.
    """
    now_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M UTC")
    scope_label = f"campaign {campaign_id}" if campaign_id else "GLOBAL (no campaign_id filter)"
    print(f"[Workflow 8.5] Campaign Status Aggregator - {now_str} - scope: {scope_label}")

    tables = load_all(
        send_logs_path=send_logs_path,
        engagement_path=engagement_path,
        followup_logs_path=followup_logs_path,
        followup_queue_path=followup_queue_path,
        followup_blocked_path=followup_blocked_path,
        final_send_queue_path=final_send_queue_path,
        enriched_leads_path=enriched_leads_path,
        campaign_id=campaign_id,
    )

    sent_count = len(tables["send_logs"])
    engagement_count = len(tables["engagement"])
    followup_count = len(tables["followup_logs"])
    queue_count = len(tables["followup_queue"])
    blocked_count = len(tables["followup_blocked"])

    sent_only = sum(
        1 for r in tables["send_logs"].values() if r.get("send_status") == "sent"
    )
    dry_run_only = sum(
        1 for r in tables["send_logs"].values() if r.get("send_status") == "dry_run"
    )

    print(
        f"[Workflow 8.5] Loaded:\n"
        f"  scope           : {scope_label}\n"
        f"  send_logs       : {sent_count} contacts "
        f"(smtp_sent={sent_only}, dry_run={dry_run_only})\n"
        f"  engagement      : {engagement_count} tracking records\n"
        f"  followup_logs   : {followup_count} contacts with follow-up sent "
        f"(campaign-filtered: {'yes' if campaign_id else 'no - global mode'})\n"
        f"  followup_queue  : {queue_count} queued\n"
        f"  followup_blocked: {blocked_count} blocked/deferred"
    )

    policy_summary_data = _load_json_safe(policy_summary_path)
    send_batch_data = _load_json_safe(send_batch_summary_path)
    run_dir = final_send_queue_path.parent
    quality_report = _build_quality_report(
        raw_leads_path=run_dir / Path(str(RAW_LEADS_FILE)).name,
        qualified_leads_path=run_dir / Path(str(QUALIFIED_LEADS_FILE)).name,
        scored_contacts_path=run_dir / Path(str(SCORED_CONTACTS_FILE)).name,
        final_send_queue_path=final_send_queue_path,
        send_batch_data=send_batch_data,
    )

    if sent_count == 0:
        print(
            f"[Workflow 8.5] No sent/dry_run contacts found for scope: {scope_label} "
            f"- writing empty outputs."
        )
        _write_csv([], status_output_path)
        summary = {
            "generated_at": now_str,
            "total_contacts": 0,
            "lifecycle_status": {},
            "priority": {"high": 0, "medium": 0, "low": 0},
            "next_action": {},
            "policy": _build_policy_section(policy_summary_data, send_batch_data),
            "quality_report": quality_report,
        }
        _write_json(summary, summary_output_path)
        return summary

    merged = merge_contact_records(tables)
    print(f"[Workflow 8.5] Merged {len(merged)} contact records.")

    classified = classify_all(merged)

    _write_csv(classified, status_output_path)
    summary = build_summary(classified)
    summary["generated_at"] = now_str
    summary["policy"] = _build_policy_section(policy_summary_data, send_batch_data)
    summary["quality_report"] = quality_report
    _write_json(summary, summary_output_path)

    print(
        f"\n[Workflow 8.5] Complete:\n"
        f"  Total contacts  : {summary['total_contacts']}\n"
        + "\n".join(
            f"  {k:<22}: {v}" for k, v in summary["lifecycle_status"].items()
        )
    )
    print(
        f"\n  Priority - high: {summary['priority']['high']}  "
        f"medium: {summary['priority']['medium']}  "
        f"low: {summary['priority']['low']}"
    )
    print(
        f"\n  Quality - raw: {quality_report['raw_leads']}  "
        f"qualified: {quality_report['qualified_leads']}  "
        f"named companies: {quality_report['named_contact_companies']}  "
        f"generic-mailbox-only: {quality_report['generic_mailbox_only_companies']}"
    )
    print(f"\n[Workflow 8.5] -> {status_output_path.name} ({len(classified)} rows)")
    print(f"[Workflow 8.5] -> {summary_output_path.name}")

    return summary


if __name__ == "__main__":
    run()
