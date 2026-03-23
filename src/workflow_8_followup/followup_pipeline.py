# Workflow 8: Follow-up Automation — Pipeline Orchestrator
# Selects candidates → applies stop rules → schedules → generates drafts → writes outputs.

import csv
from datetime import datetime, timezone
from pathlib import Path

from config.settings import (
    SEND_LOGS_FILE, ENGAGEMENT_SUMMARY_FILE,
    FOLLOWUP_CANDIDATES_FILE, FOLLOWUP_QUEUE_FILE,
    FOLLOWUP_BLOCKED_FILE, FOLLOWUP_LOGS_FILE,
)
from src.workflow_8_followup.followup_selector   import select_candidates
from src.workflow_8_followup.followup_stop_rules import (
    check_stop_rules,
    classify_engagement,
    load_reply_suppression_index,
)
from src.workflow_8_followup.followup_scheduler  import build_followup_schedule
from src.workflow_8_followup.followup_generator  import generate_followup

# ---------------------------------------------------------------------------
# Output field definitions
# ---------------------------------------------------------------------------

CANDIDATE_FIELDS = [
    "company_name", "place_id", "kp_name", "kp_email",
    "subject", "tracking_id", "message_id",
    "last_send_time", "followup_stage",
    "open_count", "click_count", "engagement_status",
    "followup_reason",
    "followup_route", "original_contact_email", "original_contact_name",
]

QUEUE_FIELDS = [
    "company_name", "place_id", "kp_name", "kp_email",
    "tracking_id", "message_id",
    "followup_stage", "engagement_status",
    "due_date", "followup_subject", "followup_body",
    "followup_reason", "generation_mode", "generation_source",
    "followup_route", "original_contact_email", "original_contact_name",
]

BLOCKED_FIELDS = [
    "company_name", "kp_email", "followup_stage", "decision", "reason",
]

LOG_FIELDS = [
    "timestamp", "company_name", "place_id", "kp_email",
    "followup_stage", "engagement_status",
    "decision", "reason",
    "due_date", "is_due",
    "followup_subject", "generation_mode", "generation_source",
    "followup_route", "original_contact_email", "original_contact_name",
    "campaign_id",
]


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def _write_csv(records: list[dict], path: Path, fields: list[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)


def _append_csv(row: dict, path: Path, fields: list[str]) -> None:
    file_exists = path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


# ---------------------------------------------------------------------------
# Public run()
# ---------------------------------------------------------------------------

def run(
    now: datetime | None = None,
    send_logs_path: Path      = SEND_LOGS_FILE,
    engagement_path: Path     = ENGAGEMENT_SUMMARY_FILE,
    followup_logs_path: Path  = FOLLOWUP_LOGS_FILE,   # read: prior stage history
    log_output_path: Path | None = None,              # write: new log entries (defaults to FOLLOWUP_LOGS_FILE)
    limit: int                = 0,
    campaign_id: str          = "",
) -> dict:
    """
    Run the full Workflow 8 follow-up pipeline.

    Steps:
      1. Select candidates from send logs + engagement data
      2. Classify engagement per candidate
      3. Apply stop rules
      4. Compute due dates (scheduler)
      5. For due + allowed candidates: generate follow-up draft
      6. Write all output files
      7. Append to followup_logs.csv (persistent history)

    Returns a summary dict.
    """
    now = now or datetime.now(tz=timezone.utc)
    # Write new log entries to FOLLOWUP_LOGS_FILE unless caller overrides
    log_out = log_output_path or FOLLOWUP_LOGS_FILE

    scope_label = f"campaign {campaign_id}" if campaign_id else "GLOBAL (all campaigns)"
    print(
        f"[Workflow 8] Scope: {scope_label} — "
        f"{'campaign-scoped follow-up selection' if campaign_id else 'global CRM — evaluates ALL contacts'}"
    )

    # Step 0 (Ticket 2) — Load reply suppression index once for the batch.
    # Built from reply_logs.csv; does not require DB access.
    # reply state takes precedence over open/click engagement heuristics.
    reply_index = load_reply_suppression_index()
    if reply_index:
        print(f"[Workflow 8] Reply suppression index loaded — {len(reply_index)} emails with reply state")
    else:
        print("[Workflow 8] No reply suppression index (reply_logs.csv empty or absent)")

    # Step 1 — Select
    candidates = select_candidates(
        send_logs_path     = send_logs_path,
        engagement_path    = engagement_path,
        followup_logs_path = followup_logs_path,
        campaign_id        = campaign_id,
    )

    if limit > 0:
        candidates = candidates[:limit]

    if not candidates:
        print("[Workflow 8] No follow-up candidates found.")
        _write_csv([], FOLLOWUP_CANDIDATES_FILE, CANDIDATE_FIELDS)
        _write_csv([], FOLLOWUP_QUEUE_FILE,      QUEUE_FIELDS)
        _write_csv([], FOLLOWUP_BLOCKED_FILE,    BLOCKED_FIELDS)
        return {"candidates": 0, "queued": 0, "blocked": 0, "deferred": 0,
                "followup_1": 0, "followup_2": 0, "followup_3": 0}

    print(
        f"[Workflow 8] Processing {len(candidates)} candidates — "
        f"now: {now.strftime('%Y-%m-%dT%H:%M')} UTC"
    )

    queued:   list[dict] = []
    blocked:  list[dict] = []
    counters = {
        "candidates": len(candidates),
        "queued":     0,
        "blocked":    0,
        "deferred":   0,
        "followup_1": 0,
        "followup_2": 0,
        "followup_3": 0,
    }

    for i, candidate in enumerate(candidates, 1):
        name  = candidate.get("company_name") or candidate.get("kp_email", f"record {i}")
        stage = candidate.get("followup_stage", "")

        # Step 2 — Classify engagement
        engagement_status = classify_engagement(
            int(candidate.get("open_count",  0) or 0),
            int(candidate.get("click_count", 0) or 0),
        )
        candidate["engagement_status"] = engagement_status

        print(f"[Workflow 8] ({i}/{len(candidates)}) {name} | {stage} | {engagement_status}")

        # Step 3 — Stop rules (reply_index passed for reply-state awareness)
        stop = check_stop_rules(candidate, reply_index=reply_index)
        if not stop["allowed"]:
            decision = stop["decision"]
            reason   = stop["reason"]
            print(f"[Workflow 8]   {decision.upper()} — {reason}")
            counters[decision] = counters.get(decision, 0) + 1
            blocked.append({
                "company_name":  candidate.get("company_name", ""),
                "kp_email":      candidate.get("kp_email", ""),
                "followup_stage": stage,
                "decision":      decision,
                "reason":        reason,
            })
            _append_csv(
                {
                    "timestamp":       now.isoformat(),
                    "company_name":    candidate.get("company_name", ""),
                    "place_id":        candidate.get("place_id", ""),
                    "kp_email":        candidate.get("kp_email", ""),
                    "followup_stage":  stage,
                    "engagement_status": engagement_status,
                    "decision":        decision,
                    "reason":          reason,
                    "due_date":        "",
                    "is_due":          "",
                    "followup_subject": "",
                    "generation_mode": "",
                    "generation_source": "",
                    "followup_route": candidate.get("followup_route", ""),
                    "original_contact_email": candidate.get("original_contact_email", ""),
                    "original_contact_name": candidate.get("original_contact_name", ""),
                    "campaign_id":     campaign_id,
                },
                log_out, LOG_FIELDS,
            )
            continue

        # Step 4 — Schedule
        schedule = build_followup_schedule(candidate, now=now)
        due_date = schedule["due_date"]
        is_due   = schedule["is_due"]
        action   = schedule["scheduled_action"]

        print(f"[Workflow 8]   Schedule: {action} | due: {due_date[:10] if due_date else '?'}")

        if action == "wait":
            counters["deferred"] += 1
            blocked.append({
                "company_name":  candidate.get("company_name", ""),
                "kp_email":      candidate.get("kp_email", ""),
                "followup_stage": stage,
                "decision":      "deferred",
                "reason":        schedule["schedule_reason"],
            })
            _append_csv(
                {
                    "timestamp":       now.isoformat(),
                    "company_name":    candidate.get("company_name", ""),
                    "place_id":        candidate.get("place_id", ""),
                    "kp_email":        candidate.get("kp_email", ""),
                    "followup_stage":  stage,
                    "engagement_status": engagement_status,
                    "decision":        "deferred",
                    "reason":          schedule["schedule_reason"],
                    "due_date":        due_date,
                    "is_due":          str(is_due),
                    "followup_subject": "",
                    "generation_mode": "",
                    "generation_source": "",
                    "followup_route": candidate.get("followup_route", ""),
                    "original_contact_email": candidate.get("original_contact_email", ""),
                    "original_contact_name": candidate.get("original_contact_name", ""),
                    "campaign_id":     campaign_id,
                },
                log_out, LOG_FIELDS,
            )
            continue

        if action == "blocked":
            counters["blocked"] += 1
            blocked.append({
                "company_name":  candidate.get("company_name", ""),
                "kp_email":      candidate.get("kp_email", ""),
                "followup_stage": stage,
                "decision":      "blocked",
                "reason":        schedule["schedule_reason"],
            })
            continue

        # Step 5 — Generate draft
        draft, gen_mode, gen_source = generate_followup(candidate)
        followup_subject = draft.get("subject", "")
        followup_body    = draft.get("body", "")

        print(f"[Workflow 8]   [{gen_source}] → {followup_subject[:55]}")

        # Track stage counts
        stage_key = stage if stage in ("followup_1", "followup_2", "followup_3") else "followup_1"
        counters[stage_key] = counters.get(stage_key, 0) + 1
        counters["queued"]  += 1

        queued.append({
            "company_name":     candidate.get("company_name", ""),
            "place_id":         candidate.get("place_id", ""),
            "kp_name":          candidate.get("kp_name", ""),
            "kp_email":         candidate.get("kp_email", ""),
            "tracking_id":      candidate.get("tracking_id", ""),
            "message_id":       candidate.get("message_id", ""),
            "followup_stage":   stage,
            "engagement_status": engagement_status,
            "due_date":         due_date,
            "followup_subject": followup_subject,
            "followup_body":    followup_body,
            "followup_reason":  candidate.get("followup_reason", ""),
            "generation_mode":  gen_mode,
            "generation_source": gen_source,
            "followup_route":   candidate.get("followup_route", ""),
            "original_contact_email": candidate.get("original_contact_email", ""),
            "original_contact_name": candidate.get("original_contact_name", ""),
        })

        # Append to persistent log
        _append_csv(
            {
                "timestamp":        now.isoformat(),
                "company_name":     candidate.get("company_name", ""),
                "place_id":         candidate.get("place_id", ""),
                "kp_email":         candidate.get("kp_email", ""),
                "followup_stage":   stage,
                "engagement_status": engagement_status,
                "decision":         "followup",
                "reason":           "Due and allowed",
                "due_date":         due_date,
                "is_due":           str(is_due),
                "followup_subject": followup_subject,
                "generation_mode":  gen_mode,
                "generation_source": gen_source,
                "followup_route":   candidate.get("followup_route", ""),
                "original_contact_email": candidate.get("original_contact_email", ""),
                "original_contact_name": candidate.get("original_contact_name", ""),
                "campaign_id":      campaign_id,
            },
            followup_logs_path, LOG_FIELDS,
        )

    # Write output files
    _write_csv(candidates, FOLLOWUP_CANDIDATES_FILE, CANDIDATE_FIELDS)
    _write_csv(queued,     FOLLOWUP_QUEUE_FILE,      QUEUE_FIELDS)
    _write_csv(blocked,    FOLLOWUP_BLOCKED_FILE,    BLOCKED_FIELDS)

    print(
        f"\n[Workflow 8] Complete:\n"
        f"  Candidates   : {counters['candidates']}\n"
        f"  Queued       : {counters['queued']}\n"
        f"  Deferred     : {counters['deferred']}\n"
        f"  Blocked      : {counters['blocked']}\n"
        f"  followup_1   : {counters['followup_1']}\n"
        f"  followup_2   : {counters['followup_2']}\n"
        f"  followup_3   : {counters['followup_3']}"
    )
    print(f"[Workflow 8] → {FOLLOWUP_CANDIDATES_FILE.name} ({len(candidates)} rows)")
    print(f"[Workflow 8] → {FOLLOWUP_QUEUE_FILE.name} ({len(queued)} rows)")
    print(f"[Workflow 8] → {FOLLOWUP_BLOCKED_FILE.name} ({len(blocked)} rows)")
    print(f"[Workflow 8] → {FOLLOWUP_LOGS_FILE.name} (append-only)")

    return counters


if __name__ == "__main__":
    run()
