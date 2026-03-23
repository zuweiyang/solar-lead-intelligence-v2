# Workflow 8.5: Campaign Status Aggregator — Status Classifier
# Assigns lifecycle_status, next_action, and priority_flag to each merged contact record.
#
# Lifecycle statuses (mutually exclusive, evaluated in priority order):
#   followup_sent      — a follow-up has been sent (logged in followup_logs with decision=followup)
#   followup_queued    — follow-up is in the active queue (ready to send)
#   followup_deferred  — follow-up is scheduled but not yet due
#   followup_blocked   — follow-up has been blocked by stop rules
#   clicked_no_reply   — contact clicked a link in the initial email
#   opened_no_click    — contact opened the email but did not click
#   sent_no_open       — email was sent but no open recorded
#   completed          — max follow-up stage reached, no further action
#   not_sent           — record exists but no sent initial email
#   unknown            — cannot determine status


_MAX_STAGE = 3
_STAGE_NUMS = {"followup_1": 1, "followup_2": 2, "followup_3": 3}


def _stage_num(stage: str) -> int:
    return _STAGE_NUMS.get((stage or "").strip(), 0)


def classify_status(record: dict) -> dict:
    """
    Classify one merged contact record.

    Returns the record enriched with:
        lifecycle_status  — string label
        next_action       — what should happen next
        priority_flag     — "high" | "medium" | "low"
    """
    # Read fields
    send_status    = (record.get("initial_send_status") or "").strip()
    open_count     = int(record.get("open_count",  0) or 0)
    click_count    = int(record.get("click_count", 0) or 0)
    last_fu_stage  = (record.get("last_followup_stage")    or "").strip()
    queued_stage   = (record.get("queued_followup_stage")  or "").strip()
    blocked_dec    = (record.get("followup_block_decision") or "").strip()
    queued_due     = (record.get("queued_followup_due")    or "").strip()

    was_sent = send_status in {"sent", "dry_run"}

    # --- not_sent ---
    if not was_sent:
        rec = record.copy()
        rec["lifecycle_status"] = "not_sent"
        rec["next_action"]      = "send_initial"
        rec["priority_flag"]    = "low"
        return rec

    # --- completed: last follow-up stage was followup_3 (max) ---
    if _stage_num(last_fu_stage) >= _MAX_STAGE:
        rec = record.copy()
        rec["lifecycle_status"] = "completed"
        rec["next_action"]      = "no_action"
        rec["priority_flag"]    = "low"
        return rec

    # --- followup_sent: at least one follow-up has been sent ---
    if last_fu_stage:
        # Determine next stage
        next_stage_num = _stage_num(last_fu_stage) + 1
        if next_stage_num > _MAX_STAGE:
            lifecycle = "completed"
            next_action = "no_action"
            priority = "low"
        else:
            lifecycle = "followup_sent"
            next_action = f"send_followup_{next_stage_num}"
            priority = "medium" if click_count > 0 else "low"
        rec = record.copy()
        rec["lifecycle_status"] = lifecycle
        rec["next_action"]      = next_action
        rec["priority_flag"]    = priority
        return rec

    # --- followup_queued: a follow-up is ready to send ---
    if queued_stage and blocked_dec not in {"blocked", "deferred"}:
        rec = record.copy()
        rec["lifecycle_status"] = "followup_queued"
        rec["next_action"]      = f"send_followup_{_stage_num(queued_stage) or 1}"
        rec["priority_flag"]    = "high" if click_count > 0 else "medium"
        return rec

    # --- followup_deferred: blocked with decision=="deferred" ---
    if blocked_dec == "deferred":
        rec = record.copy()
        rec["lifecycle_status"] = "followup_deferred"
        rec["next_action"]      = f"wait_until_due" + (f" ({queued_due[:10]})" if queued_due else "")
        rec["priority_flag"]    = "low"
        return rec

    # --- followup_blocked: hard stop rule triggered ---
    if blocked_dec == "blocked":
        rec = record.copy()
        rec["lifecycle_status"] = "followup_blocked"
        rec["next_action"]      = "blocked"
        rec["priority_flag"]    = "low"
        return rec

    # --- engagement-based statuses for initial email only ---
    if click_count > 0:
        rec = record.copy()
        rec["lifecycle_status"] = "clicked_no_reply"
        rec["next_action"]      = "review_clicked_contact"
        rec["priority_flag"]    = "high"
        return rec

    if open_count >= 2:
        rec = record.copy()
        rec["lifecycle_status"] = "opened_no_click"
        rec["next_action"]      = "send_followup_1"
        rec["priority_flag"]    = "medium"
        return rec

    if open_count == 1:
        rec = record.copy()
        rec["lifecycle_status"] = "opened_no_click"
        rec["next_action"]      = "send_followup_1"
        rec["priority_flag"]    = "medium"
        return rec

    # --- sent but no open ---
    rec = record.copy()
    rec["lifecycle_status"] = "sent_no_open"
    rec["next_action"]      = "send_followup_1"
    rec["priority_flag"]    = "medium"
    return rec


def classify_all(records: list[dict]) -> list[dict]:
    return [classify_status(r) for r in records]


def build_summary(records: list[dict]) -> dict:
    """Aggregate lifecycle_status counts and priority breakdown."""
    status_counts: dict[str, int] = {}
    priority_counts: dict[str, int] = {"high": 0, "medium": 0, "low": 0}
    next_action_counts: dict[str, int] = {}

    for r in records:
        ls = r.get("lifecycle_status", "unknown")
        status_counts[ls] = status_counts.get(ls, 0) + 1

        pf = r.get("priority_flag", "low")
        priority_counts[pf] = priority_counts.get(pf, 0) + 1

        na = r.get("next_action", "")
        if na:
            next_action_counts[na] = next_action_counts.get(na, 0) + 1

    return {
        "total_contacts":    len(records),
        "lifecycle_status":  status_counts,
        "priority":          priority_counts,
        "next_action":       next_action_counts,
    }
