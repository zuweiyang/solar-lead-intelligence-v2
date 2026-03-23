# Workflow 8.5: Campaign Status Aggregator — Data Merger
# Joins all loaded tables into a flat per-contact record.
#
# Join strategy:
#   1. Primary universe = send_logs (contacts who have been sent an initial email)
#   2. Enrich from engagement (matched by tracking_id from send_log)
#   3. Enrich from followup_logs / followup_queue / followup_blocked (by kp_email key)
#   4. Enrich enriched_leads / final_send_queue for pipeline context

from pathlib import Path


def _norm(v: str) -> str:
    return (v or "").strip().lower()


def _email_key(record: dict) -> str:
    email = _norm(record.get("kp_email") or record.get("email") or "")
    return f"email:{email}" if email else ""


def merge_contact_records(tables: dict[str, dict]) -> list[dict]:
    """
    Build one merged record per contact.

    Each merged record contains flat fields from all available sources.
    Fields from later sources overwrite only if the source field is non-empty.
    """
    send_logs       = tables.get("send_logs",       {})
    engagement      = tables.get("engagement",      {})
    followup_logs   = tables.get("followup_logs",   {})
    followup_queue  = tables.get("followup_queue",  {})
    followup_blocked = tables.get("followup_blocked", {})
    final_send_queue = tables.get("final_send_queue", {})
    enriched_leads  = tables.get("enriched_leads",  {})

    merged: list[dict] = []

    for pid_key, send_row in send_logs.items():
        rec: dict = {}

        # --- Base identity fields from send_log ---
        rec["place_id"]    = send_row.get("place_id", "")
        rec["company_name"] = send_row.get("company_name", "")
        rec["kp_name"]     = send_row.get("kp_name", "")
        rec["kp_email"]    = send_row.get("kp_email", "")
        rec["tracking_id"] = send_row.get("tracking_id", "")
        rec["message_id"]  = send_row.get("message_id", "")

        # --- Policy context (P1-3C — from send_log, stamped by P1-3B) ---
        rec["send_policy_action"] = send_row.get("send_policy_action", "")
        rec["send_policy_reason"] = send_row.get("send_policy_reason", "")

        # --- Send fields ---
        rec["initial_send_time"]    = send_row.get("timestamp", "")
        rec["initial_send_status"]  = send_row.get("send_status", "")
        rec["initial_subject"]      = send_row.get("subject", "")
        rec["initial_provider"]     = send_row.get("provider", "")

        # --- Engagement (match by tracking_id) ---
        tid = rec["tracking_id"]
        eng = engagement.get(tid, {}) if tid else {}
        rec["open_count"]       = int(eng.get("open_count",  0) or 0)
        rec["click_count"]      = int(eng.get("click_count", 0) or 0)
        rec["first_open_time"]  = eng.get("first_open_time", "")
        rec["last_open_time"]   = eng.get("last_open_time",  "")
        rec["first_click_time"] = eng.get("first_click_time", "")
        rec["last_click_time"]  = eng.get("last_click_time",  "")

        # --- Follow-up logs (match by kp_email key) ---
        ekey = _email_key(send_row)
        fl_row = followup_logs.get(ekey, {})
        rec["last_followup_stage"]  = fl_row.get("followup_stage", "")
        rec["last_followup_time"]   = fl_row.get("timestamp", "")
        rec["last_followup_subject"] = fl_row.get("followup_subject", "")

        # --- Followup queue (match by pid_key first, then email key) ---
        fq_row = followup_queue.get(pid_key) or followup_queue.get(ekey, {})
        rec["queued_followup_stage"]   = fq_row.get("followup_stage", "")
        rec["queued_followup_due"]     = fq_row.get("due_date", "")
        rec["queued_followup_subject"] = fq_row.get("followup_subject", "")

        # --- Followup blocked (match by kp_email key) ---
        fb_row = followup_blocked.get(ekey, {})
        rec["followup_block_reason"]   = fb_row.get("reason", "")
        rec["followup_block_decision"] = fb_row.get("decision", "")

        # --- Final send queue (optional enrichment) ---
        fsq_row = final_send_queue.get(pid_key) or final_send_queue.get(ekey, {})
        rec["approval_status"]   = fsq_row.get("approval_status", "")
        rec["overall_score"]     = fsq_row.get("overall_score", "")

        # --- Enriched leads (optional) ---
        el_row = enriched_leads.get(pid_key) or enriched_leads.get(ekey, {})
        rec["industry"]     = el_row.get("industry", "")
        rec["company_size"] = el_row.get("company_size", "")
        rec["city"]         = el_row.get("city", el_row.get("address", ""))

        merged.append(rec)

    return merged
