# Workflow 7.8 — Reply Intelligence: Pipeline Entrypoint (Tickets 1 + 2)
#
# Orchestrates the full reply-intelligence cycle:
#   1. Fetch recent inbound emails from Gmail INBOX
#   2. Skip messages already logged (dedup by gmail_message_id)
#   3. For each new reply, resolve outbound thread message IDs (Level 1a support)
#   4. Match reply to a prior outbound send_log row (deterministic, 4-level)
#   5. Classify reply intent using rule-based priority cascade (Ticket 2)
#   6. Derive operational state transitions (Ticket 2)
#   7. Persist to reply_logs.csv and reply_events DB table
#   8. Update DB classification columns for the persisted row (Ticket 2)
#   9. Print a summary
#
# Public entry point:  run(hours_back, max_results, our_email)

import sqlite3
from collections import Counter
from datetime import datetime, timezone

from config.settings import DATABASE_FILE, SMTP_FROM_EMAIL
from src.workflow_7_5_engagement_tracking.engagement_logger import (
    append_engagement_event,
    build_event_row,
)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run(
    hours_back: int = 72,
    max_results: int = 100,
    our_email: str = "",
) -> dict:
    """
    Run the Reply Intelligence pipeline (fetch + match + classify + persist).

    Args:
        hours_back:   look-back window for Gmail fetch (default 72 h)
        max_results:  maximum messages to fetch from Gmail (default 100)
        our_email:    our sending address; used to exclude self-sends and
                      identify outbound messages in thread lookups.
                      Falls back to SMTP_FROM_EMAIL env var if not provided.

    Returns a summary dict:
        {
            "fetched":            int,   # raw messages retrieved from Gmail
            "skipped_dedup":      int,   # already in reply_logs.csv
            "matched":            int,   # successfully matched to a send_log row
            "unmatched":          int,   # no send_log match found
            "manual_review":      int,   # flagged for manual review
            "errors":             int,   # parse/persist failures
            "by_reply_type":      dict,  # {reply_type: count} (Ticket 2)
            "suppressed":         int,   # suppression_status=suppressed
            "paused":             int,   # suppression_status=paused
            "handoff_to_human":   int,   # suppression_status=handoff_to_human
        }
    """
    from src.workflow_7_8_reply_intelligence.reply_fetcher import (
        _get_gmail_read_service,
        fetch_recent_replies,
        get_thread_outbound_message_ids,
    )
    from src.workflow_7_8_reply_intelligence.reply_matcher import (
        load_send_logs,
        build_send_log_index,
        match_reply,
    )
    from src.workflow_7_8_reply_intelligence.reply_classifier import (
        classify_reply,
        apply_classification_to_reply,
    )
    from src.workflow_7_8_reply_intelligence.reply_state_manager import (
        derive_state,
        apply_state_to_reply,
    )
    from src.workflow_7_8_reply_intelligence.reply_logger import (
        load_reply_logs,
        append_reply_log,
        log_reply_to_db,
    )
    from src.database.db_schema import create_all_tables, migrate_schema

    sender = our_email or SMTP_FROM_EMAIL
    print(f"[Workflow 7.8] Starting reply intelligence pipeline")
    print(f"[Workflow 7.8]   our_email={sender!r}  hours_back={hours_back}  max_results={max_results}")

    # --- Auth once, reuse service object across all calls ---
    try:
        service = _get_gmail_read_service()
    except RuntimeError as exc:
        print(f"[Workflow 7.8] Gmail auth failed: {exc}")
        return _empty_summary()

    # --- Fetch ---
    try:
        replies = fetch_recent_replies(
            hours_back=hours_back,
            max_results=max_results,
            our_email=sender,
            service=service,
        )
    except RuntimeError as exc:
        print(f"[Workflow 7.8] Fetch failed: {exc}")
        return _empty_summary()

    fetched = len(replies)

    # --- Load send logs once and build index ---
    send_logs = load_send_logs()
    index     = build_send_log_index(send_logs)
    print(f"[Workflow 7.8]   Send log index built — {len(send_logs)} rows loaded")

    # --- Open DB connection once for the batch, ensure schema is up-to-date ---
    try:
        conn = sqlite3.connect(str(DATABASE_FILE))
        create_all_tables(conn)
        migrate_schema(conn)
    except Exception as exc:
        print(f"[Workflow 7.8]   DB connection failed: {exc} — CSV-only mode")
        conn = None

    # --- Pre-build dedup set (O(1) per-reply check, avoids O(n²) CSV scan) ---
    # Uses stable Gmail message IDs as dedup keys.
    logged_ids: set[str] = {
        row.get("gmail_message_id", "")
        for row in load_reply_logs()
        if row.get("gmail_message_id")
    }
    print(f"[Workflow 7.8]   Dedup index built — {len(logged_ids)} previously logged replies")

    # --- Per-reply processing ---
    counters: dict = {
        "skipped_dedup":    0,
        "matched":          0,
        "unmatched":        0,
        "manual_review":    0,
        "errors":           0,
        "suppressed":       0,
        "paused":           0,
        "handoff_to_human": 0,
    }
    type_counter: Counter = Counter()

    for reply in replies:
        try:
            # --- Dedup: skip if already persisted from a previous run ---
            if reply.gmail_message_id and reply.gmail_message_id in logged_ids:
                counters["skipped_dedup"] += 1
                continue

            # --- Level 1a support: resolve outbound message IDs in the same thread ---
            outbound_ids = []
            if reply.gmail_thread_id and sender:
                outbound_ids = get_thread_outbound_message_ids(
                    reply.gmail_thread_id, service, sender
                )

            # --- Match against send_logs (Ticket 1) ---
            match_reply(reply, index, outbound_thread_ids=outbound_ids)

            # --- Classify intent (Ticket 2) ---
            result = classify_reply(reply)
            apply_classification_to_reply(reply, result)

            # --- Derive and apply operational state (Ticket 2) ---
            state = derive_state(reply.reply_type)
            apply_state_to_reply(reply, state)

            # --- Persist to CSV (primary store) ---
            append_reply_log(reply)
            # Update in-memory dedup set so a second occurrence in the same batch is skipped
            if reply.gmail_message_id:
                logged_ids.add(reply.gmail_message_id)

            # --- Bounce events also flow into engagement logs for analytics / DB sync ---
            if reply.reply_type == "bounce" and reply.matched_kp_email:
                append_engagement_event(
                    build_event_row(
                        tracking_id=reply.matched_tracking_id,
                        message_id=reply.gmail_message_id,
                        company_name=reply.matched_company_name,
                        kp_email=reply.matched_kp_email,
                        event_type="bounce",
                    )
                )

            # --- Persist to DB and update classification columns ---
            if conn:
                try:
                    row_id = log_reply_to_db(conn, reply)
                    if row_id:
                        _update_classification_in_db(conn, reply)
                except Exception as db_exc:
                    print(f"[Workflow 7.8]   DB insert failed for {reply.gmail_message_id}: {db_exc}")

            # --- Update counters ---
            if reply.matched:
                counters["matched"] += 1
            else:
                counters["unmatched"] += 1
            if reply.manual_review_required:
                counters["manual_review"] += 1

            type_counter[reply.reply_type] += 1

            sup = getattr(reply, "suppression_status", "")
            if sup == "suppressed":
                counters["suppressed"] += 1
            elif sup == "paused":
                counters["paused"] += 1
            elif sup == "handoff_to_human":
                counters["handoff_to_human"] += 1

        except Exception as exc:
            print(f"[Workflow 7.8]   Error processing reply {getattr(reply, 'gmail_message_id', '?')}: {exc}")
            counters["errors"] += 1

    if conn:
        conn.close()

    # --- Summary ---
    new_total = fetched - counters["skipped_dedup"]
    print(
        f"[Workflow 7.8] Pipeline complete — "
        f"fetched={fetched}  new={new_total}  "
        f"matched={counters['matched']}  unmatched={counters['unmatched']}  "
        f"manual_review={counters['manual_review']}  errors={counters['errors']}"
    )
    if type_counter:
        print(f"[Workflow 7.8]   Reply types: {dict(type_counter)}")
    print(
        f"[Workflow 7.8]   State: suppressed={counters['suppressed']}  "
        f"paused={counters['paused']}  handoff={counters['handoff_to_human']}"
    )

    return {
        "fetched":          fetched,
        "skipped_dedup":    counters["skipped_dedup"],
        "matched":          counters["matched"],
        "unmatched":        counters["unmatched"],
        "manual_review":    counters["manual_review"],
        "errors":           counters["errors"],
        "by_reply_type":    dict(type_counter),
        "suppressed":       counters["suppressed"],
        "paused":           counters["paused"],
        "handoff_to_human": counters["handoff_to_human"],
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _empty_summary() -> dict:
    return {
        "fetched": 0, "skipped_dedup": 0, "matched": 0,
        "unmatched": 0, "manual_review": 0, "errors": 0,
        "by_reply_type": {}, "suppressed": 0, "paused": 0, "handoff_to_human": 0,
    }


def _update_classification_in_db(conn: sqlite3.Connection, reply) -> None:
    """Update the Ticket 2 classification columns on an existing reply_events row."""
    from src.database.db_utils import update_reply_classification
    update_reply_classification(conn, reply)
