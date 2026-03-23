"""
Workflow 9.6 — Streamlit Campaign Control Panel: UI Actions

Helper functions for manual operator actions:
- refresh_dashboard_state()
- manual_send_followup_1()
- get_high_priority_rows()
- get_company_detail()
"""
from __future__ import annotations

import os
import traceback
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.workflow_9_5_streamlit_control_panel.debug_log import log

from src.workflow_9_5_streamlit_control_panel.ui_state import (
    load_high_priority_leads,
    load_followup_1_candidates,
    get_company_detail as _get_company_detail,
)


# ---------------------------------------------------------------------------
# Refresh
# ---------------------------------------------------------------------------

def refresh_dashboard_state() -> None:
    """
    Invalidate Streamlit cached data so all loaders re-read from disk.
    Call this after a run or when the user clicks "Refresh".
    """
    log.action("refresh_dashboard_state() — clearing Streamlit cache")
    try:
        import streamlit as st
        st.cache_data.clear()
        log.action("Streamlit cache cleared successfully")
    except Exception as exc:
        log.warn("Could not clear Streamlit cache (may be outside Streamlit context)", exc=exc)


# ---------------------------------------------------------------------------
# High-priority rows
# ---------------------------------------------------------------------------

def get_high_priority_rows() -> list[dict]:
    """Return high-priority leads from campaign_status.csv."""
    return load_high_priority_leads()


# ---------------------------------------------------------------------------
# Company detail
# ---------------------------------------------------------------------------

def get_company_detail(company_name: str) -> dict | None:
    """Return detailed lifecycle data for a single company."""
    return _get_company_detail(company_name)


# ---------------------------------------------------------------------------
# Manual Send followup_1
# ---------------------------------------------------------------------------

class FollowupSendResult:
    """Holds the outcome of a manual followup_1 send attempt."""

    def __init__(
        self,
        attempted: int = 0,
        sent: int = 0,
        dry_run: int = 0,
        blocked: int = 0,
        errors: int = 0,
        send_mode: str = "dry_run",
        messages: list[str] | None = None,
    ):
        self.attempted = attempted
        self.sent      = sent
        self.dry_run   = dry_run
        self.blocked   = blocked
        self.errors    = errors
        self.send_mode = send_mode
        self.messages  = messages or []


class BatchCloudDeployResult:
    """Holds the outcome of a batch cloud deploy action."""

    def __init__(
        self,
        success: bool = False,
        requested: int = 0,
        deployed: int = 0,
        skipped: int = 0,
        failed: int = 0,
        messages: list[str] | None = None,
    ):
        self.success = success
        self.requested = requested
        self.deployed = deployed
        self.skipped = skipped
        self.failed = failed
        self.messages = messages or []


def manual_send_followup_1(send_mode: str = "dry_run") -> FollowupSendResult:
    """
    Load followup_queue.csv, filter stage=followup_1, run each record through
    the existing send guard + sender + logger pipeline.

    Respects send_mode ("dry_run" | "smtp") — does NOT bypass safety checks.
    Returns a FollowupSendResult summary.
    """
    log.pipeline("manual_send_followup_1()", send_mode=send_mode)
    candidates = load_followup_1_candidates()
    log.pipeline("followup_1 candidates loaded", count=len(candidates))
    if not candidates:
        log.warn("No followup_1 candidates found — returning early")
        return FollowupSendResult(
            send_mode=send_mode,
            messages=["No followup_1 candidates found in followup_queue.csv."],
        )

    # Inject send_mode into environment so email_sender respects it
    original_mode = os.environ.get("EMAIL_SEND_MODE")
    os.environ["EMAIL_SEND_MODE"] = send_mode

    result = FollowupSendResult(send_mode=send_mode)

    try:
        from src.workflow_7_email_sending.send_guard  import run_checks
        from src.workflow_7_email_sending.email_sender import send_one
        from src.workflow_7_email_sending.send_logger  import (
            load_recent_logs, append_send_log, build_log_row,
        )
    except ImportError as exc:
        result.messages.append(f"Send pipeline not available: {exc}")
        return result

    recent_logs = load_recent_logs()

    for row in candidates:
        result.attempted += 1
        company = row.get("company_name", "")
        email   = row.get("kp_email", "")

        # Map followup queue fields → send guard expected fields
        record = {
            "company_name": company,
            "place_id":     row.get("place_id", ""),
            "kp_name":      row.get("kp_name", ""),
            "kp_email":     email,
            "subject":      row.get("followup_subject", ""),
            "email_body":   row.get("followup_body", ""),
            "approval_status": "approved",   # followup_queue rows are pre-approved
        }

        # Run guard checks
        guard = run_checks(record, recent_logs)
        if not guard["allowed"]:
            result.blocked += 1
            result.messages.append(
                f"BLOCKED  {company} <{email}>: {guard['reason']}"
            )
            continue

        # Send
        try:
            send_result = send_one(record)
            status = send_result.get("send_status", "failed")

            log_row = build_log_row(record, guard, send_result)
            append_send_log(log_row)
            recent_logs.append(log_row)   # keep in-memory dedup current

            if status == "sent":
                result.sent += 1
                result.messages.append(f"SENT     {company} <{email}>")
            elif status == "dry_run":
                result.dry_run += 1
                result.messages.append(f"DRY-RUN  {company} <{email}>")
            else:
                result.errors += 1
                err = send_result.get("error_message", "unknown error")
                result.messages.append(f"FAILED   {company} <{email}>: {err}")
        except Exception as exc:
            result.errors += 1
            result.messages.append(f"ERROR    {company} <{email}>: {exc}")

    # Restore original env
    if original_mode is None:
        os.environ.pop("EMAIL_SEND_MODE", None)
    else:
        os.environ["EMAIL_SEND_MODE"] = original_mode

    return result


def trigger_cloud_batch_deploy(limit: int = 0, campaign_ids: list[str] | None = None) -> BatchCloudDeployResult:
    """
    Batch deploy ready runs to GCS for cloud send.

    Uses the same deploy_run_to_gcloud.py logic as the CLI and returns a
    compact result for Streamlit display.
    """
    log.pipeline("trigger_cloud_batch_deploy()", limit=limit, selected=len(campaign_ids or []))
    try:
        from scripts.deploy_run_to_gcloud import _discover_ready_campaigns, deploy_runs
    except Exception:
        return BatchCloudDeployResult(
            success=False,
            messages=[traceback.format_exc()],
        )

    try:
        selected_ids = list(dict.fromkeys(campaign_ids or []))
        if not selected_ids:
            selected_ids = _discover_ready_campaigns(force=False, limit=limit)
        if not selected_ids:
            return BatchCloudDeployResult(
                success=True,
                messages=["No ready runs found for cloud deploy."],
            )

        summary = deploy_runs(selected_ids, force=False)
        messages = [
            f"{row.get('result', '').upper():8} {row.get('campaign_id', '')} {row.get('reason', '')}".rstrip()
            for row in summary.get("results", [])
        ]
        return BatchCloudDeployResult(
            success=int(summary.get("failed", 0)) == 0,
            requested=int(summary.get("requested", 0)),
            deployed=int(summary.get("deployed", 0)),
            skipped=int(summary.get("skipped", 0)),
            failed=int(summary.get("failed", 0)),
            messages=messages,
        )
    except Exception:
        return BatchCloudDeployResult(
            success=False,
            messages=[traceback.format_exc()],
        )
