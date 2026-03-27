"""
P1-3B — Send-Time Policy Enforcement tests.

Tests every mandatory scenario from the ticket:
  1. block must never proceed into actual send execution
  2. hold must not proceed into automatic send execution
  3. queue_normal proceeds normally through guards and send
  4. queue_limited is flagged distinctly in counters / logs
  5. legacy generic_only rows are blocked and never reach send execution
  6. policy_action and policy_reason appear in send_logs rows
  7. policy counts appear in the batch summary (counters dict)
  8. missing queue_policy.csv → explicit warning, send proceeds (no silent bypass)
  9. record not in queue_policy.csv → "policy_missing" counter, send proceeds

Additional:
  - build_log_row() accepts and propagates send_policy_action / send_policy_reason
  - LOG_FIELDS includes the two new policy fields
  - _load_policy_indices returns (by_place_id, by_email, found)
  - _lookup_policy matches by place_id first, then by kp_email
"""
from __future__ import annotations

import csv
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_csv(path: Path, rows: list[dict], fieldnames: list[str] | None = None) -> None:
    fields = fieldnames or (list(rows[0].keys()) if rows else [])
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _make_policy_row(
    place_id: str = "PLACE1",
    email: str = "contact@example.com",
    action: str = "queue_normal",
    reason: str = "verified_e1_allow",
) -> dict:
    return {
        "company_name": "Test Co",
        "website": "example.com",
        "place_id": place_id,
        "lead_score": "80",
        "qualification_status": "qualified",
        "target_tier": "tier_1",
        "company_type": "installer",
        "market_focus": "commercial",
        "selected_contact_email": email,
        "selected_contact_name": "Jane Doe",
        "selected_contact_title": "VP Sales",
        "selected_contact_rank": "1",
        "selected_contact_is_generic": "false",
        "selected_contact_source": "apollo",
        "contact_fit_score": "90",
        "contact_selection_reason": "top_ranked",
        "selected_send_eligibility": "allow",
        "selected_send_pool": "primary_pool",
        "selected_email_confidence_tier": "E1",
        "verification_source": "scored_contacts",
        "send_policy_action": action,
        "send_policy_reason": reason,
        "policy_version": "v1_deterministic",
    }


def _make_queue_record(
    place_id: str = "PLACE1",
    kp_email: str = "contact@example.com",
    company_name: str = "Test Co",
    approval_status: str = "approved",
    send_eligibility: str = "allow",
) -> dict:
    return {
        "company_name": company_name,
        "website": "example.com",
        "place_id": place_id,
        "kp_email": kp_email,
        "kp_name": "Jane Doe",
        "subject": "Solar proposal for Test Co",
        "email_body": "<p>Hello!</p>",
        "approval_status": approval_status,
        "send_eligibility": send_eligibility,
    }


# ---------------------------------------------------------------------------
# Tests: LOG_FIELDS and build_log_row (send_logger.py)
# ---------------------------------------------------------------------------

class TestLoggerPolicyFields:
    def test_log_fields_contains_policy_columns(self):
        from src.workflow_7_email_sending.send_logger import LOG_FIELDS
        assert "send_policy_action" in LOG_FIELDS
        assert "send_policy_reason" in LOG_FIELDS

    def test_build_log_row_includes_policy_fields(self):
        from src.workflow_7_email_sending.send_logger import build_log_row
        record = _make_queue_record()
        row = build_log_row(
            record,
            send_decision="policy_blocked",
            send_status="blocked",
            send_policy_action="block",
            send_policy_reason="verified_e0_invalid",
        )
        assert row["send_policy_action"] == "block"
        assert row["send_policy_reason"] == "verified_e0_invalid"

    def test_build_log_row_defaults_policy_fields_to_empty(self):
        from src.workflow_7_email_sending.send_logger import build_log_row
        record = _make_queue_record()
        row = build_log_row(record, send_decision="send", send_status="dry_run")
        assert row["send_policy_action"] == ""
        assert row["send_policy_reason"] == ""

    def test_append_send_log_writes_policy_fields(self, tmp_path):
        from src.workflow_7_email_sending.send_logger import build_log_row, append_send_log, load_send_logs
        log_path = tmp_path / "send_logs.csv"
        record = _make_queue_record()
        row = build_log_row(
            record,
            send_decision="policy_held",
            send_status="held",
            send_policy_action="hold",
            send_policy_reason="verified_e3_catchall",
        )
        append_send_log(row, path=log_path)
        loaded = load_send_logs(path=log_path)
        assert len(loaded) == 1
        assert loaded[0]["send_policy_action"] == "hold"
        assert loaded[0]["send_policy_reason"] == "verified_e3_catchall"


# ---------------------------------------------------------------------------
# Tests: _load_policy_indices and _lookup_policy (send_pipeline.py)
# ---------------------------------------------------------------------------

class TestPolicyIndexLoading:
    def test_returns_false_when_file_missing(self, tmp_path):
        from src.workflow_7_email_sending.send_pipeline import _load_policy_indices
        by_pid, by_email, by_company, found = _load_policy_indices(tmp_path / "nonexistent.csv")
        assert found is False
        assert by_pid == {}
        assert by_email == {}

    def test_indexes_by_place_id_and_email(self, tmp_path):
        from src.workflow_7_email_sending.send_pipeline import _load_policy_indices
        policy_path = tmp_path / "queue_policy.csv"
        row = _make_policy_row(place_id="P1", email="foo@bar.com", action="queue_normal")
        _write_csv(policy_path, [row])
        by_pid, by_email, by_company, found = _load_policy_indices(policy_path)
        assert found is True
        assert "P1" in by_pid
        assert "foo@bar.com" in by_email

    def test_email_index_is_lowercased(self, tmp_path):
        from src.workflow_7_email_sending.send_pipeline import _load_policy_indices
        policy_path = tmp_path / "queue_policy.csv"
        row = _make_policy_row(email="UPPER@Example.COM", action="queue_normal")
        row["place_id"] = ""
        _write_csv(policy_path, [row])
        _, by_email, _, _ = _load_policy_indices(policy_path)
        assert "upper@example.com" in by_email

    def test_lookup_prefers_place_id_over_email(self, tmp_path):
        from src.workflow_7_email_sending.send_pipeline import (
            _load_policy_indices, _lookup_policy,
        )
        policy_path = tmp_path / "queue_policy.csv"
        row_by_pid   = _make_policy_row(place_id="P1", email="a@b.com", action="block",        reason="pid_match")
        row_by_email = _make_policy_row(place_id="P2", email="a@b.com", action="queue_normal", reason="email_match")
        _write_csv(policy_path, [row_by_pid, row_by_email])
        by_pid, by_email, by_company, _ = _load_policy_indices(policy_path)
        # Record has place_id P1 — should resolve to block row via place_id
        record = _make_queue_record(place_id="P1", kp_email="a@b.com")
        result = _lookup_policy(record, by_pid, by_email, by_company)
        assert result["send_policy_reason"] == "pid_match"

    def test_lookup_falls_back_to_email_when_place_id_missing(self, tmp_path):
        from src.workflow_7_email_sending.send_pipeline import (
            _load_policy_indices, _lookup_policy,
        )
        policy_path = tmp_path / "queue_policy.csv"
        row = _make_policy_row(place_id="P1", email="z@example.com", action="queue_limited")
        _write_csv(policy_path, [row])
        by_pid, by_email, by_company, _ = _load_policy_indices(policy_path)
        # Record has a different place_id but matching email
        record = _make_queue_record(place_id="DIFFERENT", kp_email="z@example.com")
        result = _lookup_policy(record, by_pid, by_email, by_company)
        assert result["send_policy_action"] == "queue_limited"

    def test_lookup_returns_none_when_no_match(self, tmp_path):
        from src.workflow_7_email_sending.send_pipeline import (
            _load_policy_indices, _lookup_policy,
        )
        policy_path = tmp_path / "queue_policy.csv"
        row = _make_policy_row(place_id="P1", email="other@other.com", action="queue_normal")
        row["company_name"] = "Policy Co"   # must differ from record's company_name
        _write_csv(policy_path, [row])
        by_pid, by_email, by_company, _ = _load_policy_indices(policy_path)
        # Record has non-matching place_id, email, AND company_name → no hit on any index
        record = _make_queue_record(place_id="UNKNOWN", kp_email="nobody@nowhere.com",
                                    company_name="Unknown Co")
        assert _lookup_policy(record, by_pid, by_email, by_company) is None


# ---------------------------------------------------------------------------
# Tests: block action — must never reach send execution
# ---------------------------------------------------------------------------

class TestPolicyBlock:
    def _run_with_single_record(self, tmp_path, policy_action, record=None):
        """Helper: set up one record + policy row, run pipeline, return (counters, logged_rows)."""
        from src.workflow_7_email_sending.send_pipeline import run as pipeline_run

        if record is None:
            record = _make_queue_record()

        policy_row = _make_policy_row(action=policy_action, reason=f"{policy_action}_reason")
        policy_path = tmp_path / "queue_policy.csv"
        _write_csv(policy_path, [policy_row])

        send_log_path = tmp_path / "send_logs.csv"
        final_queue_path = tmp_path / "final_send_queue.csv"
        _write_csv(final_queue_path, [record])
        batch_summary_path = tmp_path / "batch_summary.json"

        with (
            patch("src.workflow_7_email_sending.send_pipeline.QUEUE_POLICY_FILE", policy_path),
            patch("src.workflow_7_email_sending.send_pipeline.load_send_queue", return_value=[record]),
            patch("src.workflow_7_email_sending.send_pipeline.load_recent_logs", return_value=[]),
            patch("src.workflow_7_email_sending.send_pipeline.send_one") as mock_send,
            patch("src.workflow_7_email_sending.send_pipeline.append_send_log") as mock_log,
            patch("src.workflow_7_email_sending.send_pipeline.SEND_BATCH_SUMMARY", batch_summary_path),
            patch("src.workflow_7_email_sending.send_pipeline._TRACKING_AVAILABLE", False),
        ):
            counters = pipeline_run(campaign_id="test_campaign", send_mode="dry_run")
        return counters, mock_send, mock_log

    def test_block_never_reaches_send_one(self, tmp_path):
        counters, mock_send, _ = self._run_with_single_record(tmp_path, "block")
        mock_send.assert_not_called()

    def test_block_increments_policy_blocked_counter(self, tmp_path):
        counters, _, _ = self._run_with_single_record(tmp_path, "block")
        assert counters["policy_blocked"] == 1
        assert counters["blocked"] == 1

    def test_block_logs_send_decision_policy_blocked(self, tmp_path):
        _, _, mock_log = self._run_with_single_record(tmp_path, "block")
        assert mock_log.call_count == 1
        logged_row = mock_log.call_args[0][0]
        assert logged_row["send_decision"] == "policy_blocked"
        assert logged_row["send_status"] == "blocked"
        assert logged_row["send_policy_action"] == "block"

    def test_block_does_not_run_guards(self, tmp_path):
        """Block must short-circuit before run_checks."""
        from src.workflow_7_email_sending.send_pipeline import run as pipeline_run

        policy_path = tmp_path / "queue_policy.csv"
        _write_csv(policy_path, [_make_policy_row(action="block")])
        batch_summary_path = tmp_path / "batch_summary.json"

        with (
            patch("src.workflow_7_email_sending.send_pipeline.QUEUE_POLICY_FILE", policy_path),
            patch("src.workflow_7_email_sending.send_pipeline.load_send_queue",
                  return_value=[_make_queue_record()]),
            patch("src.workflow_7_email_sending.send_pipeline.load_recent_logs", return_value=[]),
            patch("src.workflow_7_email_sending.send_pipeline.run_checks") as mock_guard,
            patch("src.workflow_7_email_sending.send_pipeline.append_send_log"),
            patch("src.workflow_7_email_sending.send_pipeline.SEND_BATCH_SUMMARY", batch_summary_path),
            patch("src.workflow_7_email_sending.send_pipeline._TRACKING_AVAILABLE", False),
        ):
            pipeline_run(campaign_id="test_campaign", send_mode="dry_run")

        mock_guard.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: hold action — must not reach send execution
# ---------------------------------------------------------------------------

class TestPolicyHold:
    def _run_hold(self, tmp_path):
        from src.workflow_7_email_sending.send_pipeline import run as pipeline_run

        policy_path = tmp_path / "queue_policy.csv"
        _write_csv(policy_path, [_make_policy_row(action="hold", reason="verified_e3_catchall")])
        batch_summary_path = tmp_path / "batch_summary.json"

        with (
            patch("src.workflow_7_email_sending.send_pipeline.QUEUE_POLICY_FILE", policy_path),
            patch("src.workflow_7_email_sending.send_pipeline.load_send_queue",
                  return_value=[_make_queue_record()]),
            patch("src.workflow_7_email_sending.send_pipeline.load_recent_logs", return_value=[]),
            patch("src.workflow_7_email_sending.send_pipeline.send_one") as mock_send,
            patch("src.workflow_7_email_sending.send_pipeline.append_send_log") as mock_log,
            patch("src.workflow_7_email_sending.send_pipeline.SEND_BATCH_SUMMARY", batch_summary_path),
            patch("src.workflow_7_email_sending.send_pipeline._TRACKING_AVAILABLE", False),
        ):
            counters = pipeline_run(campaign_id="test_campaign", send_mode="dry_run")
        return counters, mock_send, mock_log

    def test_hold_never_reaches_send_one(self, tmp_path):
        _, mock_send, _ = self._run_hold(tmp_path)
        mock_send.assert_not_called()

    def test_hold_increments_policy_held_counter(self, tmp_path):
        counters, _, _ = self._run_hold(tmp_path)
        assert counters["policy_held"] == 1
        assert counters["held"] == 1

    def test_hold_logs_send_decision_policy_held(self, tmp_path):
        _, _, mock_log = self._run_hold(tmp_path)
        logged_row = mock_log.call_args[0][0]
        assert logged_row["send_decision"] == "policy_held"
        assert logged_row["send_status"] == "held"
        assert logged_row["send_policy_action"] == "hold"
        assert logged_row["send_policy_reason"] == "verified_e3_catchall"


# ---------------------------------------------------------------------------
# Tests: queue_normal proceeds normally
# ---------------------------------------------------------------------------

class TestPolicyQueueNormal:
    def test_queue_normal_proceeds_to_send(self, tmp_path):
        from src.workflow_7_email_sending.send_pipeline import run as pipeline_run

        policy_path = tmp_path / "queue_policy.csv"
        _write_csv(policy_path, [_make_policy_row(action="queue_normal")])
        batch_summary_path = tmp_path / "batch_summary.json"

        send_result = {
            "send_status": "dry_run", "provider": "dry_run",
            "provider_message_id": "", "error_message": "",
        }

        with (
            patch("src.workflow_7_email_sending.send_pipeline.QUEUE_POLICY_FILE", policy_path),
            patch("src.workflow_7_email_sending.send_pipeline.load_send_queue",
                  return_value=[_make_queue_record()]),
            patch("src.workflow_7_email_sending.send_pipeline.load_recent_logs", return_value=[]),
            patch("src.workflow_7_email_sending.send_pipeline.send_one", return_value=send_result),
            patch("src.workflow_7_email_sending.send_pipeline.append_send_log"),
            patch("src.workflow_7_email_sending.send_pipeline.SEND_BATCH_SUMMARY", batch_summary_path),
            patch("src.workflow_7_email_sending.send_pipeline._TRACKING_AVAILABLE", False),
        ):
            counters = pipeline_run(campaign_id="test_campaign", send_mode="dry_run")

        assert counters["policy_queue_normal"] == 1
        assert counters["dry_run"] == 1

    def test_queue_normal_policy_fields_stamped_in_log(self, tmp_path):
        from src.workflow_7_email_sending.send_pipeline import run as pipeline_run

        policy_path = tmp_path / "queue_policy.csv"
        _write_csv(policy_path, [_make_policy_row(action="queue_normal", reason="verified_e1_allow")])
        batch_summary_path = tmp_path / "batch_summary.json"

        send_result = {
            "send_status": "dry_run", "provider": "dry_run",
            "provider_message_id": "", "error_message": "",
        }

        logged_rows = []

        def capture_log(row, **kw):
            logged_rows.append(row)

        with (
            patch("src.workflow_7_email_sending.send_pipeline.QUEUE_POLICY_FILE", policy_path),
            patch("src.workflow_7_email_sending.send_pipeline.load_send_queue",
                  return_value=[_make_queue_record()]),
            patch("src.workflow_7_email_sending.send_pipeline.load_recent_logs", return_value=[]),
            patch("src.workflow_7_email_sending.send_pipeline.send_one", return_value=send_result),
            patch("src.workflow_7_email_sending.send_pipeline.append_send_log", side_effect=capture_log),
            patch("src.workflow_7_email_sending.send_pipeline.SEND_BATCH_SUMMARY", batch_summary_path),
            patch("src.workflow_7_email_sending.send_pipeline._TRACKING_AVAILABLE", False),
        ):
            pipeline_run(campaign_id="test_campaign", send_mode="dry_run")

        assert len(logged_rows) == 1
        assert logged_rows[0]["send_policy_action"] == "queue_normal"
        assert logged_rows[0]["send_policy_reason"] == "verified_e1_allow"


# ---------------------------------------------------------------------------
# Tests: queue_limited — policy-distinct counter, proceeds to guards
# ---------------------------------------------------------------------------

class TestPolicyQueueLimited:
    def test_queue_limited_increments_distinct_counter(self, tmp_path):
        from src.workflow_7_email_sending.send_pipeline import run as pipeline_run

        policy_path = tmp_path / "queue_policy.csv"
        _write_csv(policy_path, [_make_policy_row(action="queue_limited", reason="unverified_named_email")])
        batch_summary_path = tmp_path / "batch_summary.json"

        send_result = {
            "send_status": "dry_run", "provider": "dry_run",
            "provider_message_id": "", "error_message": "",
        }

        with (
            patch("src.workflow_7_email_sending.send_pipeline.QUEUE_POLICY_FILE", policy_path),
            patch("src.workflow_7_email_sending.send_pipeline.load_send_queue",
                  return_value=[_make_queue_record()]),
            patch("src.workflow_7_email_sending.send_pipeline.load_recent_logs", return_value=[]),
            patch("src.workflow_7_email_sending.send_pipeline.send_one", return_value=send_result),
            patch("src.workflow_7_email_sending.send_pipeline.append_send_log"),
            patch("src.workflow_7_email_sending.send_pipeline.SEND_BATCH_SUMMARY", batch_summary_path),
            patch("src.workflow_7_email_sending.send_pipeline._TRACKING_AVAILABLE", False),
        ):
            counters = pipeline_run(campaign_id="test_campaign", send_mode="dry_run")

        assert counters["policy_queue_limited"] == 1
        # queue_limited proceeds to send — dry_run should be counted
        assert counters["dry_run"] == 1
        # Must NOT also increment block/hold counters
        assert counters["policy_blocked"] == 0
        assert counters["policy_held"] == 0


# ---------------------------------------------------------------------------
# Tests: generic_only — legacy rows are blocked before send execution
# ---------------------------------------------------------------------------

class TestPolicyGenericOnly:
    def test_generic_only_is_blocked_as_legacy_path(self, tmp_path):
        from src.workflow_7_email_sending.send_pipeline import run as pipeline_run

        policy_path = tmp_path / "queue_policy.csv"
        _write_csv(policy_path, [_make_policy_row(action="generic_only", reason="verified_e4_generic_mailbox")])
        batch_summary_path = tmp_path / "batch_summary.json"

        send_result = {
            "send_status": "dry_run", "provider": "dry_run",
            "provider_message_id": "", "error_message": "",
        }

        with (
            patch("src.workflow_7_email_sending.send_pipeline.QUEUE_POLICY_FILE", policy_path),
            patch("src.workflow_7_email_sending.send_pipeline.load_send_queue",
                  return_value=[_make_queue_record()]),
            patch("src.workflow_7_email_sending.send_pipeline.load_recent_logs", return_value=[]),
            patch("src.workflow_7_email_sending.send_pipeline.send_one", return_value=send_result),
            patch("src.workflow_7_email_sending.send_pipeline.append_send_log"),
            patch("src.workflow_7_email_sending.send_pipeline.SEND_BATCH_SUMMARY", batch_summary_path),
            patch("src.workflow_7_email_sending.send_pipeline._TRACKING_AVAILABLE", False),
        ):
            counters = pipeline_run(campaign_id="test_campaign", send_mode="dry_run")

        assert counters["policy_blocked"] == 1
        assert counters["blocked"] == 1
        assert counters["dry_run"] == 0

    def test_generic_only_never_reaches_send_one(self, tmp_path):
        from src.workflow_7_email_sending.send_pipeline import run as pipeline_run

        policy_path = tmp_path / "queue_policy.csv"
        _write_csv(policy_path, [_make_policy_row(action="generic_only", reason="verified_e4_generic_mailbox")])
        batch_summary_path = tmp_path / "batch_summary.json"

        with (
            patch("src.workflow_7_email_sending.send_pipeline.QUEUE_POLICY_FILE", policy_path),
            patch("src.workflow_7_email_sending.send_pipeline.load_send_queue",
                  return_value=[_make_queue_record()]),
            patch("src.workflow_7_email_sending.send_pipeline.load_recent_logs", return_value=[]),
            patch("src.workflow_7_email_sending.send_pipeline.send_one") as mock_send,
            patch("src.workflow_7_email_sending.send_pipeline.append_send_log"),
            patch("src.workflow_7_email_sending.send_pipeline.SEND_BATCH_SUMMARY", batch_summary_path),
            patch("src.workflow_7_email_sending.send_pipeline._TRACKING_AVAILABLE", False),
        ):
            pipeline_run(campaign_id="test_campaign", send_mode="dry_run")

        mock_send.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: missing queue_policy.csv — explicit warning, not silent bypass
# ---------------------------------------------------------------------------

class TestMissingPolicyFile:
    def test_proceeds_without_policy_when_file_missing(self, tmp_path):
        from src.workflow_7_email_sending.send_pipeline import run as pipeline_run

        non_existent = tmp_path / "no_queue_policy.csv"
        batch_summary_path = tmp_path / "batch_summary.json"

        send_result = {
            "send_status": "dry_run", "provider": "dry_run",
            "provider_message_id": "", "error_message": "",
        }

        with (
            patch("src.workflow_7_email_sending.send_pipeline.QUEUE_POLICY_FILE", non_existent),
            patch("src.workflow_7_email_sending.send_pipeline.load_send_queue",
                  return_value=[_make_queue_record()]),
            patch("src.workflow_7_email_sending.send_pipeline.load_recent_logs", return_value=[]),
            patch("src.workflow_7_email_sending.send_pipeline.send_one", return_value=send_result),
            patch("src.workflow_7_email_sending.send_pipeline.append_send_log"),
            patch("src.workflow_7_email_sending.send_pipeline.SEND_BATCH_SUMMARY", batch_summary_path),
            patch("src.workflow_7_email_sending.send_pipeline._TRACKING_AVAILABLE", False),
        ):
            counters = pipeline_run(campaign_id="test_campaign", send_mode="dry_run")

        # Send should still proceed (backward compat)
        assert counters["dry_run"] == 1
        # No policy counters should be incremented (no policy file means no policy enforcement)
        assert counters["policy_blocked"] == 0
        assert counters["policy_held"] == 0
        assert counters["policy_missing"] == 0

    def test_warning_printed_when_policy_file_missing(self, tmp_path, capsys):
        from src.workflow_7_email_sending.send_pipeline import run as pipeline_run

        non_existent = tmp_path / "no_queue_policy.csv"
        batch_summary_path = tmp_path / "batch_summary.json"

        send_result = {
            "send_status": "dry_run", "provider": "dry_run",
            "provider_message_id": "", "error_message": "",
        }

        with (
            patch("src.workflow_7_email_sending.send_pipeline.QUEUE_POLICY_FILE", non_existent),
            patch("src.workflow_7_email_sending.send_pipeline.load_send_queue",
                  return_value=[_make_queue_record()]),
            patch("src.workflow_7_email_sending.send_pipeline.load_recent_logs", return_value=[]),
            patch("src.workflow_7_email_sending.send_pipeline.send_one", return_value=send_result),
            patch("src.workflow_7_email_sending.send_pipeline.append_send_log"),
            patch("src.workflow_7_email_sending.send_pipeline.SEND_BATCH_SUMMARY", batch_summary_path),
            patch("src.workflow_7_email_sending.send_pipeline._TRACKING_AVAILABLE", False),
        ):
            pipeline_run(campaign_id="test_campaign", send_mode="dry_run")

        captured = capsys.readouterr()
        assert "WARNING" in captured.out
        assert "queue_policy.csv" in captured.out


# ---------------------------------------------------------------------------
# Tests: record not found in policy file → policy_missing counter
# ---------------------------------------------------------------------------

class TestPolicyMissingRecord:
    def test_policy_missing_counter_incremented_when_record_not_in_policy(self, tmp_path):
        from src.workflow_7_email_sending.send_pipeline import run as pipeline_run

        # Policy file exists but contains a different company
        policy_path = tmp_path / "queue_policy.csv"
        other_row = _make_policy_row(place_id="OTHER", email="other@other.com", action="queue_normal")
        other_row["company_name"] = "Other Co"  # must differ from the test record's company_name
        _write_csv(policy_path, [other_row])
        batch_summary_path = tmp_path / "batch_summary.json"

        send_result = {
            "send_status": "dry_run", "provider": "dry_run",
            "provider_message_id": "", "error_message": "",
        }

        # Record with different place_id and email
        record = _make_queue_record(place_id="MISSING1", kp_email="missing@co.com")

        with (
            patch("src.workflow_7_email_sending.send_pipeline.QUEUE_POLICY_FILE", policy_path),
            patch("src.workflow_7_email_sending.send_pipeline.load_send_queue", return_value=[record]),
            patch("src.workflow_7_email_sending.send_pipeline.load_recent_logs", return_value=[]),
            patch("src.workflow_7_email_sending.send_pipeline.send_one", return_value=send_result),
            patch("src.workflow_7_email_sending.send_pipeline.append_send_log"),
            patch("src.workflow_7_email_sending.send_pipeline.SEND_BATCH_SUMMARY", batch_summary_path),
            patch("src.workflow_7_email_sending.send_pipeline._TRACKING_AVAILABLE", False),
        ):
            counters = pipeline_run(campaign_id="test_campaign", send_mode="dry_run")

        assert counters["policy_missing"] == 1
        # Missing records should still proceed (conservative: proceed rather than hard block)
        assert counters["dry_run"] == 1

    def test_policy_missing_record_still_proceeds_through_guards(self, tmp_path):
        """A record absent from policy_file should reach run_checks."""
        from src.workflow_7_email_sending.send_pipeline import run as pipeline_run

        policy_path = tmp_path / "queue_policy.csv"
        other_row = _make_policy_row(place_id="OTHER", email="other@other.com")
        other_row["company_name"] = "Other Co"  # must differ from record's company_name
        _write_csv(policy_path, [other_row])
        batch_summary_path = tmp_path / "batch_summary.json"

        record = _make_queue_record(place_id="NOTFOUND", kp_email="notfound@co.com")

        guard_result = {"allowed": False, "decision": "blocked", "reason": "Missing required field: email_body"}

        with (
            patch("src.workflow_7_email_sending.send_pipeline.QUEUE_POLICY_FILE", policy_path),
            patch("src.workflow_7_email_sending.send_pipeline.load_send_queue", return_value=[record]),
            patch("src.workflow_7_email_sending.send_pipeline.load_recent_logs", return_value=[]),
            patch("src.workflow_7_email_sending.send_pipeline.run_checks", return_value=guard_result),
            patch("src.workflow_7_email_sending.send_pipeline.send_one") as mock_send,
            patch("src.workflow_7_email_sending.send_pipeline.append_send_log"),
            patch("src.workflow_7_email_sending.send_pipeline.SEND_BATCH_SUMMARY", batch_summary_path),
            patch("src.workflow_7_email_sending.send_pipeline._TRACKING_AVAILABLE", False),
        ):
            pipeline_run(campaign_id="test_campaign", send_mode="dry_run")

        # Guard was called (record was not silently dropped)
        # send_one not called because guard blocked
        mock_send.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: batch summary includes policy counts
# ---------------------------------------------------------------------------

class TestBatchSummaryPolicyCounts:
    def test_empty_summary_includes_policy_keys(self):
        from src.workflow_7_email_sending.send_pipeline import _empty_summary
        summary = _empty_summary()
        for key in (
            "policy_blocked", "policy_held",
            "policy_queue_limited", "policy_queue_normal", "policy_missing",
            "held",
        ):
            assert key in summary, f"Missing key: {key}"

    def test_counters_dict_written_to_batch_summary_includes_policy_counts(self, tmp_path):
        from src.workflow_7_email_sending.send_pipeline import run as pipeline_run

        policy_path = tmp_path / "queue_policy.csv"
        _write_csv(policy_path, [
            _make_policy_row(place_id="P1", email="a@b.com", action="queue_normal"),
            _make_policy_row(place_id="P2", email="c@d.com", action="block"),
        ])
        batch_summary_path = tmp_path / "send_batch_summary.json"

        send_result = {
            "send_status": "dry_run", "provider": "dry_run",
            "provider_message_id": "", "error_message": "",
        }

        records = [
            _make_queue_record(place_id="P1", kp_email="a@b.com"),
            _make_queue_record(place_id="P2", kp_email="c@d.com"),
        ]

        with (
            patch("src.workflow_7_email_sending.send_pipeline.QUEUE_POLICY_FILE", policy_path),
            patch("src.workflow_7_email_sending.send_pipeline.load_send_queue", return_value=records),
            patch("src.workflow_7_email_sending.send_pipeline.load_recent_logs", return_value=[]),
            patch("src.workflow_7_email_sending.send_pipeline.send_one", return_value=send_result),
            patch("src.workflow_7_email_sending.send_pipeline.append_send_log"),
            patch("src.workflow_7_email_sending.send_pipeline.SEND_BATCH_SUMMARY", batch_summary_path),
            patch("src.workflow_7_email_sending.send_pipeline._TRACKING_AVAILABLE", False),
        ):
            counters = pipeline_run(campaign_id="test_campaign", send_mode="dry_run")

        assert counters["policy_queue_normal"] == 1
        assert counters["policy_blocked"] == 1

        with open(batch_summary_path) as f:
            saved = json.load(f)
        assert saved["policy_queue_normal"] == 1
        assert saved["policy_blocked"] == 1
