"""
P1-3C — Policy Visibility / Reporting / Compatibility tests.

Mandatory scenarios:
  1. Queue vs send summary consistency — counts comparable, keys aligned
  2. policy_summary.json written by queue_policy step with all queue-stage keys
  3. campaign_status_summary.json has `policy` section with queue_stage + send_stage
  4. STATUS_FIELDS contains send_policy_action and send_policy_reason
  5. status_merger pulls policy fields from send_log rows
  6. Policy traceability — record traceable from queue_policy.csv to campaign_status.csv
  7. Compatibility — missing policy_summary.json / send_batch_summary.json don't crash
  8. Malformed rows don't destroy reporting
  9. RunPaths has policy_summary_file field
 10. POLICY_SUMMARY_FILE constant in settings
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
from unittest.mock import patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_csv(path: Path, rows: list[dict], fieldnames: list[str] | None = None) -> None:
    fields = fieldnames or (list(rows[0].keys()) if rows else [])
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _make_run_paths(tmp_path: Path, campaign_id: str = "test-p13c") -> "RunPaths":
    from config.run_paths import RunPaths
    run_dir = tmp_path / "runs" / campaign_id
    run_dir.mkdir(parents=True, exist_ok=True)
    return RunPaths(
        campaign_id=campaign_id,
        run_dir=run_dir,
        company_analysis_file=run_dir / "company_analysis.json",
        buyer_filter_file=run_dir / "buyer_filter.json",
        qualified_leads_file=run_dir / "qualified_leads.csv",
        disqualified_leads_file=run_dir / "disqualified_leads.csv",
        enriched_leads_file=run_dir / "enriched_leads.csv",
        enriched_contacts_file=run_dir / "enriched_contacts.csv",
        scored_contacts_file=run_dir / "scored_contacts.csv",
        verified_enriched_leads_file=run_dir / "verified_enriched_leads.csv",
        research_signal_raw_file=run_dir / "research_signal_raw.json",
        research_signals_file=run_dir / "research_signals.json",
        queue_policy_file=run_dir / "queue_policy.csv",
        policy_summary_file=run_dir / "policy_summary.json",
    )


def _make_policy_row(
    place_id: str = "P1",
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


def _make_scored_contact(
    company_name: str = "Test Co",
    place_id: str = "P1",
    kp_email: str = "contact@example.com",
    kp_name: str = "Jane Doe",
    is_primary: str = "true",
    send_eligibility: str = "allow",
) -> dict:
    return {
        "company_name": company_name,
        "place_id": place_id,
        "website": "example.com",
        "kp_email": kp_email,
        "kp_name": kp_name,
        "kp_title": "VP Sales",
        "is_primary_contact": is_primary,
        "is_generic_mailbox": "false",
        "send_eligibility": send_eligibility,
        "send_pool": "primary_pool",
        "email_confidence_tier": "E1",
        "enrichment_source": "apollo",
        "contact_fit_score": "90",
        "contact_priority_rank": "1",
        "contact_selection_reason": "top_ranked",
        "lead_score": "80",
        "qualification_status": "qualified",
        "target_tier": "tier_1",
        "company_type": "installer",
        "market_focus": "commercial",
    }


def _make_send_log_row(
    place_id: str = "P1",
    kp_email: str = "contact@example.com",
    company_name: str = "Test Co",
    send_status: str = "dry_run",
    campaign_id: str = "test-p13c",
    send_policy_action: str = "queue_normal",
    send_policy_reason: str = "verified_e1_allow",
) -> dict:
    return {
        "timestamp": "2026-03-20T10:00:00+00:00",
        "campaign_id": campaign_id,
        "send_mode": "dry_run",
        "company_name": company_name,
        "place_id": place_id,
        "kp_name": "Jane Doe",
        "kp_email": kp_email,
        "subject": "Test Subject",
        "send_decision": "send",
        "send_status": send_status,
        "decision_reason": "All checks passed",
        "provider": "dry_run",
        "provider_message_id": "",
        "error_message": "",
        "tracking_id": "",
        "message_id": "",
        "send_policy_action": send_policy_action,
        "send_policy_reason": send_policy_reason,
    }


# ---------------------------------------------------------------------------
# Tests: RunPaths and settings constants
# ---------------------------------------------------------------------------

class TestRunPathsAndConstants:
    def test_run_paths_has_policy_summary_file_field(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        assert hasattr(paths, "policy_summary_file")
        assert str(paths.policy_summary_file).endswith("policy_summary.json")

    def test_policy_summary_file_under_run_dir(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        assert paths.policy_summary_file.parent == paths.run_dir

    def test_policy_summary_file_constant_in_settings(self):
        from config.settings import POLICY_SUMMARY_FILE
        assert str(POLICY_SUMMARY_FILE).endswith("policy_summary.json")

    def test_for_campaign_includes_policy_summary_file(self, tmp_path):
        from config.settings import RUNS_DIR
        with patch("config.run_paths._runs_dir", return_value=tmp_path / "runs"):
            from config.run_paths import RunPaths
            paths = RunPaths.for_campaign("testcampaign")
        assert hasattr(paths, "policy_summary_file")
        assert paths.policy_summary_file.name == "policy_summary.json"


# ---------------------------------------------------------------------------
# Tests: policy_summary.json written by queue_policy_pipeline
# ---------------------------------------------------------------------------

class TestPolicySummaryJson:
    def _run_queue_policy(self, tmp_path, scored_rows, verified_rows=None):
        from src.workflow_6_queue_policy.queue_policy_pipeline import run
        paths = _make_run_paths(tmp_path)
        _write_csv(paths.scored_contacts_file, scored_rows)
        if verified_rows:
            _write_csv(paths.verified_enriched_leads_file, verified_rows)
        run(paths=paths)
        return paths

    def test_policy_summary_file_written(self, tmp_path):
        scored = [_make_scored_contact()]
        paths = self._run_queue_policy(tmp_path, scored)
        assert paths.policy_summary_file.exists()

    def test_policy_summary_has_generated_at(self, tmp_path):
        scored = [_make_scored_contact()]
        paths = self._run_queue_policy(tmp_path, scored)
        with open(paths.policy_summary_file) as f:
            data = json.load(f)
        assert "generated_at" in data
        assert data["generated_at"]  # non-empty

    def test_policy_summary_has_policy_version(self, tmp_path):
        scored = [_make_scored_contact()]
        paths = self._run_queue_policy(tmp_path, scored)
        with open(paths.policy_summary_file) as f:
            data = json.load(f)
        assert data.get("policy_version") == "v1_deterministic"

    def test_policy_summary_has_queue_stage(self, tmp_path):
        scored = [_make_scored_contact()]
        paths = self._run_queue_policy(tmp_path, scored)
        with open(paths.policy_summary_file) as f:
            data = json.load(f)
        qs = data.get("queue_stage", {})
        for key in ("total", "queue_normal", "queue_limited", "hold",
                    "generic_only", "block", "named_primary", "generic_primary", "errors"):
            assert key in qs, f"Missing queue_stage key: {key}"

    def test_policy_summary_counts_match_output(self, tmp_path):
        scored = [
            _make_scored_contact(company_name="A", place_id="P1", send_eligibility="allow"),
            _make_scored_contact(company_name="B", place_id="P2", send_eligibility="block"),
        ]
        paths = self._run_queue_policy(tmp_path, scored)
        with open(paths.policy_summary_file) as f:
            data = json.load(f)
        qs = data["queue_stage"]
        assert qs["total"] == 2
        assert qs["queue_normal"] == 1
        assert qs["block"] == 1

    def test_policy_summary_written_even_on_empty_input(self, tmp_path):
        """Empty scored_contacts.csv should still produce policy_summary.json."""
        paths = _make_run_paths(tmp_path)
        _write_csv(paths.scored_contacts_file, [])
        from src.workflow_6_queue_policy.queue_policy_pipeline import run
        run(paths=paths)
        assert paths.policy_summary_file.exists()
        with open(paths.policy_summary_file) as f:
            data = json.load(f)
        assert data["queue_stage"]["total"] == 0

    def test_policy_summary_written_when_no_scored_file(self, tmp_path):
        """Missing scored_contacts.csv should still produce policy_summary.json."""
        paths = _make_run_paths(tmp_path)
        # Do NOT create scored_contacts_file
        from src.workflow_6_queue_policy.queue_policy_pipeline import run
        run(paths=paths)
        assert paths.policy_summary_file.exists()


# ---------------------------------------------------------------------------
# Tests: STATUS_FIELDS contains policy columns
# ---------------------------------------------------------------------------

class TestStatusFields:
    def test_status_fields_contains_send_policy_action(self):
        from src.workflow_8_5_campaign_status.status_pipeline import STATUS_FIELDS
        assert "send_policy_action" in STATUS_FIELDS

    def test_status_fields_contains_send_policy_reason(self):
        from src.workflow_8_5_campaign_status.status_pipeline import STATUS_FIELDS
        assert "send_policy_reason" in STATUS_FIELDS

    def test_policy_fields_near_send_fields(self):
        """Policy fields should be adjacent to send fields (not at end)."""
        from src.workflow_8_5_campaign_status.status_pipeline import STATUS_FIELDS
        provider_idx = STATUS_FIELDS.index("initial_provider")
        action_idx   = STATUS_FIELDS.index("send_policy_action")
        # Should be within 3 positions of each other
        assert abs(provider_idx - action_idx) <= 3


# ---------------------------------------------------------------------------
# Tests: status_merger pulls policy fields from send_log
# ---------------------------------------------------------------------------

class TestStatusMergerPolicyFields:
    def test_merger_includes_send_policy_action(self):
        from src.workflow_8_5_campaign_status.status_merger import merge_contact_records
        send_log = _make_send_log_row(
            send_policy_action="queue_normal",
            send_policy_reason="verified_e1_allow",
        )
        tables = {
            "send_logs": {"pid:P1": send_log},
            "engagement": {}, "followup_logs": {},
            "followup_queue": {}, "followup_blocked": {},
            "final_send_queue": {}, "enriched_leads": {},
        }
        merged = merge_contact_records(tables)
        assert len(merged) == 1
        assert merged[0]["send_policy_action"] == "queue_normal"

    def test_merger_includes_send_policy_reason(self):
        from src.workflow_8_5_campaign_status.status_merger import merge_contact_records
        send_log = _make_send_log_row(
            send_policy_action="queue_limited",
            send_policy_reason="unverified_named_email",
        )
        tables = {
            "send_logs": {"pid:P1": send_log},
            "engagement": {}, "followup_logs": {},
            "followup_queue": {}, "followup_blocked": {},
            "final_send_queue": {}, "enriched_leads": {},
        }
        merged = merge_contact_records(tables)
        assert merged[0]["send_policy_reason"] == "unverified_named_email"

    def test_merger_defaults_policy_fields_to_empty_when_missing_from_log(self):
        """Pre-P1-3B send_logs rows without policy fields degrade gracefully."""
        from src.workflow_8_5_campaign_status.status_merger import merge_contact_records
        send_log = _make_send_log_row()
        del send_log["send_policy_action"]
        del send_log["send_policy_reason"]
        tables = {
            "send_logs": {"pid:P1": send_log},
            "engagement": {}, "followup_logs": {},
            "followup_queue": {}, "followup_blocked": {},
            "final_send_queue": {}, "enriched_leads": {},
        }
        merged = merge_contact_records(tables)
        assert merged[0]["send_policy_action"] == ""
        assert merged[0]["send_policy_reason"] == ""


# ---------------------------------------------------------------------------
# Tests: campaign_status_summary.json has policy section
# ---------------------------------------------------------------------------

class TestCampaignStatusPolicySection:
    def _run_status(self, tmp_path, send_logs, policy_summary=None, send_batch=None):
        """Helper: run the status pipeline with controlled inputs."""
        from src.workflow_8_5_campaign_status.status_pipeline import run as status_run
        from src.workflow_8_5_campaign_status.status_pipeline import STATUS_FIELDS

        send_log_path  = tmp_path / "send_logs.csv"
        summary_path   = tmp_path / "campaign_status_summary.json"
        status_csv     = tmp_path / "campaign_status.csv"
        policy_json    = tmp_path / "policy_summary.json"
        batch_json     = tmp_path / "send_batch_summary.json"

        _write_csv(send_log_path, send_logs)

        if policy_summary is not None:
            with open(policy_json, "w") as f:
                json.dump(policy_summary, f)

        if send_batch is not None:
            with open(batch_json, "w") as f:
                json.dump(send_batch, f)

        result = status_run(
            campaign_id="test-p13c",
            send_logs_path=send_log_path,
            engagement_path=tmp_path / "no_engagement.csv",
            followup_logs_path=tmp_path / "no_followup.csv",
            followup_queue_path=tmp_path / "no_queue.csv",
            followup_blocked_path=tmp_path / "no_blocked.csv",
            final_send_queue_path=tmp_path / "no_fsq.csv",
            enriched_leads_path=tmp_path / "no_enriched.csv",
            status_output_path=status_csv,
            summary_output_path=summary_path,
            policy_summary_path=policy_json,
            send_batch_summary_path=batch_json,
        )
        return result, summary_path

    def test_summary_has_policy_key(self, tmp_path):
        rows = [_make_send_log_row()]
        result, _ = self._run_status(tmp_path, rows)
        assert "policy" in result

    def test_summary_policy_has_queue_stage(self, tmp_path):
        policy_data = {
            "queue_stage": {"total": 5, "queue_normal": 3, "block": 2},
        }
        rows = [_make_send_log_row()]
        result, _ = self._run_status(tmp_path, rows, policy_summary=policy_data)
        assert result["policy"]["queue_stage"]["total"] == 5
        assert result["policy"]["queue_stage"]["queue_normal"] == 3

    def test_summary_policy_has_send_stage(self, tmp_path):
        batch_data = {
            "total": 5, "sent": 2, "dry_run": 1, "failed": 0,
            "blocked": 1, "held": 1, "deferred": 0, "breaker_blocked": 0,
            "policy_blocked": 1, "policy_held": 1,
            "policy_queue_limited": 1,
            "policy_queue_normal": 2, "policy_missing": 0,
        }
        rows = [_make_send_log_row()]
        result, _ = self._run_status(tmp_path, rows, send_batch=batch_data)
        ss = result["policy"]["send_stage"]
        assert ss["total"] == 5
        assert ss["policy_blocked"] == 1
        assert ss["policy_queue_normal"] == 2

    def test_summary_policy_section_written_to_json_file(self, tmp_path):
        rows = [_make_send_log_row()]
        _, summary_path = self._run_status(tmp_path, rows)
        with open(summary_path) as f:
            saved = json.load(f)
        assert "policy" in saved

    def test_summary_policy_empty_stages_when_no_files(self, tmp_path):
        """Missing policy_summary.json and send_batch_summary.json → empty sub-dicts."""
        rows = [_make_send_log_row()]
        result, _ = self._run_status(tmp_path, rows)
        assert result["policy"]["queue_stage"] == {}
        assert result["policy"]["send_stage"] == {}

    def test_summary_policy_present_even_with_zero_sent_contacts(self, tmp_path):
        """Status pipeline writes empty outputs when no sent contacts — policy still present."""
        result, _ = self._run_status(tmp_path, [])  # empty send_logs
        assert "policy" in result


# ---------------------------------------------------------------------------
# Tests: cross-stage comparison
# ---------------------------------------------------------------------------

class TestCrossStageComparison:
    def test_queue_stage_total_and_send_stage_total_are_comparable(self, tmp_path):
        """Both queue_stage.total and send_stage.total exist as int-like values."""
        from src.workflow_8_5_campaign_status.status_pipeline import run as status_run

        policy_data = {
            "queue_stage": {"total": 10, "queue_normal": 6, "block": 4},
        }
        batch_data = {
            "total": 10, "sent": 5, "dry_run": 0, "failed": 1,
            "blocked": 4, "held": 0, "deferred": 0, "breaker_blocked": 0,
            "policy_blocked": 4, "policy_held": 0,
            "policy_queue_limited": 1,
            "policy_queue_normal": 5, "policy_missing": 0,
        }

        policy_json = tmp_path / "ps.json"
        batch_json  = tmp_path / "sb.json"
        send_log    = tmp_path / "sl.csv"

        with open(policy_json, "w") as f:
            json.dump(policy_data, f)
        with open(batch_json, "w") as f:
            json.dump(batch_data, f)
        _write_csv(send_log, [_make_send_log_row()])

        result = status_run(
            campaign_id="test-p13c",
            send_logs_path=send_log,
            engagement_path=tmp_path / "no.csv",
            followup_logs_path=tmp_path / "no.csv",
            followup_queue_path=tmp_path / "no.csv",
            followup_blocked_path=tmp_path / "no.csv",
            final_send_queue_path=tmp_path / "no.csv",
            enriched_leads_path=tmp_path / "no.csv",
            status_output_path=tmp_path / "status.csv",
            summary_output_path=tmp_path / "summary.json",
            policy_summary_path=policy_json,
            send_batch_summary_path=batch_json,
        )
        # queue total vs send total should match
        assert result["policy"]["queue_stage"]["total"] == result["policy"]["send_stage"]["total"]
        # policy_blocked should equal block count
        assert result["policy"]["queue_stage"]["block"] == result["policy"]["send_stage"]["policy_blocked"]


# ---------------------------------------------------------------------------
# Tests: policy traceability
# ---------------------------------------------------------------------------

class TestPolicyTraceability:
    def test_queue_policy_to_status_traceable_by_place_id(self, tmp_path):
        """
        A record in queue_policy.csv should be traceable to campaign_status.csv
        via place_id.
        """
        from src.workflow_8_5_campaign_status.status_pipeline import run as status_run

        send_log = tmp_path / "send_logs.csv"
        status_out = tmp_path / "status.csv"
        summary_out = tmp_path / "summary.json"
        policy_json = tmp_path / "ps.json"
        batch_json  = tmp_path / "sb.json"

        send_row = _make_send_log_row(
            place_id="TRACEABLE-P1",
            kp_email="trace@example.com",
            send_policy_action="queue_normal",
            send_policy_reason="verified_e1_allow",
        )
        _write_csv(send_log, [send_row])

        status_run(
            campaign_id="test-p13c",
            send_logs_path=send_log,
            engagement_path=tmp_path / "no.csv",
            followup_logs_path=tmp_path / "no.csv",
            followup_queue_path=tmp_path / "no.csv",
            followup_blocked_path=tmp_path / "no.csv",
            final_send_queue_path=tmp_path / "no.csv",
            enriched_leads_path=tmp_path / "no.csv",
            status_output_path=status_out,
            summary_output_path=summary_out,
            policy_summary_path=policy_json,
            send_batch_summary_path=batch_json,
        )

        with open(status_out, newline="") as f:
            rows = list(csv.DictReader(f))

        assert len(rows) == 1
        r = rows[0]
        assert r["place_id"] == "TRACEABLE-P1"
        assert r["kp_email"] == "trace@example.com"
        assert r["send_policy_action"] == "queue_normal"
        assert r["send_policy_reason"] == "verified_e1_allow"

    def test_policy_reason_visible_alongside_lifecycle_status(self, tmp_path):
        """
        Policy reason should be in the same campaign_status.csv row as lifecycle_status.
        """
        from src.workflow_8_5_campaign_status.status_pipeline import run as status_run

        send_log   = tmp_path / "send_logs.csv"
        status_out = tmp_path / "status.csv"

        send_row = _make_send_log_row(
            send_policy_action="queue_limited",
            send_policy_reason="unverified_named_email",
        )
        _write_csv(send_log, [send_row])

        status_run(
            campaign_id="test-p13c",
            send_logs_path=send_log,
            engagement_path=tmp_path / "no.csv",
            followup_logs_path=tmp_path / "no.csv",
            followup_queue_path=tmp_path / "no.csv",
            followup_blocked_path=tmp_path / "no.csv",
            final_send_queue_path=tmp_path / "no.csv",
            enriched_leads_path=tmp_path / "no.csv",
            status_output_path=status_out,
            summary_output_path=tmp_path / "summary.json",
            policy_summary_path=tmp_path / "no.json",
            send_batch_summary_path=tmp_path / "no.json",
        )

        with open(status_out, newline="") as f:
            rows = list(csv.DictReader(f))

        assert rows[0]["send_policy_action"] == "queue_limited"
        assert rows[0]["lifecycle_status"]   # non-empty (classified)


# ---------------------------------------------------------------------------
# Tests: backward compatibility / graceful degradation
# ---------------------------------------------------------------------------

class TestBackwardCompatibility:
    def test_missing_policy_summary_file_does_not_crash_status(self, tmp_path):
        from src.workflow_8_5_campaign_status.status_pipeline import run as status_run

        send_log = tmp_path / "send_logs.csv"
        _write_csv(send_log, [_make_send_log_row()])

        # policy_summary_path and send_batch_summary_path point to non-existent files
        result = status_run(
            campaign_id="test-p13c",
            send_logs_path=send_log,
            engagement_path=tmp_path / "no.csv",
            followup_logs_path=tmp_path / "no.csv",
            followup_queue_path=tmp_path / "no.csv",
            followup_blocked_path=tmp_path / "no.csv",
            final_send_queue_path=tmp_path / "no.csv",
            enriched_leads_path=tmp_path / "no.csv",
            status_output_path=tmp_path / "status.csv",
            summary_output_path=tmp_path / "summary.json",
            policy_summary_path=tmp_path / "MISSING_ps.json",
            send_batch_summary_path=tmp_path / "MISSING_sb.json",
        )
        assert result is not None
        assert "policy" in result
        assert result["policy"]["queue_stage"] == {}
        assert result["policy"]["send_stage"] == {}

    def test_malformed_policy_summary_json_does_not_crash_status(self, tmp_path):
        from src.workflow_8_5_campaign_status.status_pipeline import run as status_run

        send_log   = tmp_path / "send_logs.csv"
        policy_bad = tmp_path / "bad_ps.json"

        _write_csv(send_log, [_make_send_log_row()])
        policy_bad.write_text("{this is not valid json !!!")

        result = status_run(
            campaign_id="test-p13c",
            send_logs_path=send_log,
            engagement_path=tmp_path / "no.csv",
            followup_logs_path=tmp_path / "no.csv",
            followup_queue_path=tmp_path / "no.csv",
            followup_blocked_path=tmp_path / "no.csv",
            final_send_queue_path=tmp_path / "no.csv",
            enriched_leads_path=tmp_path / "no.csv",
            status_output_path=tmp_path / "status.csv",
            summary_output_path=tmp_path / "summary.json",
            policy_summary_path=policy_bad,
            send_batch_summary_path=tmp_path / "MISSING_sb.json",
        )
        assert result is not None  # no crash

    def test_send_logs_without_policy_fields_degrade_gracefully(self, tmp_path):
        """
        Pre-P1-3B send_logs rows (no send_policy_action column) should produce
        empty policy fields in campaign_status.csv, not a crash.
        """
        from src.workflow_8_5_campaign_status.status_pipeline import run as status_run

        send_log = tmp_path / "send_logs.csv"
        # Build a row WITHOUT policy fields
        row = {
            "timestamp": "2026-03-20T10:00:00+00:00",
            "campaign_id": "test-p13c",
            "send_mode": "dry_run",
            "company_name": "Old Co",
            "place_id": "OLD-P1",
            "kp_name": "Alice",
            "kp_email": "alice@old.com",
            "subject": "Old Subject",
            "send_decision": "send",
            "send_status": "dry_run",
            "decision_reason": "All checks passed",
            "provider": "dry_run",
            "provider_message_id": "",
            "error_message": "",
            "tracking_id": "",
            "message_id": "",
            # Note: no send_policy_action or send_policy_reason columns
        }
        _write_csv(send_log, [row], fieldnames=list(row.keys()))

        status_out = tmp_path / "status.csv"
        result = status_run(
            campaign_id="test-p13c",
            send_logs_path=send_log,
            engagement_path=tmp_path / "no.csv",
            followup_logs_path=tmp_path / "no.csv",
            followup_queue_path=tmp_path / "no.csv",
            followup_blocked_path=tmp_path / "no.csv",
            final_send_queue_path=tmp_path / "no.csv",
            enriched_leads_path=tmp_path / "no.csv",
            status_output_path=status_out,
            summary_output_path=tmp_path / "summary.json",
            policy_summary_path=tmp_path / "MISSING_ps.json",
            send_batch_summary_path=tmp_path / "MISSING_sb.json",
        )
        assert result is not None
        with open(status_out, newline="") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 1
        # Policy fields should be empty (not missing key or crash)
        assert rows[0].get("send_policy_action", "") == ""
