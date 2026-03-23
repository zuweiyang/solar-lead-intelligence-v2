"""
Regression tests for the run-scoped path architecture.

Covers:
  - Two consecutive campaigns do not overwrite each other's artifacts
  - Resume reads only the target run directory
  - CRM logs are written only to data/crm/
  - Campaign artifacts are written only to data/runs/<campaign_id>/
  - No unexpected writes to the legacy shared data root
  - _RunPath / _CrmPath support the actual Path operations used by the codebase
"""
from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

import pytest

# Ensure project root is on sys.path when running from any directory
_PROJECT_ROOT = Path(__file__).parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import config.run_context as run_context
from config.settings import (
    DATA_DIR,
    RUNS_DIR,
    CRM_DIR,
    # process-level (real Path — must never move)
    DATABASE_FILE,
    CAMPAIGN_LOCK_FILE,
    CAMPAIGN_RUN_STATE_FILE,
    # campaign-scoped (_RunPath)
    SEARCH_TASKS_FILE,
    RAW_LEADS_FILE,
    COMPANY_PAGES_FILE,
    COMPANY_TEXT_FILE,
    COMPANY_ANALYSIS_FILE,
    QUALIFIED_LEADS_FILE,
    ENRICHED_LEADS_FILE,
    GENERATED_EMAILS_FILE,
    SCORED_EMAILS_FILE,
    SEND_QUEUE_FILE,
    REJECTED_EMAILS_FILE,
    REPAIRED_EMAILS_FILE,
    RESCORED_EMAILS_FILE,
    FINAL_SEND_QUEUE_FILE,
    FINAL_REJECTED_FILE,
    SEND_BATCH_SUMMARY,
    COMPANY_OPENINGS_FILE,
    COMPANY_SIGNALS_FILE,
    FOLLOWUP_CANDIDATES_FILE,
    FOLLOWUP_QUEUE_FILE,
    FOLLOWUP_BLOCKED_FILE,
    CAMPAIGN_STATUS_FILE,
    CAMPAIGN_STATUS_SUMMARY,
    CAMPAIGN_RUNNER_LOGS_FILE,
    EMAIL_REPAIR_ERRORS_FILE,
    ENGAGEMENT_SUMMARY_FILE,
    # global CRM (_CrmPath)
    SEND_LOGS_FILE,
    ENGAGEMENT_LOGS_FILE,
    FOLLOWUP_LOGS_FILE,
    CRM_DATABASE_FILE,
    # proxy classes for isinstance checks
    _RunPath,
    _CrmPath,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def clear_run_context():
    """Ensure run context is cleared before and after every test."""
    run_context.clear_active_run()
    yield
    run_context.clear_active_run()


# ---------------------------------------------------------------------------
# 1. Proxy class identity checks
# ---------------------------------------------------------------------------

class TestProxyTypes:
    """All settings constants are the correct proxy type."""

    _RUN_SCOPED = [
        SEARCH_TASKS_FILE, RAW_LEADS_FILE, COMPANY_PAGES_FILE,
        COMPANY_TEXT_FILE, COMPANY_ANALYSIS_FILE, QUALIFIED_LEADS_FILE,
        ENRICHED_LEADS_FILE, GENERATED_EMAILS_FILE, SCORED_EMAILS_FILE,
        SEND_QUEUE_FILE, REJECTED_EMAILS_FILE, REPAIRED_EMAILS_FILE,
        RESCORED_EMAILS_FILE, FINAL_SEND_QUEUE_FILE, FINAL_REJECTED_FILE,
        SEND_BATCH_SUMMARY, COMPANY_OPENINGS_FILE, COMPANY_SIGNALS_FILE,
        FOLLOWUP_CANDIDATES_FILE, FOLLOWUP_QUEUE_FILE, FOLLOWUP_BLOCKED_FILE,
        CAMPAIGN_STATUS_FILE, CAMPAIGN_STATUS_SUMMARY,
        CAMPAIGN_RUNNER_LOGS_FILE, EMAIL_REPAIR_ERRORS_FILE,
        ENGAGEMENT_SUMMARY_FILE,
    ]
    _CRM_GLOBAL = [
        SEND_LOGS_FILE, ENGAGEMENT_LOGS_FILE, FOLLOWUP_LOGS_FILE,
        CRM_DATABASE_FILE,
    ]
    _PROCESS_LEVEL = [
        DATABASE_FILE, CAMPAIGN_LOCK_FILE, CAMPAIGN_RUN_STATE_FILE,
    ]

    def test_campaign_scoped_are_run_path(self):
        for p in self._RUN_SCOPED:
            assert isinstance(p, _RunPath), f"{p!r} should be _RunPath"

    def test_crm_global_are_crm_path(self):
        for p in self._CRM_GLOBAL:
            assert isinstance(p, _CrmPath), f"{p!r} should be _CrmPath"

    def test_process_level_are_real_paths(self):
        for p in self._PROCESS_LEVEL:
            assert isinstance(p, Path), f"{p!r} should be a real pathlib.Path"
            assert not isinstance(p, (_RunPath, _CrmPath))


# ---------------------------------------------------------------------------
# 2. _RunPath proxy operations
# ---------------------------------------------------------------------------

class TestRunPathProtocol:
    """_RunPath must support every Path operation used by workflow files."""

    def test_str_returns_resolved_path(self):
        run_context.set_active_run("proto-001")
        assert str(RAW_LEADS_FILE) == str(RUNS_DIR / "proto-001" / "raw_leads.csv")

    def test_fspath_returns_resolved_string(self):
        run_context.set_active_run("proto-001")
        assert os.fspath(RAW_LEADS_FILE) == str(RUNS_DIR / "proto-001" / "raw_leads.csv")

    def test_name_attribute(self):
        run_context.set_active_run("proto-001")
        assert RAW_LEADS_FILE.name == "raw_leads.csv"

    def test_suffix_attribute(self):
        run_context.set_active_run("proto-001")
        assert RAW_LEADS_FILE.suffix == ".csv"

    def test_parent_attribute_is_run_dir(self):
        run_context.set_active_run("proto-001")
        assert RAW_LEADS_FILE.parent == RUNS_DIR / "proto-001"

    def test_truediv_appends_to_resolved_path(self):
        run_context.set_active_run("proto-001")
        result = RAW_LEADS_FILE.parent / "other.csv"
        assert result == RUNS_DIR / "proto-001" / "other.csv"

    def test_exists_returns_false_for_missing_file(self):
        run_context.set_active_run("proto-exists-test")
        assert RAW_LEADS_FILE.exists() is False

    def test_open_write_and_read_via_proxy(self, tmp_path, monkeypatch):
        """open(proxy, ...) must work — exercises __fspath__."""
        # Redirect RUNS_DIR to tmp_path so we don't touch real data dir
        monkeypatch.setattr("config.settings.RUNS_DIR", tmp_path / "runs")
        run_context.set_active_run("write-test")
        with open(RAW_LEADS_FILE, "w", encoding="utf-8") as fh:
            fh.write("company_name\nAcme Solar\n")
        with open(RAW_LEADS_FILE, "r", encoding="utf-8") as fh:
            content = fh.read()
        assert "Acme Solar" in content

    def test_parent_mkdir_works(self, tmp_path, monkeypatch):
        """path.parent.mkdir(parents=True, exist_ok=True) — used by many workflows."""
        monkeypatch.setattr("config.settings.RUNS_DIR", tmp_path / "runs")
        run_context.set_active_run("mkdir-test")
        RAW_LEADS_FILE.parent.mkdir(parents=True, exist_ok=True)
        assert (tmp_path / "runs" / "mkdir-test").is_dir()

    def test_unlink_removes_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr("config.settings.RUNS_DIR", tmp_path / "runs")
        run_context.set_active_run("unlink-test")
        # Use os.fspath() so the path is resolved AFTER the monkeypatch is applied
        actual_path = Path(os.fspath(RAW_LEADS_FILE))
        actual_path.parent.mkdir(parents=True, exist_ok=True)
        actual_path.write_text("data")
        RAW_LEADS_FILE.unlink()
        assert not actual_path.exists()

    def test_csv_dictreader_via_proxy(self, tmp_path, monkeypatch):
        """csv.DictReader(open(proxy, ...)) — primary read pattern in codebase."""
        monkeypatch.setattr("config.settings.RUNS_DIR", tmp_path / "runs")
        run_context.set_active_run("csv-test")
        run_dir = tmp_path / "runs" / "csv-test"
        run_dir.mkdir(parents=True)
        (run_dir / "raw_leads.csv").write_text("company_name,city\nAcme,Toronto\n")
        with open(RAW_LEADS_FILE, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        assert rows[0]["company_name"] == "Acme"

    def test_fallback_when_no_run_active(self):
        """With no active run, _RunPath resolves to DATA_DIR (legacy fallback)."""
        run_context.clear_active_run()
        resolved = str(RAW_LEADS_FILE)
        assert resolved == str(DATA_DIR / "raw_leads.csv")


# ---------------------------------------------------------------------------
# 3. _CrmPath proxy operations
# ---------------------------------------------------------------------------

class TestCrmPathProtocol:
    """_CrmPath must always resolve to data/crm/ regardless of run context."""

    def test_resolves_to_crm_dir_outside_run(self):
        run_context.clear_active_run()
        assert str(SEND_LOGS_FILE) == str(CRM_DIR / "send_logs.csv")

    def test_resolves_to_crm_dir_inside_run(self):
        run_context.set_active_run("some-campaign")
        assert str(SEND_LOGS_FILE) == str(CRM_DIR / "send_logs.csv")

    def test_not_affected_by_active_run_id(self):
        run_context.set_active_run("campaign-A")
        path_a = str(SEND_LOGS_FILE)
        run_context.set_active_run("campaign-B")
        path_b = str(SEND_LOGS_FILE)
        assert path_a == path_b == str(CRM_DIR / "send_logs.csv")

    def test_name_attribute(self):
        assert SEND_LOGS_FILE.name == "send_logs.csv"
        assert ENGAGEMENT_LOGS_FILE.name == "engagement_logs.csv"
        assert FOLLOWUP_LOGS_FILE.name == "followup_logs.csv"

    def test_fspath(self):
        assert os.fspath(SEND_LOGS_FILE) == str(CRM_DIR / "send_logs.csv")

    def test_open_write_and_read(self, tmp_path, monkeypatch):
        monkeypatch.setattr("config.settings.CRM_DIR", tmp_path / "crm")
        run_context.set_active_run("any-run")
        with open(SEND_LOGS_FILE, "w", encoding="utf-8") as fh:
            fh.write("timestamp,campaign_id\n2026-01-01,abc\n")
        with open(SEND_LOGS_FILE, "r", encoding="utf-8") as fh:
            content = fh.read()
        assert "abc" in content


# ---------------------------------------------------------------------------
# 4. Two consecutive campaigns — isolation guarantee
# ---------------------------------------------------------------------------

class TestCampaignIsolation:
    """Artifacts written in campaign A must not appear in campaign B's directory."""

    def test_distinct_runs_get_distinct_directories(self):
        run_context.set_active_run("campaign-A")
        dir_a = str(RAW_LEADS_FILE.parent)
        run_context.set_active_run("campaign-B")
        dir_b = str(RAW_LEADS_FILE.parent)
        assert dir_a != dir_b
        assert "campaign-A" in dir_a
        assert "campaign-B" in dir_b

    def test_write_to_run_A_not_visible_in_run_B(self, tmp_path, monkeypatch):
        monkeypatch.setattr("config.settings.RUNS_DIR", tmp_path / "runs")
        # Write artifact in campaign A
        run_context.set_active_run("campaign-A")
        path_a = Path(os.fspath(RAW_LEADS_FILE))
        path_a.parent.mkdir(parents=True, exist_ok=True)
        path_a.write_text("company_name\nA-Corp\n")
        # Switch to campaign B — same constant, different resolution
        run_context.set_active_run("campaign-B")
        assert not RAW_LEADS_FILE.exists(), (
            "Campaign B should not see campaign A's raw_leads.csv"
        )

    def test_write_to_run_B_not_visible_in_run_A(self, tmp_path, monkeypatch):
        monkeypatch.setattr("config.settings.RUNS_DIR", tmp_path / "runs")
        run_context.set_active_run("campaign-B")
        path_b = Path(os.fspath(RAW_LEADS_FILE))
        path_b.parent.mkdir(parents=True, exist_ok=True)
        path_b.write_text("company_name\nB-Corp\n")
        run_context.set_active_run("campaign-A")
        assert not RAW_LEADS_FILE.exists(), (
            "Campaign A should not see campaign B's raw_leads.csv"
        )

    def test_both_runs_can_coexist_on_disk(self, tmp_path, monkeypatch):
        monkeypatch.setattr("config.settings.RUNS_DIR", tmp_path / "runs")
        for cid, name in [("run-1", "Alpha Solar"), ("run-2", "Beta Solar")]:
            run_context.set_active_run(cid)
            p = Path(os.fspath(GENERATED_EMAILS_FILE))
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(f"company_name\n{name}\n")
        # Verify both still intact
        run_context.set_active_run("run-1")
        assert "Alpha Solar" in Path(os.fspath(GENERATED_EMAILS_FILE)).read_text()
        run_context.set_active_run("run-2")
        assert "Beta Solar" in Path(os.fspath(GENERATED_EMAILS_FILE)).read_text()

    def test_key_artifact_paths(self):
        """Spot-check the specific paths called out by the validation request."""
        run_context.set_active_run("run-XYZ")
        assert str(SEND_BATCH_SUMMARY)      == str(RUNS_DIR / "run-XYZ" / "send_batch_summary.json")
        assert str(CAMPAIGN_STATUS_FILE)    == str(RUNS_DIR / "run-XYZ" / "campaign_status.csv")
        assert str(CAMPAIGN_STATUS_SUMMARY) == str(RUNS_DIR / "run-XYZ" / "campaign_status_summary.json")
        assert str(FINAL_SEND_QUEUE_FILE)   == str(RUNS_DIR / "run-XYZ" / "final_send_queue.csv")
        assert str(FINAL_REJECTED_FILE)     == str(RUNS_DIR / "run-XYZ" / "final_rejected_emails.csv")


# ---------------------------------------------------------------------------
# 5. Resume reads only the target run directory
# ---------------------------------------------------------------------------

class TestResumeTargetsCorrectRun:
    """Resuming run-2 must point all file constants at run-2's directory, not run-1's."""

    def test_resume_switches_to_previous_run_directory(self, tmp_path, monkeypatch):
        monkeypatch.setattr("config.settings.RUNS_DIR", tmp_path / "runs")
        # Simulate run-1 producing artifacts
        run_context.set_active_run("run-first")
        p1 = Path(os.fspath(ENRICHED_LEADS_FILE))
        p1.parent.mkdir(parents=True, exist_ok=True)
        p1.write_text("data from run-first")
        # Simulate run-2 in progress
        run_context.set_active_run("run-second")
        p2 = Path(os.fspath(ENRICHED_LEADS_FILE))
        p2.parent.mkdir(parents=True, exist_ok=True)
        p2.write_text("data from run-second")
        # Now resume run-first: set context back
        run_context.set_active_run("run-first")
        content = Path(os.fspath(ENRICHED_LEADS_FILE)).read_text()
        assert content == "data from run-first", (
            "Resuming run-first should read run-first's enriched_leads.csv"
        )

    def test_run_context_switch_immediately_affects_all_constants(self, tmp_path, monkeypatch):
        """All _RunPath constants switch atomically when set_active_run is called."""
        monkeypatch.setattr("config.settings.RUNS_DIR", tmp_path / "runs")
        run_context.set_active_run("run-alpha")
        paths_alpha = {
            "raw_leads":        str(RAW_LEADS_FILE),
            "emails":           str(GENERATED_EMAILS_FILE),
            "campaign_status":  str(CAMPAIGN_STATUS_FILE),
            "send_batch":       str(SEND_BATCH_SUMMARY),
        }
        run_context.set_active_run("run-beta")
        paths_beta = {
            "raw_leads":        str(RAW_LEADS_FILE),
            "emails":           str(GENERATED_EMAILS_FILE),
            "campaign_status":  str(CAMPAIGN_STATUS_FILE),
            "send_batch":       str(SEND_BATCH_SUMMARY),
        }
        for key in paths_alpha:
            assert paths_alpha[key] != paths_beta[key], (
                f"{key}: paths should differ between runs"
            )
            assert "run-alpha" in paths_alpha[key]
            assert "run-beta"  in paths_beta[key]


# ---------------------------------------------------------------------------
# 6. CRM logs write only to data/crm/ — never to run directory
# ---------------------------------------------------------------------------

class TestCrmIsolation:
    """Global CRM files must never resolve into a run directory."""

    def test_crm_files_never_point_to_runs_dir(self):
        for cid in ["campaign-1", "campaign-2", "campaign-3"]:
            run_context.set_active_run(cid)
            for crm_path in [SEND_LOGS_FILE, ENGAGEMENT_LOGS_FILE, FOLLOWUP_LOGS_FILE]:
                resolved = str(crm_path)
                assert "runs" not in resolved, (
                    f"{crm_path!r} resolved to {resolved!r} which contains 'runs'"
                )
                assert str(CRM_DIR) in resolved

    def test_crm_path_stable_across_run_switches(self):
        send_log_paths = set()
        for cid in ["x1", "x2", "x3"]:
            run_context.set_active_run(cid)
            send_log_paths.add(str(SEND_LOGS_FILE))
        assert len(send_log_paths) == 1, (
            "SEND_LOGS_FILE must resolve to the same path regardless of active run"
        )

    def test_crm_write_lands_in_crm_dir(self, tmp_path, monkeypatch):
        monkeypatch.setattr("config.settings.CRM_DIR", tmp_path / "crm")
        run_context.set_active_run("campaign-Z")
        (tmp_path / "crm").mkdir(parents=True, exist_ok=True)
        with open(SEND_LOGS_FILE, "w", encoding="utf-8") as fh:
            fh.write("timestamp,campaign_id,send_status\n")
        assert (tmp_path / "crm" / "send_logs.csv").exists()
        assert not (tmp_path / "runs").exists(), (
            "Writing to SEND_LOGS_FILE must not create a runs/ directory"
        )


# ---------------------------------------------------------------------------
# 7. No unexpected writes to legacy shared data root (data/<file>)
# ---------------------------------------------------------------------------

class TestNoLegacyDataRootWrites:
    """Campaign-scoped constants must never resolve to DATA_DIR directly when a
    run is active — only to DATA_DIR/runs/<campaign_id>/."""

    _RUN_SCOPED_SAMPLE = [
        ("RAW_LEADS_FILE",        RAW_LEADS_FILE),
        ("GENERATED_EMAILS_FILE", GENERATED_EMAILS_FILE),
        ("SEND_BATCH_SUMMARY",    SEND_BATCH_SUMMARY),
        ("CAMPAIGN_STATUS_FILE",  CAMPAIGN_STATUS_FILE),
        ("FINAL_SEND_QUEUE_FILE", FINAL_SEND_QUEUE_FILE),
        ("ENRICHED_LEADS_FILE",   ENRICHED_LEADS_FILE),
        ("EMAIL_REPAIR_ERRORS_FILE", EMAIL_REPAIR_ERRORS_FILE),
    ]

    def test_no_campaign_artifact_resolves_to_bare_data_dir_when_run_active(self):
        run_context.set_active_run("live-run-001")
        for name, proxy in self._RUN_SCOPED_SAMPLE:
            resolved = Path(os.fspath(proxy))
            assert resolved.parent != DATA_DIR, (
                f"{name} resolved to {resolved} — parent is DATA_DIR, "
                "should be inside data/runs/<campaign_id>/"
            )
            assert resolved.is_relative_to(RUNS_DIR), (
                f"{name} resolved to {resolved} — not under RUNS_DIR"
            )

    def test_process_level_paths_stay_in_data_root(self):
        """DATABASE_FILE, CAMPAIGN_LOCK_FILE, CAMPAIGN_RUN_STATE_FILE must
        always be in DATA_DIR, never in a run subdirectory."""
        run_context.set_active_run("any-run")
        assert DATABASE_FILE.parent          == DATA_DIR
        assert CAMPAIGN_LOCK_FILE.parent     == DATA_DIR
        assert CAMPAIGN_RUN_STATE_FILE.parent == DATA_DIR

    def test_fallback_behavior_is_only_outside_run(self):
        """The legacy fallback (DATA_DIR) must only activate when no run is set.
        This is intentional backward-compat for callers outside campaign_runner.
        Once a run is active, proxies MUST point to the run directory."""
        # Outside run: fallback to DATA_DIR is expected
        run_context.clear_active_run()
        assert str(RAW_LEADS_FILE) == str(DATA_DIR / "raw_leads.csv")
        # Inside run: must NOT fall back to DATA_DIR
        run_context.set_active_run("active-run")
        assert str(RAW_LEADS_FILE) != str(DATA_DIR / "raw_leads.csv")
        assert str(RAW_LEADS_FILE) == str(RUNS_DIR / "active-run" / "raw_leads.csv")


# ---------------------------------------------------------------------------
# 8. run_context module-global state machine
# ---------------------------------------------------------------------------

class TestRunContextStateMachine:
    def test_initial_state_is_none(self):
        run_context.clear_active_run()
        assert run_context.get_active_campaign_id() is None

    def test_set_active_run_stores_id(self):
        run_context.set_active_run("abc-123")
        assert run_context.get_active_campaign_id() == "abc-123"

    def test_clear_active_run_returns_to_none(self):
        run_context.set_active_run("abc-123")
        run_context.clear_active_run()
        assert run_context.get_active_campaign_id() is None

    def test_set_active_run_overrides_previous(self):
        run_context.set_active_run("first")
        run_context.set_active_run("second")
        assert run_context.get_active_campaign_id() == "second"

    def test_run_path_reflects_context_change_immediately(self):
        run_context.set_active_run("v1")
        assert "v1" in str(RAW_LEADS_FILE)
        run_context.set_active_run("v2")
        assert "v2" in str(RAW_LEADS_FILE)
        assert "v1" not in str(RAW_LEADS_FILE)
