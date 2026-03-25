"""
Regression tests — Workflow 5.8 Signal Research Contract
=========================================================

These tests guard against the bug that caused campaign 98de0467 (2026-03-19) to
fail at the `signals` step with:

    [Errno 2] No such file or directory: '.../research_signal_raw.json'

Root cause (two cooperating defects in older code):
  1. signal_collector.run() returned [] immediately when enriched_leads.csv had
     0 data rows, WITHOUT writing research_signal_raw.json first.
  2. signal_summarizer.run() called open(raw_path) WITHOUT checking exists() first,
     so it raised FileNotFoundError when the collector had produced no output.

Both are now fixed:
  Fix 1 — collector writes [] to research_signal_raw.json even when there are
           no leads (lines 215-218 in signal_collector.py).
  Fix 2 — summarizer checks `if not raw_path.exists()` before open() and handles
           the missing-file case gracefully (lines 206-210 in signal_summarizer.py).

Test coverage:
  TestCollectorEmptyLeadsWritesFile    — Fix 1: empty input still produces the file
  TestCollectorRunScopedPath           — file is written under data/runs/<id>/
  TestSummarizerMissingFileGraceful    — Fix 2: no crash when raw file absent
  TestSummarizerReadsExistingRawFile   — normal path still works
  TestSignalsStepEmptyEnrichedLeads    — full step-level integration on empty input
  TestSignalsStepResumeSafety          — resuming after a prior signals failure is safe
"""
from __future__ import annotations

import csv
import json
import os
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_run_paths(run_dir: Path):
    """Construct a RunPaths instance pointing at *run_dir*."""
    from config.run_paths import RunPaths
    run_dir.mkdir(parents=True, exist_ok=True)
    return RunPaths(
        campaign_id="test-sig-contract",
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


def _write_empty_enriched_leads(run_dir: Path) -> Path:
    """Write an enriched_leads.csv with only a header row (0 data rows)."""
    path = run_dir / "enriched_leads.csv"
    header = (
        "company_name,website,place_id,company_type,market_focus,services_detected,"
        "confidence_score,classification_method,lead_score,score_breakdown,target_tier,"
        "kp_name,kp_title,kp_email,enrichment_source,site_phone,whatsapp_phone,email_sendable,"
        "contact_channel,alt_outreach_possible,manual_outreach_channel,"
        "manual_outreach_highlight,contact_trust,skip_reason"
    )
    path.write_text(header + "\n", encoding="utf-8")
    return path


def _write_enriched_leads_with_rows(run_dir: Path, rows: list[dict]) -> Path:
    """Write an enriched_leads.csv with actual data rows."""
    path = run_dir / "enriched_leads.csv"
    if not rows:
        return _write_empty_enriched_leads(run_dir)
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return path


# ---------------------------------------------------------------------------
# TestCollectorEmptyLeadsWritesFile
# Fix 1: collector must write research_signal_raw.json even when 0 leads found
# ---------------------------------------------------------------------------

class TestCollectorEmptyLeadsWritesFile:

    def test_raw_file_created_when_no_leads(self, tmp_path):
        """Collector writes an empty JSON file even when enriched_leads has 0 rows."""
        paths = _make_run_paths(tmp_path)
        _write_empty_enriched_leads(tmp_path)

        from src.workflow_5_8_signal_research.signal_collector import run as collect
        result = collect(limit=0, paths=paths)

        assert result == [], "collect() should return [] for empty input"
        assert paths.research_signal_raw_file.exists(), (
            "research_signal_raw.json must be created even when there are 0 leads — "
            "its absence causes signal_summarizer to raise FileNotFoundError"
        )

    def test_raw_file_content_is_empty_list(self, tmp_path):
        """The file written for empty input must contain a valid empty JSON array."""
        paths = _make_run_paths(tmp_path)
        _write_empty_enriched_leads(tmp_path)

        from src.workflow_5_8_signal_research.signal_collector import run as collect
        collect(limit=0, paths=paths)

        raw = json.loads(paths.research_signal_raw_file.read_text(encoding="utf-8"))
        assert raw == [], "Empty-leads run should produce [] in research_signal_raw.json"

    def test_raw_file_created_when_limit_filters_all_leads(self, tmp_path):
        """Collector writes the file even when limit=1 but the input has 0 rows."""
        paths = _make_run_paths(tmp_path)
        _write_empty_enriched_leads(tmp_path)

        from src.workflow_5_8_signal_research.signal_collector import run as collect
        result = collect(limit=1, paths=paths)

        assert result == []
        assert paths.research_signal_raw_file.exists()


# ---------------------------------------------------------------------------
# TestCollectorRunScopedPath
# The raw file must be written under data/runs/<campaign_id>/, not DATA_DIR
# ---------------------------------------------------------------------------

class TestCollectorRunScopedPath:

    def test_raw_file_written_inside_run_dir(self, tmp_path):
        """research_signal_raw.json appears inside run_dir, not in DATA_DIR."""
        paths = _make_run_paths(tmp_path)
        _write_empty_enriched_leads(tmp_path)

        from src.workflow_5_8_signal_research.signal_collector import run as collect
        collect(limit=0, paths=paths)

        # The file must be inside our temp run_dir
        assert paths.research_signal_raw_file.parent == tmp_path
        assert paths.research_signal_raw_file.parent.resolve() == tmp_path.resolve()

    def test_summarizer_writes_signals_inside_run_dir(self, tmp_path):
        """research_signals.json also appears inside run_dir."""
        paths = _make_run_paths(tmp_path)
        _write_empty_enriched_leads(tmp_path)

        from src.workflow_5_8_signal_research.signal_collector import run as collect
        from src.workflow_5_8_signal_research.signal_summarizer import run as summarize
        collect(limit=0, paths=paths)
        summarize(paths=paths)

        assert paths.research_signals_file.parent == tmp_path

    def test_two_campaigns_do_not_share_raw_files(self, tmp_path):
        """Separate run_dirs produce separate file paths (no cross-campaign bleed)."""
        run_a = tmp_path / "campaign-a"
        run_b = tmp_path / "campaign-b"
        paths_a = _make_run_paths(run_a)
        paths_b = _make_run_paths(run_b)

        assert paths_a.research_signal_raw_file != paths_b.research_signal_raw_file
        assert "campaign-a" in str(paths_a.research_signal_raw_file)
        assert "campaign-b" in str(paths_b.research_signal_raw_file)


# ---------------------------------------------------------------------------
# TestSummarizerMissingFileGraceful
# Fix 2: summarizer must NOT raise FileNotFoundError when raw file is absent
# ---------------------------------------------------------------------------

class TestSummarizerMissingFileGraceful:

    def test_summarizer_returns_empty_list_when_raw_absent(self, tmp_path):
        """summarizer.run() returns [] gracefully when research_signal_raw.json is missing."""
        paths = _make_run_paths(tmp_path)
        # Do NOT write the raw file — simulates the old collector bug

        from src.workflow_5_8_signal_research.signal_summarizer import run as summarize
        result = summarize(paths=paths)

        assert result == [], (
            "summarizer.run() must return [] (not raise FileNotFoundError) "
            "when research_signal_raw.json is absent"
        )

    def test_summarizer_writes_empty_signals_when_raw_absent(self, tmp_path):
        """summarizer writes research_signals.json as [] even when raw file is missing."""
        paths = _make_run_paths(tmp_path)

        from src.workflow_5_8_signal_research.signal_summarizer import run as summarize
        summarize(paths=paths)

        assert paths.research_signals_file.exists(), (
            "research_signals.json must be created even when raw file was absent"
        )
        content = json.loads(paths.research_signals_file.read_text(encoding="utf-8"))
        assert content == []

    def test_summarizer_does_not_raise_file_not_found(self, tmp_path):
        """summarizer.run() never raises FileNotFoundError — the original bug."""
        paths = _make_run_paths(tmp_path)

        from src.workflow_5_8_signal_research.signal_summarizer import run as summarize
        try:
            summarize(paths=paths)
        except FileNotFoundError as exc:
            pytest.fail(
                f"summarizer.run() raised FileNotFoundError — this is the regression bug: {exc}"
            )

    def test_summarizer_does_not_raise_even_with_empty_raw_file(self, tmp_path):
        """Summarizer handles an existing but empty raw file without crashing."""
        paths = _make_run_paths(tmp_path)
        # Write an empty raw file (what the collector now writes for 0 leads)
        paths.research_signal_raw_file.write_text("[]", encoding="utf-8")

        from src.workflow_5_8_signal_research.signal_summarizer import run as summarize
        result = summarize(paths=paths)

        assert result == []


# ---------------------------------------------------------------------------
# TestSummarizerReadsExistingRawFile
# Normal path: when raw file exists the summarizer processes it correctly
# ---------------------------------------------------------------------------

class TestSummarizerReadsExistingRawFile:

    def _make_raw_record(self, company_name: str = "SolarCo", website: str = "http://solarco.test") -> dict:
        return {
            "company_name": company_name,
            "website": website,
            "place_id": "place_abc123",
            "signal_sources": {
                "website": [{"url": website, "headlines": ["We completed a 500kW solar farm"], "meta": ""}],
                "social": [],
            },
        }

    def test_summarizer_processes_raw_records(self, tmp_path):
        """Summarizer returns one result per raw record."""
        paths = _make_run_paths(tmp_path)
        raw = [self._make_raw_record()]
        paths.research_signal_raw_file.write_text(
            json.dumps(raw, ensure_ascii=False), encoding="utf-8"
        )

        from src.workflow_5_8_signal_research.signal_summarizer import run as summarize
        result = summarize(paths=paths)

        assert len(result) == 1
        assert result[0]["company_name"] == "SolarCo"

    def test_summarizer_writes_output_file_when_raw_has_data(self, tmp_path):
        """research_signals.json is written when there are records to summarize."""
        paths = _make_run_paths(tmp_path)
        raw = [self._make_raw_record("Alpha Solar"), self._make_raw_record("Beta Storage")]
        paths.research_signal_raw_file.write_text(
            json.dumps(raw, ensure_ascii=False), encoding="utf-8"
        )

        from src.workflow_5_8_signal_research.signal_summarizer import run as summarize
        summarize(paths=paths)

        assert paths.research_signals_file.exists()
        summaries = json.loads(paths.research_signals_file.read_text(encoding="utf-8"))
        assert len(summaries) == 2

    def test_collect_then_summarize_end_to_end_with_empty_leads(self, tmp_path):
        """Collector + summarizer sequence completes without error for 0-lead input."""
        paths = _make_run_paths(tmp_path)
        _write_empty_enriched_leads(tmp_path)

        from src.workflow_5_8_signal_research.signal_collector import run as collect
        from src.workflow_5_8_signal_research.signal_summarizer import run as summarize

        collect_result = collect(limit=0, paths=paths)
        summarize_result = summarize(paths=paths)

        assert collect_result == []
        assert summarize_result == []
        assert paths.research_signal_raw_file.exists()
        assert paths.research_signals_file.exists()


# ---------------------------------------------------------------------------
# TestSignalsStepEmptyEnrichedLeads
# Step-level integration: run_step_5_8_signals must not fail on empty input
# This is the exact scenario that caused the 98de0467 campaign to fail.
# ---------------------------------------------------------------------------

class TestSignalsStepEmptyEnrichedLeads:

    def _setup_run_context(self, tmp_path):
        """Activate run context so require_active_run_paths() works in the step."""
        from config.run_context import set_active_run, clear_active_run
        from config.run_paths import set_active_run_paths, clear_active_run_paths

        paths = _make_run_paths(tmp_path)
        set_active_run("test-sig-contract")
        set_active_run_paths(paths)
        return paths, clear_active_run, clear_active_run_paths

    def test_step_completes_without_error_on_empty_enriched_leads(self, tmp_path):
        """run_step_5_8_signals does not raise when enriched_leads.csv has 0 data rows."""
        paths, clear_run, clear_paths = self._setup_run_context(tmp_path)
        _write_empty_enriched_leads(tmp_path)

        try:
            from src.workflow_9_campaign_runner.campaign_steps import run_step_5_8_signals
            from src.workflow_9_campaign_runner.campaign_config import CampaignConfig

            config = CampaignConfig(city="Test", country="TestCountry", enrich_limit=20)
            result = run_step_5_8_signals(config)
        finally:
            clear_run()
            clear_paths()

        assert result == [], "Step should return [] for empty enriched leads"

    def test_step_produces_both_output_files(self, tmp_path):
        """Both research_signal_raw.json and research_signals.json are created."""
        paths, clear_run, clear_paths = self._setup_run_context(tmp_path)
        _write_empty_enriched_leads(tmp_path)

        try:
            from src.workflow_9_campaign_runner.campaign_steps import run_step_5_8_signals
            from src.workflow_9_campaign_runner.campaign_config import CampaignConfig

            config = CampaignConfig(city="Test", country="TestCountry", enrich_limit=20)
            run_step_5_8_signals(config)
        finally:
            clear_run()
            clear_paths()

        assert paths.research_signal_raw_file.exists(), (
            "research_signal_raw.json must exist after signals step — "
            "its absence is the original bug that crashed campaign 98de0467"
        )
        assert paths.research_signals_file.exists()

    def test_step_does_not_raise_file_not_found(self, tmp_path):
        """run_step_5_8_signals never raises FileNotFoundError for missing raw file."""
        paths, clear_run, clear_paths = self._setup_run_context(tmp_path)
        _write_empty_enriched_leads(tmp_path)

        try:
            from src.workflow_9_campaign_runner.campaign_steps import run_step_5_8_signals
            from src.workflow_9_campaign_runner.campaign_config import CampaignConfig

            config = CampaignConfig(city="Test", country="TestCountry", enrich_limit=20)
            try:
                run_step_5_8_signals(config)
            except FileNotFoundError as exc:
                pytest.fail(
                    f"run_step_5_8_signals raised FileNotFoundError — "
                    f"regression of campaign 98de0467 bug: {exc}"
                )
        finally:
            clear_run()
            clear_paths()

    def test_step_output_files_contain_valid_json(self, tmp_path):
        """Both output files are parseable JSON after the step completes."""
        paths, clear_run, clear_paths = self._setup_run_context(tmp_path)
        _write_empty_enriched_leads(tmp_path)

        try:
            from src.workflow_9_campaign_runner.campaign_steps import run_step_5_8_signals
            from src.workflow_9_campaign_runner.campaign_config import CampaignConfig

            config = CampaignConfig(city="Test", country="TestCountry", enrich_limit=0)
            run_step_5_8_signals(config)
        finally:
            clear_run()
            clear_paths()

        raw = json.loads(paths.research_signal_raw_file.read_text(encoding="utf-8"))
        signals = json.loads(paths.research_signals_file.read_text(encoding="utf-8"))
        assert isinstance(raw, list)
        assert isinstance(signals, list)


# ---------------------------------------------------------------------------
# TestSignalsStepResumeSafety
# Resuming after a prior failed signals run must not re-raise FileNotFoundError
# ---------------------------------------------------------------------------

class TestSignalsStepResumeSafety:

    def test_signals_step_safe_when_raw_file_absent_at_start(self, tmp_path):
        """Signals step handles the case where research_signal_raw.json was never written."""
        from config.run_context import set_active_run, clear_active_run
        from config.run_paths import set_active_run_paths, clear_active_run_paths

        paths = _make_run_paths(tmp_path)
        _write_empty_enriched_leads(tmp_path)

        # Simulate a prior failed run: raw file does NOT exist
        assert not paths.research_signal_raw_file.exists()

        set_active_run("test-sig-contract")
        set_active_run_paths(paths)
        try:
            from src.workflow_9_campaign_runner.campaign_steps import run_step_5_8_signals
            from src.workflow_9_campaign_runner.campaign_config import CampaignConfig

            config = CampaignConfig(city="Test", country="TestCountry", enrich_limit=0)
            run_step_5_8_signals(config)
        except FileNotFoundError as exc:
            pytest.fail(f"Resume scenario raised FileNotFoundError: {exc}")
        finally:
            clear_active_run()
            clear_active_run_paths()

        # After the step, the file must now exist
        assert paths.research_signal_raw_file.exists()

    def test_summarizer_safe_when_called_independently_without_collector(self, tmp_path):
        """Calling summarize() alone (no prior collect) is always safe."""
        paths = _make_run_paths(tmp_path)
        # No enriched_leads.csv, no raw file — cold start

        from src.workflow_5_8_signal_research.signal_summarizer import run as summarize
        result = summarize(paths=paths)

        assert result == []
        assert paths.research_signals_file.exists()
