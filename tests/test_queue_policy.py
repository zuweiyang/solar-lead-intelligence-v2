# Tests for P1-3A — Queue Policy Enforcement (Workflow 6 — Queue Policy)
#
# Mandatory scenarios from the ticket specification:
# 1. Policy action mapping (all 5 actions)
# 2. Primary contact enforcement
# 3. Generic handling
# 4. Output/artifact correctness
# 5. Backward compatibility
# 6. Summary/reporting counts
# 7. Resilience (malformed record, missing optional fields)

from __future__ import annotations

import csv
import io
from pathlib import Path

import pytest

from src.workflow_6_queue_policy.queue_policy_models import (
    ALL_POLICY_ACTIONS,
    POLICY_BLOCK,
    POLICY_GENERIC_ONLY,
    POLICY_HOLD,
    POLICY_QUEUE_LIMITED,
    POLICY_QUEUE_NORMAL,
    QUEUE_POLICY_FIELDS,
    QUEUE_POLICY_VERSION,
    QueuePolicyRecord,
    QueuePolicyStats,
)
from src.workflow_6_queue_policy.queue_policy_rules import apply_policy, decide_policy
from src.workflow_6_queue_policy.queue_policy_pipeline import (
    _build_record,
    _build_verification_index,
    _load_scored_primaries,
    _save_queue_policy,
    load_queued_normal,
    load_queue_policy,
    run,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_scored_row(
    *,
    company_name: str = "Acme Solar",
    website: str = "https://acme.com",
    place_id: str = "pid-001",
    kp_email: str = "jane@acme.com",
    kp_name: str = "Jane Smith",
    kp_title: str = "Procurement Manager",
    is_primary_contact: str = "true",
    is_generic_mailbox: str = "false",
    send_eligibility: str = "",
    send_pool: str = "",
    email_confidence_tier: str = "",
    enrichment_source: str = "apollo",
    contact_fit_score: str = "60",
    contact_priority_rank: str = "1",
    contact_selection_reason: str = "top_scored",
) -> dict:
    return {
        "company_name": company_name,
        "website": website,
        "place_id": place_id,
        "kp_email": kp_email,
        "kp_name": kp_name,
        "kp_title": kp_title,
        "is_primary_contact": is_primary_contact,
        "is_generic_mailbox": is_generic_mailbox,
        "send_eligibility": send_eligibility,
        "send_pool": send_pool,
        "email_confidence_tier": email_confidence_tier,
        "enrichment_source": enrichment_source,
        "contact_fit_score": contact_fit_score,
        "contact_priority_rank": contact_priority_rank,
        "contact_selection_reason": contact_selection_reason,
        "lead_score": "72",
        "qualification_status": "qualified",
        "target_tier": "A",
        "company_type": "solar installer",
        "market_focus": "commercial",
    }


def _make_run_paths(tmp_path: Path, campaign_id: str = "test-001"):
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


def _write_scored_contacts(path: Path, rows: list[dict]) -> None:
    """Write rows to scored_contacts.csv with all required fields."""
    if not rows:
        all_fields = list(_make_scored_row().keys())
        path.write_text(",".join(all_fields) + "\n", encoding="utf-8")
        return
    all_fields = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=all_fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def _write_verified_leads(path: Path, rows: list[dict]) -> None:
    """Write rows to verified_enriched_leads.csv."""
    if not rows:
        path.write_text("kp_email,send_eligibility,send_pool,email_confidence_tier\n", encoding="utf-8")
        return
    fields = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


# ===========================================================================
# 1. Policy Action Mapping (decide_policy + apply_policy)
# ===========================================================================

class TestPolicyActionMapping:
    """All 5 policy actions are reachable via verified eligibility paths."""

    def test_allow_maps_to_queue_normal(self):
        action, reason = decide_policy("allow", False, True, True)
        assert action == POLICY_QUEUE_NORMAL
        assert "allow" in reason

    def test_allow_limited_maps_to_queue_limited(self):
        action, reason = decide_policy("allow_limited", False, True, True)
        assert action == POLICY_QUEUE_LIMITED
        assert "limited" in reason

    def test_hold_maps_to_hold(self):
        action, reason = decide_policy("hold", False, True, True)
        assert action == POLICY_HOLD
        assert "hold" in reason or "catchall" in reason

    def test_generic_pool_only_maps_to_block(self):
        action, reason = decide_policy("generic_pool_only", True, True, True)
        assert action == POLICY_BLOCK
        assert "generic" in reason

    def test_block_maps_to_block(self):
        action, reason = decide_policy("block", False, True, True)
        assert action == POLICY_BLOCK
        assert "e0" in reason or "block" in reason

    def test_no_email_always_blocks(self):
        # Even if eligibility says allow, no email → block
        action, reason = decide_policy("allow", False, True, False)
        assert action == POLICY_BLOCK
        assert "no_email" in reason

    def test_all_policy_constants_are_distinct(self):
        values = [POLICY_QUEUE_NORMAL, POLICY_QUEUE_LIMITED, POLICY_HOLD,
                  POLICY_GENERIC_ONLY, POLICY_BLOCK]
        assert len(set(values)) == 5

    def test_all_policy_actions_list_complete(self):
        assert set(ALL_POLICY_ACTIONS) == {
            POLICY_QUEUE_NORMAL, POLICY_QUEUE_LIMITED, POLICY_HOLD,
            POLICY_GENERIC_ONLY, POLICY_BLOCK,
        }

    def test_unknown_eligibility_with_verification_is_limited(self):
        action, reason = decide_policy("unknown_value", False, True, True)
        assert action == POLICY_QUEUE_LIMITED
        assert "unknown_eligibility" in reason


# ===========================================================================
# 2. Primary Contact Enforcement
# ===========================================================================

class TestPrimaryContactEnforcement:
    """Primary contact from P1-2B is used; fallbacks are not equal candidates."""

    def test_primary_contact_fields_populated(self, tmp_path):
        scored_row = _make_scored_row(
            kp_email="ceo@solar.com",
            kp_name="John CEO",
            kp_title="CEO",
            is_primary_contact="true",
        )
        rec = _build_record(scored_row, {})
        assert rec.selected_contact_email == "ceo@solar.com"
        assert rec.selected_contact_name == "John CEO"
        assert rec.selected_contact_title == "CEO"

    def test_only_primary_contacts_loaded(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        primary   = _make_scored_row(kp_email="primary@co.com", is_primary_contact="true")
        fallback  = _make_scored_row(kp_email="fallback@co.com", is_primary_contact="false",
                                      contact_priority_rank="2")
        _write_scored_contacts(paths.scored_contacts_file, [primary, fallback])

        primaries = _load_scored_primaries(paths.scored_contacts_file)
        assert len(primaries) == 1
        assert primaries[0]["kp_email"] == "primary@co.com"

    def test_fallback_contact_not_in_queue_policy(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        primary  = _make_scored_row(kp_email="ceo@co.com",   is_primary_contact="true",  place_id="pid-A")
        fallback = _make_scored_row(kp_email="info@co.com",   is_primary_contact="false", place_id="pid-A")
        _write_scored_contacts(paths.scored_contacts_file, [primary, fallback])

        result = run(paths=paths)
        assert result["total"] == 1

        rows = load_queue_policy(paths.queue_policy_file)
        emails = [r["selected_contact_email"] for r in rows]
        assert "ceo@co.com" in emails
        assert "info@co.com" not in emails

    def test_contact_fit_score_preserved(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        row = _make_scored_row(contact_fit_score="75", is_primary_contact="true")
        _write_scored_contacts(paths.scored_contacts_file, [row])

        run(paths=paths)
        rows = load_queue_policy(paths.queue_policy_file)
        assert rows[0]["contact_fit_score"] == "75"

    def test_contact_selection_reason_preserved(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        row = _make_scored_row(contact_selection_reason="named_primary_rank_1", is_primary_contact="true")
        _write_scored_contacts(paths.scored_contacts_file, [row])

        run(paths=paths)
        rows = load_queue_policy(paths.queue_policy_file)
        assert rows[0]["contact_selection_reason"] == "named_primary_rank_1"


# ===========================================================================
# 3. Generic Contact Handling
# ===========================================================================

class TestGenericHandling:
    """Generic mailboxes are blocked and never promoted to sendable queues."""

    def test_unverified_generic_is_blocked(self):
        action, reason = decide_policy("", True, False, True)
        assert action == POLICY_BLOCK
        assert "generic" in reason

    def test_verified_generic_pool_only_is_blocked(self):
        action, reason = decide_policy("generic_pool_only", True, True, True)
        assert action == POLICY_BLOCK

    def test_named_contact_unverified_is_queue_limited(self):
        action, reason = decide_policy("", False, False, True)
        assert action == POLICY_QUEUE_LIMITED
        assert "unverified" in reason

    def test_generic_primary_flagged_in_stats(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        row = _make_scored_row(
            kp_email="info@solar.com",
            kp_name="",
            is_generic_mailbox="true",
            is_primary_contact="true",
        )
        _write_scored_contacts(paths.scored_contacts_file, [row])

        result = run(paths=paths)
        assert result["generic_primary"] == 1
        assert result["named_primary"] == 0

    def test_generic_mailbox_company_not_treated_as_error(self, tmp_path):
        """Generic-mailbox contact as primary is acceptable, but it is blocked."""
        paths = _make_run_paths(tmp_path)
        row = _make_scored_row(
            kp_email="info@solar.com",
            is_generic_mailbox="true",
            is_primary_contact="true",
        )
        _write_scored_contacts(paths.scored_contacts_file, [row])

        result = run(paths=paths)
        assert result["errors"] == 0
        assert result["generic_only"] == 0
        assert result["block"] == 1

    def test_generic_contact_is_not_queue_normal(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        row = _make_scored_row(
            kp_email="sales@solar.com",
            is_generic_mailbox="true",
            is_primary_contact="true",
        )
        _write_scored_contacts(paths.scored_contacts_file, [row])

        result = run(paths=paths)
        rows = load_queue_policy(paths.queue_policy_file)
        assert rows[0]["send_policy_action"] != POLICY_QUEUE_NORMAL


# ===========================================================================
# 4. Output / Artifact Correctness
# ===========================================================================

class TestOutputArtifactCorrectness:
    """queue_policy.csv is written correctly with all required fields."""

    def test_output_file_written(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        row = _make_scored_row(is_primary_contact="true")
        _write_scored_contacts(paths.scored_contacts_file, [row])

        run(paths=paths)
        assert paths.queue_policy_file.exists()

    def test_output_has_all_required_fields(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        row = _make_scored_row(is_primary_contact="true")
        _write_scored_contacts(paths.scored_contacts_file, [row])

        run(paths=paths)
        rows = load_queue_policy(paths.queue_policy_file)
        assert rows
        for field in QUEUE_POLICY_FIELDS:
            assert field in rows[0], f"Missing field: {field}"

    def test_send_policy_action_is_populated(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        row = _make_scored_row(is_primary_contact="true")
        _write_scored_contacts(paths.scored_contacts_file, [row])

        run(paths=paths)
        rows = load_queue_policy(paths.queue_policy_file)
        assert rows[0]["send_policy_action"] in ALL_POLICY_ACTIONS

    def test_send_policy_reason_is_non_empty(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        row = _make_scored_row(is_primary_contact="true")
        _write_scored_contacts(paths.scored_contacts_file, [row])

        run(paths=paths)
        rows = load_queue_policy(paths.queue_policy_file)
        assert rows[0]["send_policy_reason"] != ""

    def test_policy_version_written(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        row = _make_scored_row(is_primary_contact="true")
        _write_scored_contacts(paths.scored_contacts_file, [row])

        run(paths=paths)
        rows = load_queue_policy(paths.queue_policy_file)
        assert rows[0]["policy_version"] == QUEUE_POLICY_VERSION

    def test_empty_input_writes_empty_file(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        _write_scored_contacts(paths.scored_contacts_file, [])

        result = run(paths=paths)
        assert result["total"] == 0
        assert paths.queue_policy_file.exists()

    def test_missing_input_writes_empty_file(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        # scored_contacts.csv not written at all

        result = run(paths=paths)
        assert result["total"] == 0
        assert paths.queue_policy_file.exists()

    def test_run_scoped_path(self, tmp_path):
        """queue_policy.csv is written inside the campaign run directory."""
        paths = _make_run_paths(tmp_path, "camp-xyz")
        row = _make_scored_row(is_primary_contact="true")
        _write_scored_contacts(paths.scored_contacts_file, [row])

        run(paths=paths)
        assert "camp-xyz" in str(paths.queue_policy_file)
        assert paths.queue_policy_file.exists()

    def test_multi_company_each_has_one_row(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        row_a = _make_scored_row(place_id="pid-A", kp_email="a@co.com",
                                  is_primary_contact="true")
        row_b = _make_scored_row(place_id="pid-B", kp_email="b@co.com",
                                  company_name="Solar B",
                                  is_primary_contact="true")
        _write_scored_contacts(paths.scored_contacts_file, [row_a, row_b])

        result = run(paths=paths)
        assert result["total"] == 2

        rows = load_queue_policy(paths.queue_policy_file)
        assert len(rows) == 2
        place_ids = {r["place_id"] for r in rows}
        assert place_ids == {"pid-A", "pid-B"}


# ===========================================================================
# 5. Backward Compatibility
# ===========================================================================

class TestBackwardCompatibility:
    """Existing pipeline files are not touched; queue_policy is purely additive."""

    def test_enriched_leads_file_not_modified(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        enriched_content = "company_name,kp_email\nAcme,jane@acme.com\n"
        paths.enriched_leads_file.write_text(enriched_content, encoding="utf-8")

        row = _make_scored_row(is_primary_contact="true")
        _write_scored_contacts(paths.scored_contacts_file, [row])

        run(paths=paths)

        assert paths.enriched_leads_file.read_text(encoding="utf-8") == enriched_content

    def test_scored_contacts_file_not_modified(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        row = _make_scored_row(is_primary_contact="true")
        _write_scored_contacts(paths.scored_contacts_file, [row])
        original_content = paths.scored_contacts_file.read_text(encoding="utf-8")

        run(paths=paths)

        assert paths.scored_contacts_file.read_text(encoding="utf-8") == original_content

    def test_require_active_run_paths_raised_without_context(self):
        """run() without RunPaths and without active context raises RuntimeError."""
        from config.run_paths import clear_active_run_paths, get_active_run_paths
        saved = get_active_run_paths()
        clear_active_run_paths()
        try:
            with pytest.raises(RuntimeError, match="No active RunPaths"):
                run(paths=None)
        finally:
            if saved is not None:
                from config.run_paths import set_active_run_paths
                set_active_run_paths(saved)

    def test_selected_contact_fields_in_legacy_compatible_form(self, tmp_path):
        """
        Output exposes selected contact as explicit named fields, not just opaque blobs.
        This means downstream can still extract kp_email / kp_name from a single row.
        """
        paths = _make_run_paths(tmp_path)
        row = _make_scored_row(kp_email="ceo@co.com", kp_name="The CEO",
                                is_primary_contact="true")
        _write_scored_contacts(paths.scored_contacts_file, [row])

        run(paths=paths)
        rows = load_queue_policy(paths.queue_policy_file)
        assert rows[0]["selected_contact_email"] == "ceo@co.com"
        assert rows[0]["selected_contact_name"] == "The CEO"


# ===========================================================================
# 6. Summary / Reporting
# ===========================================================================

class TestSummaryReporting:
    """Summary dict has correct counts per policy action."""

    def test_counts_match_actual_rows(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        # E1 → queue_normal
        r1 = _make_scored_row(place_id="p1", kp_email="e1@co.com",
                               send_eligibility="allow", is_primary_contact="true")
        # E2 → queue_limited
        r2 = _make_scored_row(place_id="p2", kp_email="e2@co.com",
                               send_eligibility="allow_limited", is_primary_contact="true")
        # E3 → hold
        r3 = _make_scored_row(place_id="p3", kp_email="e3@co.com",
                               send_eligibility="hold", is_primary_contact="true")
        # E4 → block
        r4 = _make_scored_row(place_id="p4", kp_email="info@co.com",
                               send_eligibility="generic_pool_only",
                               is_generic_mailbox="true", is_primary_contact="true")
        # E0 → block
        r5 = _make_scored_row(place_id="p5", kp_email="bad@co.com",
                               send_eligibility="block", is_primary_contact="true")
        # Write with send_eligibility populated → verification_source=scored_contacts
        _write_scored_contacts(paths.scored_contacts_file, [r1, r2, r3, r4, r5])

        result = run(paths=paths)
        assert result["total"]         == 5
        assert result["queue_normal"]  == 1
        assert result["queue_limited"] == 1
        assert result["hold"]          == 1
        assert result["generic_only"]  == 0
        assert result["block"]         == 2

    def test_named_vs_generic_primary_counts(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        named   = _make_scored_row(place_id="p1", kp_email="ceo@co.com",
                                    is_generic_mailbox="false", is_primary_contact="true")
        generic = _make_scored_row(place_id="p2", kp_email="info@co.com",
                                    is_generic_mailbox="true",  is_primary_contact="true",
                                    company_name="Acme B")
        _write_scored_contacts(paths.scored_contacts_file, [named, generic])

        result = run(paths=paths)
        assert result["named_primary"]   == 1
        assert result["generic_primary"] == 1

    def test_error_count_in_result(self, tmp_path):
        """Summary reports error_count (should be 0 for clean data)."""
        paths = _make_run_paths(tmp_path)
        row = _make_scored_row(is_primary_contact="true")
        _write_scored_contacts(paths.scored_contacts_file, [row])

        result = run(paths=paths)
        assert "errors" in result
        assert result["errors"] == 0

    def test_output_file_path_in_result(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        row = _make_scored_row(is_primary_contact="true")
        _write_scored_contacts(paths.scored_contacts_file, [row])

        result = run(paths=paths)
        assert "output_file" in result
        assert "queue_policy.csv" in result["output_file"]

    def test_queue_policy_stats_record_method(self):
        stats = QueuePolicyStats()
        rec = QueuePolicyRecord()
        rec.send_policy_action = POLICY_QUEUE_NORMAL
        rec.selected_contact_is_generic = "false"
        rec.selected_send_eligibility = "allow"
        rec.selected_send_pool = "primary_pool"
        stats.record(rec)
        assert stats.queue_normal_count == 1
        assert stats.named_primary_count == 1


# ===========================================================================
# 7. Verification Integration
# ===========================================================================

class TestVerificationIntegration:
    """Verification data from verified_enriched_leads.csv is used when available."""

    def test_verified_leads_enrich_scored_contact(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        # scored_contacts has no send_eligibility
        row = _make_scored_row(kp_email="ceo@co.com", send_eligibility="",
                                is_primary_contact="true")
        _write_scored_contacts(paths.scored_contacts_file, [row])

        # verified_enriched_leads.csv has E1 for the same email
        ver = [{"kp_email": "ceo@co.com", "send_eligibility": "allow",
                "send_pool": "primary_pool", "email_confidence_tier": "E1"}]
        _write_verified_leads(paths.verified_enriched_leads_file, ver)

        result = run(paths=paths)
        rows = load_queue_policy(paths.queue_policy_file)
        assert rows[0]["send_policy_action"] == POLICY_QUEUE_NORMAL
        assert rows[0]["verification_source"] == "verified_leads"
        assert rows[0]["selected_email_confidence_tier"] == "E1"

    def test_scored_contacts_verification_takes_precedence(self, tmp_path):
        """send_eligibility already on scored_contacts row is used before the index."""
        paths = _make_run_paths(tmp_path)
        # scored_contacts has E1 allow already
        row = _make_scored_row(kp_email="ceo@co.com",
                                send_eligibility="allow",
                                send_pool="primary_pool",
                                email_confidence_tier="E1",
                                is_primary_contact="true")
        _write_scored_contacts(paths.scored_contacts_file, [row])

        # verified_leads says block — should be IGNORED
        ver = [{"kp_email": "ceo@co.com", "send_eligibility": "block",
                "send_pool": "blocked_pool", "email_confidence_tier": "E0"}]
        _write_verified_leads(paths.verified_enriched_leads_file, ver)

        result = run(paths=paths)
        rows = load_queue_policy(paths.queue_policy_file)
        assert rows[0]["send_policy_action"] == POLICY_QUEUE_NORMAL
        assert rows[0]["verification_source"] == "scored_contacts"

    def test_no_verification_data_uses_fallback(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        row = _make_scored_row(kp_email="ceo@co.com", send_eligibility="",
                                is_primary_contact="true")
        _write_scored_contacts(paths.scored_contacts_file, [row])
        # No verified_enriched_leads.csv written

        run(paths=paths)
        rows = load_queue_policy(paths.queue_policy_file)
        assert rows[0]["verification_source"] == "fallback"
        # Unverified named email → queue_limited (conservative)
        assert rows[0]["send_policy_action"] == POLICY_QUEUE_LIMITED

    def test_verification_index_case_insensitive(self, tmp_path):
        """Email lookup in verification index is case-insensitive."""
        index = _build_verification_index.__wrapped__ \
            if hasattr(_build_verification_index, "__wrapped__") \
            else None
        # Direct test via _build_record
        scored_row = _make_scored_row(kp_email="CEO@Solar.Com", send_eligibility="",
                                       is_primary_contact="true")
        ver_index = {"ceo@solar.com": {
            "kp_email": "ceo@solar.com",
            "send_eligibility": "allow",
            "send_pool": "primary_pool",
            "email_confidence_tier": "E1",
        }}
        rec = _build_record(scored_row, ver_index)
        assert rec.selected_send_eligibility == "allow"
        assert rec.verification_source == "verified_leads"


# ===========================================================================
# 8. Resilience
# ===========================================================================

class TestResilience:
    """One bad record must not crash the batch; missing fields degrade safely."""

    def test_malformed_record_does_not_crash_batch(self, tmp_path, monkeypatch):
        """
        If one record causes an unexpected exception in _build_record or apply_policy,
        the pipeline continues and increments the error counter.
        """
        paths = _make_run_paths(tmp_path)

        good_row  = _make_scored_row(place_id="good", kp_email="good@co.com",
                                      is_primary_contact="true")
        _write_scored_contacts(paths.scored_contacts_file, [good_row])

        # Patch _build_record to raise on first call, succeed on second
        call_count = {"n": 0}
        import src.workflow_6_queue_policy.queue_policy_pipeline as pipe_mod
        original = pipe_mod._build_record

        def _patched(scored_row, vi):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise ValueError("simulated build error")
            return original(scored_row, vi)

        monkeypatch.setattr(pipe_mod, "_build_record", _patched)

        # Add a second row so the pipeline has something to succeed on
        second_row = _make_scored_row(place_id="good2", kp_email="good2@co.com",
                                       company_name="B", is_primary_contact="true")
        _write_scored_contacts(paths.scored_contacts_file, [good_row, second_row])

        result = run(paths=paths)
        # One succeeded, one errored
        assert result["errors"] == 1
        assert result["total"] == 1  # only the successful one counted

    def test_missing_kp_email_field_blocks_record(self, tmp_path):
        """Contact with no email is blocked, not treated as queue_normal."""
        paths = _make_run_paths(tmp_path)
        row = _make_scored_row(kp_email="", is_primary_contact="true")
        _write_scored_contacts(paths.scored_contacts_file, [row])

        run(paths=paths)
        rows = load_queue_policy(paths.queue_policy_file)
        assert rows[0]["send_policy_action"] == POLICY_BLOCK
        assert "no_email" in rows[0]["send_policy_reason"]

    def test_missing_optional_verification_degrades_gracefully(self, tmp_path):
        """Missing send_eligibility → fallback rules, not a crash."""
        paths = _make_run_paths(tmp_path)
        row = _make_scored_row(is_primary_contact="true")
        del row["send_eligibility"]  # simulate missing field
        _write_scored_contacts(paths.scored_contacts_file, [row])

        result = run(paths=paths)
        assert result["errors"] == 0
        rows = load_queue_policy(paths.queue_policy_file)
        assert rows[0]["send_policy_action"] in ALL_POLICY_ACTIONS

    def test_missing_scored_contacts_file_is_graceful(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        # No scored_contacts.csv written
        result = run(paths=paths)
        assert result["total"] == 0
        assert result["errors"] == 0

    def test_empty_scored_contacts_file_is_graceful(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        _write_scored_contacts(paths.scored_contacts_file, [])
        result = run(paths=paths)
        assert result["total"] == 0

    def test_missing_policy_critical_fields_uses_conservative_block(self, tmp_path):
        """
        Missing policy-critical verification data → conservative not queue_normal.
        Named unverified → queue_limited (not queue_normal).
        """
        paths = _make_run_paths(tmp_path)
        row = _make_scored_row(
            kp_email="jane@solar.com",
            is_generic_mailbox="false",
            send_eligibility="",  # no verification
            is_primary_contact="true",
        )
        _write_scored_contacts(paths.scored_contacts_file, [row])

        run(paths=paths)
        rows = load_queue_policy(paths.queue_policy_file)
        # Must NOT silently promote to queue_normal without verification
        assert rows[0]["send_policy_action"] != POLICY_QUEUE_NORMAL


# ===========================================================================
# 9. load_queued_normal helper
# ===========================================================================

class TestLoadQueuedNormal:
    """load_queued_normal returns only queue_normal rows."""

    def test_returns_only_queue_normal_rows(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        rows = [
            _make_scored_row(place_id="p1", kp_email="e1@co.com",
                              send_eligibility="allow", is_primary_contact="true"),
            _make_scored_row(place_id="p2", kp_email="e2@co.com",
                              send_eligibility="block", is_primary_contact="true"),
            _make_scored_row(place_id="p3", kp_email="e3@co.com",
                              send_eligibility="hold", is_primary_contact="true"),
        ]
        _write_scored_contacts(paths.scored_contacts_file, rows)
        run(paths=paths)

        normal = load_queued_normal(paths.queue_policy_file)
        assert len(normal) == 1
        assert normal[0]["selected_contact_email"] == "e1@co.com"

    def test_returns_empty_when_file_missing(self, tmp_path):
        missing = tmp_path / "nonexistent_queue_policy.csv"
        assert load_queued_normal(missing) == []

    def test_returns_empty_when_no_normal_rows(self, tmp_path):
        paths = _make_run_paths(tmp_path)
        row = _make_scored_row(kp_email="", is_primary_contact="true")  # no email → block
        _write_scored_contacts(paths.scored_contacts_file, [row])
        run(paths=paths)

        normal = load_queued_normal(paths.queue_policy_file)
        assert normal == []


# ===========================================================================
# 10. QueuePolicyRecord serialisation
# ===========================================================================

class TestQueuePolicyRecordSerialisation:
    """to_csv_row() produces correct output for DictWriter."""

    def test_all_fields_present_in_csv_row(self):
        rec = QueuePolicyRecord()
        rec.send_policy_action = POLICY_QUEUE_NORMAL
        row = rec.to_csv_row()
        for field in QUEUE_POLICY_FIELDS:
            assert field in row

    def test_policy_version_in_csv_row(self):
        rec = QueuePolicyRecord()
        row = rec.to_csv_row()
        assert row["policy_version"] == QUEUE_POLICY_VERSION
