"""
Tests — P1-2B Contact Scoring + Priority Selection (Workflow 5.6).

Covers the 9 mandatory test scenarios from the ticket spec:
  1. Named vs generic ordering
  2. Title-based ordering
  3. Source quality behaviour
  4. Email quality behaviour (verification tiers)
  5. Tie-break determinism
  6. Primary / fallback assignment
  7. Wrong-person preparation (alternate_contact_review_candidate)
  8. Backward compatibility (load_primary_contacts)
  9. Persistence correctness (pipeline output file)
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.workflow_5_6_contact_scoring.contact_scoring_models import (
    CONTACT_SCORING_VERSION,
    ScoredContact,
)
from src.workflow_5_6_contact_scoring.contact_scoring_rules import (
    assign_priority,
    compute_contact_fit_score,
    score_email_quality,
    score_generic_penalty,
    score_source,
    score_title,
    title_bucket,
)
from src.workflow_5_6_contact_scoring.contact_scoring_pipeline import (
    load_primary_contacts,
    run as scoring_run,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_contact(
    *,
    kp_name:              str = "Jane Smith",
    kp_title:             str = "",
    kp_email:             str = "jane@example.com",
    enrichment_source:    str = "apollo",
    is_generic_mailbox:   str = "false",
    contact_rank:         str = "1",
    email_confidence_tier: str = "",
    company_name:         str = "Acme Solar",
    place_id:             str = "pid-001",
) -> ScoredContact:
    sc = ScoredContact()
    sc.kp_name             = kp_name
    sc.kp_title            = kp_title
    sc.kp_email            = kp_email
    sc.enrichment_source   = enrichment_source
    sc.is_generic_mailbox  = is_generic_mailbox
    sc.contact_rank        = contact_rank
    sc.email_confidence_tier = email_confidence_tier
    sc.company_name        = company_name
    sc.place_id            = place_id
    return sc


def _scored(contact: ScoredContact) -> ScoredContact:
    return compute_contact_fit_score(contact)


def _make_run_paths(tmp_path: Path, campaign_id: str):
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


def _write_enriched_contacts(path: Path, rows: list[dict]) -> None:
    """Write a minimal enriched_contacts.csv for pipeline tests."""
    path.parent.mkdir(parents=True, exist_ok=True)
    # Use union of all keys as fieldnames
    fieldnames = list({k for row in rows for k in row})
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Fixture: clear active RunPaths
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def reset_run_paths():
    from config import run_paths as _rp
    _rp.clear_active_run_paths()
    yield
    _rp.clear_active_run_paths()


# ---------------------------------------------------------------------------
# 1. Named vs generic ordering
# ---------------------------------------------------------------------------

class TestNamedVsGenericOrdering:

    def test_named_contact_outranks_generic(self):
        named = _scored(_make_contact(
            kp_name="Jane Smith", kp_title="Procurement Manager",
            kp_email="jane@solar.com", enrichment_source="apollo",
            is_generic_mailbox="false",
        ))
        generic = _scored(_make_contact(
            kp_name="", kp_title="",
            kp_email="info@solar.com", enrichment_source="guessed",
            is_generic_mailbox="true",
        ))
        ranked = assign_priority([generic, named])
        assert ranked[0].kp_email == "jane@solar.com"
        assert ranked[0].is_primary_contact is True
        assert ranked[1].kp_email == "info@solar.com"
        assert ranked[1].is_primary_contact is False

    def test_generic_becomes_primary_when_only_option(self):
        generic = _scored(_make_contact(
            kp_email="info@solar.com", is_generic_mailbox="true",
            kp_title="", enrichment_source="guessed",
        ))
        ranked = assign_priority([generic])
        assert ranked[0].is_primary_contact is True
        assert ranked[0].contact_priority_bucket == "primary"

    def test_generic_penalty_applied(self):
        generic = _make_contact(is_generic_mailbox="true")
        _scored(generic)
        assert generic.generic_penalty == -25

    def test_non_generic_has_zero_penalty(self):
        named = _make_contact(is_generic_mailbox="false")
        _scored(named)
        assert named.generic_penalty == 0

    def test_generic_bucket_is_generic_fallback_when_not_primary(self):
        named = _scored(_make_contact(
            kp_email="ceo@solar.com", is_generic_mailbox="false",
            kp_title="CEO", enrichment_source="apollo",
        ))
        generic = _scored(_make_contact(
            kp_email="info@solar.com", is_generic_mailbox="true",
            enrichment_source="guessed",
        ))
        ranked = assign_priority([generic, named])
        generic_contact = next(c for c in ranked if c.kp_email == "info@solar.com")
        assert generic_contact.contact_priority_bucket == "generic_fallback"


# ---------------------------------------------------------------------------
# 2. Title-based ordering
# ---------------------------------------------------------------------------

class TestTitleScoring:

    def test_procurement_is_tier_A(self):
        sc = _make_contact(kp_title="Procurement Manager")
        ts, note = score_title(sc)
        assert ts == 40
        assert "tier_A" in note

    def test_owner_is_tier_A(self):
        sc = _make_contact(kp_title="Owner")
        ts, _ = score_title(sc)
        assert ts == 40

    def test_ceo_is_tier_A(self):
        sc = _make_contact(kp_title="CEO")
        ts, _ = score_title(sc)
        assert ts == 40

    def test_director_is_tier_B(self):
        sc = _make_contact(kp_title="Operations Director")
        ts, note = score_title(sc)
        assert ts == 30
        assert "tier_B" in note

    def test_general_manager_is_tier_B(self):
        sc = _make_contact(kp_title="General Manager")
        ts, _ = score_title(sc)
        assert ts == 30

    def test_project_manager_is_tier_C(self):
        sc = _make_contact(kp_title="Project Manager")
        ts, _ = score_title(sc)
        assert ts == 20

    def test_generic_manager_is_tier_D(self):
        sc = _make_contact(kp_title="Manager")
        ts, _ = score_title(sc)
        assert ts == 10

    def test_empty_title_is_zero(self):
        sc = _make_contact(kp_title="")
        ts, _ = score_title(sc)
        assert ts == 0

    def test_unknown_title_is_5(self):
        sc = _make_contact(kp_title="Fleet Planner")  # no tier match
        ts, note = score_title(sc)
        assert ts == 5
        assert "unknown" in note

    def test_procurement_outranks_weak_title(self):
        proc = _scored(_make_contact(kp_title="Purchasing Manager", enrichment_source="apollo"))
        weak = _scored(_make_contact(kp_title="Manager", enrichment_source="apollo", kp_email="mgr@solar.com"))
        ranked = assign_priority([weak, proc])
        assert ranked[0].kp_title == "Purchasing Manager"

    def test_title_bucket_helpers(self):
        assert title_bucket("Procurement Director") == "procurement/owner/CEO"
        assert title_bucket("Operations Manager") == "director/GM/operations"
        assert title_bucket("Project Manager") == "project/technical/engineering"
        assert title_bucket("Manager") == "generic_manager/specialist"
        assert title_bucket("") == "no_title"


# ---------------------------------------------------------------------------
# 3. Source quality behaviour
# ---------------------------------------------------------------------------

class TestSourceScoring:

    def test_apollo_highest_source_score(self):
        sc = _make_contact(enrichment_source="apollo")
        ss, _ = score_source(sc)
        assert ss == 20

    def test_hunter_second(self):
        sc = _make_contact(enrichment_source="hunter")
        ss, _ = score_source(sc)
        assert ss == 18

    def test_website_middle(self):
        sc = _make_contact(enrichment_source="website")
        ss, _ = score_source(sc)
        assert ss == 10

    def test_guessed_low(self):
        sc = _make_contact(enrichment_source="guessed")
        ss, _ = score_source(sc)
        assert ss == 3

    def test_mock_zero(self):
        sc = _make_contact(enrichment_source="mock")
        ss, _ = score_source(sc)
        assert ss == 0

    def test_apollo_outranks_website_with_equal_title(self):
        apollo = _scored(_make_contact(
            kp_title="Project Manager", enrichment_source="apollo",
            kp_email="pm@solar.com",
        ))
        website = _scored(_make_contact(
            kp_title="Project Manager", enrichment_source="website",
            kp_email="site@solar.com",
        ))
        ranked = assign_priority([website, apollo])
        assert ranked[0].enrichment_source == "apollo"


# ---------------------------------------------------------------------------
# 4. Email quality behaviour
# ---------------------------------------------------------------------------

class TestEmailQualityScoring:

    def test_e1_highest(self):
        sc = _make_contact(email_confidence_tier="E1")
        eqs, _ = score_email_quality(sc)
        assert eqs == 20

    def test_e2_middle(self):
        sc = _make_contact(email_confidence_tier="E2")
        eqs, _ = score_email_quality(sc)
        assert eqs == 12

    def test_e3_low(self):
        sc = _make_contact(email_confidence_tier="E3")
        eqs, _ = score_email_quality(sc)
        assert eqs == 5

    def test_e4_very_low(self):
        sc = _make_contact(email_confidence_tier="E4")
        eqs, _ = score_email_quality(sc)
        assert eqs == 3

    def test_e0_zero(self):
        sc = _make_contact(email_confidence_tier="E0")
        eqs, _ = score_email_quality(sc)
        assert eqs == 0

    def test_missing_tier_returns_default(self):
        sc = _make_contact(email_confidence_tier="")
        eqs, note = score_email_quality(sc)
        assert eqs == 8
        assert "unverified" in note

    def test_case_insensitive_tier(self):
        sc_lower = _make_contact(email_confidence_tier="e1")
        sc_upper = _make_contact(email_confidence_tier="E1")
        eqs_l, _ = score_email_quality(sc_lower)
        eqs_u, _ = score_email_quality(sc_upper)
        assert eqs_l == eqs_u == 20

    def test_e1_outranks_e3_in_selection(self):
        e1_contact = _scored(_make_contact(
            kp_title="Manager", enrichment_source="hunter",
            email_confidence_tier="E1", kp_email="verified@solar.com",
        ))
        e3_contact = _scored(_make_contact(
            kp_title="Manager", enrichment_source="hunter",
            email_confidence_tier="E3", kp_email="catchall@solar.com",
        ))
        ranked = assign_priority([e3_contact, e1_contact])
        assert ranked[0].email_confidence_tier == "E1"

    def test_missing_tier_does_not_crash(self):
        sc = _make_contact(email_confidence_tier="")
        compute_contact_fit_score(sc)  # must not raise
        assert sc.email_quality_score == 8


# ---------------------------------------------------------------------------
# 5. Tie-break determinism
# ---------------------------------------------------------------------------

class TestTieBreakDeterminism:

    def test_same_score_stable_by_kp_name(self):
        """Two identically scored contacts → lexically earlier kp_name wins."""
        # Both: apollo, tier_D title, no verification → same score
        alice = _scored(_make_contact(
            kp_name="Alice", kp_title="Manager",
            kp_email="alice@solar.com", enrichment_source="apollo",
        ))
        bob = _scored(_make_contact(
            kp_name="Bob", kp_title="Manager",
            kp_email="bob@solar.com", enrichment_source="apollo",
        ))
        assert alice.contact_fit_score == bob.contact_fit_score
        ranked = assign_priority([bob, alice])
        # "alice" < "bob" lexically
        assert ranked[0].kp_name == "Alice"

    def test_repeated_calls_same_order(self):
        """assign_priority must produce the same order on repeated calls."""
        contacts = [
            _scored(_make_contact(kp_name=n, kp_title="Manager",
                                  kp_email=f"{n.lower()}@solar.com",
                                  enrichment_source="apollo"))
            for n in ["Zara", "Mike", "Anna"]
        ]
        order1 = [c.kp_name for c in assign_priority(list(contacts))]
        order2 = [c.kp_name for c in assign_priority(list(contacts))]
        assert order1 == order2

    def test_non_generic_beats_generic_at_equal_fit_score(self):
        """When fit scores are equal (after penalty), non-generic ranks first."""
        # After generic penalty (−25), a generic contact with high title score
        # may still match a non-generic with a low score.
        # Confirm non-generic wins when fit scores are the same.
        non_generic = _scored(_make_contact(
            kp_email="person@solar.com", is_generic_mailbox="false",
            kp_title="", enrichment_source="none", email_confidence_tier="",
        ))
        generic = _scored(_make_contact(
            kp_email="info@solar.com", is_generic_mailbox="true",
            kp_title="", enrichment_source="none", email_confidence_tier="",
        ))
        # Force equal fit scores by manual override
        non_generic.contact_fit_score = generic.contact_fit_score
        ranked = assign_priority([generic, non_generic])
        assert ranked[0].is_generic_mailbox == "false"


# ---------------------------------------------------------------------------
# 6. Primary / fallback assignment
# ---------------------------------------------------------------------------

class TestPrimaryFallbackAssignment:

    def test_exactly_one_primary_per_company(self):
        contacts = [
            _scored(_make_contact(kp_email=f"c{i}@solar.com", kp_title="Manager"))
            for i in range(3)
        ]
        ranked = assign_priority(contacts)
        primaries = [c for c in ranked if c.is_primary_contact]
        assert len(primaries) == 1

    def test_rank_1_is_primary(self):
        contacts = [_scored(_make_contact(kp_email=f"c{i}@solar.com")) for i in range(2)]
        ranked = assign_priority(contacts)
        assert ranked[0].contact_priority_rank == 1
        assert ranked[0].is_primary_contact is True

    def test_rank_2_plus_are_fallback(self):
        contacts = [_scored(_make_contact(kp_email=f"c{i}@solar.com")) for i in range(3)]
        ranked = assign_priority(contacts)
        for c in ranked[1:]:
            assert c.is_fallback_contact is True
            assert c.is_primary_contact is False

    def test_single_contact_is_primary(self):
        contacts = [_scored(_make_contact())]
        ranked = assign_priority(contacts)
        assert ranked[0].is_primary_contact is True
        assert ranked[0].is_fallback_contact is False

    def test_empty_company_returns_empty(self):
        ranked = assign_priority([])
        assert ranked == []

    def test_priority_ranks_sequential(self):
        contacts = [_scored(_make_contact(kp_email=f"c{i}@solar.com")) for i in range(4)]
        ranked = assign_priority(contacts)
        ranks = [c.contact_priority_rank for c in ranked]
        assert ranks == [1, 2, 3, 4]

    def test_named_fallback_bucket(self):
        primary = _scored(_make_contact(
            kp_email="ceo@solar.com", kp_title="CEO",
            is_generic_mailbox="false",
        ))
        fallback = _scored(_make_contact(
            kp_email="ops@solar.com", kp_title="Operations Manager",
            is_generic_mailbox="false", kp_name="Bob Smith",
        ))
        ranked = assign_priority([fallback, primary])
        fb = next(c for c in ranked if c.kp_email == "ops@solar.com")
        assert fb.contact_priority_bucket == "fallback"


# ---------------------------------------------------------------------------
# 7. Wrong-person preparation
# ---------------------------------------------------------------------------

class TestWrongPersonPreparation:

    def test_named_fallback_is_review_candidate(self):
        primary = _scored(_make_contact(
            kp_email="ceo@solar.com", kp_title="CEO",
            is_generic_mailbox="false",
        ))
        named_fallback = _scored(_make_contact(
            kp_email="ops@solar.com", kp_title="Operations Manager",
            is_generic_mailbox="false", kp_name="Bob Smith",
        ))
        ranked = assign_priority([named_fallback, primary])
        fb = next(c for c in ranked if c.kp_email == "ops@solar.com")
        assert fb.alternate_contact_review_candidate is True

    def test_generic_fallback_is_not_review_candidate(self):
        primary = _scored(_make_contact(
            kp_email="ceo@solar.com", kp_title="CEO",
        ))
        generic_fallback = _scored(_make_contact(
            kp_email="info@solar.com", is_generic_mailbox="true",
            enrichment_source="guessed",
        ))
        ranked = assign_priority([generic_fallback, primary])
        gf = next(c for c in ranked if c.kp_email == "info@solar.com")
        assert gf.alternate_contact_review_candidate is False

    def test_primary_is_never_review_candidate(self):
        contacts = [_scored(_make_contact(kp_email=f"c{i}@solar.com")) for i in range(3)]
        ranked = assign_priority(contacts)
        assert ranked[0].alternate_contact_review_candidate is False

    def test_no_auto_send_fields_set(self):
        """Selection fields must not include any automatic next-contact send trigger."""
        contacts = [_scored(_make_contact(kp_email=f"c{i}@solar.com")) for i in range(2)]
        ranked = assign_priority(contacts)
        for c in ranked:
            # These fields must NOT exist on ScoredContact
            assert not hasattr(c, "auto_send_next")
            assert not hasattr(c, "trigger_reroute")


# ---------------------------------------------------------------------------
# 8. Backward compatibility — load_primary_contacts
# ---------------------------------------------------------------------------

class TestLoadPrimaryContacts:

    def test_returns_primary_contacts_by_place_id(self, tmp_path):
        """load_primary_contacts returns {place_id: row} for is_primary_contact=true rows."""
        scored_path = tmp_path / "scored_contacts.csv"
        rows = [
            {"place_id": "pid-1", "company_name": "A", "kp_email": "a@a.com",
             "is_primary_contact": "true", "website": ""},
            {"place_id": "pid-1", "company_name": "A", "kp_email": "b@a.com",
             "is_primary_contact": "false", "website": ""},
            {"place_id": "pid-2", "company_name": "B", "kp_email": "c@b.com",
             "is_primary_contact": "true", "website": ""},
        ]
        with open(scored_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

        result = load_primary_contacts(scored_path)
        assert set(result.keys()) == {"pid-1", "pid-2"}
        assert result["pid-1"]["kp_email"] == "a@a.com"
        assert result["pid-2"]["kp_email"] == "c@b.com"

    def test_returns_empty_when_file_missing(self, tmp_path):
        missing = tmp_path / "no_such_file.csv"
        result = load_primary_contacts(missing)
        assert result == {}

    def test_only_one_primary_per_place_id(self, tmp_path):
        scored_path = tmp_path / "scored_contacts.csv"
        rows = [
            {"place_id": "pid-x", "kp_email": "first@x.com",
             "is_primary_contact": "true", "website": "", "company_name": "X"},
            {"place_id": "pid-x", "kp_email": "second@x.com",
             "is_primary_contact": "true", "website": "", "company_name": "X"},
        ]
        with open(scored_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        result = load_primary_contacts(scored_path)
        # First primary wins
        assert result["pid-x"]["kp_email"] == "first@x.com"


# ---------------------------------------------------------------------------
# 9. Persistence correctness — pipeline output
# ---------------------------------------------------------------------------

class TestPipelinePersistence:

    def _minimal_contact_row(self, **overrides) -> dict:
        base = {
            "company_name": "Acme Solar", "website": "https://acme.com",
            "place_id": "pid-001", "company_type": "solar installer",
            "market_focus": "commercial", "services_detected": "",
            "confidence_score": "0.8", "classification_method": "ai",
            "lead_score": "70", "score_breakdown": "", "target_tier": "A",
            "kp_name": "Jane Smith", "kp_title": "Procurement Manager",
            "kp_email": "jane@acme.com", "enrichment_source": "apollo",
            "site_phone": "", "whatsapp_phone": "", "email_sendable": "true",
            "contact_channel": "email", "alt_outreach_possible": "false",
            "contact_trust": "trusted", "skip_reason": "",
            "contact_rank": "1", "is_generic_mailbox": "false",
        }
        base.update(overrides)
        return base

    def test_pipeline_writes_scored_contacts_file(self, tmp_path):
        from config import run_paths as _rp
        rp = _make_run_paths(tmp_path, "persist-001")
        _write_enriched_contacts(rp.enriched_contacts_file, [self._minimal_contact_row()])
        _rp.set_active_run_paths(rp)
        scoring_run(paths=rp)
        assert rp.scored_contacts_file.exists()

    def test_pipeline_empty_input_writes_empty_file(self, tmp_path):
        from config import run_paths as _rp
        rp = _make_run_paths(tmp_path, "persist-empty")
        # Write header-only CSV
        with open(rp.enriched_contacts_file, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=["company_name"]).writeheader()
        _rp.set_active_run_paths(rp)
        result = scoring_run(paths=rp)
        assert result == []
        assert rp.scored_contacts_file.exists()

    def test_pipeline_missing_input_writes_empty_file(self, tmp_path):
        from config import run_paths as _rp
        rp = _make_run_paths(tmp_path, "persist-missing")
        _rp.set_active_run_paths(rp)
        result = scoring_run(paths=rp)
        assert result == []
        assert rp.scored_contacts_file.exists()

    def test_pipeline_output_has_expected_fields(self, tmp_path):
        from config import run_paths as _rp
        rp = _make_run_paths(tmp_path, "persist-fields")
        _write_enriched_contacts(rp.enriched_contacts_file, [self._minimal_contact_row()])
        _rp.set_active_run_paths(rp)
        scoring_run(paths=rp)
        with open(rp.scored_contacts_file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 1
        row = rows[0]
        assert "contact_fit_score" in row
        assert "contact_priority_rank" in row
        assert "is_primary_contact" in row
        assert "contact_priority_bucket" in row
        assert "contact_scoring_version" in row

    def test_pipeline_primary_contact_marked_in_output(self, tmp_path):
        from config import run_paths as _rp
        rp = _make_run_paths(tmp_path, "persist-primary")
        rows = [
            self._minimal_contact_row(
                kp_email="ceo@acme.com", kp_title="CEO",
                enrichment_source="apollo", contact_rank="1",
            ),
            self._minimal_contact_row(
                kp_email="info@acme.com", kp_title="",
                enrichment_source="guessed", contact_rank="2",
                is_generic_mailbox="true",
            ),
        ]
        _write_enriched_contacts(rp.enriched_contacts_file, rows)
        _rp.set_active_run_paths(rp)
        scoring_run(paths=rp)
        with open(rp.scored_contacts_file, newline="", encoding="utf-8") as f:
            output = list(csv.DictReader(f))
        primary = next(r for r in output if r["is_primary_contact"] == "true")
        assert primary["kp_email"] == "ceo@acme.com"

    def test_pipeline_multi_company_isolation(self, tmp_path):
        """Contacts from different companies ranked independently."""
        from config import run_paths as _rp
        rp = _make_run_paths(tmp_path, "persist-multi")
        rows = [
            # Company 1 — generic contact first in file
            self._minimal_contact_row(
                place_id="c1", company_name="Alpha Solar",
                kp_email="info@alpha.com", kp_title="", is_generic_mailbox="true",
                contact_rank="2", enrichment_source="guessed",
            ),
            # Company 1 — named contact
            self._minimal_contact_row(
                place_id="c1", company_name="Alpha Solar",
                kp_email="ceo@alpha.com", kp_title="CEO", is_generic_mailbox="false",
                contact_rank="1", enrichment_source="apollo",
            ),
            # Company 2 — only a generic
            self._minimal_contact_row(
                place_id="c2", company_name="Beta Solar",
                kp_email="sales@beta.com", kp_title="", is_generic_mailbox="true",
                contact_rank="1", enrichment_source="guessed",
            ),
        ]
        _write_enriched_contacts(rp.enriched_contacts_file, rows)
        _rp.set_active_run_paths(rp)
        scoring_run(paths=rp)
        with open(rp.scored_contacts_file, newline="", encoding="utf-8") as f:
            output = list(csv.DictReader(f))

        # Company 1 primary must be the named CEO
        c1_primary = next(r for r in output if r["place_id"] == "c1" and r["is_primary_contact"] == "true")
        assert c1_primary["kp_email"] == "ceo@alpha.com"
        # Company 2 primary must be the generic (only option)
        c2_primary = next(r for r in output if r["place_id"] == "c2" and r["is_primary_contact"] == "true")
        assert c2_primary["kp_email"] == "sales@beta.com"

    def test_scoring_version_written_in_output(self, tmp_path):
        from config import run_paths as _rp
        rp = _make_run_paths(tmp_path, "persist-version")
        _write_enriched_contacts(rp.enriched_contacts_file, [self._minimal_contact_row()])
        _rp.set_active_run_paths(rp)
        scoring_run(paths=rp)
        with open(rp.scored_contacts_file, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        assert rows[0]["contact_scoring_version"] == CONTACT_SCORING_VERSION

    def test_require_active_run_paths_raised_without_context(self):
        from config.run_paths import clear_active_run_paths, require_active_run_paths
        clear_active_run_paths()
        with pytest.raises(RuntimeError, match="No active RunPaths"):
            require_active_run_paths()


# ---------------------------------------------------------------------------
# Fit score integration tests
# ---------------------------------------------------------------------------

class TestFitScoreIntegration:

    def test_breakdown_string_contains_all_components(self):
        sc = _scored(_make_contact(
            kp_title="Procurement Manager", enrichment_source="apollo",
            email_confidence_tier="E1", is_generic_mailbox="false",
        ))
        bd = sc.contact_score_breakdown
        assert "title=" in bd
        assert "source=" in bd
        assert "email_quality=" in bd
        assert "generic_penalty=" in bd

    def test_fit_score_floored_at_zero(self):
        """A contact with all worst settings must not produce negative fit score."""
        sc = _scored(_make_contact(
            kp_title="", enrichment_source="none",
            email_confidence_tier="E0", is_generic_mailbox="true",
        ))
        assert sc.contact_fit_score >= 0

    def test_selection_reason_populated(self):
        contacts = [_scored(_make_contact())]
        ranked = assign_priority(contacts)
        assert ranked[0].contact_selection_reason != ""

    def test_high_scoring_contact_example(self):
        sc = _scored(_make_contact(
            kp_title="Procurement Manager",  # tier_A → 40
            enrichment_source="apollo",       # → 20
            email_confidence_tier="E1",       # → 20
            is_generic_mailbox="false",       # → 0 penalty
        ))
        assert sc.contact_fit_score == 80  # 40 + 20 + 20 + 0
