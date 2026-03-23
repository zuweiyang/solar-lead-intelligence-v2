"""
Ticket P1-1B — Lead Scoring Upgrade with Buyer Relevance: Test Suite

Run from the project root:
    py scripts/test_lead_scorer_p1_1b.py

Covers:
    Group A: Buyer boost improves strong commercial EPC score
    Group B: Residential installer receives penalty + fails threshold
    Group C: Manufacturer/competitor hard-capped below threshold
    Group D: Consultant hard-capped below threshold
    Group E: Media/directory hard-capped below threshold
    Group F: Value chain role adjustments affect score correctly
    Group G: Explainability fields populated and meaningful
    Group H: Fallback — buyer_filter.json missing → v1 solar-only scoring
    Group I: Fallback — v1 scoring preserved (solar-only scores match v1 logic)
    Group J: Output compatibility — qualified_leads has all expected fields
    Group K: Score range 0–100 for all role/market combinations
    Group L: Threshold enforcement — qualified vs rejected consistent
    Group M: Pipeline load_records() prefers buyer_filter.json over company_analysis.json

No real API calls. No real files required for most groups.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _assert(condition: bool, name: str, detail: str = "") -> None:
    if condition:
        print(f"  [PASS] {name}")
    else:
        print(f"  [FAIL] {name}" + (f" — {detail}" if detail else ""))
        sys.exit(1)


def _make_analysis_record(
    company_type: str = "solar installer",
    market_focus: str = "commercial",
    company_name: str = "Test Co",
    website: str = "https://test.com",
    place_id: str = "test_001",
    confidence_score: float = 0.85,
    classification_method: str = "ai",
) -> dict:
    """Make a raw company_analysis.json record (no buyer filter fields)."""
    return {
        "company_name":          company_name,
        "website":               website,
        "place_id":              place_id,
        "company_type":          company_type,
        "market_focus":          market_focus,
        "services_detected":     [],
        "confidence_score":      confidence_score,
        "classification_method": classification_method,
    }


def _make_bf_record(
    company_type: str = "solar installer",
    market_focus: str = "commercial",
    value_chain_role: str = "installer",
    buyer_likelihood_score: int = 7,
    procurement_relevance_score: int = 7,
    market_fit_score: int = 7,
    project_signal_strength: int = 3,
    competitor_flag: bool = False,
    manufacturer_flag: bool = False,
    consultant_flag: bool = False,
    media_or_directory_flag: bool = False,
    negative_residential_flag: bool = False,
    negative_targeting_reasons: list | None = None,
    company_name: str = "Test Co",
    website: str = "https://test.com",
    place_id: str = "test_001",
    confidence_score: float = 0.85,
    classification_method: str = "ai",
) -> dict:
    """Make a buyer_filter.json-enriched record (has buyer filter fields)."""
    return {
        "company_name":              company_name,
        "website":                   website,
        "place_id":                  place_id,
        "company_type":              company_type,
        "market_focus":              market_focus,
        "services_detected":         [],
        "confidence_score":          confidence_score,
        "classification_method":     classification_method,
        # Buyer filter fields (P1-1A)
        "value_chain_role":          value_chain_role,
        "value_chain_reason":        f"company_type={company_type!r}",
        "buyer_likelihood_score":    buyer_likelihood_score,
        "procurement_relevance_score": procurement_relevance_score,
        "market_fit_score":          market_fit_score,
        "project_signal_strength":   project_signal_strength,
        "commercial_signal_strength": 3,
        "utility_signal_strength":   0,
        "installer_signal_strength": 4,
        "developer_signal_strength": 0,
        "distributor_signal_strength": 0,
        "competitor_flag":           competitor_flag,
        "manufacturer_flag":         manufacturer_flag,
        "consultant_flag":           consultant_flag,
        "media_or_directory_flag":   media_or_directory_flag,
        "negative_residential_flag": negative_residential_flag,
        "buyer_filter_reason":       f"role={value_chain_role}; prs={procurement_relevance_score}/10",
        "negative_targeting_reasons": negative_targeting_reasons or [],
    }


def _score(record: dict) -> dict:
    from src.workflow_5_lead_scoring.lead_scorer import score_company
    return score_company(record)


# ---------------------------------------------------------------------------
# Group A: Buyer boost improves strong commercial EPC score
# ---------------------------------------------------------------------------

def test_group_a_buyer_boost() -> None:
    print("\n[Group A] Buyer boost improves strong commercial EPC score")

    # Strong commercial EPC with high PRS + project signals
    rec = _make_bf_record(
        company_type="solar EPC",
        market_focus="commercial",
        value_chain_role="epc_or_contractor",
        procurement_relevance_score=10,
        project_signal_strength=7,
    )
    scored = _score(rec)

    # Should score well above threshold
    _assert(scored["lead_score"] >= 75,
            f"strong EPC bls→lead_score≥75, got {scored['lead_score']}")
    _assert(scored["buyer_relevance_component"] > 0,
            f"buyer_relevance_component > 0, got {scored['buyer_relevance_component']}")
    _assert(scored["solar_relevance_component"] > 0,
            f"solar_relevance_component > 0, got {scored['solar_relevance_component']}")
    _assert(scored["scoring_version"] == "v2_with_buyer_filter",
            f"scoring_version is v2, got {scored['scoring_version']!r}")

    # Same company WITHOUT buyer filter fields (fallback) should score lower
    rec_v1 = _make_analysis_record(
        company_type="solar EPC", market_focus="commercial"
    )
    scored_v1 = _score(rec_v1)

    _assert(
        scored["lead_score"] > scored_v1["lead_score"],
        f"EPC with BF ({scored['lead_score']}) > EPC without BF ({scored_v1['lead_score']})",
    )
    _assert(scored_v1["scoring_version"] == "v1_solar_only",
            f"v1 record gets v1_solar_only, got {scored_v1['scoring_version']!r}")


# ---------------------------------------------------------------------------
# Group B: Residential installer receives penalty + fails threshold
# ---------------------------------------------------------------------------

def test_group_b_residential_scoring() -> None:
    print("\n[Group B] Residential installer qualifies — scores lower than commercial, not blocked")

    # Residential installers ARE valid outbound targets (buy mounting/racking).
    # The residential flag is informational only; no penalty is applied.
    # The only scoring difference vs commercial is the market_focus bonus:
    #   residential: -5  vs  commercial: +20
    rec = _make_bf_record(
        company_type="solar installer",
        market_focus="residential",
        value_chain_role="installer",
        procurement_relevance_score=4,
        project_signal_strength=0,
        negative_residential_flag=True,
        negative_targeting_reasons=["primary market: residential/homeowner"],
    )
    scored = _score(rec)

    # No penalty applied for residential flag
    _assert(scored["negative_targeting_penalty"] == 0,
            f"residential flag → negative_targeting_penalty=0, got {scored['negative_targeting_penalty']}")
    # Residential installer still qualifies (threshold 45)
    # Score: 40 (installer) + (-5) (residential) + 10 (website) + 6 (buyer boost) = 51
    _assert(scored["lead_score"] >= 45,
            f"residential installer still qualifies (≥45), got {scored['lead_score']}")
    _assert(scored["qualification_status"] == "qualified",
            f"qualification_status=qualified, got {scored['qualification_status']!r}")
    # Flag is preserved in output for operator visibility
    _assert(scored.get("negative_residential_flag") is True
            or str(scored.get("negative_residential_flag")).lower() == "true",
            "negative_residential_flag preserved in output (informational)")

    # Commercial installer should score higher (better market bonus)
    rec_com = _make_bf_record(
        company_type="solar installer",
        market_focus="commercial",
        value_chain_role="installer",
        procurement_relevance_score=8,
        project_signal_strength=4,
        negative_residential_flag=False,
    )
    scored_com = _score(rec_com)
    _assert(
        scored_com["lead_score"] > scored["lead_score"],
        f"commercial installer ({scored_com['lead_score']}) > residential installer ({scored['lead_score']})",
    )


# ---------------------------------------------------------------------------
# Group C: Manufacturer/competitor hard-capped below threshold
# ---------------------------------------------------------------------------

def test_group_c_manufacturer_cap() -> None:
    print("\n[Group C] Manufacturer/competitor hard-capped below threshold")

    rec = _make_bf_record(
        company_type="solar energy company",
        market_focus="mixed",
        value_chain_role="manufacturer",
        procurement_relevance_score=1,
        project_signal_strength=0,
        competitor_flag=True,
        manufacturer_flag=True,
        negative_targeting_reasons=["manufacturer/competitor detected"],
    )
    scored = _score(rec)

    _assert(scored["lead_score"] <= 30,
            f"manufacturer lead_score ≤ 30, got {scored['lead_score']}")
    _assert(scored["lead_score"] < 45,
            f"manufacturer fails threshold (< 45), got {scored['lead_score']}")
    _assert(scored["qualification_status"] == "rejected",
            f"qualification_status=rejected, got {scored['qualification_status']!r}")

    # Score breakdown should mention the manufacturer role adjustment
    breakdown_str = " | ".join(str(x) for x in scored["score_breakdown"])
    _assert("manufacturer" in breakdown_str.lower(),
            f"score_breakdown mentions manufacturer: {breakdown_str!r}")

    # A high-type company (solar EPC) classified as manufacturer should be
    # hard-capped at 30. Solar score = 70, role adj = -20, raw = 53 → cap fires.
    rec_epc_mfr = _make_bf_record(
        company_type="solar EPC",
        market_focus="commercial",
        value_chain_role="manufacturer",
        procurement_relevance_score=2,
        project_signal_strength=0,
        manufacturer_flag=True,
        competitor_flag=True,
    )
    scored_epc_mfr = _score(rec_epc_mfr)
    epc_mfr_bd = " | ".join(str(x) for x in scored_epc_mfr["score_breakdown"])
    _assert(scored_epc_mfr["lead_score"] <= 30,
            f"EPC classified as manufacturer hard-capped ≤ 30, got {scored_epc_mfr['lead_score']}")
    _assert("hard_cap" in epc_mfr_bd or "cap" in epc_mfr_bd.lower(),
            f"EPC/manufacturer breakdown mentions hard_cap: {epc_mfr_bd!r}")


# ---------------------------------------------------------------------------
# Group D: Consultant hard-capped below threshold
# ---------------------------------------------------------------------------

def test_group_d_consultant_cap() -> None:
    print("\n[Group D] Consultant hard-capped below threshold")

    rec = _make_bf_record(
        company_type="solar energy company",
        market_focus="mixed",
        value_chain_role="consultant",
        procurement_relevance_score=2,
        project_signal_strength=0,
        consultant_flag=True,
        negative_targeting_reasons=["advisory/consulting firm detected"],
    )
    scored = _score(rec)

    _assert(scored["lead_score"] <= 30,
            f"consultant lead_score ≤ 30, got {scored['lead_score']}")
    _assert(scored["qualification_status"] == "rejected",
            f"qualification_status=rejected, got {scored['qualification_status']!r}")
    _assert(scored["value_chain_adjustment"] <= -15,
            f"value_chain_adjustment ≤ -15 for consultant, got {scored['value_chain_adjustment']}")


# ---------------------------------------------------------------------------
# Group E: Media/directory hard-capped below threshold
# ---------------------------------------------------------------------------

def test_group_e_media_cap() -> None:
    print("\n[Group E] Media/directory hard-capped below threshold")

    rec = _make_bf_record(
        company_type="solar energy company",
        market_focus="mixed",
        value_chain_role="media_or_directory",
        procurement_relevance_score=1,
        project_signal_strength=0,
        media_or_directory_flag=True,
        negative_targeting_reasons=["media/directory detected"],
    )
    scored = _score(rec)

    _assert(scored["lead_score"] <= 25,
            f"media lead_score ≤ 25, got {scored['lead_score']}")
    _assert(scored["qualification_status"] == "rejected",
            f"qualification_status=rejected, got {scored['qualification_status']!r}")
    _assert(scored["value_chain_adjustment"] <= -20,
            f"value_chain_adjustment ≤ -20 for media, got {scored['value_chain_adjustment']}")


# ---------------------------------------------------------------------------
# Group F: Value chain role adjustments affect score correctly
# ---------------------------------------------------------------------------

def test_group_f_role_adjustments() -> None:
    print("\n[Group F] Value chain role adjustments affect score correctly")

    def _score_role(role: str, company_type: str, market: str, prs: int) -> int:
        rec = _make_bf_record(
            company_type=company_type,
            market_focus=market,
            value_chain_role=role,
            procurement_relevance_score=prs,
            project_signal_strength=2,
        )
        return _score(rec)["lead_score"]

    # EPC should score higher than plain installer (role +3 bonus)
    epc_score     = _score_role("epc_or_contractor", "solar EPC",       "commercial", 8)
    inst_score    = _score_role("installer",          "solar installer", "commercial", 8)
    _assert(epc_score >= inst_score,
            f"EPC role ({epc_score}) ≥ installer role ({inst_score})")

    # Unclear role should score lower than same company with clear installer role
    unclear_score = _score_role("unclear", "solar installer", "commercial", 8)
    _assert(inst_score > unclear_score,
            f"installer role ({inst_score}) > unclear role ({unclear_score})")

    # Developer should qualify
    dev_score = _score_role("developer", "solar developer", "utility-scale", 6)
    _assert(dev_score >= 45,
            f"developer utility-scale → qualifies (≥45), got {dev_score}")

    # Distributor should qualify with moderate signals
    dist_score = _score_role("distributor", "solar component distributor", "mixed", 6)
    _assert(dist_score >= 45,
            f"distributor mixed → qualifies (≥45), got {dist_score}")

    # Manufacturer should NOT qualify
    mfr_rec = _make_bf_record(
        company_type="solar energy company", market_focus="mixed",
        value_chain_role="manufacturer", procurement_relevance_score=1,
        manufacturer_flag=True, competitor_flag=True,
    )
    mfr_score = _score(mfr_rec)["lead_score"]
    _assert(mfr_score < 45,
            f"manufacturer → fails threshold (<45), got {mfr_score}")


# ---------------------------------------------------------------------------
# Group G: Explainability fields populated and meaningful
# ---------------------------------------------------------------------------

def test_group_g_explainability() -> None:
    print("\n[Group G] Explainability fields populated and meaningful")

    rec = _make_bf_record(
        company_type="solar EPC",
        market_focus="commercial",
        value_chain_role="epc_or_contractor",
        procurement_relevance_score=9,
        project_signal_strength=5,
    )
    scored = _score(rec)

    # qualification_reason_summary
    reason = scored.get("qualification_reason_summary", "")
    _assert(bool(reason), f"qualification_reason_summary is non-empty: {reason!r}")
    _assert("role=" in reason,
            f"reason mentions role: {reason!r}")
    _assert("score=" in reason or "final=" in reason or str(scored["lead_score"]) in reason,
            f"reason mentions final score: {reason!r}")
    _assert("QUALIFIED" in reason or "REJECTED" in reason,
            f"reason mentions QUALIFIED/REJECTED: {reason!r}")

    # Score components
    _assert(isinstance(scored["solar_relevance_component"], int),
            "solar_relevance_component is int")
    _assert(isinstance(scored["buyer_relevance_component"], int),
            "buyer_relevance_component is int")
    _assert(isinstance(scored["value_chain_adjustment"], int),
            "value_chain_adjustment is int")
    _assert(isinstance(scored["negative_targeting_penalty"], int),
            "negative_targeting_penalty is int")
    _assert(scored["scoring_version"] == "v2_with_buyer_filter",
            f"scoring_version is v2, got {scored['scoring_version']!r}")

    # score_breakdown is a list and non-empty
    breakdown = scored.get("score_breakdown", [])
    _assert(isinstance(breakdown, list) and len(breakdown) > 0,
            f"score_breakdown non-empty list, got {breakdown!r}")

    # For a rejected record, reason should say REJECTED
    rec_rej = _make_bf_record(
        company_type="solar energy company",
        market_focus="mixed",
        value_chain_role="manufacturer",
        procurement_relevance_score=1,
        manufacturer_flag=True,
        competitor_flag=True,
    )
    scored_rej = _score(rec_rej)
    reason_rej = scored_rej.get("qualification_reason_summary", "")
    _assert("REJECTED" in reason_rej,
            f"rejected record reason says REJECTED: {reason_rej!r}")
    _assert(scored_rej["qualification_status"] == "rejected",
            f"qualification_status=rejected, got {scored_rej['qualification_status']!r}")


# ---------------------------------------------------------------------------
# Group H: Fallback — missing buyer_filter.json → v1 solar-only scoring
# ---------------------------------------------------------------------------

def test_group_h_fallback_missing_bf() -> None:
    print("\n[Group H] Fallback — missing buyer_filter.json → v1 solar-only")

    _scorer_mod = "src.workflow_5_lead_scoring.lead_scorer"
    with tempfile.TemporaryDirectory() as tmp:
        fake_analyses = [
            _make_analysis_record(company_type="solar installer", market_focus="commercial"),
            _make_analysis_record(company_type="solar EPC", market_focus="commercial"),
        ]
        ca_path  = Path(tmp) / "company_analysis.json"
        bf_path  = Path(tmp) / "buyer_filter.json"   # does NOT exist
        ql_path  = Path(tmp) / "qualified_leads.csv"
        dq_path  = Path(tmp) / "disqualified_leads.csv"

        ca_path.write_text(json.dumps(fake_analyses), encoding="utf-8")

        with patch(f"{_scorer_mod}.BUYER_FILTER_FILE",      bf_path), \
             patch(f"{_scorer_mod}.COMPANY_ANALYSIS_FILE",  ca_path), \
             patch(f"{_scorer_mod}.QUALIFIED_LEADS_FILE",   ql_path), \
             patch(f"{_scorer_mod}.DISQUALIFIED_LEADS_FILE",dq_path):
            from src.workflow_5_lead_scoring.lead_scorer import load_records
            records, used_bf = load_records()

        _assert(not used_bf,
                "used_bf=False when buyer_filter.json missing")
        _assert(len(records) == 2,
                f"loaded 2 records from fallback, got {len(records)}")
        # Records should not have value_chain_role (no BF data)
        _assert(not records[0].get("value_chain_role"),
                "fallback records have no value_chain_role")


# ---------------------------------------------------------------------------
# Group I: Fallback — v1 solar-only scores match v1 formula
# ---------------------------------------------------------------------------

def test_group_i_v1_fallback_scores() -> None:
    print("\n[Group I] Fallback — v1 solar-only scores match v1 formula")

    # When no buyer filter fields present, scores should be identical to v1 logic
    # v1 formula: type + market + website + confidence_penalty + legacy_penalties
    cases = [
        ("solar installer",             "commercial",    40 + 20 + 10),   # 70
        ("solar EPC",                   "commercial",    40 + 20 + 10),   # 70
        ("solar developer",             "utility-scale", 35 + 25 + 10),   # 70
        ("solar energy company",        "mixed",         20 + 10 + 10),   # 40
        ("solar component distributor", "mixed",         25 + 10 + 10),   # 45
        ("solar installer",             "residential",   40 + (-5) + 10), # 45
    ]

    for company_type, market_focus, expected_solar_score in cases:
        rec    = _make_analysis_record(company_type=company_type, market_focus=market_focus)
        scored = _score(rec)

        _assert(scored["scoring_version"] == "v1_solar_only",
                f"{company_type} fallback → v1_solar_only")
        _assert(scored["buyer_relevance_component"] == 0,
                f"{company_type} fallback → buyer_relevance_component=0")
        _assert(scored["value_chain_adjustment"] == 0,
                f"{company_type} fallback → value_chain_adjustment=0")
        _assert(scored["negative_targeting_penalty"] == 0,
                f"{company_type} fallback → negative_targeting_penalty=0")

        expected_final = max(0, min(100, expected_solar_score))
        _assert(scored["lead_score"] == expected_final,
                f"{company_type}/{market_focus} v1 score={expected_final}",
                f"got {scored['lead_score']} (solar={scored['solar_relevance_component']})")


# ---------------------------------------------------------------------------
# Group J: Output compatibility — expected fields present in qualified_leads.csv
# ---------------------------------------------------------------------------

def test_group_j_output_compatibility() -> None:
    print("\n[Group J] Output compatibility — qualified_leads.csv fields")

    _scorer_mod = "src.workflow_5_lead_scoring.lead_scorer"
    import csv as _csv

    with tempfile.TemporaryDirectory() as tmp:
        records = [
            _make_bf_record(
                company_type="solar EPC",
                market_focus="commercial",
                value_chain_role="epc_or_contractor",
                procurement_relevance_score=9,
                project_signal_strength=5,
                company_name="Good EPC Co",
                place_id="j001",
            ),
            _make_bf_record(
                company_type="solar energy company",
                market_focus="mixed",
                value_chain_role="manufacturer",
                procurement_relevance_score=1,
                manufacturer_flag=True,
                competitor_flag=True,
                company_name="Bad Mfr Co",
                place_id="j002",
            ),
        ]
        bf_path  = Path(tmp) / "buyer_filter.json"
        ql_path  = Path(tmp) / "qualified_leads.csv"
        dq_path  = Path(tmp) / "disqualified_leads.csv"
        bf_path.write_text(json.dumps(records), encoding="utf-8")

        with patch(f"{_scorer_mod}.BUYER_FILTER_FILE",      bf_path), \
             patch(f"{_scorer_mod}.COMPANY_ANALYSIS_FILE",  Path(tmp) / "ca.json"), \
             patch(f"{_scorer_mod}.QUALIFIED_LEADS_FILE",   ql_path), \
             patch(f"{_scorer_mod}.DISQUALIFIED_LEADS_FILE", dq_path):
            from src.workflow_5_lead_scoring.lead_scorer import run as _run
            qualified = _run()

        # qualified_leads.csv must contain original fields
        _assert(ql_path.exists(), "qualified_leads.csv created")
        with open(ql_path, newline="", encoding="utf-8") as f:
            reader = _csv.DictReader(f)
            rows   = list(reader)

        _assert(len(rows) >= 1, f"at least 1 qualified row, got {len(rows)}")
        if rows:
            row = rows[0]
            for field in ["company_name", "website", "company_type", "market_focus",
                          "lead_score", "score_breakdown", "target_tier"]:
                _assert(field in row, f"original field '{field}' present in qualified_leads.csv")
            for field in ["qualification_status", "qualification_reason_summary",
                          "solar_relevance_component", "buyer_relevance_component",
                          "value_chain_adjustment", "negative_targeting_penalty",
                          "scoring_version", "value_chain_role", "buyer_likelihood_score"]:
                _assert(field in row, f"P1-1B field '{field}' present in qualified_leads.csv")
            _assert(row["qualification_status"] == "qualified",
                    f"qualified row has status=qualified, got {row['qualification_status']!r}")

        # disqualified_leads.csv should also exist
        _assert(dq_path.exists(), "disqualified_leads.csv created")
        with open(dq_path, newline="", encoding="utf-8") as f:
            dq_rows = list(_csv.DictReader(f))
        _assert(len(dq_rows) >= 1, f"at least 1 disqualified row, got {len(dq_rows)}")


# ---------------------------------------------------------------------------
# Group K: Score range 0–100 for all role/market combinations
# ---------------------------------------------------------------------------

def test_group_k_score_range() -> None:
    print("\n[Group K] Score range 0–100 for all role/market combinations")

    roles_and_types = [
        ("installer",          "solar installer",             "commercial"),
        ("epc_or_contractor",  "solar EPC",                   "commercial"),
        ("developer",          "solar developer",             "utility-scale"),
        ("distributor",        "solar component distributor", "mixed"),
        ("manufacturer",       "solar energy company",        "mixed"),
        ("consultant",         "solar energy company",        "mixed"),
        ("media_or_directory", "solar energy company",        "mixed"),
        ("unclear",            "solar energy company",        "mixed"),
        ("installer",          "solar installer",             "residential"),
        ("epc_or_contractor",  "solar EPC",                   "utility-scale"),
    ]

    for role, ct, mf in roles_and_types:
        rec    = _make_bf_record(
            company_type=ct, market_focus=mf, value_chain_role=role,
            manufacturer_flag=(role == "manufacturer"),
            competitor_flag=(role == "manufacturer"),
            consultant_flag=(role == "consultant"),
            media_or_directory_flag=(role == "media_or_directory"),
            negative_residential_flag=(mf == "residential"),
        )
        scored = _score(rec)
        ls     = scored["lead_score"]
        _assert(0 <= ls <= 100, f"{role}/{mf} → lead_score in [0,100]", f"got {ls}")

        # Components should also be in reasonable ranges
        _assert(
            isinstance(scored["solar_relevance_component"], int),
            f"{role}/{mf} → solar_relevance_component is int"
        )
        _assert(
            0 <= scored["buyer_relevance_component"] <= 20,
            f"{role}/{mf} → buyer_relevance_component in [0,20]",
            f"got {scored['buyer_relevance_component']}"
        )


# ---------------------------------------------------------------------------
# Group L: Threshold enforcement
# ---------------------------------------------------------------------------

def test_group_l_threshold() -> None:
    print("\n[Group L] Threshold enforcement — qualified vs rejected consistent")

    from src.workflow_5_lead_scoring.lead_scorer import QUALIFIED_THRESHOLD

    # Good companies qualify
    good_cases = [
        _make_bf_record("solar EPC",       "commercial",    "epc_or_contractor",
                        procurement_relevance_score=9, project_signal_strength=5),
        _make_bf_record("solar installer", "commercial",    "installer",
                        procurement_relevance_score=8, project_signal_strength=3),
        _make_bf_record("solar developer", "utility-scale", "developer",
                        procurement_relevance_score=6, project_signal_strength=4),
        _make_bf_record("solar component distributor", "mixed", "distributor",
                        procurement_relevance_score=6, project_signal_strength=2),
    ]
    for rec in good_cases:
        scored = _score(rec)
        _assert(
            scored["lead_score"] >= QUALIFIED_THRESHOLD,
            f"{rec['company_type']}/{rec['market_focus']} qualifies (≥{QUALIFIED_THRESHOLD})",
            f"got {scored['lead_score']}"
        )

    # Bad companies fail — residential installer is NOT in this list (it qualifies)
    bad_cases = [
        _make_bf_record("solar energy company", "mixed", "manufacturer",
                        procurement_relevance_score=1, manufacturer_flag=True, competitor_flag=True),
        _make_bf_record("solar energy company", "mixed", "consultant",
                        procurement_relevance_score=2, consultant_flag=True),
        _make_bf_record("solar energy company", "mixed", "media_or_directory",
                        procurement_relevance_score=1, media_or_directory_flag=True),
    ]
    for rec in bad_cases:
        scored = _score(rec)
        _assert(
            scored["lead_score"] < QUALIFIED_THRESHOLD,
            f"{rec['value_chain_role']}/{rec['market_focus']} rejected (<{QUALIFIED_THRESHOLD})",
            f"got {scored['lead_score']}"
        )

    # Threshold is 45 (unchanged)
    _assert(QUALIFIED_THRESHOLD == 45, f"QUALIFIED_THRESHOLD=45, got {QUALIFIED_THRESHOLD}")


# ---------------------------------------------------------------------------
# Group M: load_records() prefers buyer_filter.json
# ---------------------------------------------------------------------------

def test_group_m_load_prefers_bf() -> None:
    print("\n[Group M] load_records() prefers buyer_filter.json over company_analysis.json")

    _scorer_mod = "src.workflow_5_lead_scoring.lead_scorer"

    with tempfile.TemporaryDirectory() as tmp:
        # Both files exist
        bf_records = [
            _make_bf_record("solar EPC", "commercial", "epc_or_contractor",
                            company_name="BF Company", place_id="m001"),
        ]
        ca_records = [
            _make_analysis_record("solar installer", "commercial",
                                  company_name="CA Company", place_id="m002"),
        ]

        bf_path = Path(tmp) / "buyer_filter.json"
        ca_path = Path(tmp) / "company_analysis.json"
        bf_path.write_text(json.dumps(bf_records), encoding="utf-8")
        ca_path.write_text(json.dumps(ca_records), encoding="utf-8")

        with patch(f"{_scorer_mod}.BUYER_FILTER_FILE",     bf_path), \
             patch(f"{_scorer_mod}.COMPANY_ANALYSIS_FILE", ca_path):
            from src.workflow_5_lead_scoring.lead_scorer import load_records
            records, used_bf = load_records()

        _assert(used_bf, "used_bf=True when buyer_filter.json exists")
        _assert(records[0].get("company_name") == "BF Company",
                f"loaded from buyer_filter.json (name=BF Company), got {records[0].get('company_name')!r}")

    # When buyer_filter.json absent → fallback to company_analysis.json
    with tempfile.TemporaryDirectory() as tmp:
        ca_records = [_make_analysis_record("solar installer", "commercial",
                                            company_name="CA Only Co", place_id="m003")]
        ca_path = Path(tmp) / "company_analysis.json"
        bf_path = Path(tmp) / "buyer_filter.json"   # does NOT exist
        ca_path.write_text(json.dumps(ca_records), encoding="utf-8")

        with patch(f"{_scorer_mod}.BUYER_FILTER_FILE",     bf_path), \
             patch(f"{_scorer_mod}.COMPANY_ANALYSIS_FILE", ca_path):
            records2, used_bf2 = load_records()

        _assert(not used_bf2, "used_bf=False when buyer_filter.json absent")
        _assert(records2[0].get("company_name") == "CA Only Co",
                "fallback loads from company_analysis.json")

    # Limit parameter works
    with tempfile.TemporaryDirectory() as tmp:
        many = [_make_bf_record(place_id=f"m_{i}", company_name=f"Co {i}",
                                value_chain_role="installer") for i in range(10)]
        bf_path = Path(tmp) / "buyer_filter.json"
        bf_path.write_text(json.dumps(many), encoding="utf-8")
        ca_path = Path(tmp) / "company_analysis.json"

        with patch(f"{_scorer_mod}.BUYER_FILTER_FILE",     bf_path), \
             patch(f"{_scorer_mod}.COMPANY_ANALYSIS_FILE", ca_path):
            records3, _ = load_records(limit=3)

        _assert(len(records3) == 3, f"limit=3 respected, got {len(records3)}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 68)
    print("P1-1B — Lead Scoring Upgrade with Buyer Relevance: Test Suite")
    print("=" * 68)

    test_group_a_buyer_boost()
    test_group_b_residential_scoring()
    test_group_c_manufacturer_cap()
    test_group_d_consultant_cap()
    test_group_e_media_cap()
    test_group_f_role_adjustments()
    test_group_g_explainability()
    test_group_h_fallback_missing_bf()
    test_group_i_v1_fallback_scores()
    test_group_j_output_compatibility()
    test_group_k_score_range()
    test_group_l_threshold()
    test_group_m_load_prefers_bf()

    print("\n" + "=" * 68)
    print("All tests passed.")
    print("=" * 68)
