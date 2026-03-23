"""
Ticket P1-1A — Buyer Filter / Value Chain Classification: Test Suite

Run from the project root:
    py scripts/test_buyer_filter.py

Covers:
    Group A: Value-chain role classification (8 role types)
    Group B: Negative targeting — residential flag
    Group C: Negative targeting — manufacturer/competitor flag
    Group D: Negative targeting — consultant flag
    Group E: Negative targeting — media/directory flag
    Group F: Positive buyer signals (commercial/project/procurement)
    Group G: Ambiguous / unclear companies → conservative scores
    Group H: Score semantics (ranges, cap, direction)
    Group I: Pipeline persistence — output artifact is run-scoped
    Group J: Summary counts accuracy
    Group K: Pipeline resilience — missing file, bad records
    Group L: BuyerFilterResult serialisation round-trip
    Group M: Score ordering — better companies score higher

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


def _make_record(
    company_type: str = "solar installer",
    market_focus: str = "commercial",
    company_name: str = "Test Co",
    website: str = "https://test.com",
    place_id: str = "test_place_001",
    services_detected: list | None = None,
    confidence_score: float = 0.85,
) -> dict:
    return {
        "company_name":       company_name,
        "website":            website,
        "place_id":           place_id,
        "company_type":       company_type,
        "market_focus":       market_focus,
        "services_detected":  services_detected or [],
        "confidence_score":   confidence_score,
        "classification_method": "ai",
    }


def _apply(record: dict, text: str = "") -> "BuyerFilterResult":
    from src.workflow_4_5_buyer_filter.buyer_filter_pipeline import apply_buyer_filter
    return apply_buyer_filter(record, text)


# ---------------------------------------------------------------------------
# Group A: Value-chain role classification
# ---------------------------------------------------------------------------

def test_group_a_role_classification() -> None:
    print("\n[Group A] Value-chain role classification")

    cases = [
        ("solar installer",            "commercial",   "",                              "installer"),
        ("solar panel installer",      "commercial",   "",                              "installer"),
        ("battery storage installer",  "commercial",   "",                              "installer"),
        ("solar EPC",                  "commercial",   "",                              "epc_or_contractor"),
        ("solar contractor",           "mixed",        "",                              "epc_or_contractor"),
        ("BESS integrator",            "commercial",   "",                              "epc_or_contractor"),
        ("solar developer",            "utility-scale","",                              "developer"),
        ("solar farm developer",       "utility-scale","",                              "developer"),
        ("solar component distributor","mixed",        "",                              "distributor"),
        # Manufacturer text override
        ("solar energy company",       "mixed",
         "We are a manufacturer of solar panels. Our factory produces OEM modules.",    "manufacturer"),
        # Consultant override for unclear
        ("solar energy company",       "mixed",
         "We provide energy consulting and advisory services for feasibility studies.", "consultant"),
        # Media override for unclear
        ("solar energy company",       "mixed",
         "Read the latest solar news and blog articles on our renewable directory.",   "media_or_directory"),
        # No signals → unclear
        ("solar energy company",       "mixed",        "",                              "unclear"),
        ("other",                      "mixed",        "",                              "unclear"),
    ]

    for company_type, market_focus, text, expected_role in cases:
        rec = _make_record(company_type=company_type, market_focus=market_focus)
        bf  = _apply(rec, text)
        _assert(
            bf.value_chain_role == expected_role,
            f"{company_type!r} + text_signals → role={expected_role}",
            f"got {bf.value_chain_role!r}",
        )


# ---------------------------------------------------------------------------
# Group B: Negative targeting — residential flag
# ---------------------------------------------------------------------------

def test_group_b_residential_negative() -> None:
    print("\n[Group B] Negative targeting — residential/homeowner flag")

    # Residential market_focus alone triggers the flag
    rec = _make_record(company_type="solar installer", market_focus="residential")
    bf  = _apply(rec, "")
    _assert(bf.negative_residential_flag, "market_focus=residential → negative_residential_flag")
    _assert(len(bf.negative_targeting_reasons) > 0, "negative_targeting_reasons non-empty")

    # Residential language in text triggers the flag even if market_focus is mixed
    residential_text = "We help homeowners go solar. Home solar for every household."
    rec2 = _make_record(company_type="solar installer", market_focus="mixed")
    bf2  = _apply(rec2, residential_text)
    _assert(bf2.negative_residential_flag, "residential text → negative_residential_flag (market mixed)")

    # Commercial company with no residential signals should NOT get the flag
    rec3 = _make_record(company_type="solar EPC", market_focus="commercial")
    bf3  = _apply(rec3, "We execute commercial solar EPC projects for industrial clients.")
    _assert(not bf3.negative_residential_flag, "commercial EPC → no residential flag")

    # Residential flag reduces buyer_likelihood_score relative to commercial
    rec_res  = _make_record(company_type="solar installer", market_focus="residential")
    rec_com  = _make_record(company_type="solar installer", market_focus="commercial")
    bf_res   = _apply(rec_res, "")
    bf_com   = _apply(rec_com, "")
    _assert(
        bf_com.buyer_likelihood_score > bf_res.buyer_likelihood_score,
        "commercial installer scores higher than residential installer",
        f"commercial={bf_com.buyer_likelihood_score} residential={bf_res.buyer_likelihood_score}",
    )


# ---------------------------------------------------------------------------
# Group C: Negative targeting — manufacturer/competitor flag
# ---------------------------------------------------------------------------

def test_group_c_manufacturer_flag() -> None:
    print("\n[Group C] Negative targeting — manufacturer/competitor flag")

    mfr_text = (
        "We manufacture solar mounting racking systems in our factory. "
        "OEM products available. Distributors wanted worldwide."
    )
    rec = _make_record(company_type="solar energy company", market_focus="mixed")
    bf  = _apply(rec, mfr_text)

    _assert(bf.manufacturer_flag,  "manufacturer text → manufacturer_flag")
    _assert(bf.competitor_flag,    "manufacturer text → competitor_flag")
    _assert(bf.value_chain_role == "manufacturer", "manufacturer text → role=manufacturer")
    _assert(bf.procurement_relevance_score <= 3,
            f"manufacturer prs capped ≤3, got {bf.procurement_relevance_score}")
    _assert(bf.buyer_likelihood_score <= 3,
            f"manufacturer bls capped ≤3, got {bf.buyer_likelihood_score}")
    _assert(any("manufacturer" in r for r in bf.negative_targeting_reasons),
            "negative_targeting_reasons contains manufacturer mention")

    # No manufacturer text → no flag
    rec2 = _make_record(company_type="solar installer", market_focus="commercial")
    bf2  = _apply(rec2, "We install solar panels on commercial rooftops.")
    _assert(not bf2.manufacturer_flag, "clean installer text → no manufacturer_flag")


# ---------------------------------------------------------------------------
# Group D: Negative targeting — consultant flag
# ---------------------------------------------------------------------------

def test_group_d_consultant_flag() -> None:
    print("\n[Group D] Negative targeting — consultant flag")

    cns_text = (
        "Our consulting firm provides energy advisory and feasibility studies. "
        "We offer independent engineer reports and policy advisory services."
    )
    rec = _make_record(company_type="solar energy company", market_focus="mixed")
    bf  = _apply(rec, cns_text)

    _assert(bf.consultant_flag, "consultant text → consultant_flag")
    _assert(bf.procurement_relevance_score <= 3,
            f"consultant prs capped ≤3, got {bf.procurement_relevance_score}")
    _assert(any("consultant" in r for r in bf.negative_targeting_reasons),
            "negative_targeting_reasons contains consultant mention")

    # EPC with no consulting language → no consultant flag
    rec2 = _make_record(company_type="solar EPC", market_focus="commercial")
    bf2  = _apply(rec2, "We build large-scale commercial EPC solar projects.")
    _assert(not bf2.consultant_flag, "EPC text → no consultant_flag")


# ---------------------------------------------------------------------------
# Group E: Negative targeting — media/directory flag
# ---------------------------------------------------------------------------

def test_group_e_media_flag() -> None:
    print("\n[Group E] Negative targeting — media/directory flag")

    media_text = (
        "Read the latest solar news and blog articles. "
        "Our renewable energy directory lists companies worldwide. "
        "Solar industry association membership."
    )
    rec = _make_record(company_type="solar energy company", market_focus="mixed")
    bf  = _apply(rec, media_text)

    _assert(bf.media_or_directory_flag, "media text → media_or_directory_flag")
    _assert(bf.buyer_likelihood_score <= 2,
            f"media bls capped ≤2, got {bf.buyer_likelihood_score}")
    _assert(any("media" in r for r in bf.negative_targeting_reasons),
            "negative_targeting_reasons contains media mention")

    # Real installer → no media flag
    rec2 = _make_record(company_type="solar installer", market_focus="commercial")
    bf2  = _apply(rec2, "We install solar on commercial buildings and warehouses.")
    _assert(not bf2.media_or_directory_flag, "commercial installer → no media_flag")


# ---------------------------------------------------------------------------
# Group F: Positive buyer signals
# ---------------------------------------------------------------------------

def test_group_f_positive_signals() -> None:
    print("\n[Group F] Positive buyer signals — commercial/project/procurement")

    strong_text = (
        "We are a commercial solar EPC contractor. "
        "Our project portfolio includes commissioned rooftop solar installations "
        "on warehouses, schools, and hospitals. "
        "Case studies available: 500kW commercial installation completed Q3. "
        "Industrial rooftop solar procurement and deployment services."
    )
    rec = _make_record(company_type="solar EPC", market_focus="commercial")
    bf  = _apply(rec, strong_text)

    _assert(bf.buyer_likelihood_score >= 7,
            f"strong commercial EPC → bls≥7, got {bf.buyer_likelihood_score}")
    _assert(bf.procurement_relevance_score >= 7,
            f"strong commercial EPC → prs≥7, got {bf.procurement_relevance_score}")
    _assert(bf.commercial_signal_strength >= 3,
            f"commercial text → commercial_signal_strength≥3, got {bf.commercial_signal_strength}")
    _assert(bf.project_signal_strength >= 3,
            f"project text → project_signal_strength≥3, got {bf.project_signal_strength}")
    _assert(bf.value_chain_role == "epc_or_contractor",
            f"solar EPC → epc_or_contractor, got {bf.value_chain_role!r}")

    # Utility-scale developer with strong signals
    utility_text = (
        "We develop utility-scale solar farms. Our pipeline includes 10MW and 50MW projects. "
        "Land acquisition, PPA negotiation, and grid connection services. "
        "Offtake agreements and permitting support."
    )
    rec2 = _make_record(company_type="solar developer", market_focus="utility-scale")
    bf2  = _apply(rec2, utility_text)
    _assert(bf2.value_chain_role == "developer", "solar developer → developer role")
    _assert(bf2.utility_signal_strength >= 3,
            f"utility text → utility_signal_strength≥3, got {bf2.utility_signal_strength}")
    _assert(bf2.developer_signal_strength >= 3,
            f"developer text → developer_signal_strength≥3, got {bf2.developer_signal_strength}")


# ---------------------------------------------------------------------------
# Group G: Ambiguous / unclear companies → conservative scores
# ---------------------------------------------------------------------------

def test_group_g_ambiguous_conservative() -> None:
    print("\n[Group G] Ambiguous companies → conservative / unclear classification")

    # Generic "solar energy company" with no text → unclear role, middle score
    rec = _make_record(company_type="solar energy company", market_focus="mixed")
    bf  = _apply(rec, "")

    _assert(bf.value_chain_role == "unclear",
            f"no-signal solar energy company → unclear role, got {bf.value_chain_role!r}")
    _assert(bf.buyer_likelihood_score <= 6,
            f"unclear company → bls not aggressively promoted, got {bf.buyer_likelihood_score}")

    # "other" company with zero solar text → unclear, minimal scores
    rec2 = _make_record(company_type="other", market_focus="mixed")
    bf2  = _apply(rec2, "We supply industrial chemicals and logistics services.")
    _assert(bf2.value_chain_role == "unclear",
            f"other company → unclear role, got {bf2.value_chain_role!r}")
    _assert(bf2.buyer_likelihood_score <= 5,
            f"non-solar other → bls≤5, got {bf2.buyer_likelihood_score}")

    # Vague "solar solutions" language without specifics → unclear
    vague_text = "We provide solar solutions and green energy services."
    rec3 = _make_record(company_type="solar energy company", market_focus="mixed")
    bf3  = _apply(rec3, vague_text)
    _assert(bf3.buyer_likelihood_score <= 6,
            f"vague solar solutions → bls not over-promoted, got {bf3.buyer_likelihood_score}")


# ---------------------------------------------------------------------------
# Group H: Score semantics — range, direction, cap
# ---------------------------------------------------------------------------

def test_group_h_score_semantics() -> None:
    print("\n[Group H] Score semantics — range 0–10, no overflows")

    # All valid company_type variants should produce in-range scores
    types_and_markets = [
        ("solar installer",            "commercial"),
        ("solar EPC",                  "utility-scale"),
        ("solar contractor",           "mixed"),
        ("solar developer",            "utility-scale"),
        ("solar farm developer",       "utility-scale"),
        ("battery storage installer",  "commercial"),
        ("BESS integrator",            "commercial"),
        ("solar component distributor","mixed"),
        ("solar energy company",       "residential"),
        ("other",                      "mixed"),
    ]

    for ct, mf in types_and_markets:
        rec = _make_record(company_type=ct, market_focus=mf)
        bf  = _apply(rec, "")
        for field_name, val in [
            ("buyer_likelihood_score",    bf.buyer_likelihood_score),
            ("procurement_relevance_score", bf.procurement_relevance_score),
            ("market_fit_score",          bf.market_fit_score),
            ("project_signal_strength",   bf.project_signal_strength),
            ("commercial_signal_strength",bf.commercial_signal_strength),
            ("utility_signal_strength",   bf.utility_signal_strength),
            ("installer_signal_strength", bf.installer_signal_strength),
            ("developer_signal_strength", bf.developer_signal_strength),
            ("distributor_signal_strength",bf.distributor_signal_strength),
        ]:
            _assert(
                0 <= val <= 10,
                f"{ct}/{mf} — {field_name} in [0,10]",
                f"got {val}",
            )

    # Installer with very strong commercial text should reach high scores
    installer_text = (
        "commercial solar installation commercial project industrial rooftop "
        "case study project portfolio commissioned deployed warehouse school hospital "
        "commercial rooftop corporate office building c&i"
    )
    rec_strong = _make_record(company_type="solar installer", market_focus="commercial")
    bf_strong  = _apply(rec_strong, installer_text)
    _assert(bf_strong.buyer_likelihood_score >= 7,
            f"strong installer → bls≥7, got {bf_strong.buyer_likelihood_score}")


# ---------------------------------------------------------------------------
# Group I: Persistence — output written to run-scoped path
# ---------------------------------------------------------------------------

def test_group_i_persistence() -> None:
    print("\n[Group I] Persistence — buyer_filter.json written to run-scoped path")

    # BUYER_FILTER_FILE must be a _RunPath (campaign-scoped, not process-level)
    from config.settings import BUYER_FILTER_FILE
    path_str = str(BUYER_FILTER_FILE).replace("\\", "/")
    # When no run is active it falls back to DATA_DIR; confirm it's not a fixed CRM path
    _assert("crm" not in path_str.lower(),
            f"BUYER_FILTER_FILE is not CRM-scoped (got {path_str!r})")
    _assert("buyer_filter.json" in path_str,
            f"BUYER_FILTER_FILE filename is buyer_filter.json (got {path_str!r})")

    # Pipeline writes to the expected path using a temp dir
    with tempfile.TemporaryDirectory() as tmp:
        analyses = [
            {
                "company_name": "Sun Power EPC",
                "website": "https://sunpowerepc.com",
                "place_id": "place_i_001",
                "company_type": "solar EPC",
                "market_focus": "commercial",
                "services_detected": ["commercial solar", "EPC"],
                "confidence_score": 0.9,
                "classification_method": "ai",
            }
        ]
        text_index = {"place_i_001": "commercial EPC project commissioning delivered"}

        fake_analysis_path = Path(tmp) / "company_analysis.json"
        fake_text_path     = Path(tmp) / "company_text.json"
        fake_output_path   = Path(tmp) / "buyer_filter.json"

        fake_analysis_path.write_text(json.dumps(analyses), encoding="utf-8")
        fake_text_path.write_text(
            json.dumps([{"place_id": "place_i_001", "company_text": "commercial EPC project commissioning delivered"}]),
            encoding="utf-8",
        )

        _pipe_mod = "src.workflow_4_5_buyer_filter.buyer_filter_pipeline"
        with patch(f"{_pipe_mod}.COMPANY_ANALYSIS_FILE", fake_analysis_path), \
             patch(f"{_pipe_mod}.COMPANY_TEXT_FILE", fake_text_path), \
             patch(f"{_pipe_mod}.BUYER_FILTER_FILE", fake_output_path):
            from src.workflow_4_5_buyer_filter.buyer_filter_pipeline import run
            summary = run()

        _assert(fake_output_path.exists(), "buyer_filter.json created")
        with open(fake_output_path, encoding="utf-8") as f:
            result = json.load(f)
        _assert(isinstance(result, list), "buyer_filter.json is a list")
        _assert(len(result) == 1, f"one record written (got {len(result)})")

        row = result[0]
        _assert("value_chain_role" in row, "value_chain_role field present")
        _assert("buyer_likelihood_score" in row, "buyer_likelihood_score field present")
        _assert("procurement_relevance_score" in row, "procurement_relevance_score present")
        _assert("market_fit_score" in row, "market_fit_score present")
        _assert("negative_residential_flag" in row, "negative_residential_flag present")
        _assert("competitor_flag" in row, "competitor_flag present")
        _assert("buyer_filter_reason" in row, "buyer_filter_reason present")
        # Original fields must be preserved
        _assert(row.get("company_type") == "solar EPC", "company_type preserved")
        _assert(row.get("company_name") == "Sun Power EPC", "company_name preserved")

        # Summary structure
        _assert(summary.get("total") == 1, f"summary.total=1 (got {summary.get('total')})")
        _assert("likely_buyer" in summary, "summary has likely_buyer key")
        _assert("by_role" in summary, "summary has by_role key")


# ---------------------------------------------------------------------------
# Group J: Summary counts accuracy
# ---------------------------------------------------------------------------

def test_group_j_summary_counts() -> None:
    print("\n[Group J] Summary counts accuracy")

    from src.workflow_4_5_buyer_filter.buyer_filter_pipeline import _build_summary

    # Construct synthetic enriched records
    records = [
        # likely buyer (bls=8): commercial EPC
        {"value_chain_role": "epc_or_contractor", "buyer_likelihood_score": 8,
         "negative_residential_flag": False, "competitor_flag": False,
         "manufacturer_flag": False, "consultant_flag": False, "media_or_directory_flag": False},
        # residential negative
        {"value_chain_role": "installer", "buyer_likelihood_score": 4,
         "negative_residential_flag": True, "competitor_flag": False,
         "manufacturer_flag": False, "consultant_flag": False, "media_or_directory_flag": False},
        # competitor/manufacturer
        {"value_chain_role": "manufacturer", "buyer_likelihood_score": 1,
         "negative_residential_flag": False, "competitor_flag": True,
         "manufacturer_flag": True, "consultant_flag": False, "media_or_directory_flag": False},
        # consultant
        {"value_chain_role": "consultant", "buyer_likelihood_score": 2,
         "negative_residential_flag": False, "competitor_flag": False,
         "manufacturer_flag": False, "consultant_flag": True, "media_or_directory_flag": False},
        # media/directory
        {"value_chain_role": "media_or_directory", "buyer_likelihood_score": 1,
         "negative_residential_flag": False, "competitor_flag": False,
         "manufacturer_flag": False, "consultant_flag": False, "media_or_directory_flag": True},
        # unclear
        {"value_chain_role": "unclear", "buyer_likelihood_score": 4,
         "negative_residential_flag": False, "competitor_flag": False,
         "manufacturer_flag": False, "consultant_flag": False, "media_or_directory_flag": False},
        # another likely buyer (bls=7)
        {"value_chain_role": "installer", "buyer_likelihood_score": 7,
         "negative_residential_flag": False, "competitor_flag": False,
         "manufacturer_flag": False, "consultant_flag": False, "media_or_directory_flag": False},
    ]

    s = _build_summary(records)

    _assert(s["total"] == 7,                    f"total=7 (got {s['total']})")
    _assert(s["likely_buyer"] == 2,             f"likely_buyer=2 (got {s['likely_buyer']})")
    _assert(s["residential_negative"] == 1,     f"residential_negative=1 (got {s['residential_negative']})")
    _assert(s["competitor_manufacturer"] == 1,  f"competitor_manufacturer=1 (got {s['competitor_manufacturer']})")
    _assert(s["consultant_media"] == 2,         f"consultant_media=2 (got {s['consultant_media']})")
    _assert(s["unclear"] == 1,                  f"unclear=1 (got {s['unclear']})")

    # by_role counts
    by_role = s["by_role"]
    _assert(by_role.get("installer") == 2,         f"by_role installer=2 (got {by_role.get('installer')})")
    _assert(by_role.get("epc_or_contractor") == 1, f"by_role epc=1")
    _assert(by_role.get("manufacturer") == 1,      f"by_role manufacturer=1")
    _assert(by_role.get("consultant") == 1,        f"by_role consultant=1")


# ---------------------------------------------------------------------------
# Group K: Pipeline resilience — missing file, bad records
# ---------------------------------------------------------------------------

def test_group_k_resilience() -> None:
    print("\n[Group K] Pipeline resilience")

    # Missing company_analysis.json → returns error summary, doesn't crash
    _pipe_mod = "src.workflow_4_5_buyer_filter.buyer_filter_pipeline"
    with tempfile.TemporaryDirectory() as tmp:
        fake_path = Path(tmp) / "company_analysis.json"
        fake_out  = Path(tmp) / "buyer_filter.json"
        with patch(f"{_pipe_mod}.COMPANY_ANALYSIS_FILE", fake_path), \
             patch(f"{_pipe_mod}.BUYER_FILTER_FILE", fake_out):
            from src.workflow_4_5_buyer_filter.buyer_filter_pipeline import run as _run
            result = _run()
        _assert(result.get("total", -1) == 0,   "missing analysis file → total=0 (no crash)")
        _assert(result.get("errors", 0) >= 1,   "missing analysis file → errors≥1")

    # Missing company_text.json → _load_company_texts returns {} without crashing
    from src.workflow_4_5_buyer_filter.buyer_filter_pipeline import _load_company_texts
    with tempfile.TemporaryDirectory() as tmp:
        missing_text = Path(tmp) / "company_text.json"
        with patch(f"{_pipe_mod}.COMPANY_TEXT_FILE", missing_text):
            text_idx = _load_company_texts()
        _assert(text_idx == {}, "missing text file → _load_company_texts returns {}")

    # apply_buyer_filter with empty text still produces a valid result (no crash)
    from src.workflow_4_5_buyer_filter.buyer_filter_pipeline import apply_buyer_filter
    rec_no_text = _make_record(company_type="solar EPC", market_focus="commercial")
    bf_no_text  = apply_buyer_filter(rec_no_text, "")
    _assert(bf_no_text.value_chain_role == "epc_or_contractor",
            "EPC with no text → correct role without crash")
    _assert(0 <= bf_no_text.buyer_likelihood_score <= 10,
            "EPC with no text → in-range bls (pipeline processes records)")

    # apply_buyer_filter never raises on bad input
    from src.workflow_4_5_buyer_filter.buyer_filter_pipeline import apply_buyer_filter
    bf = apply_buyer_filter({}, "")
    _assert(bf.value_chain_role == "unclear", "empty record → unclear role (no crash)")
    _assert(0 <= bf.buyer_likelihood_score <= 10, "empty record → in-range bls")


# ---------------------------------------------------------------------------
# Group L: BuyerFilterResult serialisation round-trip
# ---------------------------------------------------------------------------

def test_group_l_serialisation() -> None:
    print("\n[Group L] BuyerFilterResult serialisation round-trip")

    from src.workflow_4_5_buyer_filter.buyer_filter_models import BuyerFilterResult, ROLE_INSTALLER

    r = BuyerFilterResult(
        value_chain_role="installer",
        value_chain_reason="company_type='solar installer' → installer",
        buyer_likelihood_score=8,
        procurement_relevance_score=7,
        market_fit_score=8,
        project_signal_strength=5,
        commercial_signal_strength=4,
        utility_signal_strength=0,
        installer_signal_strength=6,
        developer_signal_strength=0,
        distributor_signal_strength=0,
        competitor_flag=False,
        manufacturer_flag=False,
        consultant_flag=False,
        media_or_directory_flag=False,
        negative_residential_flag=False,
        buyer_filter_reason="role=installer; procurement_relevance=7/10",
        negative_targeting_reasons=[],
    )

    d = r.to_dict()
    _assert(isinstance(d, dict),                        "to_dict() returns dict")
    _assert(d["value_chain_role"] == "installer",       "role serialised")
    _assert(d["buyer_likelihood_score"] == 8,           "bls serialised")
    _assert(d["negative_residential_flag"] is False,    "flag serialised as bool")
    _assert(isinstance(d["negative_targeting_reasons"], list), "reasons is list")

    # Round-trip through JSON
    json_str = json.dumps(d)
    restored = json.loads(json_str)
    _assert(restored["value_chain_role"] == "installer", "role survives JSON round-trip")
    _assert(restored["buyer_likelihood_score"] == 8,    "bls survives JSON round-trip")
    _assert(restored["negative_residential_flag"] is False, "flag survives JSON round-trip")


# ---------------------------------------------------------------------------
# Group M: Score ordering — better companies score higher
# ---------------------------------------------------------------------------

def test_group_m_score_ordering() -> None:
    print("\n[Group M] Score ordering — stronger companies rank higher")

    # commercial EPC > residential installer
    epc_text = (
        "We are a commercial solar EPC contractor. "
        "Our projects include commissioned 1MW commercial installations. "
        "Industrial rooftop case studies. Project portfolio available."
    )
    rec_epc = _make_record(company_type="solar EPC", market_focus="commercial")
    rec_res = _make_record(company_type="solar installer", market_focus="residential")
    bf_epc = _apply(rec_epc, epc_text)
    bf_res = _apply(rec_res, "")

    _assert(
        bf_epc.buyer_likelihood_score > bf_res.buyer_likelihood_score,
        f"commercial EPC ({bf_epc.buyer_likelihood_score}) > residential installer ({bf_res.buyer_likelihood_score})",
    )

    # developer > manufacturer
    rec_dev = _make_record(company_type="solar developer", market_focus="utility-scale")
    rec_mfr = _make_record(company_type="solar energy company", market_focus="mixed")
    bf_dev  = _apply(rec_dev, "We develop utility-scale solar projects with PPA agreements.")
    bf_mfr  = _apply(rec_mfr, "We manufacture solar racking in our factory. OEM products.")

    _assert(
        bf_dev.buyer_likelihood_score > bf_mfr.buyer_likelihood_score,
        f"developer ({bf_dev.buyer_likelihood_score}) > manufacturer ({bf_mfr.buyer_likelihood_score})",
    )

    # distributor > consultant
    rec_dist = _make_record(company_type="solar component distributor", market_focus="mixed")
    rec_cns  = _make_record(company_type="solar energy company", market_focus="mixed")
    bf_dist  = _apply(rec_dist, "We distribute solar components and mounting hardware wholesale.")
    bf_cns   = _apply(rec_cns, "Our advisory firm provides energy consulting and feasibility studies.")

    _assert(
        bf_dist.buyer_likelihood_score > bf_cns.buyer_likelihood_score,
        f"distributor ({bf_dist.buyer_likelihood_score}) > consultant ({bf_cns.buyer_likelihood_score})",
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 68)
    print("P1-1A — Buyer Filter / Value Chain Classification: Test Suite")
    print("=" * 68)

    test_group_a_role_classification()
    test_group_b_residential_negative()
    test_group_c_manufacturer_flag()
    test_group_d_consultant_flag()
    test_group_e_media_flag()
    test_group_f_positive_signals()
    test_group_g_ambiguous_conservative()
    test_group_h_score_semantics()
    test_group_i_persistence()
    test_group_j_summary_counts()
    test_group_k_resilience()
    test_group_l_serialisation()
    test_group_m_score_ordering()

    print("\n" + "=" * 68)
    print("All tests passed.")
    print("=" * 68)
