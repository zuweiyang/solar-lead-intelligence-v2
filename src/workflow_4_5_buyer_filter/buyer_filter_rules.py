# Workflow 4.5 — Buyer Filter: Scoring Rules
#
# Extracts text-based signals from website content and produces all numeric
# buyer-filter scores. All logic is deterministic and auditable.
#
# Score range: 0–10 for all fields.
# Conservative policy: weak/missing evidence → conservative scores, not inflated.

from src.workflow_4_5_buyer_filter.buyer_filter_models import (
    BuyerFilterResult,
    ROLE_INSTALLER,
    ROLE_EPC_OR_CONTRACTOR,
    ROLE_DEVELOPER,
    ROLE_DISTRIBUTOR,
    ROLE_MANUFACTURER,
    ROLE_CONSULTANT,
    ROLE_MEDIA_OR_DIRECTORY,
    ROLE_ASSOCIATION,
    ROLE_UNCLEAR,
)


# ---------------------------------------------------------------------------
# Signal keyword sets — each represents one scoring dimension
# ---------------------------------------------------------------------------

# Commercial-oriented signals (C&I / business / industrial buyers)
_COMMERCIAL_SIGNALS: frozenset[str] = frozenset({
    "commercial", "commercial solar", "commercial project",
    "industrial", "industrial solar", "c&i",
    "corporate", "business client", "office building",
    "warehouse", "factory", "school", "hospital",
    "municipality", "government building", "retail",
    "commercial rooftop", "commercial installation",
    "shopping mall", "data center", "car park",
})

# Utility-scale signals
_UTILITY_SIGNALS: frozenset[str] = frozenset({
    "utility", "utility-scale", "utility scale",
    "megawatt", "mw ", " mwp", "grid-scale", "grid scale",
    "power purchase agreement", "ppa", "offtake agreement",
    "land acquisition", "ground-mount", "ground mount",
    "large-scale", "100kw", "500kw", "1mw", "10mw",
})

# Project-oriented signals (indicates execution, not just selling)
_PROJECT_SIGNALS: frozenset[str] = frozenset({
    "project", "solar project", "our projects", "completed project",
    "project portfolio", "case study", "case studies",
    "kw installed", "kw completed", "mw installed",
    "commissioning", "commissioned",
    "deployment", "deployed",
    "installation completed", "installation delivered",
    "phase 1", "phase 2",
    "epc project", "construction", "handover",
    "portfolio", "track record",
})

# Installation execution signals (hands-on doing, not just design/advisory)
_INSTALLER_SIGNALS: frozenset[str] = frozenset({
    "install", "installer", "installation", "installing",
    "mount", "mounting", "racking", "commissioning",
    "deploy", "deployment", "on-site", "site survey",
    "rooftop", "rooftop solar", "panel installation",
    "solar system installation", "put solar",
})

# Developer signals (project origination, not just execution)
_DEVELOPER_SIGNALS: frozenset[str] = frozenset({
    "develop", "developer", "development",
    "project development", "project pipeline",
    "land acquisition", "offtake", "ppa",
    "permit", "permitting", "interconnection",
    "utility project", "grid connection",
    "solar farm development", "project origination",
    "greenfield", "brownfield",
})

# Distribution / supply signals
_DISTRIBUTOR_SIGNALS: frozenset[str] = frozenset({
    "distribut", "distribution", "distributor",
    "wholesale", "wholesaler", "resell", "reseller",
    "trading", "trader", "supply chain", "importer",
    "exporter", "logistics", "warehouse stock",
    "in stock", "dealer", "dealership",
})

# Procurement / sourcing signals (they BUY products)
_PROCUREMENT_SIGNALS: frozenset[str] = frozenset({
    "procurement", "sourcing", "purchasing",
    "supply chain", "supplier", "vendor",
    "materials", "equipment sourcing", "bill of materials",
    "bom", "components needed", "hardware",
})

# Residential / homeowner signals (negative for our ICP)
_RESIDENTIAL_SIGNALS: frozenset[str] = frozenset({
    "homeowner", "home owner", "residential", "household",
    "home solar", "your home", "domestic", "save on bills",
    "electricity bill savings", "family home", "house",
    "private home", "rooftop for homes",
})


# ---------------------------------------------------------------------------
# Signal counting helper
# ---------------------------------------------------------------------------

def _count_signals(text_lower: str, keywords: frozenset[str]) -> int:
    """Count how many distinct keywords appear in text_lower."""
    return sum(1 for kw in keywords if kw in text_lower)


def _score_from_count(count: int, multiplier: int = 2, cap: int = 10) -> int:
    """Convert a raw keyword count to a 0–10 score."""
    return min(cap, count * multiplier)


# ---------------------------------------------------------------------------
# Signal extraction — populates all signal strength scores
# ---------------------------------------------------------------------------

def extract_signals(company_text: str, result: BuyerFilterResult) -> None:
    """
    Scan company_text and populate all signal_strength fields on result.
    All scores in range 0–10. Mutates result in-place.
    """
    text_lower = (company_text or "").lower()

    result.commercial_signal_strength  = _score_from_count(
        _count_signals(text_lower, _COMMERCIAL_SIGNALS), multiplier=2)
    result.utility_signal_strength     = _score_from_count(
        _count_signals(text_lower, _UTILITY_SIGNALS), multiplier=3)
    result.project_signal_strength     = _score_from_count(
        _count_signals(text_lower, _PROJECT_SIGNALS), multiplier=2)
    result.installer_signal_strength   = _score_from_count(
        _count_signals(text_lower, _INSTALLER_SIGNALS), multiplier=2)
    result.developer_signal_strength   = _score_from_count(
        _count_signals(text_lower, _DEVELOPER_SIGNALS), multiplier=2)
    result.distributor_signal_strength = _score_from_count(
        _count_signals(text_lower, _DISTRIBUTOR_SIGNALS), multiplier=2)


# ---------------------------------------------------------------------------
# Score computation — market fit, procurement relevance, buyer likelihood
# ---------------------------------------------------------------------------

# Baseline procurement relevance by value chain role.
# We manufacture solar mounting systems and sell to: installers, EPCs,
# developers, and distributors. Manufacturers of competing products are
# not buyers. Consultants and media are not buyers.
_ROLE_PROCUREMENT_BASELINE: dict[str, int] = {
    ROLE_INSTALLER:          8,
    ROLE_EPC_OR_CONTRACTOR:  8,
    ROLE_DEVELOPER:          6,
    ROLE_DISTRIBUTOR:        6,
    ROLE_MANUFACTURER:       1,   # competitor / not a buyer of our product
    ROLE_CONSULTANT:         2,
    ROLE_MEDIA_OR_DIRECTORY: 1,
    ROLE_ASSOCIATION:        1,
    ROLE_UNCLEAR:            4,   # conservative middle ground
}


def compute_market_fit_score(market_focus: str, result: BuyerFilterResult) -> int:
    """
    Compute market_fit_score (0–10) from market_focus and signal strengths.

    Commercial/utility → high fit for our ICP.
    Residential → low fit.
    Mixed → middle.
    """
    mf = (market_focus or "").lower()

    if mf == "utility-scale":
        base = 9
    elif mf == "commercial":
        base = 8
    elif mf == "mixed":
        base = 6
    elif mf == "residential":
        base = 3
    else:
        base = 5   # unknown / unclear

    # Adjust for observed signal strength
    if result.commercial_signal_strength >= 4:
        base = min(10, base + 1)
    if result.utility_signal_strength >= 4:
        base = min(10, base + 1)
    if result.negative_residential_flag and mf != "residential":
        # residential language present even though market_focus isn't residential
        base = max(0, base - 1)

    return base


def compute_procurement_relevance_score(result: BuyerFilterResult) -> int:
    """
    Compute procurement_relevance_score (0–10).

    Reflects: how likely is this company to procure solar mounting systems?
    """
    base = _ROLE_PROCUREMENT_BASELINE.get(result.value_chain_role, 4)

    # Positive adjustments
    if result.project_signal_strength >= 3:
        base = min(10, base + 1)   # active project execution → more procurement
    if result.commercial_signal_strength >= 4:
        base = min(10, base + 1)
    if _count_signals("", frozenset()) == 0:
        pass  # no-op placeholder

    # Negative adjustments
    if result.negative_residential_flag:
        base = max(0, base - 2)   # residential focus = lower C&I procurement
    if result.manufacturer_flag:
        base = min(base, 2)       # cap at 2 if manufacturer (they make, not buy)
    if result.consultant_flag:
        base = min(base, 3)
    if result.media_or_directory_flag:
        base = min(base, 2)

    return max(0, min(10, base))


def compute_buyer_likelihood_score(result: BuyerFilterResult) -> int:
    """
    Compute buyer_likelihood_score (0–10).

    Synthesises: procurement relevance + market fit.
    This is the top-line score downstream scoring (P1-1B) will weight most.
    """
    # Weighted average: 60% procurement relevance, 40% market fit
    raw = (result.procurement_relevance_score * 6 + result.market_fit_score * 4) // 10

    # Hard caps for clearly disqualifying roles
    if result.manufacturer_flag:
        raw = min(raw, 3)
    if result.media_or_directory_flag:
        raw = min(raw, 2)
    if result.consultant_flag:
        raw = min(raw, 3)

    return max(0, min(10, raw))


# ---------------------------------------------------------------------------
# Reason generation
# ---------------------------------------------------------------------------

def build_buyer_filter_reason(result: BuyerFilterResult) -> str:
    """
    Produce a human-readable explanation of buyer_likelihood_score.
    """
    parts = [
        f"role={result.value_chain_role}",
        f"procurement_relevance={result.procurement_relevance_score}/10",
        f"market_fit={result.market_fit_score}/10",
        f"project_signals={result.project_signal_strength}/10",
    ]
    if result.negative_residential_flag:
        parts.append("residential-heavy penalty applied")
    if result.manufacturer_flag:
        parts.append("manufacturer/competitor cap applied")
    if result.consultant_flag:
        parts.append("consultant cap applied")
    if result.media_or_directory_flag:
        parts.append("media/directory cap applied")
    return "; ".join(parts)


# ---------------------------------------------------------------------------
# Top-level scoring entry point
# ---------------------------------------------------------------------------

def compute_all_scores(
    market_focus: str,
    result: BuyerFilterResult,
) -> None:
    """
    Compute and assign all numeric scores to result in-place.
    Call AFTER extract_signals() and classify_value_chain() have run.
    """
    result.market_fit_score            = compute_market_fit_score(market_focus, result)
    result.procurement_relevance_score = compute_procurement_relevance_score(result)
    result.buyer_likelihood_score      = compute_buyer_likelihood_score(result)
    result.buyer_filter_reason         = build_buyer_filter_reason(result)
