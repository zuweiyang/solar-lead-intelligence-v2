# Workflow 4.5 — Buyer Filter: Value Chain Classifier
#
# Maps Workflow 4's company_type + text signals → a structured value_chain_role
# and sets negative targeting flags.
#
# This is a downstream commercial interpretation layer — it does NOT duplicate
# Workflow 4's classification. Workflow 4 answers "what kind of solar company
# is this?". This module answers "where in the value chain do they sit, and
# are they a likely buyer of solar mounting products?".

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
# Direct company_type → value_chain_role mapping
# ---------------------------------------------------------------------------

_TYPE_TO_ROLE: dict[str, str] = {
    "solar installer":            ROLE_INSTALLER,
    "solar panel installer":      ROLE_INSTALLER,
    "battery storage installer":  ROLE_INSTALLER,
    "solar epc":                  ROLE_EPC_OR_CONTRACTOR,
    "solar contractor":           ROLE_EPC_OR_CONTRACTOR,
    "bess integrator":            ROLE_EPC_OR_CONTRACTOR,
    "solar developer":            ROLE_DEVELOPER,
    "solar farm developer":       ROLE_DEVELOPER,
    "solar component distributor": ROLE_DISTRIBUTOR,
    "solar energy company":       ROLE_UNCLEAR,   # needs text signals
    "other":                      ROLE_UNCLEAR,   # needs text signals
}


# ---------------------------------------------------------------------------
# Text-based negative signal keyword sets
# ---------------------------------------------------------------------------

_MANUFACTURER_KEYWORDS: frozenset[str] = frozenset({
    # Own-product language — clearly they make what they sell
    "our factory", "production line", "produced by", "we produce",
    "oem supplier", "oem product",
    "distributor wanted", "looking for distributors",
    "our panel", "our module", "our inverter",
    # Specific product-type manufacturers — direct competitors
    "solar panel manufacturer", "module manufacturer",
    "inverter manufacturer", "mounting manufacturer",
    "racking manufacturer", "racking supplier",
    # Keep "manufacturer" as standalone (e.g. "we are a manufacturer of solar panels")
    # but remove bare "manufacture" / "manufacturing" / "factory" —
    # these appear on any company's About page describing their supply chain,
    # and on distributor/installer websites describing their suppliers.
    "fabricat",
})

_CONSULTANT_KEYWORDS: frozenset[str] = frozenset({
    # Pure advisory signals — companies whose primary business is advice, not procurement
    "consulting", "consultant", "consultancy",
    "advisory", "advisors", "advisory firm",
    "policy advisory", "independent engineer",
    "technical advisory", "financial advisory", "project advisory",
    # Removed: "energy audit", "feasibility study", "market research",
    # "due diligence", "energy assessment" — EPC and installer companies routinely
    # offer these as ancillary services alongside active installation and procurement.
    # Keeping them caused false consultant flags on legitimate hardware buyers.
})

_MEDIA_KEYWORDS: frozenset[str] = frozenset({
    # Specific media/directory signals — NOT generic web content
    "industry publication", "trade publication",
    "directory", "database of companies",
    "solar directory", "solar news", "renewable news",
    "editorial", "journalist", "podcast",
    "industry association", "trade association", "member organization",
    "non-profit", "nonprofit", "advocacy group", "regulatory body",
    "government agency",
    # Removed: "news", "blog", "article", "press release", "media"
    # Almost every solar company website has a News section, a Blog, or links
    # to articles — these are not evidence of being a media company.
    # "media" alone fires on "social media" and "media coverage" on company pages.
})

_ASSOCIATION_KEYWORDS: frozenset[str] = frozenset({
    "association", "trade body", "industry body",
    "chamber of commerce", "advocacy", "non-profit",
    "nonprofit", "member association", "standards body",
    "certification body",
})

_RESIDENTIAL_STRONG_KEYWORDS: frozenset[str] = frozenset({
    "homeowner", "home owner", "home solar",
    "residential solar", "solar for your home",
    "save on your electricity bill", "household",
    "house solar", "solar for homes", "residential installation",
    "home energy", "domestic solar", "your home",
    "rooftop for homes", "family home", "private home",
    "home battery", "powerwall for homes",
})


# ---------------------------------------------------------------------------
# Text signal detection helpers
# ---------------------------------------------------------------------------

def _contains_any(text_lower: str, keywords: frozenset[str]) -> list[str]:
    """Return list of keywords found in text_lower."""
    return [kw for kw in keywords if kw in text_lower]


def _count_any(text_lower: str, keywords: frozenset[str]) -> int:
    return sum(1 for kw in keywords if kw in text_lower)


# ---------------------------------------------------------------------------
# Main classification function
# ---------------------------------------------------------------------------

def classify_value_chain(
    company_type: str,
    market_focus: str,
    company_text: str,
    result: BuyerFilterResult,
) -> None:
    """
    Classify value_chain_role and set negative flags on result in-place.

    Args:
        company_type:  Workflow 4 company_type (e.g. "solar installer")
        market_focus:  Workflow 4 market_focus (e.g. "residential")
        company_text:  Raw website text (may be empty)
        result:        BuyerFilterResult to populate (mutated in-place)
    """
    text_lower = (company_text or "").lower()
    ct_lower   = (company_type or "").strip().lower()

    # 1. Start from the direct type-to-role mapping
    initial_role = _TYPE_TO_ROLE.get(ct_lower, ROLE_UNCLEAR)
    role         = initial_role
    role_reason  = f"company_type={company_type!r} → {role}"

    # 2. Detect manufacturer signals (strongest negative signal)
    mfr_hits = _contains_any(text_lower, _MANUFACTURER_KEYWORDS)
    if mfr_hits:
        result.manufacturer_flag = True
        result.competitor_flag   = True   # manufacturers are typically competitors, not buyers
        result.negative_targeting_reasons.append(
            f"manufacturer signals detected: {', '.join(mfr_hits[:3])}"
        )
        # Override role if it wasn't already something more specific
        if role in (ROLE_UNCLEAR, ROLE_INSTALLER, ROLE_EPC_OR_CONTRACTOR):
            role        = ROLE_MANUFACTURER
            role_reason = f"manufacturer keywords override company_type={company_type!r}"

    # 3. Detect consultant signals
    cns_hits = _contains_any(text_lower, _CONSULTANT_KEYWORDS)
    if cns_hits:
        result.consultant_flag = True
        result.negative_targeting_reasons.append(
            f"consultant/advisory signals detected: {', '.join(cns_hits[:3])}"
        )
        if role in (ROLE_UNCLEAR,):
            role        = ROLE_CONSULTANT
            role_reason = f"consultant keywords detected; company_type={company_type!r}"

    # 4. Detect media / directory signals
    media_hits = _contains_any(text_lower, _MEDIA_KEYWORDS)
    if media_hits:
        result.media_or_directory_flag = True
        result.negative_targeting_reasons.append(
            f"media/directory signals detected: {', '.join(media_hits[:3])}"
        )
        if role in (ROLE_UNCLEAR,):
            role        = ROLE_MEDIA_OR_DIRECTORY
            role_reason = f"media/directory keywords detected; company_type={company_type!r}"

    # 5. Detect association signals
    assoc_hits = _contains_any(text_lower, _ASSOCIATION_KEYWORDS)
    if assoc_hits and role in (ROLE_UNCLEAR,):
        role        = ROLE_ASSOCIATION
        role_reason = f"association/non-buyer keywords detected: {', '.join(assoc_hits[:2])}"
        result.negative_targeting_reasons.append(
            f"association/non-buyer signals: {', '.join(assoc_hits[:3])}"
        )

    # 6. Detect residential-heavy signals
    res_hits = _contains_any(text_lower, _RESIDENTIAL_STRONG_KEYWORDS)
    market_is_residential = market_focus.lower() == "residential"
    if res_hits or market_is_residential:
        result.negative_residential_flag = True
        reasons = []
        if market_is_residential:
            reasons.append(f"market_focus=residential")
        if res_hits:
            reasons.append(f"residential-heavy language: {', '.join(res_hits[:3])}")
        result.negative_targeting_reasons.append(
            "residential-heavy focus: " + "; ".join(reasons)
        )

    # 7. Assign final role and reason
    result.value_chain_role   = role
    result.value_chain_reason = role_reason
