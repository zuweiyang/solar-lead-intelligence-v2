# Workflow 6.2: Signal Fact Extractor
#
# Extracts structured, grounded facts from a best_signal string.
# Extraction is rule-based only — no LLM involved.
# Every returned item is a literal substring or near-verbatim phrase
# from the signal text. Nothing is inferred or invented.
#
# Used by:
#   signal_pipeline.py  → stores signal_facts in company_openings.json
#   email_generator.py  → constrains LLM opening generation
#   email_rewriter.py   → constrains LLM repair opening

import re

# ---------------------------------------------------------------------------
# Location whitelist — only terms explicitly stated in the signal are returned
# ---------------------------------------------------------------------------

_LOCATION_TERMS: list[str] = [
    # Canadian provinces / territories
    "Alberta", "British Columbia", "BC", "Ontario", "Quebec", "Saskatchewan",
    "Manitoba", "Nova Scotia", "New Brunswick", "Prince Edward Island", "PEI",
    "Newfoundland", "Labrador", "Northwest Territories", "Nunavut", "Yukon",
    # Canadian cities (solar-relevant markets)
    "Edmonton", "Calgary", "Vancouver", "Victoria", "Toronto", "Ottawa",
    "Winnipeg", "Regina", "Saskatoon", "Kelowna", "Lethbridge", "Red Deer",
    "Leduc", "Fort McMurray", "Grande Prairie", "Airdrie", "Medicine Hat",
    # Canadian regions
    "Northern Alberta", "Southern Alberta", "Western Canada", "Eastern Canada",
    "Pacific Northwest",
    # US states — captured only if literally present, never invented
    "Texas", "California", "Arizona", "Nevada", "Colorado", "Washington",
    "Oregon", "Florida", "New York", "New Jersey", "Massachusetts",
    # Middle East / GCC
    "United Arab Emirates", "UAE", "Dubai", "Abu Dhabi", "Sharjah", "Ajman",
    "Ras Al Khaimah", "Fujairah", "Umm Al Quwain",
    "Saudi Arabia", "KSA", "Riyadh", "Jeddah", "Dammam",
    "Qatar", "Doha", "Kuwait", "Bahrain", "Oman", "Muscat",
    "GCC", "MENA",
    # Other major solar markets
    "Australia", "India", "Germany", "Spain", "Italy", "France",
    "United Kingdom", "UK",
    "South Africa", "Nigeria", "Kenya",
    "Pakistan", "Bangladesh",
    "Southeast Asia", "Southeast",
]

# Longer terms first so _dedup_locations correctly suppresses sub-terms
_LOCATION_TERMS.sort(key=len, reverse=True)

_LOCATION_PATTERNS: list[tuple[str, re.Pattern]] = [
    (term, re.compile(r"\b" + re.escape(term) + r"\b", re.IGNORECASE))
    for term in _LOCATION_TERMS
]

# ---------------------------------------------------------------------------
# Solar / storage technology terms — word-boundary matched
# ---------------------------------------------------------------------------

_TECH_TERMS: list[str] = [
    "battery storage", "battery backup", "BESS",
    "Powerwall", "Tesla Powerwall", "Enphase", "Franklin", "Sonnen",
    "EV charging", "EV charger",
    "rooftop solar", "ground mount", "ground-mount",
    "utility-scale", "solar farm", "solar park",
    "microinverter", "string inverter", "solar carport",
    "agrivoltaic", "bifacial",
    # Additional terms observed in international markets
    "micro-grid", "microgrid", "solar-hybrid", "solar hybrid",
    "off-grid", "off grid", "hybrid solar",
    "carport solar", "floating solar", "BIPV",
    "solar panels", "solar panel", "inverter", "inverters",
    "net metering", "feed-in tariff", "virtual power plant",
    "solar water pump", "solar irrigation",
    "EPC", "engineering procurement construction",
]

# ---------------------------------------------------------------------------
# Capacity / scale — verbatim figures only
# ---------------------------------------------------------------------------

_SCALE_RE = re.compile(
    r"\b(\d[\d,.]*\s*(?:kw|kilowatt|mw|megawatt|gw|gigawatt|kwh|mwh|gwh))\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Notable achievement cues — captured as verbatim short phrases
# ---------------------------------------------------------------------------

_ACHIEVEMENT_RE = re.compile(
    r"(?:"
    r"\blargest\b|\bfirst\b|\brecord\b"
    r"|\bawarded?\b|\bcompleted?\b|\binstalled?\b"
    r"|\bcommissioned?\b|\bdelivered?\b|\bbuilt\b"
    r").{5,80}",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_facts(signal: str) -> dict:
    """
    Extract grounded facts from a signal string.

    Returns a dict with keys:
      locations        — geographic terms explicitly present in the signal
      technologies     — solar/storage technologies explicitly named
      scale_mentions   — capacity figures (kW/MW/etc.) verbatim from signal
      market_segments  — residential/commercial/etc. explicitly stated
      notable_facts    — short verbatim phrases around achievement cues
      has_usable_facts — True if any non-empty category was found

    Everything returned is grounded in the source text.
    Nothing is inferred or invented.
    """
    text = (signal or "").strip()
    if not text:
        return _empty()

    # Locations — only terms literally present in the text
    raw_locs = [term for term, pat in _LOCATION_PATTERNS if pat.search(text)]
    locations = _dedup_locations(raw_locs)

    # Technologies — word-boundary matched
    technologies = [
        t for t in _TECH_TERMS
        if re.search(r"\b" + re.escape(t) + r"\b", text, re.IGNORECASE)
    ]

    # Scale mentions — verbatim extractions
    scale_mentions = list(dict.fromkeys(
        m.group(1).strip() for m in _SCALE_RE.finditer(text)
    ))

    # Market segments
    _MARKET_TERMS = [
        "residential", "commercial", "agricultural", "industrial",
        "utility-scale", "builder",
    ]
    market_segments = [
        seg for seg in _MARKET_TERMS
        if re.search(r"\b" + re.escape(seg) + r"\b", text, re.IGNORECASE)
    ]

    # Notable facts — verbatim short phrases around achievement cues
    seen_facts: set[str] = set()
    notable_facts: list[str] = []
    for m in _ACHIEVEMENT_RE.finditer(text):
        phrase = m.group(0).strip().rstrip(".,;(")
        words = phrase.split()
        if 5 <= len(words) <= 15:
            key = phrase.lower()
            if key not in seen_facts:
                seen_facts.add(key)
                notable_facts.append(phrase)
        if len(notable_facts) >= 3:
            break

    has_usable = bool(technologies or scale_mentions or notable_facts or locations or market_segments)

    return {
        "locations":        locations,
        "technologies":     technologies,
        "scale_mentions":   scale_mentions,
        "market_segments":  market_segments,
        "notable_facts":    notable_facts,
        "has_usable_facts": has_usable,
    }


def format_facts_for_prompt(facts: dict) -> str:
    """
    Format signal_facts as a structured block for LLM prompts.

    Uses explicit "(none stated in source)" so the model cannot fill gaps.
    This acts as a whitelist: only listed items are permitted in the output.
    """
    def _fmt(items: list) -> str:
        return ", ".join(items) if items else "(none stated in source)"

    return (
        "Signal facts — extracted from source (use ONLY these in the opening):\n"
        f"  Technologies stated : {_fmt(facts.get('technologies', []))}\n"
        f"  Markets stated      : {_fmt(facts.get('market_segments', []))}\n"
        f"  Locations stated    : {_fmt(facts.get('locations', []))}\n"
        f"  Scale/capacity      : {_fmt(facts.get('scale_mentions', []))}\n"
        f"  Notable facts       : {_fmt(facts.get('notable_facts', []))}\n"
        "\n"
        "GROUNDING RULES (strictly enforced):\n"
        "- Use ONLY items listed above; do not reference anything not present\n"
        '- If a field says "(none stated in source)": that item does not exist — do not add it\n'
        "- Especially: if Locations = \"(none stated in source)\", write NO geographic reference\n"
        "- If Scale/capacity = \"(none stated in source)\", write NO kW/MW figures"
    )


def _dedup_locations(locations: list[str]) -> list[str]:
    """
    Remove sub-terms when a longer containing term is already present.
    e.g. ["Northern Alberta", "Alberta"] → ["Northern Alberta"]
    """
    result = []
    for loc in locations:
        is_sub = any(
            loc.lower() != other.lower() and loc.lower() in other.lower()
            for other in locations
        )
        if not is_sub:
            result.append(loc)
    return result


def _empty() -> dict:
    return {
        "locations":        [],
        "technologies":     [],
        "scale_mentions":   [],
        "market_segments":  [],
        "notable_facts":    [],
        "has_usable_facts": False,
    }
