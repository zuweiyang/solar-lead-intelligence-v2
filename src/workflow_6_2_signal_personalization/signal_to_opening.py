# Workflow 6.2: Signal-based Personalization — Signal → Opening Line Converter
# Converts a raw signal string into a natural cold email opening sentence.
#
# Rules:
#   - Under 18 words
#   - Natural language, not marketing copy
#   - Fallback: "I came across {company_name} while looking at solar installers."

import re


# ---------------------------------------------------------------------------
# Signal normalizer
# ---------------------------------------------------------------------------

def _normalize_signal(signal: str) -> str:
    """
    Normalise LinkedIn/web-formatted signal text before regex pattern matching.

    Converts bullet characters (•), em/en dashes (–—), and pipe separators
    into period separators so the lookahead terminators (?:\\.|,|$) in
    _PATTERNS can fire correctly.  Without this, pattern 3 (rooftop project)
    silently fails on multi-line LinkedIn text like:

        "largest rooftop system in Northern Alberta\\n • Mosaic Centre – winner…"

    because the • character is neither \\w nor \\s nor , and the non-greedy
    capture group has nowhere to stop.

    The normalization is applied ONLY for pattern matching; the original signal
    text continues to be stored/returned unmodified.
    """
    # Bullet chars used in LinkedIn lists (• U+2022, · U+00B7)
    signal = re.sub(r'\s*[•·]\s*', '. ', signal)
    # Em dash / en dash used as text separators (–  — )
    signal = re.sub(r'\s*[–—]\s*', '. ', signal)
    # Pipe separators common in LinkedIn taglines ("Company | Tagline")
    signal = re.sub(r'\s*\|\s*', '. ', signal)
    # Bare newlines → single space (preserves sentence flow)
    signal = re.sub(r'\n+', ' ', signal)
    # Collapse consecutive periods and spaces produced by the replacements above
    signal = re.sub(r'\.{2,}', '.', signal)
    signal = re.sub(r' {2,}', ' ', signal)
    return signal.strip()


# ---------------------------------------------------------------------------
# Pattern-based converters
# Each pattern is (compiled_regex, template_fn).
# template_fn receives the re.Match object and returns the opening string.
# Patterns are tried in order; first match wins.
# ---------------------------------------------------------------------------

def _cap(s: str) -> str:
    """Capitalise first letter."""
    return s[:1].upper() + s[1:] if s else s


def _location_phrase(m: re.Match, group: int = 0) -> str:
    """Extract location text from a capture group, cleaned up."""
    try:
        loc = m.group(group).strip().rstrip(".")
        return f" in {loc}" if loc else ""
    except IndexError:
        return ""


# Location terms that are too generic to name in an opening line
_GENERIC_LOCS = frozenset({
    "us", "the us", "usa", "u.s.", "u.s.a.", "america",
    "canada", "uk", "the uk", "australia",
    "the area", "area", "their area", "your area",
})

# Specific service terms required for Pattern 5 ("now offering X") to fire.
# Prevents marketing taglines like "fast, easy installation" producing openers.
_OFFER_TERMS_RE = re.compile(
    r"\b(ev\s+charg\w+|battery\s+storage|energy\s+storage|solar\s+storage"
    r"|off[- ]grid|financing|ppa|power\s+purchase|monitoring|o&m|maintenance)\b",
    re.IGNORECASE,
)


def _location_is_usable(loc: str) -> bool:
    return len(loc) > 3 and loc.lower() not in _GENERIC_LOCS


_PATTERNS: list[tuple[re.Pattern, object]] = [

    # "Installed Xkw/MW ..." — capacity figure is specific enough to name
    (re.compile(
        r"install\w*\s+([\d.,]+\s*(?:kw|mw|kilowatt|megawatt)[^,.\n]*)",
        re.IGNORECASE,
    ),
     lambda m: f"Your team recently completed a {m.group(1).strip().rstrip('.')} installation."),

    # "Completed ... project"
    (re.compile(
        r"complet\w+\s+(?:a\s+)?(.{5,40}?)\s+(?:project|system|install)",
        re.IGNORECASE,
    ),
     lambda m: f"Your team recently completed a {m.group(1).strip()} project."),

    # "rooftop ... in [location]"
    (re.compile(
        r"rooftop\s+(?:solar\s+)?(?:system|install\w*|project)\s+(?:in|at)\s+([\w\s,]+?)(?:\.|,|$)",
        re.IGNORECASE,
    ),
     lambda m: f"Your team has rooftop installation work{_location_phrase(m, 1)}."),

    # "installed/deployed/commissioned ... in [Named Location]" — skip generic locations
    (re.compile(
        r"(?:install\w*|deploy\w*|commission\w*)\s+.{3,40}?\s+in\s+([\w\s]+?)(?:\.|,|$)",
        re.IGNORECASE,
    ),
     lambda m: (
         f"Your team has done installation work in {m.group(1).strip().rstrip('.')}."
         if _location_is_usable(m.group(1).strip().rstrip("."))
         else ""   # skip — location too generic; fall through to next pattern
     )),

    # "Now offering [specific service]" — only fires for named services, not taglines
    (re.compile(
        r"(?:now\s+)?offer\w*\s+((?:ev\s+charg\w+|battery\s+storage|energy\s+storage"
        r"|solar\s+storage|off[- ]grid|financing|ppa|monitoring|o&m|maintenance).{0,40}?)"
        r"(?:\s+install\w*)?(?:\.|,|$)",
        re.IGNORECASE,
    ),
     lambda m: f"Your team offers {m.group(1).strip().lower().rstrip('.')}."),

    # "Tesla Powerwall / [Battery product] installations"
    (re.compile(
        r"(powerwall|tesla|enphase|franklin|sonnen|battery\s+storage|bess)\s+install",
        re.IGNORECASE,
    ),
     lambda m: f"Your team offers {_cap(m.group(1))} installations."),

    # "battery storage / BESS / named product"
    (re.compile(
        r"(battery\s+storage|bess|powerwall|enphase|sonnen|franklin)",
        re.IGNORECASE,
    ),
     lambda m: f"Your team specialises in {_cap(m.group(1))} solutions."),

    # "Expanded to / now serving [location/market]"
    (re.compile(
        r"(?:expand\w*|now\s+serv\w*)\s+(?:to\s+|in\s+)?([\w\s,]+?)(?:\.|,|$)",
        re.IGNORECASE,
    ),
     lambda m: f"Your team is expanding into {m.group(1).strip().rstrip('.')}."),

    # "Hiring [role]"
    (re.compile(
        r"(?:hiring|looking for|seeking)\s+(.{5,40}?)(?:\.|,|$)",
        re.IGNORECASE,
    ),
     lambda m: "It looks like your team is expanding the installation side."),

    # "Solar farm / utility-scale project"
    (re.compile(
        r"solar\s+farm|utility[- ]scale\s+(?:solar|project|system)",
        re.IGNORECASE,
    ),
     lambda m: "Your team is active in utility-scale solar work."),

    # "Commercial solar / commercial project"
    (re.compile(
        r"commercial\s+(?:solar|install\w*|project|rooftop)",
        re.IGNORECASE,
    ),
     lambda m: "It looks like your team focuses on commercial solar installation."),

    # "Residential solar / home solar"
    (re.compile(
        r"(?:residential|home)\s+solar",
        re.IGNORECASE,
    ),
     lambda m: "It looks like your team specialises in residential solar installation."),
]


def _facts_based_fallback(facts: dict, company_name: str, company_type: str = "") -> str:
    """
    Generate a specific opener from structured signal_facts when no regex pattern
    matches the raw signal text.  Uses only items explicitly present in facts —
    never invents details.  Falls back to the type-aware _fallback() if facts are
    too thin to form a meaningful sentence.
    """
    name     = (company_name or "your company").strip()
    techs    = facts.get("technologies",    [])
    locs     = facts.get("locations",       [])
    markets  = facts.get("market_segments", [])
    scale    = facts.get("scale_mentions",  [])

    # Product-type tech terms don't work in "handles X work" templates —
    # fall through to market-based phrasing for those.
    _PRODUCT_TECHS = frozenset({
        "solar panels", "solar panel", "inverter", "inverters",
        "epc", "engineering procurement construction",
    })
    service_techs = [t for t in techs if t.lower() not in _PRODUCT_TECHS]
    is_epc = "epc" in {t.lower() for t in techs}

    has_com = "commercial"    in markets
    has_ind = "industrial"    in markets
    has_res = "residential"   in markets
    has_uti = "utility-scale" in markets

    # Prefer most specific combination first
    if service_techs and locs:
        return f"It looks like {name} handles {service_techs[0].lower()} projects in {locs[0]}."
    if service_techs and scale:
        return f"It looks like {name} works on {scale[0]} {service_techs[0].lower()} projects."
    if service_techs:
        return f"It looks like {name} specialises in {service_techs[0].lower()} solutions."
    if is_epc and locs:
        return f"It looks like {name} handles EPC solar projects in {locs[0]}."
    if is_epc:
        return f"It looks like {name} is an EPC solar contractor."
    if scale and locs:
        return f"It looks like {name} delivers large-scale solar projects in {locs[0]}."

    # Market + location combos (more informative than bare-location fallback)
    if (has_com or has_ind or has_res) and locs:
        if has_com and has_ind and has_res:
            return f"It looks like {name} handles residential, commercial, and industrial solar work in {locs[0]}."
        if has_com and has_ind:
            return f"It looks like {name} focuses on commercial and industrial solar installation in {locs[0]}."
        if has_com:
            return f"It looks like {name} focuses on commercial solar installation in {locs[0]}."
        if has_res:
            return f"It looks like {name} specialises in residential solar installation in {locs[0]}."
        if has_ind:
            return f"It looks like {name} serves industrial solar clients in {locs[0]}."

    # Location only
    if locs:
        return f"It looks like {name} is active in solar installation in {locs[0]}."

    # Market-only fallbacks — better than pure generic
    has_com = "commercial"   in markets
    has_ind = "industrial"   in markets
    has_res = "residential"  in markets
    has_uti = "utility-scale" in markets

    if has_com and has_ind and has_res:
        return f"It looks like {name} handles residential, commercial, and industrial solar work."
    if has_com and has_ind:
        return f"It looks like {name} focuses on commercial and industrial solar installation."
    if has_uti:
        return f"It looks like {name} is active in utility-scale solar projects."
    if has_com:
        return f"It looks like {name} focuses on commercial solar installation."
    if has_res:
        return f"It looks like {name} specialises in residential solar installation."
    if has_ind:
        return f"It looks like {name} serves industrial solar clients."

    return _fallback(company_name, company_type)


def signal_to_opening_line(
    signal: str,
    company_name: str = "",
    facts: dict | None = None,
    company_type: str = "",
) -> str:
    """
    Convert a raw signal string into a natural cold email opening sentence.

    Returns a string of ≤18 words. Falls back to a type-aware opener if no
    pattern matches, using company_type to produce more specific language.

    Normalises the signal text before pattern matching so that LinkedIn bullet
    characters (•), em-dashes (–), and pipe separators do not prevent specific
    patterns (e.g. rooftop project location) from matching.
    """
    signal = (signal or "").strip()
    if not signal:
        if facts and facts.get("has_usable_facts"):
            return _facts_based_fallback(facts, company_name, company_type)
        return _fallback(company_name, company_type)

    normalized = _normalize_signal(signal)

    for pattern, template_fn in _PATTERNS:
        m = pattern.search(normalized)
        if m:
            try:
                result = template_fn(m)
                if not result:
                    # Template returned "" to signal this match should be skipped
                    continue
                # Enforce word count limit
                words = result.split()
                if len(words) > 18:
                    result = " ".join(words[:18]).rstrip(",") + "."
                return result
            except Exception:
                continue

    # No regex pattern matched — use structured facts if available
    if facts and facts.get("has_usable_facts"):
        return _facts_based_fallback(facts, company_name, company_type)
    return _fallback(company_name, company_type)


_TYPE_FALLBACK_PHRASES: dict[str, str] = {
    # Keys are lowercase — matched against company_type.lower()
    "solar epc":                  "handles end-to-end solar EPC projects",
    "solar contractor":           "handles solar installation and contracting work",
    "solar developer":            "develops solar energy projects",
    "solar installer":            "handles solar installation work",
    "solar panel installer":      "installs solar panel systems",
    "solar energy company":       "is active in solar energy installation",
    "solar component distributor": "supplies solar components to installers",
    "battery storage installer":  "handles battery storage installations",
    "bess integrator":            "integrates battery energy storage systems",
    "solar farm developer":       "develops utility-scale solar projects",
}


def _fallback(company_name: str, company_type: str = "") -> str:
    name  = (company_name or "").strip()
    ctype = (company_type or "").strip().lower()
    phrase = _TYPE_FALLBACK_PHRASES.get(ctype, "is active in solar installation")
    subject = name if name else "your company"
    return f"It looks like {subject} {phrase}."
