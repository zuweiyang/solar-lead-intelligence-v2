# Workflow 4: AI Company Analysis
# Classifies companies from company_text.json using AI.
# Provider waterfall: OpenRouter → Anthropic → keyword-rule fallback.

import csv
import json
import time
from pathlib import Path

import requests
import tldextract

from config.settings import (
    RAW_LEADS_FILE,
    COMPANY_TEXT_FILE,
    COMPANY_ANALYSIS_FILE,
    ANTHROPIC_API_KEY,
    OPENROUTER_API_KEY,
    CLASSIFICATION_CACHE_FILE,
)

# OpenRouter model — cheap and fast for classification
OPENROUTER_MODEL = "anthropic/claude-3-haiku"
ANTHROPIC_MODEL  = "claude-haiku-4-5-20251001"
AI_DELAY         = 0.5     # seconds between API calls
MAX_TEXT_CHARS   = 5000

# Classification cache thresholds
CLASSIFICATION_REUSE_MIN_CONFIDENCE  = 0.65   # cached result reused if confidence >= this
CLASSIFICATION_OVERRIDE_MARGIN       = 0.15   # new result replaces cache only if confidence > cached + margin

COMPANY_TYPES = [
    "solar installer",
    "solar EPC",
    "solar contractor",
    "solar developer",
    "solar energy company",
    "solar panel installer",
    "solar component distributor",
    "solar farm developer",
    "battery storage installer",
    "BESS integrator",
    "other",
]


# ---------------------------------------------------------------------------
# Pre-analysis solar relevance filter
# Avoids spending an AI call on companies with no solar/storage content at all.
# ---------------------------------------------------------------------------

_SOLAR_CORE_TERMS: frozenset[str] = frozenset({
    "solar panel", "solar system", "solar installation", "solar installer",
    "solar contractor", "solar epc", "solar energy", "photovoltaic", " pv ",
    "battery storage", "bess", "energy storage", "solar project",
    "solar array", "solar module", "solar inverter", "rooftop solar",
    "solar farm", "solar power", "solar solution", "solar developer",
    "solar component", "solar equipment", "solar product", "solar service",
    "pv system", "pv installation", "solar mounting", "solar pump",
})


def _is_solar_relevant(text: str, company_name: str = "") -> bool:
    """Return True if text or company name contains any solar/storage content.

    Strategy:
    1. If the text (or company name) contains "solar" anywhere → pass.
       This is intentionally broad — Solar Gard (film), TOTALSOLAR (Dutch site),
       Navitas Solar (bare navigation text) are all allowed through and handled
       correctly by the AI classifier.
    2. Otherwise check for storage/PV compound terms (battery storage, bess, etc.).
    3. If neither matches, the company has zero solar content → classify as "other"
       without an API call.  Typically catches: industrial automation vendors,
       logistics companies, aluminum fabricators, consumer electronics brands.
    """
    text_lower = text.lower()
    name_lower = (company_name or "").lower()

    # Fast path: "solar" anywhere in text or company name
    # Also catch "renewable/renewables" in company name (e.g. "ENAR Renewables")
    # These appear when crawl failed so text is empty — name is the only signal.
    if ("solar" in text_lower or "solar" in name_lower
            or "renewab" in name_lower or "renewab" in text_lower):
        return True

    # Storage / PV terms that don't use the word "solar"
    return any(term in text_lower for term in _SOLAR_CORE_TERMS)

MARKET_FOCUSES = ["residential", "commercial", "utility-scale", "mixed"]

CLASSIFICATION_PROMPT = """\
You are a B2B sales analyst for a solar mounting systems manufacturer.

Analyze the following company text and classify the company.

Return ONLY a valid JSON object with these exact fields:
- company_type: one of {types}
- market_focus: one of {markets}
- services_detected: list of specific services mentioned (max 5 items)
- confidence_score: float 0.0–1.0 indicating how confident you are

Classification guidance:

USE "solar component distributor" when the company primarily distributes, resells,
or trades solar panels, inverters, mounting hardware, batteries, or solar system
components — WITHOUT performing installation work themselves. Examples: Growatt
distributors, panel wholesalers, solar equipment trading companies.

USE "other" when the company's primary business is clearly NOT solar PV installation,
solar development, or energy storage integration. Assign "other" for:
- Industrial automation or control systems companies (Beckhoff, SCADA, PLC vendors)
- Consumer battery brands (e.g. Energizer, Duracell) — these are NOT energy storage integrators
- LED lighting or street lighting companies — solar street lights alone ≠ solar PV installer
- Insulation, thermal, or HVAC companies where solar is a minor add-on
- Aluminum, fabrication, or workshop services
- Logistics, freight, or supply chain companies
- Technology parks, business incubators, or government entities
- IT companies that happen to use "BESS" or "energy" in their name (software/IT focus)
- Window film or solar film distributors (not photovoltaic)
- General trading companies with no clear solar installation services
- Companies that mention "solar" only incidentally (e.g. "solar-powered office", "reduce utility bills")

DO NOT assign "solar farm developer" unless the text explicitly describes utility-scale
ground-mount solar project development (MW-scale, land acquisition, offtake agreements).
Merely mentioning "utility savings" or "utility bills" is not grounds for this label.

DO NOT assign "battery storage installer" unless the text explicitly describes
battery storage system installation as a core service (not just selling batteries).

If solar installation is mentioned alongside a non-solar primary business (insulation,
aluminum, IT), classify the primary business and use "other".

Prefer a lower confidence_score (0.3–0.5) when the evidence is ambiguous.

Company text:
{text}
""".format(
    types=", ".join(f'"{t}"' for t in COMPANY_TYPES),
    markets=", ".join(f'"{m}"' for m in MARKET_FOCUSES),
    text="{text}",   # filled per call
)


# ---------------------------------------------------------------------------
# Data loading helpers
# ---------------------------------------------------------------------------

def _load_name_map() -> dict[str, str]:
    """Return place_id → company_name from raw_leads.csv."""
    try:
        with open(RAW_LEADS_FILE, newline="", encoding="utf-8") as f:
            return {
                r["place_id"]: r["company_name"]
                for r in csv.DictReader(f)
                if r.get("place_id") and r.get("company_name")
            }
    except FileNotFoundError:
        return {}


def load_company_texts(limit: int = 0) -> list[dict]:
    """Load records from company_text.json, optionally capped at limit."""
    with open(COMPANY_TEXT_FILE, encoding="utf-8") as f:
        records = json.load(f)
    name_map = _load_name_map()
    for r in records:
        r.setdefault("company_name", name_map.get(r.get("place_id", ""), ""))
    return records[:limit] if limit else records


# ---------------------------------------------------------------------------
# AI classification — provider waterfall
# ---------------------------------------------------------------------------

def _parse_ai_raw(raw: str, context: str = "") -> dict:
    """
    Strip markdown fences and parse JSON from an AI response.
    Handles: empty response, bare control chars, extra trailing text.
    """
    from src.workflow_6_email_generation.ai_json_utils import parse_ai_json
    return parse_ai_json(raw, context=context)


def _classify_with_openrouter(text: str) -> dict:
    """Call OpenRouter API (supports any model, billed per token)."""
    prompt = CLASSIFICATION_PROMPT.format(text=text[:MAX_TEXT_CHARS])
    resp = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type":  "application/json",
        },
        json={
            "model":      OPENROUTER_MODEL,
            "messages":   [{"role": "user", "content": prompt}],
            "max_tokens": 256,
        },
        timeout=30,
    )
    resp.raise_for_status()
    raw = resp.json()["choices"][0]["message"]["content"]
    return _parse_ai_raw(raw)


def _classify_with_anthropic(text: str) -> dict:
    """Call Anthropic SDK directly."""
    import anthropic as _anthropic
    client = _anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    prompt = CLASSIFICATION_PROMPT.format(text=text[:MAX_TEXT_CHARS])
    message = client.messages.create(
        model=ANTHROPIC_MODEL,
        max_tokens=256,
        messages=[{"role": "user", "content": prompt}],
    )
    return _parse_ai_raw(message.content[0].text)


def _classify_with_ai(text: str) -> dict:
    """Provider waterfall: OpenRouter → Anthropic."""
    if OPENROUTER_API_KEY:
        return _classify_with_openrouter(text)
    if ANTHROPIC_API_KEY:
        return _classify_with_anthropic(text)
    raise RuntimeError("No AI provider available")


# ---------------------------------------------------------------------------
# Keyword fallback (no API key required)
# ---------------------------------------------------------------------------

_KW_TYPE_MAP = [
    (["bess", "battery energy storage system"],                        "BESS integrator"),
    # "energy storage" alone is too broad; require "battery storage" explicitly
    (["battery storage"],                                              "battery storage installer"),
    # "utility" alone catches utility-bill copy; require solar context
    (["solar farm", "utility-scale solar", "utility solar", "grid-scale solar"], "solar farm developer"),
    (["epc", "engineering, procurement"],                              "solar EPC"),
    # "develop" alone catches any company with generic prose; require solar context
    (["solar development", "project developer", "renewable developer"], "solar developer"),
    (["distributor", "distribution", "trading", "wholesale", "reseller"],
                                                                       "solar component distributor"),
    (["contractor"],                                                   "solar contractor"),
    (["solar panel", "panel installation"],                            "solar panel installer"),
    (["solar installer", "solar installation", "solar installs"],      "solar installer"),
    (["solar energy", "solar company", "solar provider"],              "solar energy company"),
]

_KW_MARKET_MAP = [
    (["utility", "utility-scale", "megawatt", "mw ", "grid-scale"], "utility-scale"),
    (["commercial", "business", "industrial", "c&i"],               "commercial"),
    (["residential", "homeowner", "home", "rooftop"],               "residential"),
]

_KW_SERVICES = [
    "solar installation", "battery storage", "EPC", "O&M", "maintenance",
    "energy storage", "BESS", "off-grid", "rooftop solar", "utility-scale solar",
    "commercial solar", "residential solar", "solar panels", "inverter",
    "mounting systems", "solar financing", "power purchase agreement",
]


# Installation/integration context required before assigning BESS-related types
# in the rules fallback. Prevents battery product pages from being mis-typed
# as BESS integrators when there is no actual install/integration evidence.
_BESS_INSTALL_CONTEXT: frozenset[str] = frozenset({
    "install", "installation", "integrate", "integration", "integrator",
    "commissioning", "deployment", "system integration",
})

_BESS_RULE_LABELS: frozenset[str] = frozenset({
    "BESS integrator", "battery storage installer",
})


def _classify_with_rules(text: str) -> dict:
    """Keyword-based fallback classifier. No API calls required."""
    lower = text.lower()

    company_type = "solar energy company"
    for keywords, label in _KW_TYPE_MAP:
        if any(kw in lower for kw in keywords):
            # BESS/battery types require explicit install/integration context.
            # Without it the rule fires on mere product mentions (distributors,
            # consultants, manufacturers) — fall through to the next rule instead.
            if label in _BESS_RULE_LABELS:
                if not any(ctx in lower for ctx in _BESS_INSTALL_CONTEXT):
                    continue
            company_type = label
            break

    market_focus = "mixed"
    hits = []
    for keywords, label in _KW_MARKET_MAP:
        if any(kw in lower for kw in keywords):
            hits.append(label)
    if len(hits) == 1:
        market_focus = hits[0]
    elif len(hits) > 1:
        market_focus = "mixed"

    services = [s for s in _KW_SERVICES if s.lower() in lower][:5]

    # Confidence is lower for rule-based — reflect that
    confidence = 0.55 if company_type != "solar energy company" else 0.35

    return {
        "company_type":      company_type,
        "market_focus":      market_focus,
        "services_detected": services,
        "confidence_score":  confidence,
    }


# ---------------------------------------------------------------------------
# Classification cache (global CRM — spans all campaigns)
# ---------------------------------------------------------------------------

_classification_cache: dict | None = None


def _domain_from_url(url: str) -> str:
    ext = tldextract.extract(url or "")
    return f"{ext.domain}.{ext.suffix}".lower() if ext.domain else ""


def _load_classification_cache() -> dict:
    """Load classification cache lazily (once per process)."""
    global _classification_cache
    if _classification_cache is not None:
        return _classification_cache

    _classification_cache = {"place_ids": {}, "domains": {}}
    try:
        cache_path = Path(str(CLASSIFICATION_CACHE_FILE))
        if cache_path.exists():
            with open(cache_path, encoding="utf-8") as f:
                data = json.load(f)
            _classification_cache = {
                "place_ids": data.get("place_ids", {}),
                "domains":   data.get("domains",   {}),
            }
    except Exception as exc:
        print(f"[Workflow 4]   Cache load failed: {exc}")
    return _classification_cache


def _save_classification_cache() -> None:
    try:
        cache_path = Path(str(CLASSIFICATION_CACHE_FILE))
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(_classification_cache, f, indent=2, ensure_ascii=False)
    except Exception as exc:
        print(f"[Workflow 4]   Cache save failed: {exc}")


# ---------------------------------------------------------------------------
# Per-company classification
# ---------------------------------------------------------------------------

def classify_company(record: dict, use_ai: bool = True) -> dict:
    """
    Classify one company. Tries AI first if use_ai=True and key is available,
    falls back to keyword rules on any failure.

    Pre-filter: companies whose text contains no solar/storage core terms are
    immediately classified as "other" without spending an API call.
    """
    text      = record.get("company_text", "")
    place_id  = record.get("place_id", "")
    website   = record.get("website",   "")
    domain    = _domain_from_url(website)

    # Pre-filter: skip AI entirely for obviously non-solar companies
    name = record.get("company_name", "")
    if not _is_solar_relevant(text, company_name=name):
        result_dict = {
            "company_name":      name,
            "website":           website,
            "place_id":          place_id,
            "company_type":      "other",
            "market_focus":      "mixed",
            "services_detected": [],
            "confidence_score":  0.95,
            "classification_method": "pre-filter",
        }
        _update_cache(place_id, domain, result_dict)
        return result_dict

    # Check classification cache (avoid duplicate AI calls for same company)
    cache   = _load_classification_cache()
    cached  = cache["place_ids"].get(place_id) or cache["domains"].get(domain)
    cached_confidence = float(cached.get("confidence_score", 0)) if cached else 0.0

    if cached and cached_confidence >= CLASSIFICATION_REUSE_MIN_CONFIDENCE:
        print(
            f"[Workflow 4]   Cache hit ({place_id or domain}): "
            f"{cached['company_type']} conf={cached_confidence:.2f} — reusing"
        )
        return {
            "company_name":          name or cached.get("company_name", ""),
            "website":               website or cached.get("website", ""),
            "place_id":              place_id,
            "company_type":          cached["company_type"],
            "market_focus":          cached["market_focus"],
            "services_detected":     cached["services_detected"],
            "confidence_score":      cached_confidence,
            "classification_method": "classification_reused_from_history",
        }

    result: dict | None = None
    method = "rules"

    if use_ai and (OPENROUTER_API_KEY or ANTHROPIC_API_KEY):
        try:
            result = _classify_with_ai(text)
            method = "ai"
        except Exception as exc:
            print(f"[Workflow 4]   AI error, falling back to rules: {exc}")

    if result is None:
        result = _classify_with_rules(text)

    new_confidence = round(float(result.get("confidence_score", 0.0)), 2)

    # Determine final method code if overriding a cached classification
    if cached and new_confidence <= cached_confidence + CLASSIFICATION_OVERRIDE_MARGIN:
        # New result isn't meaningfully better — keep cached type but log the run result
        method = "classification_reused_from_history"
        result = cached
        new_confidence = cached_confidence
    elif cached:
        method = "classification_overridden_by_new_evidence"

    result_dict = {
        "company_name":          name,
        "website":               website,
        "place_id":              place_id,
        "company_type":          result.get("company_type", "other"),
        "market_focus":          result.get("market_focus", "mixed"),
        "services_detected":     result.get("services_detected", []),
        "confidence_score":      new_confidence,
        "classification_method": method,
    }
    _update_cache(place_id, domain, result_dict)
    return result_dict


def _update_cache(place_id: str, domain: str, result_dict: dict) -> None:
    """Store a classification result in the module-level cache and persist to disk."""
    cache = _load_classification_cache()
    entry = {k: result_dict[k] for k in (
        "company_type", "market_focus", "services_detected", "confidence_score"
    ) if k in result_dict}
    if place_id:
        cache["place_ids"][place_id] = entry
    if domain:
        cache["domains"][domain] = entry
    _save_classification_cache()


# ---------------------------------------------------------------------------
# Public run()
# ---------------------------------------------------------------------------

def run(limit: int = 0) -> list[dict]:
    """
    Classify all companies from company_text.json.

    Args:
        limit: cap on records to process (0 = all)

    Returns:
        List of analysis records saved to company_analysis.json.
    """
    records = load_company_texts(limit=limit)
    use_ai = bool(OPENROUTER_API_KEY or ANTHROPIC_API_KEY)

    if use_ai:
        provider = "openrouter" if OPENROUTER_API_KEY else "anthropic"
        model    = OPENROUTER_MODEL if OPENROUTER_API_KEY else ANTHROPIC_MODEL
        print(f"[Workflow 4] AI mode — {provider} / {model}")
    else:
        print("[Workflow 4] No AI provider configured — using keyword fallback")

    results: list[dict] = []

    for i, record in enumerate(records, 1):
        name = record.get("company_name") or record.get("website", f"record {i}")
        print(f"[Workflow 4] ({i}/{len(records)}) {name}")

        result = classify_company(record, use_ai=use_ai)
        results.append(result)

        print(
            f"[Workflow 4]   → {result['company_type']} | "
            f"{result['market_focus']} | "
            f"confidence {result['confidence_score']:.2f} [{result['classification_method']}]"
        )

        if use_ai and i < len(records):
            time.sleep(AI_DELAY)

    COMPANY_ANALYSIS_FILE.write_text(
        json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"\n[Workflow 4] Saved {len(results)} analyses → {COMPANY_ANALYSIS_FILE}")
    return results


if __name__ == "__main__":
    run()
