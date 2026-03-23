"""
Workflow 5 — Lead Scoring  (P1-1B: Dual-Axis Qualification)

Upgrades lead scoring from a single solar-relevance axis to a two-axis model:

  Axis 1: Solar relevance  — is this company in the solar/storage ecosystem?
  Axis 2: Buyer relevance  — is this company a likely procurement target?

Input priority:
  1. buyer_filter.json  (Workflow 4.5, P1-1A output) — preferred.
                         Contains all company_analysis fields plus structured
                         buyer-fit fields (BLS, PRS, value_chain_role, flags…).
  2. company_analysis.json (Workflow 4 output) — fallback when 4.5 has not run.
     In fallback mode the behaviour is identical to the v1 scoring.

Output:
  qualified_leads.csv   — extended with scoring components and buyer-fit fields
  disqualified_leads.csv — with disqualification reasons

Qualification threshold: QUALIFIED_THRESHOLD = 45  (unchanged from v1)
  Rationale: The dual-axis formula widens the distribution — strong commercial
  targets now score 80-95; manufacturer/consultant/media/residential are hard-
  capped at 25-30 (safely below 45). The 45 threshold continues to produce a
  commercially sound cut without any numerical adjustment.

scoring_version written per record:
  "v2_with_buyer_filter" — buyer_filter.json used (P1-1A fields available)
  "v1_solar_only"        — fallback to company_analysis.json
"""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from config.settings import (
    BUYER_FILTER_FILE,
    COMPANY_ANALYSIS_FILE,
    DISQUALIFIED_LEADS_FILE,
    QUALIFIED_LEADS_FILE,
)
from config.run_paths import RunPaths

# ---------------------------------------------------------------------------
# Qualification threshold
# ---------------------------------------------------------------------------

QUALIFIED_THRESHOLD = 40
"""
Lowered from 45 → 40 to capture more borderline leads.

Updated score distribution:
  Strong commercial EPC/installer (with buyer signals)  →  70–95   ← qualified
  Distributor (with buyer signals)                      →  50–70   ← qualified
  Moderate solar company (mixed signals)                →  40–55   ← qualified
  Residential-only installer                            →  25–36   ← rejected
  Manufacturer / competitor (after keyword narrowing)   →  ≤ 30    ← hard-capped, rejected
  Consultant (pure advisory)                            →  ≤ 30    ← hard-capped, rejected
  Media / directory                                     →  ≤ 25    ← hard-capped, rejected
  Unclear / marginal solar company                      →  20–39   ← borderline, most rejected

Rationale: false manufacturer/consultant/media flags were inflating rejections.
After narrowing those keyword lists, the legitimate cut-off sits at 40.
"""

SCORING_VERSION_WITH_BF = "v2_with_buyer_filter"
SCORING_VERSION_FALLBACK = "v1_solar_only"

# ---------------------------------------------------------------------------
# Axis 1 — Solar Relevance  (unchanged from v1)
# ---------------------------------------------------------------------------

TYPE_SCORES: dict[str, int] = {
    "solar installer":             36,
    "solar epc":                   44,
    "solar developer":             36,
    "battery storage installer":   30,
    "solar farm developer":        25,
    "solar panel installer":       36,
    "solar contractor":            39,
    "solar energy company":        20,
    "bess integrator":             30,
    "solar component distributor": 40,
}

MARKET_SCORES: dict[str, int] = {
    "commercial":    24,
    "utility-scale": 25,
    "residential":   -5,
    "mixed":         7,
}

WEBSITE_BONUS = 10

CONFIDENCE_PENALTY_THRESHOLD = 0.50
CONFIDENCE_PENALTY           = -8

# Legacy text penalties — applied against company_type (and services for non-consulting)
PENALTY_RULES: list[tuple[list[str], int]] = [
    (["consulting", "consultant", "advisory"],        -30),
    (["marketing", "agency", "advertising", "media"], -30),
    (["training", "education", "school", "academy"],  -20),
]

# ---------------------------------------------------------------------------
# Axis 2 — Buyer Relevance  (new in P1-1B)
# ---------------------------------------------------------------------------

BUYER_RELEVANCE_MAX = 20   # maximum additive boost from buyer relevance

# Weights for buyer relevance component from P1-1A fields
PRS_WEIGHT = 1.5   # procurement_relevance_score (0–10)
PSS_WEIGHT = 0.5   # project_signal_strength      (0–10)

# ---------------------------------------------------------------------------
# Value chain role adjustments  (new in P1-1B)
# ---------------------------------------------------------------------------

# Applied ADDITIVELY to the running score when value_chain_role is available.
_ROLE_ADJUSTMENTS: dict[str, int] = {
    "installer":              0,     # neutral — type_score already captures this
    "epc_or_contractor":     +3,    # slight boost — primary direct buyers
    "developer":              0,     # neutral — good buyers at project stage
    "distributor":            0,     # neutral — channel partner
    "manufacturer":          -20,   # strong penalty — competitor, not a buyer
    "consultant":            -20,   # strong penalty — advisory only, no procurement
    "media_or_directory":    -25,   # very strong penalty — media/directory
    "association_or_nonbuyer": -20, # strong penalty — non-commercial entity
    "unclear":               -5,    # small penalty — insufficient signal
}

# ---------------------------------------------------------------------------
# Negative targeting penalties  (new in P1-1B, flag-level)
# ---------------------------------------------------------------------------

# Residential installers ARE valid outbound targets — they buy mounting/racking.
# The market_focus="residential" score (-5 from MARKET_SCORES) is the only
# distinction needed; no additional penalty is applied for the residential flag.
# The flag is preserved in output for operator visibility only.
RESIDENTIAL_FLAG_PENALTY = 0   # informational only; no scoring penalty

# competitor_flag adds a small extra penalty ONLY when role != manufacturer
# (edge case: competitor detected via non-standard text but role wasn't overridden)
COMPETITOR_EXTRA_PENALTY = -10

# ---------------------------------------------------------------------------
# Hard score caps  (safety net, applied after all additive logic)
# ---------------------------------------------------------------------------

_HARD_CAPS: dict[str, int] = {
    "manufacturer_flag":       30,   # below threshold
    "competitor_flag":         30,   # below threshold
    "consultant_flag":         30,   # below threshold
    "media_or_directory_flag": 25,   # well below threshold
}

# ---------------------------------------------------------------------------
# Target-tier stratification  (updated in P1-1B to use value_chain_role)
# ---------------------------------------------------------------------------

_TIER_A_ROLES: frozenset[str] = frozenset({"installer", "epc_or_contractor"})
_TIER_B_ROLES: frozenset[str] = frozenset({"developer"})

_TIER_CORE_TYPES: frozenset[str] = frozenset({
    "solar installer", "solar epc", "solar contractor", "solar panel installer",
})
_TIER_SECONDARY_TYPES: frozenset[str] = frozenset({
    "solar developer", "solar energy company", "solar farm developer",
    "battery storage installer", "bess integrator",
})


def _target_tier(
    company_type: str,
    confidence: float,
    method: str,
    value_chain_role: str = "",
) -> str:
    """
    Compute send-priority tier.

    P1-1B: use value_chain_role as primary signal when available;
    fall back to company_type-based logic for v1 compatibility.

    A — core installer/EPC at high confidence
    B — secondary type or moderate confidence
    C — low confidence, rules-based, or distributor without strong AI support
    """
    if value_chain_role:
        if value_chain_role in _TIER_A_ROLES:
            return "A" if confidence >= 0.65 else "B"
        if value_chain_role in _TIER_B_ROLES:
            return "B" if (confidence >= 0.65 and method == "ai") else "C"
        if value_chain_role == "distributor":
            return "B" if (confidence >= 0.75 and method == "ai") else "C"
        return "C"

    # Fallback — original v1 company_type logic
    ct = company_type.lower()
    if ct in _TIER_CORE_TYPES:
        return "A" if confidence >= 0.65 else "B"
    if ct in _TIER_SECONDARY_TYPES:
        return "B" if (confidence >= 0.65 and method == "ai") else "C"
    if ct == "solar component distributor":
        return "B" if (confidence >= 0.75 and method == "ai") else "C"
    return "C"


# ---------------------------------------------------------------------------
# Output field lists
# ---------------------------------------------------------------------------

QUALIFIED_FIELDS: list[str] = [
    # Original fields (backward-compatible)
    "company_name", "website", "place_id",
    "company_type", "market_focus", "services_detected",
    "confidence_score", "classification_method",
    # Scoring output (preserved from v1)
    "lead_score", "score_breakdown", "target_tier",
    # P1-1B — scoring components
    "qualification_status",
    "qualification_reason_summary",
    "solar_relevance_component",
    "buyer_relevance_component",
    "value_chain_adjustment",
    "negative_targeting_penalty",
    "scoring_version",
    # P1-1B — buyer filter pass-throughs (useful for downstream workflows)
    "value_chain_role",
    "buyer_likelihood_score",
    "procurement_relevance_score",
    "market_fit_score",
    "project_signal_strength",
    "negative_residential_flag",
    "competitor_flag",
    "manufacturer_flag",
    "consultant_flag",
    "media_or_directory_flag",
    "buyer_filter_reason",
]

DISQUALIFIED_FIELDS: list[str] = [
    "company_name", "website", "place_id",
    "company_type", "market_focus", "services_detected",
    "confidence_score",
    "lead_score", "score_breakdown",
    "disqualification_reason",
    # P1-1B additions
    "solar_relevance_component",
    "buyer_relevance_component",
    "value_chain_adjustment",
    "negative_targeting_penalty",
    "scoring_version",
    "value_chain_role",
    "buyer_likelihood_score",
    "procurement_relevance_score",
    "negative_residential_flag",
    "competitor_flag",
    "manufacturer_flag",
    "consultant_flag",
    "media_or_directory_flag",
]


# ---------------------------------------------------------------------------
# Axis 1 helpers — Solar relevance (unchanged from v1)
# ---------------------------------------------------------------------------

def _apply_legacy_penalties(company_type: str, services: list[str]) -> tuple[int, list[str]]:
    """
    Return (total_penalty, list_of_reasons).

    Consulting/advisory: matched against company_type only.
    Marketing/training:  matched against company_type OR services_detected.
    Unchanged from v1.
    """
    ct_lower      = company_type.lower()
    svc_lower     = " ".join(s.lower() for s in services)
    type_only_kws = {"consulting", "consultant", "advisory"}
    total   = 0
    reasons = []
    for keywords, penalty in PENALTY_RULES:
        kw_set = set(keywords)
        if kw_set & type_only_kws:
            match = any(kw in ct_lower for kw in keywords)
        else:
            match = any(kw in ct_lower or kw in svc_lower for kw in keywords)
        if match:
            total += penalty
            reasons.append(f"{penalty} (legacy penalty: {keywords[0]})")
    return total, reasons


def _compute_solar_relevance(record: dict) -> tuple[int, list[str]]:
    """
    Compute solar relevance component (Axis 1).

    Identical to v1 scoring logic.  Returns (solar_score, breakdown_items).
    solar_score may be negative (e.g. residential market + no type match).
    """
    company_type = record.get("company_type", "").lower()
    market_focus = record.get("market_focus", "").lower()
    services     = record.get("services_detected", [])
    if isinstance(services, str):
        services = [s.strip() for s in services.split(";") if s.strip()]
    website    = record.get("website", "")
    confidence = float(record.get("confidence_score", 1.0))

    score     = 0
    breakdown = []

    type_pts = TYPE_SCORES.get(company_type, 0)
    if type_pts:
        score += type_pts
        breakdown.append(f"+{type_pts} (solar type: {company_type})")

    market_pts = MARKET_SCORES.get(market_focus, 0)
    if market_pts:
        score += market_pts
        sign = "+" if market_pts > 0 else ""
        breakdown.append(f"{sign}{market_pts} (market: {market_focus})")

    if website:
        score += WEBSITE_BONUS
        breakdown.append(f"+{WEBSITE_BONUS} (website present)")

    penalty_total, penalty_reasons = _apply_legacy_penalties(company_type, services)
    if penalty_total:
        score += penalty_total
        breakdown.extend(penalty_reasons)

    if confidence < CONFIDENCE_PENALTY_THRESHOLD:
        score += CONFIDENCE_PENALTY
        breakdown.append(f"{CONFIDENCE_PENALTY} (low confidence: {confidence:.2f})")

    return score, breakdown


# ---------------------------------------------------------------------------
# Axis 2 helpers — Buyer relevance (new in P1-1B)
# ---------------------------------------------------------------------------

def _compute_buyer_relevance(record: dict) -> tuple[int, list[str]]:
    """
    Compute buyer relevance boost from P1-1A fields.

    Formula: min(BUYER_RELEVANCE_MAX, round(PRS × 1.5 + PSS × 0.5))

    Returns (boost, breakdown_items).
    Returns (0, []) when P1-1A fields are absent.
    """
    prs = int(record.get("procurement_relevance_score", 0) or 0)
    pss = int(record.get("project_signal_strength",    0) or 0)

    if prs == 0 and pss == 0:
        return 0, []

    raw   = prs * PRS_WEIGHT + pss * PSS_WEIGHT
    boost = min(BUYER_RELEVANCE_MAX, round(raw))
    breakdown = []
    if boost > 0:
        breakdown.append(
            f"+{boost} (buyer relevance: prs={prs}/10, project_signal={pss}/10)"
        )
    return boost, breakdown


def _compute_role_adjustment(record: dict) -> tuple[int, list[str]]:
    """
    Compute value chain role adjustment from value_chain_role.

    Returns (adjustment, breakdown_items).
    Returns (0, []) when value_chain_role is absent.
    """
    role = record.get("value_chain_role", "")
    if not role:
        return 0, []

    adj       = _ROLE_ADJUSTMENTS.get(role, 0)
    breakdown = []
    sign      = "+" if adj > 0 else ""
    marker    = f"{sign}{adj}" if adj != 0 else "+0"
    breakdown.append(f"{marker} (value_chain_role: {role})")
    return adj, breakdown


def _compute_negative_targeting_penalty(record: dict) -> tuple[int, list[str]]:
    """
    Compute additional negative-targeting penalties (flag-level).

    Only residential_flag is penalised here — manufacturer / consultant /
    media flags are already captured by _ROLE_ADJUSTMENTS to avoid
    double-penalisation.

    competitor_flag adds a small extra penalty when role != manufacturer
    (edge case where competitor text was detected but role wasn't overridden).

    Returns (penalty_magnitude, breakdown_items) where penalty_magnitude > 0.
    The caller SUBTRACTS this from the running score.
    """
    penalty   = 0
    breakdown = []

    if record.get("negative_residential_flag"):
        mag = abs(RESIDENTIAL_FLAG_PENALTY)
        penalty += mag
        breakdown.append(f"{RESIDENTIAL_FLAG_PENALTY} (residential-focused target)")

    role = record.get("value_chain_role", "")
    if record.get("competitor_flag") and role != "manufacturer":
        mag = abs(COMPETITOR_EXTRA_PENALTY)
        penalty += mag
        breakdown.append(f"{COMPETITOR_EXTRA_PENALTY} (competitor flag, non-manufacturer role)")

    return penalty, breakdown


def _apply_hard_caps(score: int, record: dict) -> tuple[int, list[str]]:
    """
    Apply hard score caps for high-confidence negative-targeting categories.

    Takes the MOST RESTRICTIVE cap when multiple flags are set.
    Returns (capped_score, list_of_cap_notes).
    """
    notes     = []
    cap: int | None = None

    if record.get("manufacturer_flag") or record.get("competitor_flag"):
        cap_val = _HARD_CAPS["manufacturer_flag"]
        if score > cap_val:
            cap = cap_val if cap is None else min(cap, cap_val)
            notes.append(f"hard_cap={cap_val} (manufacturer/competitor)")

    if record.get("media_or_directory_flag"):
        cap_val = _HARD_CAPS["media_or_directory_flag"]
        if score > cap_val:
            cap = cap_val if cap is None else min(cap, cap_val)
            notes.append(f"hard_cap={cap_val} (media/directory)")

    if record.get("consultant_flag"):
        cap_val = _HARD_CAPS["consultant_flag"]
        if score > cap_val:
            cap = cap_val if cap is None else min(cap, cap_val)
            notes.append(f"hard_cap={cap_val} (consultant)")

    if cap is not None:
        return min(score, cap), notes
    return score, notes


# ---------------------------------------------------------------------------
# Qualification reason builder
# ---------------------------------------------------------------------------

def _build_qualification_reason(
    record: dict,
    solar: int,
    buyer: int,
    role_adj: int,
    neg_penalty: int,
    final: int,
    qualified: bool,
) -> str:
    """Build a human-readable single-line qualification reason."""
    parts = []

    role = record.get("value_chain_role", "")
    if role:
        parts.append(f"role={role}")

    parts.append(f"solar={solar}")

    if buyer:
        parts.append(f"buyer_boost=+{buyer}")
    if role_adj != 0:
        sign = "+" if role_adj > 0 else ""
        parts.append(f"role_adj={sign}{role_adj}")
    if neg_penalty:
        parts.append(f"neg_penalty=-{neg_penalty}")

    parts.append(f"score={final}")
    parts.append("QUALIFIED" if qualified else f"REJECTED (threshold={QUALIFIED_THRESHOLD})")

    neg_reasons = record.get("negative_targeting_reasons", [])
    if isinstance(neg_reasons, list) and neg_reasons:
        parts.append(f"flags=[{'; '.join(neg_reasons)}]")
    elif isinstance(neg_reasons, str) and neg_reasons:
        parts.append(f"flags=[{neg_reasons}]")

    return "; ".join(parts)


# ---------------------------------------------------------------------------
# Main scoring function
# ---------------------------------------------------------------------------

def score_company(record: dict) -> dict:
    """
    Compute the dual-axis lead_score for one company record.

    When buyer filter fields are present (value_chain_role is set):
      1. Solar relevance (Axis 1) — type + market + website + confidence + legacy penalties
      2. Buyer relevance boost (Axis 2) — from PRS + project_signal_strength
      3. Value chain role adjustment — installer/EPC: 0/+3; manufacturer: -20; etc.
      4. Negative targeting penalty — residential flag: -15; competitor extra: -10
      5. Hard caps — manufacturer/competitor ≤ 30; media ≤ 25; consultant ≤ 30
      scoring_version = "v2_with_buyer_filter"

    When buyer filter fields are absent (fallback from company_analysis.json):
      Axis 1 only — identical to v1 behaviour.
      scoring_version = "v1_solar_only"

    Returns record enriched with all scoring fields listed in QUALIFIED_FIELDS.
    """
    company_type = record.get("company_type", "").lower()
    confidence   = float(record.get("confidence_score", 1.0))
    method       = record.get("classification_method", "")
    # Presence of value_chain_role signals that buyer_filter.json was loaded
    has_bf = bool(record.get("value_chain_role"))

    # Axis 1: solar relevance
    solar_score, solar_bd = _compute_solar_relevance(record)

    # Axis 2: buyer relevance (0 when no BF data)
    buyer_boost, buyer_bd = _compute_buyer_relevance(record) if has_bf else (0, [])

    # Value chain role adjustment (0 when no BF data)
    role_adj, role_bd = _compute_role_adjustment(record) if has_bf else (0, [])

    # Negative targeting penalty (0 when no BF data)
    neg_penalty, neg_bd = (
        _compute_negative_targeting_penalty(record) if has_bf else (0, [])
    )

    # Combine and clip
    raw_score = solar_score + buyer_boost + role_adj - neg_penalty
    clipped   = max(0, min(100, raw_score))

    # Hard caps
    final_score, cap_notes = (
        _apply_hard_caps(clipped, record) if has_bf else (clipped, [])
    )

    score_breakdown = solar_bd + buyer_bd + role_bd + neg_bd + cap_notes

    value_chain_role = record.get("value_chain_role", "")
    tier = _target_tier(company_type, confidence, method, value_chain_role)

    scoring_version = SCORING_VERSION_WITH_BF if has_bf else SCORING_VERSION_FALLBACK
    qualified       = final_score >= QUALIFIED_THRESHOLD

    reason = _build_qualification_reason(
        record, solar_score, buyer_boost, role_adj, neg_penalty, final_score, qualified,
    )

    return {
        **record,
        # Scoring output (v1-compatible fields)
        "lead_score":      final_score,
        "score_breakdown": score_breakdown,
        "target_tier":     tier,
        # P1-1B: scoring components
        "qualification_status":         "qualified" if qualified else "rejected",
        "qualification_reason_summary": reason,
        "solar_relevance_component":    solar_score,
        "buyer_relevance_component":    buyer_boost,
        "value_chain_adjustment":       role_adj,
        "negative_targeting_penalty":   neg_penalty,
        "scoring_version":              scoring_version,
    }


# ---------------------------------------------------------------------------
# Batch scoring
# ---------------------------------------------------------------------------

def score_all(records: list[dict]) -> list[dict]:
    """Score all records; return sorted by lead_score descending."""
    return sorted(
        [score_company(r) for r in records],
        key=lambda r: r["lead_score"],
        reverse=True,
    )


def filter_qualified(scored: list[dict]) -> tuple[list[dict], list[dict]]:
    """Return (qualified, disqualified) split at QUALIFIED_THRESHOLD."""
    qualified    = [r for r in scored if r["lead_score"] >= QUALIFIED_THRESHOLD]
    disqualified = [r for r in scored if r["lead_score"] <  QUALIFIED_THRESHOLD]
    print(
        f"[Workflow 5] {len(scored)} scored → "
        f"{len(qualified)} qualified (score ≥ {QUALIFIED_THRESHOLD}), "
        f"{len(disqualified)} disqualified"
    )
    for r in disqualified:
        breakdown = " | ".join(str(x) for x in r.get("score_breakdown", []))
        role      = r.get("value_chain_role", "")
        role_tag  = f" role={role}" if role else ""
        print(
            f"[Workflow 5]   DISQUALIFIED: {r.get('company_name', '?')} "
            f"score={r['lead_score']}{role_tag} [{breakdown}]"
        )
    return qualified, disqualified


# ---------------------------------------------------------------------------
# Data loading — buyer_filter.json preferred, company_analysis.json fallback
# ---------------------------------------------------------------------------

def load_records(
    limit: int = 0,
    paths: RunPaths | None = None,
) -> tuple[list[dict], bool]:
    """
    Load records for scoring.

    Tries buyer_filter.json (P1-1A output) first.
    Falls back to company_analysis.json when buyer_filter.json is missing or empty.

    Returns:
        (records, used_buyer_filter)
        used_buyer_filter: True when buyer_filter.json was successfully loaded.
    """
    # Resolve concrete file paths — prefer explicit RunPaths, fall back to
    # legacy _RunPath constants for backward compatibility (standalone use).
    bf_path = paths.buyer_filter_file if paths else Path(str(BUYER_FILTER_FILE))
    ca_path = paths.company_analysis_file if paths else Path(str(COMPANY_ANALYSIS_FILE))

    if bf_path.exists():
        try:
            with open(bf_path, encoding="utf-8") as f:
                records = json.load(f)
            if records:
                limited = records[:limit] if limit else records
                print(
                    f"[Workflow 5] Loaded {len(limited)} records from buyer_filter.json "
                    f"(scoring: {SCORING_VERSION_WITH_BF})"
                )
                return limited, True
        except Exception as exc:
            print(f"[Workflow 5] Could not load buyer_filter.json ({exc}), falling back")

    with open(ca_path, encoding="utf-8") as f:
        records = json.load(f)
    limited = records[:limit] if limit else records
    print(
        f"[Workflow 5] Loaded {len(limited)} records from company_analysis.json "
        f"(scoring: {SCORING_VERSION_FALLBACK})"
    )
    return limited, False


def load_analyses(limit: int = 0) -> list[dict]:
    """Backward-compatible loader — returns company_analysis.json records directly."""
    with open(COMPANY_ANALYSIS_FILE, encoding="utf-8") as f:
        records = json.load(f)
    return records[:limit] if limit else records


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def _serialize_lead_row(lead: dict, fields: list[str]) -> dict:
    row: dict[str, Any] = {}
    for k in fields:
        v = lead.get(k, "")
        if k == "services_detected":
            raw = lead.get("services_detected", [])
            v = "; ".join(raw) if isinstance(raw, list) else str(raw)
        elif k == "score_breakdown":
            raw = lead.get("score_breakdown", [])
            v = " | ".join(str(x) for x in raw) if isinstance(raw, list) else str(raw)
        elif isinstance(v, list):
            v = " | ".join(str(x) for x in v)
        elif isinstance(v, bool):
            v = str(v)
        row[k] = v
    return row


def save_qualified(leads: list[dict], out_path: Path | None = None) -> None:
    path = out_path or Path(str(QUALIFIED_LEADS_FILE))
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=QUALIFIED_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(_serialize_lead_row(r, QUALIFIED_FIELDS) for r in leads)
    print(f"[Workflow 5] Saved {len(leads)} qualified leads → {path}")


def save_disqualified(leads: list[dict], out_path: Path | None = None) -> None:
    """Write disqualified_leads.csv with rich disqualification reasons."""
    rows = []
    for lead in leads:
        breakdown     = lead.get("score_breakdown", [])
        penalty_parts = [b for b in breakdown if isinstance(b, str) and b.startswith("-")]

        if penalty_parts:
            reason = (
                f"score {lead['lead_score']} below threshold; penalties: "
                + ", ".join(penalty_parts)
            )
        else:
            reason = f"score {lead['lead_score']} below threshold {QUALIFIED_THRESHOLD}"

        # Append negative targeting context when available
        neg_reasons = lead.get("negative_targeting_reasons", [])
        if isinstance(neg_reasons, list) and neg_reasons:
            reason += f"; negative flags: {'; '.join(neg_reasons)}"
        elif isinstance(neg_reasons, str) and neg_reasons:
            reason += f"; negative flags: {neg_reasons}"

        row = _serialize_lead_row(lead, DISQUALIFIED_FIELDS)
        row["disqualification_reason"] = reason
        rows.append(row)

    path = out_path or Path(str(DISQUALIFIED_LEADS_FILE))
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=DISQUALIFIED_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"[Workflow 5] Saved {len(leads)} disqualified leads → {path}")


# ---------------------------------------------------------------------------
# Operator-visible summary
# ---------------------------------------------------------------------------

def _print_scoring_summary(
    total: int,
    qualified: list[dict],
    disqualified: list[dict],
    used_buyer_filter: bool,
) -> None:
    """Print P1-impact-visible summary so operators can see how buyer relevance changed things."""
    q_count = len(qualified)
    d_count = len(disqualified)

    residential_neg  = sum(1 for r in disqualified if r.get("negative_residential_flag"))
    competitor_mfr   = sum(
        1 for r in disqualified if r.get("competitor_flag") or r.get("manufacturer_flag")
    )
    consultant_media = sum(
        1 for r in disqualified if r.get("consultant_flag") or r.get("media_or_directory_flag")
    )

    version = SCORING_VERSION_WITH_BF if used_buyer_filter else SCORING_VERSION_FALLBACK
    print(
        f"\n[Workflow 5] Lead Scoring Summary ({version}):\n"
        f"  Total processed         : {total}\n"
        f"  Qualified (score≥{QUALIFIED_THRESHOLD})  : {q_count}\n"
        f"  Disqualified            : {d_count}"
    )

    if used_buyer_filter and d_count:
        by_role: dict[str, int] = {}
        for r in disqualified:
            role = r.get("value_chain_role", "")
            if role:
                by_role[role] = by_role.get(role, 0) + 1

        print(
            f"\n  Disqualification breakdown (buyer-relevance factors):\n"
            f"    Residential-focused   : {residential_neg}\n"
            f"    Competitor/manufacturer: {competitor_mfr}\n"
            f"    Consultant/media      : {consultant_media}"
        )
        if by_role:
            print(f"\n  Disqualified by value-chain role:")
            for role, count in sorted(by_role.items()):
                print(f"    {role:<24}: {count}")
    print()


# ---------------------------------------------------------------------------
# Public run()
# ---------------------------------------------------------------------------

def run(limit: int = 0, paths: RunPaths | None = None) -> list[dict]:
    """
    Score companies and write qualified leads to CSV.

    P1-1B behaviour:
      - Reads buyer_filter.json (P1-1A) when available → dual-axis scoring
      - Falls back to company_analysis.json → v1 solar-only scoring
      - Writes extended qualified_leads.csv (new scoring component fields added)
      - Writes disqualified_leads.csv with disqualification reasons
      - Prints operator-visible summary with buyer-relevance breakdown

    Args:
        limit: cap on records to process (0 = all)
        paths: explicit RunPaths from campaign_runner; if None, fetched from
               the active global (standalone / backward-compat invocation).

    Returns:
        List of qualified lead dicts.
    """
    if paths is None:
        from config.run_paths import require_active_run_paths
        paths = require_active_run_paths()

    records, used_buyer_filter = load_records(limit=limit, paths=paths)
    scored                     = score_all(records)
    qualified, disqualified    = filter_qualified(scored)
    save_qualified(qualified,    paths.qualified_leads_file)
    save_disqualified(disqualified, paths.disqualified_leads_file)
    _print_scoring_summary(len(scored), qualified, disqualified, used_buyer_filter)
    return qualified


if __name__ == "__main__":
    run()
