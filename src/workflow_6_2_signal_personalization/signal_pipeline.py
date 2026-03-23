# Workflow 6.2: Signal-based Personalization — Pipeline Orchestrator
# 1. Load company signals (company_signals.json or research_signals.json fallback)
# 2. Rank signals per company
# 3. Convert best signal → opening line
# 4. Write data/company_openings.json

import json
from pathlib import Path

from config.settings import (
    COMPANY_SIGNALS_FILE,
    COMPANY_OPENINGS_FILE,
    RESEARCH_SIGNALS_FILE,
    ENRICHED_LEADS_FILE,
)
from src.workflow_6_2_signal_personalization.signal_loader import (
    load_company_signals, signals_by_name, _normalize_name,
)
from src.workflow_6_2_signal_personalization.signal_ranker import rank_signals
from src.workflow_6_2_signal_personalization.signal_to_opening import signal_to_opening_line
from src.workflow_6_2_signal_personalization.signal_fact_extractor import extract_facts, _empty as _empty_facts


# Re-use the same normalization logic as signal_loader so name matching is consistent
def _normalize(name: str) -> str:
    return _normalize_name(name)


def _load_lead_names(path: Path) -> list[str]:
    """Return company names from enriched_leads.csv (preserves original casing)."""
    import csv
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return [r["company_name"] for r in csv.DictReader(f) if r.get("company_name")]


def _load_lead_type_map(path: Path) -> dict[str, str]:
    """Return company_name → company_type mapping from enriched_leads.csv."""
    import csv
    if not path.exists():
        return {}
    with open(path, newline="", encoding="utf-8") as f:
        return {
            r["company_name"]: r.get("company_type", "")
            for r in csv.DictReader(f) if r.get("company_name")
        }


def generate_personalized_openings(
    signals_path: Path = COMPANY_SIGNALS_FILE,
    fallback_path: Path = RESEARCH_SIGNALS_FILE,
    leads_path: Path = ENRICHED_LEADS_FILE,
    output_path: Path = COMPANY_OPENINGS_FILE,
) -> list[dict]:
    """
    Generate personalized opening lines for all companies that have signals.

    Steps:
      1. Load signals
      2. Rank signals per company
      3. Convert best signal → opening line
      4. Optionally expand coverage to all enriched leads
         (companies without signals get a fallback opening)
      5. Write company_openings.json

    Returns list of {company_name, best_signal, opening_line} dicts.
    """
    print("[Workflow 6.2] Starting signal personalization pipeline...")

    # Step 1 — Load
    signal_records = load_company_signals(signals_path, fallback_path)
    lookup = signals_by_name(signal_records)   # lower_name → [signals]

    # Step 2+3 — Rank and convert per company
    openings: list[dict] = []
    with_signals = 0
    fallback_used = 0

    # Build set of company names to process (signals first, then any leads without signals)
    lead_names    = _load_lead_names(leads_path)
    lead_type_map = _load_lead_type_map(leads_path)
    # Deduplicate while preserving order: signals first, then leads not already covered
    seen_lower: set[str] = set()
    name_list: list[str] = []

    for r in signal_records:
        n = r["company_name"]
        if _normalize(n) not in seen_lower:
            seen_lower.add(_normalize(n))
            name_list.append(n)

    for n in lead_names:
        if _normalize(n) not in seen_lower:
            seen_lower.add(_normalize(n))
            name_list.append(n)

    for company_name in name_list:
        key     = _normalize(company_name)
        signals = lookup.get(key, [])

        best_signal = rank_signals(signals)

        company_type = lead_type_map.get(company_name, "")

        if best_signal:
            facts   = extract_facts(best_signal)
            opening = signal_to_opening_line(best_signal, company_name,
                                             facts=facts, company_type=company_type)
            with_signals += 1
        else:
            facts       = _empty_facts()
            opening     = signal_to_opening_line("", company_name,
                                                 facts=facts, company_type=company_type)
            best_signal = ""
            fallback_used += 1

        openings.append({
            "company_name": company_name,
            "best_signal":  best_signal,
            "opening_line": opening,
            "signal_facts": facts,
        })

        status = "signal" if best_signal else "fallback"
        print(f"[Workflow 6.2]   [{status}] {company_name[:40]:<40}  {opening[:70]}")

    # Step 4 — Write output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(openings, f, indent=2, ensure_ascii=False)

    print(
        f"\n[Workflow 6.2] Complete: {len(openings)} companies\n"
        f"  Signal-based openings : {with_signals}\n"
        f"  Fallback openings     : {fallback_used}\n"
        f"  Output                : {output_path}"
    )
    return openings


if __name__ == "__main__":
    generate_personalized_openings()
