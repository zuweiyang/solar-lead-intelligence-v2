# Workflow 4.5 — Buyer Filter: Pipeline Orchestrator
#
# Reads company_analysis.json (from Workflow 4) and company_text.json
# (from Workflow 3) and produces buyer_filter.json enriched with structured
# buyer-filter fields.
#
# This new artifact sits between Workflow 4 output and Workflow 5 input.
# Workflow 5 currently reads company_analysis.json unchanged (backward compat).
# Ticket P1-1B will update Workflow 5 to read buyer_filter.json instead.

import json
from pathlib import Path

from config.settings import (
    COMPANY_ANALYSIS_FILE,
    COMPANY_TEXT_FILE,
    BUYER_FILTER_FILE,
)
from config.run_paths import RunPaths
from src.workflow_4_5_buyer_filter.buyer_filter_models import BuyerFilterResult
from src.workflow_4_5_buyer_filter.value_chain_classifier import classify_value_chain
from src.workflow_4_5_buyer_filter.buyer_filter_rules import (
    extract_signals,
    compute_all_scores,
)


# ---------------------------------------------------------------------------
# Data loading helpers
# ---------------------------------------------------------------------------

def _load_analyses(limit: int = 0, paths: RunPaths | None = None) -> list[dict]:
    """Load company_analysis.json produced by Workflow 4."""
    path = paths.company_analysis_file if paths else Path(str(COMPANY_ANALYSIS_FILE))
    if not path.exists():
        raise FileNotFoundError(
            f"company_analysis.json not found at {path}. "
            "Run Workflow 4 (analyze step) before buyer_filter."
        )
    with open(path, encoding="utf-8") as f:
        records = json.load(f)
    return records[:limit] if limit else records


def _load_company_texts() -> dict[str, str]:
    """
    Load company_text.json produced by Workflow 3.
    Returns {place_id: text, website: text} for O(1) lookup.
    Missing file is non-fatal — text signals will be absent (scores default to 0).
    """
    path = Path(str(COMPANY_TEXT_FILE))
    if not path.exists():
        print("[Workflow 4.5]   company_text.json not found — text signals unavailable")
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            records = json.load(f)
        index: dict[str, str] = {}
        for r in records:
            text = r.get("company_text", "") or ""
            pid  = r.get("place_id", "")
            site = r.get("website", "")
            if pid:
                index[pid]  = text
            if site:
                index[site] = text
        return index
    except Exception as exc:
        print(f"[Workflow 4.5]   Could not load company_text.json: {exc}")
        return {}


def _get_text(record: dict, text_index: dict[str, str]) -> str:
    """Look up company text for one analysis record."""
    pid  = record.get("place_id", "")
    site = record.get("website", "")
    return text_index.get(pid) or text_index.get(site) or ""


# ---------------------------------------------------------------------------
# Core: apply buyer filter to one record
# ---------------------------------------------------------------------------

def apply_buyer_filter(record: dict, company_text: str) -> BuyerFilterResult:
    """
    Run the full buyer filter pipeline for one company record.
    Returns a populated BuyerFilterResult. Never raises — errors produce
    a conservative (low-score, unclear-role) result.
    """
    result = BuyerFilterResult()

    company_type = record.get("company_type", "") or ""
    market_focus = record.get("market_focus", "") or ""

    # Step 1: extract text-based signal strengths
    extract_signals(company_text, result)

    # Step 2: classify value chain role + set negative flags
    classify_value_chain(
        company_type=company_type,
        market_focus=market_focus,
        company_text=company_text,
        result=result,
    )

    # Step 3: compute all scores (depends on role + flags from steps 1+2)
    compute_all_scores(market_focus=market_focus, result=result)

    return result


# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------

def _build_summary(enriched: list[dict]) -> dict:
    """
    Produce summary counts from the enriched result list.
    Returns a dict suitable for console printing and downstream reporting.
    """
    from src.workflow_4_5_buyer_filter.buyer_filter_models import (
        ROLE_INSTALLER, ROLE_EPC_OR_CONTRACTOR, ROLE_DEVELOPER,
        ROLE_DISTRIBUTOR, ROLE_MANUFACTURER, ROLE_CONSULTANT,
        ROLE_MEDIA_OR_DIRECTORY, ROLE_ASSOCIATION, ROLE_UNCLEAR,
    )

    total = len(enriched)
    by_role: dict[str, int] = {}
    likely_buyer = 0
    residential_negative = 0
    competitor_mfr = 0
    consultant_media = 0
    unclear = 0

    for r in enriched:
        role = r.get("value_chain_role", ROLE_UNCLEAR)
        by_role[role] = by_role.get(role, 0) + 1

        bls = int(r.get("buyer_likelihood_score", 0))
        if bls >= 6:
            likely_buyer += 1
        if r.get("negative_residential_flag"):
            residential_negative += 1
        if r.get("competitor_flag") or r.get("manufacturer_flag"):
            competitor_mfr += 1
        if r.get("consultant_flag") or r.get("media_or_directory_flag"):
            consultant_media += 1
        if role == ROLE_UNCLEAR:
            unclear += 1

    return {
        "total":               total,
        "likely_buyer":        likely_buyer,
        "residential_negative": residential_negative,
        "competitor_manufacturer": competitor_mfr,
        "consultant_media":    consultant_media,
        "unclear":             unclear,
        "by_role":             by_role,
    }


# ---------------------------------------------------------------------------
# Public run()
# ---------------------------------------------------------------------------

def run(limit: int = 0, paths: RunPaths | None = None) -> dict:
    """
    Run the buyer filter pipeline.

    Steps:
      1. Load company_analysis.json (Workflow 4 output)
      2. Load company_text.json (Workflow 3 output) for text signal extraction
      3. For each company: extract signals → classify role → score
      4. Write buyer_filter.json (company_analysis fields + buyer_filter fields)
      5. Print summary counts

    Args:
        limit: cap on records to process (0 = all)
        paths: explicit RunPaths from campaign_runner; if None, fetched from
               the active global (standalone / backward-compat invocation).

    Returns:
      Summary dict with category counts.

    One bad record never crashes the whole batch.
    """
    if paths is None:
        from config.run_paths import require_active_run_paths
        paths = require_active_run_paths()

    print("[Workflow 4.5] Starting buyer filter pipeline")

    # Load inputs
    try:
        analyses = _load_analyses(limit=limit, paths=paths)
    except FileNotFoundError as exc:
        print(f"[Workflow 4.5] ERROR: {exc}")
        return {"total": 0, "errors": 1}

    text_index = _load_company_texts()

    print(
        f"[Workflow 4.5] Processing {len(analyses)} companies "
        f"({'with' if text_index else 'WITHOUT'} text signals)"
    )

    enriched: list[dict] = []
    errors = 0

    for i, record in enumerate(analyses, 1):
        name = record.get("company_name") or record.get("website") or f"record {i}"
        try:
            company_text = _get_text(record, text_index)
            bf = apply_buyer_filter(record, company_text)

            # Merge: analysis record + buyer filter fields
            enriched_record = {**record, **bf.to_dict()}
            enriched.append(enriched_record)

            print(
                f"[Workflow 4.5] ({i}/{len(analyses)}) {name}"
                f" → role={bf.value_chain_role}"
                f" bls={bf.buyer_likelihood_score}/10"
                f" prs={bf.procurement_relevance_score}/10"
                + (" [RESIDENTIAL]" if bf.negative_residential_flag else "")
                + (" [COMPETITOR]"  if bf.competitor_flag else "")
                + (" [CONSULTANT]"  if bf.consultant_flag else "")
                + (" [MEDIA]"       if bf.media_or_directory_flag else "")
            )

        except Exception as exc:
            print(f"[Workflow 4.5]   ERROR on {name}: {exc}")
            # Preserve original record with minimal safe defaults
            enriched.append({**record, **BuyerFilterResult().to_dict()})
            errors += 1

    # Persist buyer_filter.json — use explicit path from RunPaths (already set above)
    out_path = paths.buyer_filter_file
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(enriched, f, indent=2, ensure_ascii=False)
    print(f"[Workflow 4.5] Saved {len(enriched)} records → {out_path}")

    summary = _build_summary(enriched)
    summary["errors"] = errors

    _print_summary(summary)
    return summary


def _print_summary(summary: dict) -> None:
    total = summary.get("total", 0)
    print(
        f"\n[Workflow 4.5] Buyer Filter Summary:\n"
        f"  Total processed         : {total}\n"
        f"  Likely buyers (bls≥6)   : {summary.get('likely_buyer', 0)}\n"
        f"  Residential-negative    : {summary.get('residential_negative', 0)}\n"
        f"  Competitor/manufacturer : {summary.get('competitor_manufacturer', 0)}\n"
        f"  Consultant/media        : {summary.get('consultant_media', 0)}\n"
        f"  Unclear role            : {summary.get('unclear', 0)}\n"
        f"  Errors                  : {summary.get('errors', 0)}\n"
        f"\n  By value-chain role:"
    )
    for role, count in sorted(summary.get("by_role", {}).items()):
        print(f"    {role:<24}: {count}")


# ---------------------------------------------------------------------------
# Convenience loader for downstream workflows (P1-1B / Workflow 5)
# ---------------------------------------------------------------------------

def load_buyer_filter_results(limit: int = 0) -> list[dict]:
    """
    Load buyer_filter.json for use by downstream steps (e.g. Workflow 5 P1-1B).
    Returns [] if file is missing (backward-compatible fallback).
    """
    path = Path(str(BUYER_FILTER_FILE))
    if not path.exists():
        return []
    try:
        with open(path, encoding="utf-8") as f:
            records = json.load(f)
        return records[:limit] if limit else records
    except Exception as exc:
        print(f"[Workflow 4.5] Could not load buyer_filter.json: {exc}")
        return []


if __name__ == "__main__":
    run()
