# Workflow 6.2: Signal-based Personalization — Signal Loader
# Loads company signals from company_signals.json.
# Falls back to extracting recent_signals from research_signals.json
# if company_signals.json does not exist.

import json
import re
from pathlib import Path

from config.settings import COMPANY_SIGNALS_FILE, RESEARCH_SIGNALS_FILE

# Common legal-entity suffixes to strip before comparing names
_SUFFIX_RE = re.compile(
    r"\b(inc\.?|ltd\.?|corp\.?|llc\.?|l\.l\.c\.?|co\.?|limited|corporation|incorporated|plc\.?)\b",
    re.IGNORECASE,
)


def _normalize_name(name: str) -> str:
    """
    Normalize a company name for fuzzy matching across sources.
    Rules:
    - lowercase + strip
    - replace & → and
    - strip common legal suffixes (inc, ltd, corp, llc, co, limited, plc)
    - remove all non-alphanumeric characters
    - collapse whitespace
    """
    n = (name or "").strip().lower()
    n = n.replace("&", "and")
    n = _SUFFIX_RE.sub("", n)
    n = re.sub(r"[^a-z0-9 ]", " ", n)
    n = " ".join(n.split())
    return n


def _load_company_signals_file(path: Path) -> list[dict]:
    """
    Load company_signals.json.
    Expected format:
      [{"company_name": "...", "signals": ["signal1", "signal2", ...]}]
    """
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)

    results = []
    for entry in raw:
        name    = (entry.get("company_name") or "").strip()
        signals = entry.get("signals") or []
        if not name:
            continue
        results.append({
            "company_name":       name,
            "company_name_lower": _normalize_name(name),
            "signals":            [s for s in signals if isinstance(s, str) and s.strip()],
        })
    return results


def _load_from_research_signals(path: Path) -> list[dict]:
    """
    Fallback: derive signals from research_signals.json (Workflow 5.8 output).
    research_signals uses field name `recent_signals` (list of strings).
    """
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)

    results = []
    for entry in raw:
        name    = (entry.get("company_name") or "").strip()
        signals = entry.get("recent_signals") or []
        if not name or not signals:
            continue
        results.append({
            "company_name":       name,
            "company_name_lower": _normalize_name(name),
            "signals":            [s for s in signals if isinstance(s, str) and s.strip()],
        })
    return results


def load_company_signals(
    signals_path: Path = COMPANY_SIGNALS_FILE,
    fallback_path: Path = RESEARCH_SIGNALS_FILE,
) -> list[dict]:
    """
    Load company signals.

    Priority:
      1. company_signals.json   (Workflow 6.2 native format)
      2. research_signals.json  (Workflow 5.8 output, used as fallback)

    Returns list of dicts:
      {company_name, company_name_lower, signals: [str, ...]}
    """
    if signals_path.exists():
        records = _load_company_signals_file(signals_path)
        source = signals_path.name
    elif fallback_path.exists():
        records = _load_from_research_signals(fallback_path)
        source = f"{fallback_path.name} (fallback)"
    else:
        print("[Workflow 6.2] No signal file found — returning empty list.")
        return []

    with_signals = sum(1 for r in records if r["signals"])
    print(
        f"[Workflow 6.2] Loaded {len(records)} companies with signals "
        f"from {source} ({with_signals} with ≥1 signal)"
    )
    return records


def signals_by_name(records: list[dict]) -> dict[str, list[str]]:
    """Return {normalized_company_name → [signal strings]} lookup map."""
    return {r["company_name_lower"]: r["signals"] for r in records if r["signals"]}
