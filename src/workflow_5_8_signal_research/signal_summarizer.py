# Workflow 5.8: Company Signal Research — Signal Summarizer
# Converts raw signal text into structured outreach outputs using rule-based classification.
# Signals with a detectable date older than MAX_SIGNAL_AGE_DAYS are discarded.

import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path

from config.run_paths import RunPaths

MAX_SIGNAL_AGE_DAYS = 90

# ---------------------------------------------------------------------------
# Keyword categories
# ---------------------------------------------------------------------------

KEYWORD_CATEGORIES: dict[str, list[str]] = {
    "battery":     ["battery", "storage", "bess", "backup power"],
    "commercial":  ["commercial", "industrial", "rooftop", "business"],
    "utility":     ["solar farm", "utility scale", "megawatt", " mw "],
    "expansion":   ["hiring", "careers", "join our team", "project manager", "estimator", "electrician"],
    "residential": ["homeowner", "home solar", "residential"],
}

_SUMMARY_MAP: dict[str, str] = {
    "battery":     "Company appears active in battery storage and commercial solar work.",
    "utility":     "Company appears active in utility-scale solar project execution.",
    "commercial":  "Company signals point to commercial and industrial solar installation.",
    "expansion":   "Company appears to be expanding operations and hiring.",
    "residential": "Company signals are mostly residential solar installation activity.",
}

_ANGLE_MAP: dict[str, str] = {
    "battery":     "Mention battery storage support",
    "utility":     "Mention support for larger-scale project execution",
    "commercial":  "Mention commercial installation scalability",
    "expansion":   "Mention support for growing installation teams",
    "residential": "Mention support for residential solar operations",
}

_PRIORITY = ["battery", "utility", "commercial", "expansion", "residential"]


# ---------------------------------------------------------------------------
# Signal freshness filtering
# ---------------------------------------------------------------------------

_MONTHS = (
    "January|February|March|April|May|June|July|August|"
    "September|October|November|December|"
    "Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec"
)

# (regex, list-of-strptime-formats) — tried in order; first match wins
_DATE_SPECS: list[tuple[str, list[str]]] = [
    # ISO: 2026-03-05
    (r"\b(\d{4}-\d{2}-\d{2})\b",
     ["%Y-%m-%d"]),
    # Full date: March 5, 2026 / Mar 5 2026
    (rf"\b((?:{_MONTHS})\s+\d{{1,2}},?\s+\d{{4}})\b",
     ["%B %d, %Y", "%b %d, %Y", "%B %d %Y", "%b %d %Y"]),
    # Month + year only: March 2026 / Mar 2026
    (rf"\b((?:{_MONTHS})\s+\d{{4}})\b",
     ["%B %Y", "%b %Y"]),
]

# (regex, days_multiplier) — N captured as group(1); use 1 when no capture group
_RELATIVE_SPECS: list[tuple[str, int]] = [
    (r"\btoday\b|\bjust now\b",        0),
    (r"\byesterday\b",                 1),
    (r"\b(\d+)\s+days?\s+ago\b",       1),
    (r"\b(?:a\s+)?week\s+ago\b",       7),
    (r"\b(\d+)\s+weeks?\s+ago\b",      7),
    (r"\b(?:a\s+)?month\s+ago\b",     30),
    (r"\b(\d+)\s+months?\s+ago\b",    30),
]


def _parse_date(text: str, today: date | None = None) -> date | None:
    """Return first parseable date (absolute or relative) found in text, or None."""
    if today is None:
        today = date.today()

    # Absolute patterns
    for pattern, formats in _DATE_SPECS:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        candidate = match.group(1)
        for fmt in formats:
            try:
                return datetime.strptime(candidate, fmt).date()
            except ValueError:
                continue

    # Relative patterns
    for pattern, multiplier in _RELATIVE_SPECS:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        try:
            n = int(match.group(1)) if match.lastindex else 1
        except (IndexError, ValueError):
            n = 1
        return today - timedelta(days=n * multiplier)

    return None


def _is_fresh(text: str, today: date) -> bool:
    """Undated signals are kept; dated signals must be within MAX_SIGNAL_AGE_DAYS."""
    found = _parse_date(text, today)
    return found is None or (today - found).days <= MAX_SIGNAL_AGE_DAYS


def filter_stale_signals(text_lines: list[str]) -> tuple[list[str], int, int]:
    """
    Remove lines whose detectable date exceeds MAX_SIGNAL_AGE_DAYS.
    Returns: (fresh_lines, removed_count, kept_count)
    """
    today = date.today()
    fresh = [line for line in text_lines if _is_fresh(line, today)]
    removed = len(text_lines) - len(fresh)
    return fresh, removed, len(fresh)


# ---------------------------------------------------------------------------
# Summarization helpers
# ---------------------------------------------------------------------------

def _all_text_lines(raw: dict) -> list[str]:
    lines: list[str] = []
    for page in raw.get("signal_sources", {}).get("website", []):
        lines.extend(page.get("headlines", []))
    for social in raw.get("signal_sources", {}).get("social", []):
        lines.extend(social.get("snippets", []))
    return lines


def detect_categories(text_lines: list[str]) -> set[str]:
    combined = " ".join(text_lines).lower()
    return {cat for cat, keywords in KEYWORD_CATEGORIES.items()
            if any(kw in combined for kw in keywords)}


def _top_signals(text_lines: list[str], categories: set[str]) -> list[str]:
    """Return up to 5 signal lines scored by keyword relevance."""
    keywords = [kw for cat in categories for kw in KEYWORD_CATEGORIES[cat]]
    seen: set[str] = set()
    scored: list[tuple[int, str]] = []
    for line in text_lines:
        if line in seen or len(line) < 10:
            continue
        seen.add(line)
        score = sum(1 for kw in keywords if kw in line.lower())
        scored.append((score, line))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [line for _, line in scored[:5]]


def build_summary(categories: set[str]) -> str:
    for priority in _PRIORITY:
        if priority in categories:
            return _SUMMARY_MAP[priority]
    return "No recent activity signals detected."


def build_email_angle(categories: set[str]) -> str:
    for priority in _PRIORITY:
        if priority in categories:
            return _ANGLE_MAP[priority]
    return "General solar outreach"


def _summarize(raw: dict, fresh_lines: list[str]) -> dict:
    categories = detect_categories(fresh_lines)
    return {
        "company_name":     raw.get("company_name", ""),
        "website":          raw.get("website", ""),
        "place_id":         raw.get("place_id", ""),
        "recent_signals":   _top_signals(fresh_lines, categories),
        "research_summary": build_summary(categories),
        "email_angle":      build_email_angle(categories),
    }


def summarize_company(raw: dict) -> dict:
    """Public single-company API. Filters stale signals before summarizing."""
    fresh_lines, _, _ = filter_stale_signals(_all_text_lines(raw))
    return _summarize(raw, fresh_lines)


# ---------------------------------------------------------------------------
# Public run()
# ---------------------------------------------------------------------------

def run(paths: RunPaths | None = None) -> list[dict]:
    if paths is None:
        from config.run_paths import require_active_run_paths
        paths = require_active_run_paths()
    raw_path = paths.research_signal_raw_file
    out_path = paths.research_signals_file
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not raw_path.exists():
        print("[Workflow 5.8] No raw signals file found — writing empty signals output.")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump([], f)
        return []
    with open(raw_path, encoding="utf-8") as f:
        raw_records: list[dict] = json.load(f)

    total_removed = 0
    total_kept    = 0
    results: list[dict] = []

    for raw in raw_records:
        fresh_lines, removed, kept = filter_stale_signals(_all_text_lines(raw))
        total_removed += removed
        total_kept    += kept
        results.append(_summarize(raw, fresh_lines))

    print(f"[Workflow 5.8] Signals removed due to age : {total_removed}")
    print(f"[Workflow 5.8] Signals kept               : {total_kept}")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"[Workflow 5.8] Saved {len(results)} summarized records → {out_path}")
    return results


if __name__ == "__main__":
    run()
