# Workflow 6.2: Signal-based Personalization — Signal Ranker
# Selects the single best signal for use in a cold email opening line.
#
# Ranking priority (1 = best):
#   1. installation / project / rooftop / solar farm — concrete project work
#   2. storage / battery / powerwall / BESS         — product offering
#   3. hiring / expansion / growing                 — company growth
#   4. anything else (generic updates)

from __future__ import annotations

# Keyword tiers ordered by priority (highest first)
_TIERS: list[tuple[str, list[str]]] = [
    ("project", [
        "install", "rooftop", "solar farm", "solar project",
        "completed", "commission", "deploy", "kw", "mw", "megawatt",
    ]),
    ("storage", [
        "battery", "powerwall", "storage", "bess", "backup power",
        "energy storage", "tesla", "enphase",
    ]),
    ("expansion", [
        "hiring", "hire", "expanding", "expansion", "join our team",
        "growing", "new office", "new location", "now serving",
    ]),
]


def _tier(signal: str) -> int:
    """Return tier index (lower = higher priority); 99 if no tier matches."""
    lower = signal.lower()
    for i, (_, keywords) in enumerate(_TIERS):
        if any(kw in lower for kw in keywords):
            return i
    return 99


def _keyword_hits(signal: str) -> int:
    """Count total keyword matches across all tiers (used as tiebreaker)."""
    lower = signal.lower()
    return sum(1 for _, keywords in _TIERS for kw in keywords if kw in lower)


def rank_signals(signals: list[str]) -> str | None:
    """
    Pick the single best signal for cold email personalization.

    Returns the best signal string, or None if the list is empty.
    """
    if not signals:
        return None

    # Filter out very short or uninformative signals
    candidates = [s for s in signals if len(s.split()) >= 3]
    if not candidates:
        return signals[0]   # fallback to first if all are short

    # Sort: tier ASC (best tier first), then keyword hits DESC (richest signal first)
    ranked = sorted(candidates, key=lambda s: (_tier(s), -_keyword_hits(s)))
    return ranked[0]
