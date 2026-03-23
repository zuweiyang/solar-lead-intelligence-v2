"""
Workflow 9 — Campaign Runner: Configuration Model

Defines all parameters that control a campaign run.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

# Default keyword set mirrors Workflow 1 / keyword_generator.py
DEFAULT_KEYWORDS: list[str] = [
    "solar installer",
    "solar contractor",
    "commercial solar installer",
    "solar developer",
    "solar energy company",
    "solar EPC",
    "energy storage integrator",
    "BESS integrator",
]

# Ordered list of valid step names — also defines execution order
PIPELINE_STEPS: list[str] = [
    "search_tasks",
    "scrape",
    "crawl",
    "analyze",
    "buyer_filter",
    "score",
    "enrich",
    "contact_scoring",
    "verify",
    "signals",
    "queue_policy",
    "personalization",
    "email_generation",
    "email_quality",
    "email_repair",
    "send",
    "tracking",
    "followup",
    "campaign_status",
]

# Valid metro mode values
METRO_MODES: list[str] = ["base_only", "recommended", "custom"]


@dataclass
class CampaignConfig:
    # Geographic targeting (legacy — kept for backward compatibility)
    country: str = "Canada"
    region: str = ""          # e.g. "British Columbia"
    city: str = ""            # primary city (legacy alias for base_city)

    # Metro expansion (new in Smart Location feature)
    base_city: str = ""       # explicit primary/base city (preferred over `city`)
    metro_mode: str = "base_only"  # "base_only" | "recommended" | "custom"
    metro_sub_cities: List[str] = field(default_factory=list)
    # Final effective list of cities to search (keyword × city = one search task each).
    # Pre-computed by the UI layer from metro_mode + metro_sub_cities.
    # Falls back to get_effective_search_cities() at runtime if empty.
    search_cities: List[str] = field(default_factory=list)

    # Keywords
    keyword_mode: str = "default"   # "default" | "custom"
    keywords: List[str] = field(default_factory=list)

    # Record limits (0 = no limit)
    company_limit: int = 0
    crawl_limit: int = 0
    enrich_limit: int = 0

    # Send settings
    send_mode: str = "dry_run"        # "dry_run" | "smtp" | "gmail_api"

    # Execution control
    run_until: str = "campaign_status"   # step name to stop after
    resume: bool = False
    dry_run: bool = True


def _dedup_ordered(items: list[str]) -> list[str]:
    """Remove duplicates while preserving order."""
    seen: set[str] = set()
    out = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def get_effective_keywords(config: CampaignConfig) -> list[str]:
    """Return the keyword list to use, based on keyword_mode."""
    if config.keyword_mode == "custom" and config.keywords:
        return list(config.keywords)
    return DEFAULT_KEYWORDS


def get_effective_location(config: CampaignConfig) -> str:
    """
    Return a single location string for search tasks.
    Uses base_city if set, otherwise falls back to city, then region, then country.
    """
    base = (config.base_city or config.city or "").strip()
    if base:
        parts = [base]
        if config.region:
            parts.append(config.region)
        if config.country:
            parts.append(config.country)
        return ", ".join(parts)
    if config.region:
        return f"{config.region}, {config.country}" if config.country else config.region
    return config.country or "Canada"


def get_effective_search_cities(config: CampaignConfig) -> list[str]:
    """
    Return the final ordered list of cities to generate search tasks for.

    Priority:
    1. config.search_cities if non-empty (pre-computed by UI)
    2. Computed from metro_mode at runtime:
       - base_only  : [base_city]
       - recommended: [base_city] + recommended sub-cities from location_data
       - custom     : [base_city] + config.metro_sub_cities

    If base_city is empty but city is set, city is used as base_city.
    Returns [] only when no city is available at all.
    """
    # Use pre-computed list if available
    if config.search_cities:
        return _dedup_ordered(config.search_cities)

    base = (config.base_city or config.city or "").strip()
    if not base:
        return []

    if config.metro_mode == "recommended":
        try:
            from src.workflow_9_5_streamlit_control_panel.location_data import get_sub_cities
            subs = get_sub_cities(config.country, config.region, base)
        except Exception:
            subs = []
        return _dedup_ordered([base] + subs)

    if config.metro_mode == "custom":
        return _dedup_ordered([base] + list(config.metro_sub_cities))

    # base_only (default)
    return [base]


def validate_config(config: CampaignConfig) -> list[str]:
    """
    Return a list of validation errors (empty list = valid).
    """
    errors: list[str] = []
    if config.run_until not in PIPELINE_STEPS:
        errors.append(
            f"run_until '{config.run_until}' is not a valid step. "
            f"Valid steps: {', '.join(PIPELINE_STEPS)}"
        )
    if config.keyword_mode not in ("default", "custom"):
        errors.append(f"keyword_mode must be 'default' or 'custom', got '{config.keyword_mode}'")
    if config.keyword_mode == "custom" and not config.keywords:
        errors.append("keyword_mode is 'custom' but no keywords were provided")
    if config.send_mode not in ("dry_run", "smtp", "gmail_api"):
        errors.append(f"send_mode must be 'dry_run', 'smtp', or 'gmail_api', got '{config.send_mode}'")
    if config.metro_mode not in METRO_MODES:
        errors.append(
            f"metro_mode '{config.metro_mode}' is invalid. "
            f"Valid options: {', '.join(METRO_MODES)}"
        )
    return errors
