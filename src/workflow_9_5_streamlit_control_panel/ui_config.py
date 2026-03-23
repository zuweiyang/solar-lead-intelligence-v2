"""
Workflow 9.5 — Streamlit Campaign Control Panel: UI Configuration Model

Translates raw Streamlit form values into a validated CampaignConfig
for Workflow 9.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Allow imports from project root when loaded by Streamlit
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.workflow_9_campaign_runner.campaign_config import (
    CampaignConfig,
    PIPELINE_STEPS,
    DEFAULT_KEYWORDS,
    validate_config,
    _dedup_ordered,
)

# ---------------------------------------------------------------------------
# Form defaults shown in the Streamlit UI
# ---------------------------------------------------------------------------

UI_DEFAULTS = {
    "country":          "Canada",
    "region":           "",
    "city":             "",
    "base_city":        "",
    "metro_mode":       "base_only",
    "metro_sub_cities": [],
    "keyword_mode":     "default",
    "keywords":         "",          # comma-separated string in the UI
    "company_limit":    20,
    "crawl_limit":      20,
    "enrich_limit":     20,
    "send_mode":        "dry_run",
    "run_until":        "campaign_status",
    "dry_run":          True,
}

KEYWORD_MODE_OPTIONS = ["default", "custom"]
SEND_MODE_OPTIONS    = ["dry_run", "smtp", "gmail_api"]
RUN_UNTIL_OPTIONS    = PIPELINE_STEPS
METRO_MODE_OPTIONS   = ["base_only", "recommended", "custom"]

METRO_MODE_LABELS = {
    "base_only":    "Base city only",
    "recommended":  "Recommended metro expansion",
    "custom":       "Custom metro selection",
}


def build_campaign_config(form_values: dict) -> tuple[CampaignConfig | None, list[str]]:
    """
    Convert raw UI form values into a CampaignConfig.

    Accepts both legacy (city) and new (base_city + metro fields) form shapes.
    Returns (config, errors).  If errors is non-empty, config is None.
    """
    keyword_mode = form_values.get("keyword_mode", "default")
    raw_keywords = form_values.get("keywords", "").strip()

    # Parse comma-separated keyword string → list
    if keyword_mode == "custom" and raw_keywords:
        keywords = [k.strip() for k in raw_keywords.split(",") if k.strip()]
    else:
        keywords = []

    # ---- Location -----------------------------------------------------------
    # base_city takes priority; fall back to city for backward compatibility
    base_city = (form_values.get("base_city") or form_values.get("city") or "").strip()
    if not base_city:
        return None, ["City is required. Please enter a city name to target the campaign."]

    metro_mode       = form_values.get("metro_mode", "base_only")
    raw_sub_cities   = form_values.get("metro_sub_cities") or []
    if isinstance(raw_sub_cities, str):
        raw_sub_cities = [s.strip() for s in raw_sub_cities.split(",") if s.strip()]
    metro_sub_cities = [str(s).strip() for s in raw_sub_cities if str(s).strip()]

    # Compute final search_cities list
    if metro_mode == "base_only":
        search_cities = [base_city]
    else:
        # For both "recommended" and "custom", the sub-cities are already
        # resolved by the UI and passed in metro_sub_cities
        search_cities = _dedup_ordered([base_city] + metro_sub_cities)

    config = CampaignConfig(
        country          = (form_values.get("country") or "Canada").strip(),
        region           = (form_values.get("region")  or "").strip(),
        city             = base_city,       # keep for backward compat
        base_city        = base_city,
        metro_mode       = metro_mode,
        metro_sub_cities = metro_sub_cities,
        search_cities    = search_cities,
        keyword_mode     = keyword_mode,
        keywords         = keywords,
        company_limit    = int(form_values.get("company_limit") or 0),
        crawl_limit      = int(form_values.get("crawl_limit")   or 0),
        enrich_limit     = int(form_values.get("enrich_limit")  or 0),
        send_mode        = form_values.get("send_mode", "dry_run"),
        run_until        = form_values.get("run_until", "campaign_status"),
        resume           = False,
        dry_run          = bool(form_values.get("dry_run", True)),
    )

    errors = validate_config(config)
    if errors:
        return None, errors
    return config, []
