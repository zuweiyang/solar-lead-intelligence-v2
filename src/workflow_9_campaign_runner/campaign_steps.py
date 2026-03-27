"""
Workflow 9 — Campaign Runner: Step Wrappers

Thin orchestration-safe wrappers around each workflow's entry point.
Business logic stays inside each workflow — this layer only:
  - translates CampaignConfig into workflow call parameters
  - handles send_mode injection
  - provides optional database sync checkpoints
  - fails clearly when a required input file is missing
"""
from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import tldextract

from config.settings import (
    SEARCH_TASKS_FILE,
    RAW_LEADS_FILE,
    COMPANY_TEXT_FILE,
    ENRICHED_LEADS_FILE,
    DATABASE_FILE,
    SEND_LOGS_FILE,
    DEDUP_SKIPPED_FILE,
    VERIFIED_ENRICHED_LEADS_FILE,
)
from src.workflow_9_campaign_runner.campaign_config import (
    CampaignConfig,
    get_effective_keywords,
    get_effective_location,
    get_effective_search_cities,
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_city_region(country: str, city: str) -> str | None:
    """
    Reverse lookup: find which region (province/state) a city belongs to
    using the location_data hierarchy.

    Checks base cities first, then sub-cities.
    Returns None for unknown cities (caller should fall back to config.region).
    """
    try:
        from src.workflow_9_5_streamlit_control_panel.location_data import (
            LOCATION_HIERARCHY,
        )
        country_data = LOCATION_HIERARCHY.get(country, {})
        for region, base_cities in country_data.items():
            if city in base_cities:
                return region
            for sub_list in base_cities.values():
                if city in sub_list:
                    return region
    except Exception:
        pass
    return None


_DEDUP_WINDOW_HOURS = 72
_RAW_LEADS_FIELDS   = [
    "company_name", "address", "website", "phone", "rating",
    "category", "place_id", "source_keyword", "source_location",
]


def _domain_key(url: str) -> str:
    ext = tldextract.extract(url or "")
    return f"{ext.domain}.{ext.suffix}".lower() if ext.domain else ""


def _dedup_raw_leads() -> None:
    """
    Pre-crawl deduplication: remove leads already processed in recent runs.

    Reads send_logs.csv, builds sets of place_ids and email domains seen within
    the last 72 hours, then filters raw_leads.csv to exclude matches.
    Skipped leads are written to dedup_skipped.csv with a reason code.

    Reason codes:
      duplicate_place_recently_processed  — place_id found in recent send_logs
      duplicate_domain_recently_processed — website domain found in recent send_logs
    """
    if not RAW_LEADS_FILE.exists():
        return

    # Build recent place_id / domain sets from send_logs
    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=_DEDUP_WINDOW_HOURS)
    recent_place_ids: set[str] = set()
    recent_domains:   set[str] = set()

    if SEND_LOGS_FILE.exists():
        with open(str(SEND_LOGS_FILE), newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                ts_raw = row.get("timestamp", "")
                try:
                    ts = datetime.fromisoformat(ts_raw.strip())
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                except (ValueError, AttributeError):
                    continue
                if ts < cutoff:
                    continue
                if row.get("place_id"):
                    recent_place_ids.add(row["place_id"])
                email = row.get("kp_email", "")
                if email and "@" in email:
                    dom = _domain_key(email.split("@", 1)[1])
                    if dom:
                        recent_domains.add(dom)

    if not recent_place_ids and not recent_domains:
        return  # nothing to dedup

    # Filter raw_leads.csv
    with open(str(RAW_LEADS_FILE), newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    kept:    list[dict] = []
    skipped: list[dict] = []

    for row in rows:
        place_id   = row.get("place_id", "")
        web_domain = _domain_key(row.get("website", ""))

        if place_id and place_id in recent_place_ids:
            skipped.append({**row, "skip_reason": "duplicate_place_recently_processed"})
        elif web_domain and web_domain in recent_domains:
            skipped.append({**row, "skip_reason": "duplicate_domain_recently_processed"})
        else:
            kept.append(row)

    if not skipped:
        return

    # Overwrite raw_leads.csv with kept records only
    all_fields = list(rows[0].keys()) if rows else _RAW_LEADS_FIELDS
    with open(str(RAW_LEADS_FILE), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(kept)

    # Write dedup_skipped.csv
    skip_fields = all_fields + ["skip_reason"]
    with open(str(DEDUP_SKIPPED_FILE), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=skip_fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(skipped)

    print(
        f"[campaign_steps] Dedup: {len(kept)} kept, {len(skipped)} skipped "
        f"(within {_DEDUP_WINDOW_HOURS}h window) → {DEDUP_SKIPPED_FILE}"
    )


def _require_file(path: Path, step: str) -> None:
    """Raise RuntimeError if a required input file is missing."""
    if not path.exists():
        raise RuntimeError(
            f"[{step}] Required input file missing: {path}\n"
            "Run earlier pipeline steps first."
        )


def _csv_has_rows(path: Path) -> bool:
    """Return True when a CSV exists and has at least one data row."""
    if not path.exists():
        return False
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)
            return next(reader, None) is not None
    except OSError:
        return False


def _db_sync() -> None:
    """Run CSV → database sync if the database layer is present."""
    if not DATABASE_FILE.exists():
        return
    try:
        from src.database.db_connection import get_db_connection
        from src.database.csv_sync import sync_all
        conn = get_db_connection()
        sync_all(conn)
        conn.close()
    except Exception as exc:
        print(f"[campaign_steps] DB sync skipped (non-fatal): {exc}")


# ---------------------------------------------------------------------------
# Step 1 — Search Task Generation
# ---------------------------------------------------------------------------

def run_step_1_search_tasks(config: CampaignConfig) -> list[dict]:
    """
    Generate search tasks.

    Metro expansion: if search_cities contains multiple cities, generates
    keyword × city tasks for EACH city, each tagged with base_city and
    search_city so downstream analytics can attribute results correctly.

    Falls back to the default multi-location task set when no city is set.
    """
    keywords = get_effective_keywords(config)
    search_cities = get_effective_search_cities(config)
    base_city = (config.base_city or config.city or "").strip()

    if search_cities:
        tasks = []
        for search_city in search_cities:
            # Look up the correct region for this specific city.
            # Sub-cities may belong to a different province than the base city
            # (e.g. Gatineau is in Quebec, not Ontario like Ottawa).
            # Falls back to config.region only when the city is not in location_data.
            city_region = (
                _get_city_region(config.country, search_city) or config.region
            )
            parts = [search_city]
            if city_region:
                parts.append(city_region)
            if config.country:
                parts.append(config.country)
            location_str = ", ".join(parts)

            for kw in keywords:
                tasks.append({
                    "keyword":     kw,
                    "location":    location_str,
                    "base_city":   base_city or search_city,
                    "search_city": search_city,
                    "industry":    "solar",
                    "query":       f"{kw} {location_str}",
                    "status":      "pending",
                })

        SEARCH_TASKS_FILE.parent.mkdir(parents=True, exist_ok=True)
        SEARCH_TASKS_FILE.write_text(
            json.dumps(tasks, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        city_summary = (
            f"{len(search_cities)} cities: {', '.join(search_cities)}"
            if len(search_cities) > 1 else search_cities[0]
        )
        print(
            f"[Workflow 1] Generated {len(tasks)} search tasks "
            f"across {city_summary} → {SEARCH_TASKS_FILE}"
        )
        return tasks
    else:
        # No city configured — use default keyword_generator × locations
        from src.workflow_1_lead_generation.search_task_builder import run
        return run()


# ---------------------------------------------------------------------------
# Step 2 — Google Maps Scraping
# ---------------------------------------------------------------------------

def run_step_2_scrape(config: CampaignConfig) -> list[dict]:
    _require_file(SEARCH_TASKS_FILE, "scrape")
    from src.workflow_2_data_scraping.google_maps_scraper import run
    result = run()
    if not result or not _csv_has_rows(RAW_LEADS_FILE):
        raise RuntimeError(
            "[scrape] Google Places produced no usable raw leads. "
            "Check GOOGLE_MAPS_API_KEY, Places API access, billing, "
            "API restrictions, or upstream scrape timeouts before continuing."
        )
    _db_sync()
    return result


# ---------------------------------------------------------------------------
# Step 3 — Website Crawling + Text Extraction
#
# Artifact flow:
#   raw_leads.csv
#     → website_crawler.run()   → company_pages.json   (raw HTML / page records)
#     → content_extractor.run() → company_text.json    (clean text, consumed by Step 4)
# Both sub-steps must run before Step 4 (analyze) is called.
# ---------------------------------------------------------------------------

def run_step_3_crawl(config: CampaignConfig) -> list[dict]:
    _require_file(RAW_LEADS_FILE, "crawl")
    _dedup_raw_leads()   # remove leads processed in recent runs before crawling
    from src.workflow_3_web_crawler.website_crawler import run as crawl
    from src.workflow_3_web_crawler.content_extractor import run as extract_text
    crawl(limit=config.crawl_limit)
    return extract_text()


# ---------------------------------------------------------------------------
# Step 4 — AI Company Analysis
# ---------------------------------------------------------------------------

def run_step_4_analyze(config: CampaignConfig) -> list[dict]:
    _require_file(COMPANY_TEXT_FILE, "analyze")
    from src.workflow_4_company_analysis.company_classifier import run
    return run(limit=config.company_limit)


# ---------------------------------------------------------------------------
# Step 4.5 — Buyer Filter / Value Chain Classification  (P1-1A)
# ---------------------------------------------------------------------------

def run_step_4_5_buyer_filter(config: CampaignConfig) -> dict:
    from config.settings import COMPANY_ANALYSIS_FILE
    _require_file(COMPANY_ANALYSIS_FILE, "buyer_filter")
    from config.run_paths import require_active_run_paths
    from src.workflow_4_5_buyer_filter.buyer_filter_pipeline import run
    paths = require_active_run_paths()
    paths.log_summary("buyer_filter")
    return run(limit=config.company_limit, paths=paths)


# ---------------------------------------------------------------------------
# Step 5 — Lead Scoring
# ---------------------------------------------------------------------------

def run_step_5_score(config: CampaignConfig) -> list[dict]:
    from config.settings import COMPANY_ANALYSIS_FILE
    _require_file(COMPANY_ANALYSIS_FILE, "score")
    from config.run_paths import require_active_run_paths
    from src.workflow_5_lead_scoring.lead_scorer import run
    paths = require_active_run_paths()
    paths.log_summary("score")
    return run(limit=config.company_limit, paths=paths)


# ---------------------------------------------------------------------------
# Step 5.5 — Lead Enrichment
# ---------------------------------------------------------------------------

def run_step_5_5_enrich(config: CampaignConfig) -> list[dict]:
    from config.run_paths import require_active_run_paths
    paths = require_active_run_paths()
    _require_file(paths.qualified_leads_file, "enrich")
    from src.workflow_5_5_lead_enrichment.enricher import run
    paths.log_summary("enrich")
    result = run(limit=config.enrich_limit, paths=paths)
    _db_sync()
    return result


# ---------------------------------------------------------------------------
# Step 5.6 — Contact Scoring + Priority Selection  (P1-2B)
# ---------------------------------------------------------------------------

def run_step_5_6_contact_scoring(config: CampaignConfig) -> list:
    """
    Score and rank the multi-contact candidates produced by Workflow 5.5 (P1-2A).

    Reads:  enriched_contacts.csv  (required)
            verified_enriched_leads.csv (optional — adds email quality signal)
    Writes: scored_contacts.csv

    This step is NON-FATAL: if enriched_contacts.csv is missing or empty, an
    empty scored_contacts.csv is written and the pipeline continues.

    Note: this step scores CONTACTS — it does not touch enriched_leads.csv or
    any downstream email/send artefacts.  Backward compatibility is preserved.
    """
    from config.run_paths import require_active_run_paths
    from src.workflow_5_6_contact_scoring.contact_scoring_pipeline import run
    paths = require_active_run_paths()
    paths.log_summary("contact_scoring")
    return run(paths=paths)


# ---------------------------------------------------------------------------
# Step 5.9 — Email Verification Gateway
# ---------------------------------------------------------------------------

def run_step_5_9_verify(config: CampaignConfig) -> dict:
    """
    Run email verification on all enriched leads.

    Reads ENRICHED_LEADS_FILE, verifies each kp_email via the configured provider,
    writes VERIFIED_ENRICHED_LEADS_FILE, and persists results to the DB.

    Downstream steps (5.8, 6.2, 6) will prefer the verified file when it exists.
    This step is non-fatal: if enriched_leads.csv is missing, a summary with
    error="no_input_file" is returned and downstream steps fall back gracefully.
    """
    from config.run_paths import require_active_run_paths
    paths = require_active_run_paths()
    # Non-fatal: verification_pipeline.run() handles a missing file gracefully
    # (_require_file would turn this designed-to-degrade step into a hard stopper)
    from src.workflow_5_9_email_verification.verification_pipeline import run
    paths.log_summary("verify")
    result = run(limit=getattr(config, "enrich_limit", 0), paths=paths)
    return result


# ---------------------------------------------------------------------------
# Step 5.8 — Company Signal Research
# ---------------------------------------------------------------------------

def run_step_5_8_signals(config: CampaignConfig) -> list[dict]:
    from config.run_paths import require_active_run_paths
    paths = require_active_run_paths()
    # Accept verified file as a valid prerequisite (Step 5.9 may have run)
    signals_input = (
        paths.verified_enriched_leads_file
        if paths.verified_enriched_leads_file.exists()
        else paths.enriched_leads_file
    )
    _require_file(signals_input, "signals")
    from src.workflow_5_8_signal_research.signal_collector import run as collect
    from src.workflow_5_8_signal_research.signal_summarizer import run as summarize
    paths.log_summary("signals")
    collect(limit=config.enrich_limit, paths=paths)
    return summarize(paths=paths)


# ---------------------------------------------------------------------------
# Step 6 — Queue Policy Enforcement  (P1-3A)
# ---------------------------------------------------------------------------

def run_step_6_queue_policy(config: CampaignConfig) -> dict:
    """
    Apply queue policy to the primary contacts selected by P1-2B.

    Reads:  scored_contacts.csv  (required — from Step 5.6)
            verified_enriched_leads.csv  (optional — from Step 5.9)
    Writes: queue_policy.csv

    Non-fatal: if scored_contacts.csv is missing or empty, an empty
    queue_policy.csv is written and the pipeline continues.
    """
    from config.run_paths import require_active_run_paths
    from src.workflow_6_queue_policy.queue_policy_pipeline import run
    paths = require_active_run_paths()
    paths.log_summary("queue_policy")
    return run(paths=paths)


# ---------------------------------------------------------------------------
# Step 6.2 — Signal-based Personalization
# ---------------------------------------------------------------------------

def run_step_6_2_personalization(config: CampaignConfig) -> list[dict]:
    _require_file(ENRICHED_LEADS_FILE, "personalization")
    from src.workflow_6_2_signal_personalization.signal_pipeline import (
        generate_personalized_openings,
    )
    return generate_personalized_openings()


# ---------------------------------------------------------------------------
# Step 6 — Email Generation
# ---------------------------------------------------------------------------

def run_step_6_generate(config: CampaignConfig) -> list[dict]:
    _require_file(ENRICHED_LEADS_FILE, "email_generation")
    from src.workflow_6_email_generation.email_generator import run
    result = run(limit=config.enrich_limit)
    _db_sync()
    return result


# ---------------------------------------------------------------------------
# Step 6.5 — Email Quality Scoring
# ---------------------------------------------------------------------------

def run_step_6_5_quality(config: CampaignConfig) -> list[dict]:
    from config.settings import GENERATED_EMAILS_FILE
    _require_file(GENERATED_EMAILS_FILE, "email_quality")
    from src.workflow_6_5_email_quality.email_quality_scorer import run
    return run()


# ---------------------------------------------------------------------------
# Step 6.7 — Email Repair Loop
# ---------------------------------------------------------------------------

def run_step_6_7_repair(config: CampaignConfig) -> dict:
    from config.settings import SCORED_EMAILS_FILE
    _require_file(SCORED_EMAILS_FILE, "email_repair")
    from src.workflow_6_7_email_repair.repair_pipeline import run
    return run()


# ---------------------------------------------------------------------------
# Step 7 — Email Sending
# ---------------------------------------------------------------------------

def run_step_7_send(config: CampaignConfig) -> dict:
    from config.settings import FINAL_SEND_QUEUE_FILE
    _require_file(FINAL_SEND_QUEUE_FILE, "send")
    # Resolve campaign_id from state so it can be stamped on every send_logs row
    from src.workflow_9_campaign_runner.campaign_state import load_campaign_state
    state = load_campaign_state()
    campaign_id = (state or {}).get("campaign_id", "")
    # Inject send_mode into environment so send_pipeline respects config
    original = os.environ.get("EMAIL_SEND_MODE")
    os.environ["EMAIL_SEND_MODE"] = config.send_mode
    try:
        from src.workflow_7_email_sending.send_pipeline import run
        result = run(campaign_id=campaign_id, send_mode=config.send_mode)
    finally:
        if original is None:
            os.environ.pop("EMAIL_SEND_MODE", None)
        else:
            os.environ["EMAIL_SEND_MODE"] = original
    _db_sync()
    return result


# ---------------------------------------------------------------------------
# Step 7.5 — Open / Click Tracking (aggregation pass)
#
# SCOPE: GLOBAL CRM VIEW — aggregates engagement events across ALL campaign runs.
# This step re-aggregates engagement_logs.csv (append-only) in full.
# Results are not scoped to the current campaign_id.
# ---------------------------------------------------------------------------

def run_step_7_5_tracking(config: CampaignConfig) -> list[dict]:
    print(
        "[Workflow 7.5] SCOPE: global CRM — re-aggregates engagement_logs.csv across ALL "
        "campaign runs.  This is intentional: opens/clicks from any campaign contribute to "
        "the contact-level engagement state used by follow-up scheduling."
    )
    from src.workflow_7_5_engagement_tracking.engagement_aggregator import run
    return run()


# ---------------------------------------------------------------------------
# Step 8 — Follow-up Automation
#
# SCOPE: CAMPAIGN-SCOPED by default when running from the campaign runner.
# Only contacts whose initial send_logs row is stamped with the current
# campaign_id are evaluated for follow-up.
#
# Why campaign scope here:
#   Follow-up artifacts (followup_candidates.csv, followup_queue.csv,
#   followup_blocked.csv) are written to the current run folder and should
#   reflect THIS campaign only.  The global followup_logs (append-only) still
#   tracks the full cross-campaign history so duplicate follow-ups are
#   prevented regardless of scope.
#
# To run a global pass (e.g., to pick up contacts from prior campaigns that
# are now due for follow-up), run followup_pipeline.run() directly with
# campaign_id="" from a standalone script.
# ---------------------------------------------------------------------------

def run_step_8_followup(config: CampaignConfig) -> dict:
    from src.workflow_9_campaign_runner.campaign_state import load_campaign_state
    state = load_campaign_state()
    campaign_id = (state or {}).get("campaign_id", "")
    from src.workflow_8_followup.followup_pipeline import run
    return run(campaign_id=campaign_id)


# ---------------------------------------------------------------------------
# Step 8.5 — Campaign Status Aggregator
#
# SCOPE: CAMPAIGN-SCOPED — filtered to the current campaign_id.
# Only contacts whose send_logs row was stamped with the current campaign_id
# appear in campaign_status.csv. This makes the report answer:
# "What happened in THIS campaign?"
#
# Note: legacy send_logs rows from before campaign_id was added will NOT
# appear in campaign-scoped status (they have an empty campaign_id).
# ---------------------------------------------------------------------------

def run_step_8_5_campaign_status(config: CampaignConfig) -> dict:
    from src.workflow_9_campaign_runner.campaign_state import load_campaign_state
    state = load_campaign_state()
    campaign_id = (state or {}).get("campaign_id", "")
    from src.workflow_8_5_campaign_status.status_pipeline import run
    result = run(campaign_id=campaign_id)
    _db_sync()
    return result
