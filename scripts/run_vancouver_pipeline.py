"""
Vancouver, BC — Real End-to-End Pipeline Runner
Steps 1 → 2 → 3 → 4 → 5 → 5.5 → 6

Targets only Vancouver, British Columbia.
Caps crawling/analysis at COMPANY_LIMIT to keep runtime manageable.
"""

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import SEARCH_TASKS_FILE, DATA_DIR

# 0 = no limit (process all scraped companies)
COMPANY_LIMIT = 0

TARGET_LOCATION = "Vancouver, British Columbia"

KEYWORDS = [
    "solar installer",
    "solar contractor",
    "solar energy company",
    "battery storage installer",
    "solar EPC",
]


# ---------------------------------------------------------------------------
# Step 1 — Build Vancouver-only search tasks
# ---------------------------------------------------------------------------

def step1_build_tasks() -> list[dict]:
    print("\n" + "=" * 60)
    print("STEP 1 — Search Task Generation (Vancouver, BC)")
    print("=" * 60)

    tasks = [
        {
            "keyword":  kw,
            "location": TARGET_LOCATION,
            "industry": "solar",
            "query":    f"{kw} {TARGET_LOCATION}",
            "status":   "pending",
        }
        for kw in KEYWORDS
    ]
    SEARCH_TASKS_FILE.write_text(json.dumps(tasks, indent=2), encoding="utf-8")
    print(f"  → {len(tasks)} tasks written to {SEARCH_TASKS_FILE.name}")
    for t in tasks:
        print(f"     - {t['query']}")
    return tasks


# ---------------------------------------------------------------------------
# Step 2 — Google Maps Scraping
# ---------------------------------------------------------------------------

def step2_scrape() -> list[dict]:
    print("\n" + "=" * 60)
    print("STEP 2 — Google Maps Scraping")
    print("=" * 60)
    from src.workflow_2_data_scraping.google_maps_scraper import run as scrape
    from src.workflow_2_data_scraping.data_cleaner import run as clean
    scrape()
    leads = clean()
    print(f"  → {len(leads)} unique companies after dedup")
    return leads


# ---------------------------------------------------------------------------
# Step 3 — Website Crawling
# ---------------------------------------------------------------------------

def step3_crawl(limit: int) -> list[dict]:
    print("\n" + "=" * 60)
    print(f"STEP 3 — Website Crawling (limit: {limit} companies)")
    print("=" * 60)
    from src.workflow_3_web_crawler.website_crawler import run as crawl
    from src.workflow_3_web_crawler.content_extractor import run as extract
    results = crawl(limit=limit)
    texts   = extract(results)
    print(f"  → {len(texts)} company text records extracted")
    return texts


# ---------------------------------------------------------------------------
# Step 4 — AI Company Classification
# ---------------------------------------------------------------------------

def step4_classify() -> list[dict]:
    print("\n" + "=" * 60)
    print("STEP 4 — AI Company Analysis")
    print("=" * 60)
    from src.workflow_4_company_analysis.company_classifier import run as classify
    return classify()


# ---------------------------------------------------------------------------
# Step 5 — Lead Scoring
# ---------------------------------------------------------------------------

def step5_score() -> list[dict]:
    print("\n" + "=" * 60)
    print("STEP 5 — Lead Scoring")
    print("=" * 60)
    from src.workflow_5_lead_scoring.lead_scorer import run as score
    return score()


# ---------------------------------------------------------------------------
# Step 5.5 — Lead Enrichment
# ---------------------------------------------------------------------------

def step5_5_enrich(limit: int) -> list[dict]:
    print("\n" + "=" * 60)
    print(f"STEP 5.5 — Lead Enrichment (limit: {limit})")
    print("=" * 60)
    from src.workflow_5_5_lead_enrichment.enricher import run as enrich
    return enrich(limit=limit)


# ---------------------------------------------------------------------------
# Step 6 — Email Generation
# ---------------------------------------------------------------------------

def step6_generate(limit: int) -> list[dict]:
    print("\n" + "=" * 60)
    print(f"STEP 6 — Email Generation (limit: {limit})")
    print("=" * 60)
    from src.workflow_6_email_generation.email_generator import run as generate
    return generate(limit=limit)


# ---------------------------------------------------------------------------
# Results printer
# ---------------------------------------------------------------------------

def print_results(emails: list[dict]) -> None:
    print("\n" + "=" * 60)
    print(f"FINAL RESULTS — {len(emails)} email drafts generated")
    print("=" * 60)
    for i, e in enumerate(emails, 1):
        print(f"\n{'─' * 58}")
        print(f"[{i}] {e.get('company_name', 'Unknown')}")
        print(f"     Type    : {e.get('company_type', '-')} | Market: {e.get('market_focus', '-')}")
        print(f"     Score   : {e.get('lead_score', '-')}")
        print(f"     Contact : {e.get('kp_name', '-')} <{e.get('kp_email', '-')}>")
        print(f"     Source  : {e.get('enrichment_source', '-')} → email via [{e.get('generation_source', '-')}]")
        print(f"     Subject : {e.get('subject', '-')}")
        body = (e.get('email_body') or '').strip()
        if body:
            # Print first 300 chars
            preview = body[:300] + ("..." if len(body) > 300 else "")
            print(f"     Body    :\n       {preview.replace(chr(10), chr(10) + '       ')}")
    print(f"\n{'=' * 60}")
    print(f"Output files:")
    for f in [
        "search_tasks.json", "raw_leads.csv", "company_pages.json",
        "company_text.json", "company_analysis.json", "qualified_leads.csv",
        "enriched_leads.csv", "generated_emails.csv",
    ]:
        p = DATA_DIR / f
        if p.exists():
            size = p.stat().st_size
            print(f"  OK data/{f}  ({size:,} bytes)")
        else:
            print(f"  -- data/{f}  (not created)")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("  Solar Lead Intelligence — Vancouver, BC Pipeline")
    print(f"  Target   : {TARGET_LOCATION}")
    print(f"  Keywords : {len(KEYWORDS)}")
    print(f"  Limit    : top {COMPANY_LIMIT} companies")
    print("=" * 60)

    t0 = time.time()

    step1_build_tasks()
    step2_scrape()
    step3_crawl(limit=COMPANY_LIMIT)
    step4_classify()
    step5_score()
    step5_5_enrich(limit=COMPANY_LIMIT)
    emails = step6_generate(limit=COMPANY_LIMIT)

    elapsed = time.time() - t0
    print_results(emails)
    print(f"\nTotal runtime: {elapsed:.0f}s")


if __name__ == "__main__":
    main()
