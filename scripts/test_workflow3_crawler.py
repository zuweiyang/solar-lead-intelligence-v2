"""
Smoke test for Workflow 3 — Website Crawling & Content Extraction.

Run from the project root:
    py scripts/test_workflow3_crawler.py
"""

import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.workflow_3_web_crawler.website_crawler import run as crawl
from src.workflow_3_web_crawler.content_extractor import run as extract
from config.settings import COMPANY_PAGES_FILE, COMPANY_TEXT_FILE

TEST_LIMIT = 50   # cap during smoke test to avoid long runtimes


def main():
    print("=" * 60)
    print("Workflow 3 Smoke Test — Website Crawler & Content Extractor")
    print("=" * 60)

    # Step 1 — Crawl websites
    print(f"\n[1] Crawling up to {TEST_LIMIT} lead websites...")
    page_records = crawl(limit=TEST_LIMIT)

    total_pages = sum(len(r["pages"]) for r in page_records)
    print(f"\n    Websites crawled  : {len(page_records)}")
    print(f"    Total pages saved : {total_pages}")
    print(f"    Output file       : {COMPANY_PAGES_FILE}")

    if not page_records:
        print("\n    WARNING: No pages crawled. Check raw_leads.csv and network access.")
        sys.exit(1)

    # Step 2 — Extract text
    print(f"\n[2] Extracting company text from {len(page_records)} crawled sites...")
    text_records = extract(page_records)

    print(f"    Company text generated : {len(text_records)}")
    print(f"    Output file            : {COMPANY_TEXT_FILE}")

    # Step 3 — Page breakdown per site
    print("\n[3] Pages per site breakdown:")
    page_counts: dict[int, int] = {}
    for r in page_records:
        n = len(r["pages"])
        page_counts[n] = page_counts.get(n, 0) + 1
    for n in sorted(page_counts):
        print(f"    {n} page(s): {page_counts[n]} site(s)")

    # Step 4 — Show first company_text example
    print("\n[4] First company_text example:")
    first = text_records[0] if text_records else None
    if first:
        print(f"\n    place_id  : {first['place_id']}")
        print(f"    website   : {first['website']}")
        print(f"    pages_used: {first['pages_used']}")
        preview = first["company_text"][:800].replace("\n", " ").strip()
        print(f"\n    text preview:\n    {preview}...")
    else:
        print("    (no text records)")

    # Step 5 — Validate fields
    print("\n[5] Field validation:")
    missing_place_id = [r for r in text_records if not r.get("place_id")]
    empty_text       = [r for r in text_records if not r.get("company_text", "").strip()]
    print(f"    Missing place_id : {len(missing_place_id)}")
    print(f"    Empty text       : {len(empty_text)}")

    if missing_place_id or empty_text:
        print("    WARNING: Some records have data quality issues.")
    else:
        print("    OK — all records valid.")

    # Final summary
    print("\n" + "=" * 60)
    print(f"Workflow 3 completed")
    print(f"Websites crawled      : {len(page_records)}")
    print(f"Pages extracted       : {total_pages}")
    print(f"Company text generated: {len(text_records)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
