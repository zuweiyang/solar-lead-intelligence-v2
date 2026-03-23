"""
Smoke test for Workflow 2 — Google Maps Scraper.

Run from the project root:
    python scripts/test_workflow2_scraper.py
"""

import sys
import csv
from pathlib import Path

# Allow imports from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.workflow_2_data_scraping.google_maps_scraper import run
from config.settings import RAW_LEADS_FILE

REQUIRED_FIELDS = [
    "company_name", "address", "website", "phone",
    "rating", "category", "place_id", "source_keyword", "source_location",
]


def main():
    print("=" * 60)
    print("Workflow 2 Smoke Test — Google Maps Scraper")
    print("=" * 60)

    # Step 1 — Run scraper
    print("\n[1] Running scraper...")
    leads = run()

    # Step 2 — Print result count
    print(f"\n[2] Leads scraped: {len(leads)}")
    if not leads:
        print("    WARNING: No leads returned. Check API key and search_tasks.json.")
        sys.exit(1)

    # Step 3 — Print first 5 leads
    print("\n[3] First 5 leads:")
    for i, lead in enumerate(leads[:5], 1):
        print(f"\n  Lead {i}:")
        for field in REQUIRED_FIELDS:
            print(f"    {field:<20} {lead.get(field, '(missing)')}")

    # Step 4 — Verify place_id exists
    print("\n[4] Verifying place_id field...")
    missing_place_id = [l for l in leads if not l.get("place_id")]
    if missing_place_id:
        print(f"    WARNING: {len(missing_place_id)} leads missing place_id.")
    else:
        print(f"    OK — all {len(leads)} leads have place_id.")

    # Step 5 — Confirm raw_leads.csv was created
    print("\n[5] Checking raw_leads.csv...")
    if not RAW_LEADS_FILE.exists():
        print(f"    FAIL: {RAW_LEADS_FILE} not found.")
        sys.exit(1)

    with open(RAW_LEADS_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        csv_fields = reader.fieldnames or []
        csv_rows   = list(reader)

    missing_fields = [f for f in REQUIRED_FIELDS if f not in csv_fields]
    if missing_fields:
        print(f"    FAIL: Missing CSV columns: {missing_fields}")
        sys.exit(1)

    print(f"    OK — {RAW_LEADS_FILE.name} exists with {len(csv_rows)} rows.")
    print(f"    Columns: {csv_fields}")

    # Step 6 — Estimate API usage
    print("\n[6] API usage estimate:")
    text_search_requests = 3          # up to 3 pages × 1 task = 3 requests
    places_with_details  = sum(
        1 for l in leads if not l.get("website") or not l.get("phone")
    )
    # Pricing (as of 2024): Text Search $0.032/req, Place Details $0.017/req
    cost_text_search = text_search_requests * 0.032
    cost_details     = places_with_details  * 0.017
    total_cost       = cost_text_search + cost_details

    print(f"    Text Search requests : {text_search_requests}  (${cost_text_search:.4f})")
    print(f"    Place Details calls  : {places_with_details}  (${cost_details:.4f})")
    print(f"    Estimated total cost : ${total_cost:.4f}")
    print(f"\n    Projected cost for full pipeline (200 tasks × same ratio):")
    scale = 200
    print(f"    Text Search : {text_search_requests * scale} req  (${cost_text_search * scale:.2f})")
    print(f"    Details     : {places_with_details  * scale} req  (${cost_details     * scale:.2f})")
    print(f"    Total       : ${total_cost * scale:.2f}")

    print("\n" + "=" * 60)
    print("Workflow 2 smoke test completed successfully.")
    print("=" * 60)


if __name__ == "__main__":
    main()
