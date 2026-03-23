"""
Smoke test for Workflow 2 — Data Cleaner.

Run from the project root (after test_workflow2_scraper.py has been run):
    python scripts/test_data_cleaner.py
"""

import sys
import csv
from pathlib import Path
from collections import Counter

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.workflow_2_data_scraping.data_cleaner import load_raw_leads, clean_leads
from config.settings import RAW_LEADS_FILE


def main():
    print("=" * 60)
    print("Workflow 2 Smoke Test — Data Cleaner")
    print("=" * 60)

    # Step 1 — Load raw leads
    print(f"\n[1] Loading raw leads from {RAW_LEADS_FILE.name}...")
    if not RAW_LEADS_FILE.exists():
        print("    FAIL: raw_leads.csv not found. Run test_workflow2_scraper.py first.")
        sys.exit(1)

    raw = load_raw_leads()
    print(f"    Raw row count: {len(raw)}")

    # Step 2 — Run cleaner
    print("\n[2] Running clean_leads()...")
    cleaned = clean_leads(raw)
    print(f"    Cleaned row count: {len(cleaned)}")
    removed = len(raw) - len(cleaned)
    print(f"    Removed (duplicates / no-identifier): {removed}")

    # Step 3 — Dedup verification via place_id
    print("\n[3] Dedup verification (place_id):")
    place_ids = [r.get("place_id", "") for r in cleaned if r.get("place_id")]
    duplicates = [pid for pid, count in Counter(place_ids).items() if count > 1]

    if duplicates:
        print(f"    FAIL: {len(duplicates)} duplicate place_ids found after cleaning:")
        for pid in duplicates[:5]:
            print(f"      {pid}")
        sys.exit(1)
    else:
        print(f"    OK — {len(place_ids)} unique place_ids, no duplicates.")

    # Step 4 — Records without website
    no_website = [r for r in cleaned if not r.get("website")]
    print(f"\n[4] Leads without website : {len(no_website)}")
    print(f"    Leads with website     : {len(cleaned) - len(no_website)}")

    # Step 5 — Print first 5 cleaned leads
    print("\n[5] First 5 cleaned leads:")
    for i, lead in enumerate(cleaned[:5], 1):
        print(f"\n  Lead {i}:")
        print(f"    company_name  {lead.get('company_name', '')}")
        print(f"    place_id      {lead.get('place_id', '(none)')}")
        print(f"    website       {lead.get('website', '(none)')}")
        print(f"    phone         {lead.get('phone', '(none)')}")
        print(f"    rating        {lead.get('rating', '')}")

    print("\n" + "=" * 60)
    print("Data cleaner smoke test completed successfully.")
    print("=" * 60)


if __name__ == "__main__":
    main()
