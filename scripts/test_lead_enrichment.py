"""
Smoke test for Workflow 5.5 — Lead Enrichment.

Run from the project root:
    py scripts/test_lead_enrichment.py

Uses the first 5 qualified leads.
Runs in mock mode when APOLLO_API_KEY and HUNTER_API_KEY are both empty.
"""

import sys
import csv
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.workflow_5_5_lead_enrichment.enricher import run, load_qualified_leads
from config.settings import ENRICHED_LEADS_FILE, APOLLO_API_KEY, HUNTER_API_KEY

TEST_LIMIT = 5

REQUIRED_FIELDS = [
    "company_name", "website", "place_id",
    "company_type", "market_focus", "lead_score",
    "kp_name", "kp_title", "kp_email", "enrichment_source",
]


def main():
    print("=" * 60)
    print("Workflow 5.5 Smoke Test — Lead Enrichment")
    print("=" * 60)

    # Show active mode
    if APOLLO_API_KEY and HUNTER_API_KEY:
        mode = "LIVE (Apollo + Hunter)"
    elif APOLLO_API_KEY:
        mode = "LIVE (Apollo only, no Hunter key)"
    elif HUNTER_API_KEY:
        mode = "LIVE (Hunter only, no Apollo key)"
    else:
        mode = "MOCK (no API keys — add APOLLO_API_KEY / HUNTER_API_KEY to .env for live mode)"
    print(f"\n  Mode: {mode}")

    # Step 1 — Load input and show BEFORE state
    print(f"\n[1] Loading up to {TEST_LIMIT} qualified leads (BEFORE enrichment)...")
    before = load_qualified_leads(limit=TEST_LIMIT)
    if not before:
        print("    FAIL: No qualified leads found. Run Workflow 5 first.")
        sys.exit(1)
    print(f"    Loaded: {len(before)} leads")
    print(f"\n    {'Company':<32} {'Type':<25} {'Score':>5}")
    print(f"    {'-'*65}")
    for r in before:
        print(f"    {(r.get('company_name') or r['website'])[:31]:<32} "
              f"{r.get('company_type', '')[:24]:<25} "
              f"{r.get('lead_score', ''):>5}")

    # Step 2 — Run enrichment
    print(f"\n[2] Running enrichment...")
    enriched = run(limit=TEST_LIMIT)

    # Step 3 — AFTER comparison
    print(f"\n[3] BEFORE vs AFTER comparison:")
    print(f"\n    {'Company':<32} {'KP Name':<22} {'Title':<22} {'Email':<35} {'Source'}")
    print(f"    {'-'*120}")
    for r in enriched:
        print(
            f"    {(r.get('company_name') or r['website'])[:31]:<32} "
            f"{r.get('kp_name', '')[:21]:<22} "
            f"{r.get('kp_title', '')[:21]:<22} "
            f"{r.get('kp_email', '')[:34]:<35} "
            f"{r.get('enrichment_source', '')}"
        )

    # Step 4 — Field validation
    print(f"\n[4] Field validation:")
    errors = 0
    for r in enriched:
        missing = [f for f in REQUIRED_FIELDS if f not in r]
        if missing:
            print(f"    FAIL: {r.get('company_name', '?')} missing: {missing}")
            errors += 1
    if errors == 0:
        print(f"    OK — all {len(enriched)} records have required fields.")

    # Step 5 — Confirm file written
    print(f"\n[5] Verifying enriched_leads.csv...")
    if not ENRICHED_LEADS_FILE.exists():
        print(f"    FAIL: {ENRICHED_LEADS_FILE} not created.")
        sys.exit(1)
    with open(ENRICHED_LEADS_FILE, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"    OK — {ENRICHED_LEADS_FILE.name} written with {len(rows)} rows.")
    print(f"    Columns: {list(rows[0].keys()) if rows else '(empty)'}")

    # Step 6 — Enrichment source breakdown
    from collections import Counter
    sources = Counter(r["enrichment_source"] for r in enriched)
    found   = sum(v for k, v in sources.items() if k != "none")
    print(f"\n[6] Enrichment summary:")
    for src, count in sources.most_common():
        print(f"    {src:<12} {count} lead(s)")
    print(f"    KP found  : {found}/{len(enriched)}")

    if errors:
        sys.exit(1)

    print("\n" + "=" * 60)
    print(f"Workflow 5.5 smoke test completed successfully.")
    print(f"Leads enriched        : {len(enriched)}")
    print(f"KP contacts found     : {found}")
    print(f"enriched_leads.csv    : written")
    print("=" * 60)


if __name__ == "__main__":
    main()
