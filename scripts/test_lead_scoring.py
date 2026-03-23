"""
Smoke test for Workflow 5 — Lead Scoring.

Run from the project root:
    py scripts/test_lead_scoring.py
"""

import sys
import csv
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.workflow_5_lead_scoring.lead_scorer import (
    load_analyses, score_all, filter_qualified, save_qualified,
    QUALIFIED_THRESHOLD,
)
from config.settings import QUALIFIED_LEADS_FILE

TEST_LIMIT = 20


def main():
    print("=" * 60)
    print("Workflow 5 Smoke Test — Lead Scoring")
    print("=" * 60)

    # Step 1 — Load input
    print(f"\n[1] Loading up to {TEST_LIMIT} records from company_analysis.json...")
    records = load_analyses(limit=TEST_LIMIT)
    print(f"    Loaded: {len(records)} records")
    if not records:
        print("    FAIL: No records. Run Workflow 4 first.")
        sys.exit(1)

    # Step 2 — Score all
    print(f"\n[2] Scoring all {len(records)} companies...")
    scored = score_all(records)

    # Step 3 — Print full scored table
    print(f"\n[3] All companies scored (sorted by lead_score):")
    col = "{:<32} {:<25} {:<14} {:>5}  {}"
    print("    " + col.format("Company", "company_type", "market_focus", "Score", "Breakdown"))
    print("    " + "-" * 105)
    for r in scored:
        name = (r.get("company_name") or r["website"])[:31]
        breakdown_short = " | ".join(r["score_breakdown"])
        print("    " + col.format(
            name,
            r["company_type"][:24],
            r["market_focus"][:13],
            r["lead_score"],
            breakdown_short,
        ))

    # Step 4 — Filter and save qualified leads
    print(f"\n[4] Filtering leads with score ≥ {QUALIFIED_THRESHOLD}...")
    qualified = filter_qualified(scored)
    save_qualified(qualified)

    # Step 5 — Confirm CSV written
    print(f"\n[5] Verifying qualified_leads.csv...")
    if not QUALIFIED_LEADS_FILE.exists():
        print("    FAIL: qualified_leads.csv was not created.")
        sys.exit(1)
    with open(QUALIFIED_LEADS_FILE, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"    OK — {len(rows)} rows written.")
    print(f"    Columns: {list(rows[0].keys()) if rows else '(empty)'}")

    # Step 6 — Show qualified leads detail
    if qualified:
        print(f"\n[6] Qualified leads (score ≥ {QUALIFIED_THRESHOLD}):")
        for r in qualified:
            print(
                f"\n    {r.get('company_name') or r['website']}\n"
                f"      website      : {r['website']}\n"
                f"      type         : {r['company_type']}\n"
                f"      market       : {r['market_focus']}\n"
                f"      score        : {r['lead_score']}\n"
                f"      breakdown    : {' | '.join(r['score_breakdown'])}\n"
                f"      services     : {'; '.join(r.get('services_detected', []))}"
            )
    else:
        print(f"\n[6] No companies reached the threshold of {QUALIFIED_THRESHOLD}.")
        print("    Tip: run Workflow 4 with AI mode to improve classification accuracy.")

    # Step 7 — Score distribution
    print(f"\n[7] Score distribution:")
    buckets = {"80-100": 0, "60-79": 0, "40-59": 0, "0-39": 0}
    for r in scored:
        s = r["lead_score"]
        if s >= 80:   buckets["80-100"] += 1
        elif s >= 60: buckets["60-79"]  += 1
        elif s >= 40: buckets["40-59"]  += 1
        else:         buckets["0-39"]   += 1
    for bucket, count in buckets.items():
        bar = "█" * count
        print(f"    {bucket:>7}  {bar} {count}")

    print("\n" + "=" * 60)
    print(f"Workflow 5 smoke test completed successfully.")
    print(f"Companies scored      : {len(scored)}")
    print(f"Qualified leads       : {len(qualified)}  (score ≥ {QUALIFIED_THRESHOLD})")
    print(f"qualified_leads.csv   : written")
    print("=" * 60)


if __name__ == "__main__":
    main()
