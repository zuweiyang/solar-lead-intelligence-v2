"""
Smoke test for Workflow 4 — AI Company Classification.

Run from the project root:
    py scripts/test_company_classifier.py

Uses the first 10 companies from company_text.json.
Runs AI classification if ANTHROPIC_API_KEY is set, otherwise uses keyword fallback.
"""

import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.workflow_4_company_analysis.company_classifier import run, load_company_texts
from config.settings import COMPANY_ANALYSIS_FILE, ANTHROPIC_API_KEY

TEST_LIMIT = 10

REQUIRED_FIELDS = [
    "company_name", "website", "place_id",
    "company_type", "market_focus", "services_detected",
    "confidence_score", "classification_method",
]


def main():
    print("=" * 60)
    print("Workflow 4 Smoke Test — Company Classifier")
    print("=" * 60)

    # Show which mode will run
    if ANTHROPIC_API_KEY:
        print(f"\n  Mode: AI (Anthropic)")
    else:
        print(f"\n  Mode: keyword fallback (set ANTHROPIC_API_KEY in .env for AI mode)")

    # Step 1 — Confirm input
    print(f"\n[1] Loading first {TEST_LIMIT} companies from company_text.json...")
    records = load_company_texts(limit=TEST_LIMIT)
    print(f"    Loaded: {len(records)} records")
    if not records:
        print("    FAIL: No records found. Run Workflow 3 first.")
        sys.exit(1)

    # Step 2 — Run classification
    print(f"\n[2] Running classification...")
    results = run(limit=TEST_LIMIT)

    # Step 3 — Print structured output
    print(f"\n[3] Classification results:")
    for r in results:
        services = ", ".join(r["services_detected"]) if r["services_detected"] else "(none)"
        print(
            f"\n    {r['company_name'] or r['website']}\n"
            f"      company_type  : {r['company_type']}\n"
            f"      market_focus  : {r['market_focus']}\n"
            f"      confidence    : {r['confidence_score']:.2f}  [{r['classification_method']}]\n"
            f"      services      : {services}"
        )

    # Step 4 — Field validation
    print(f"\n[4] Field validation:")
    errors = 0
    for r in results:
        missing = [f for f in REQUIRED_FIELDS if f not in r]
        if missing:
            print(f"    FAIL: {r.get('website', '?')} missing fields: {missing}")
            errors += 1
    if errors == 0:
        print(f"    OK — all {len(results)} records have required fields.")

    # Step 5 — Confirm file written
    print(f"\n[5] Checking company_analysis.json...")
    if not COMPANY_ANALYSIS_FILE.exists():
        print(f"    FAIL: {COMPANY_ANALYSIS_FILE} was not created.")
        sys.exit(1)
    with open(COMPANY_ANALYSIS_FILE, encoding="utf-8") as f:
        saved = json.load(f)
    print(f"    OK — {COMPANY_ANALYSIS_FILE.name} written with {len(saved)} records.")

    # Step 6 — Distribution summary
    from collections import Counter
    type_counts   = Counter(r["company_type"]   for r in results)
    market_counts = Counter(r["market_focus"]   for r in results)
    method_counts = Counter(r["classification_method"] for r in results)

    print(f"\n[6] Classification distribution:")
    print(f"    Company types:")
    for t, n in type_counts.most_common():
        print(f"      {t:<30} {n}")
    print(f"    Market focus:")
    for m, n in market_counts.most_common():
        print(f"      {m:<30} {n}")
    print(f"    Method: {dict(method_counts)}")

    if errors:
        print("\n    WARNING: some field validation errors (see above).")
        sys.exit(1)

    print("\n" + "=" * 60)
    print(f"Workflow 4 smoke test completed successfully.")
    print(f"Companies analyzed    : {len(results)}")
    print(f"company_analysis.json : written")
    print("=" * 60)


if __name__ == "__main__":
    main()
