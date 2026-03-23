"""
Smoke test for Workflow 5.8 — Company Signal Research.

Run from the project root:
    py scripts/test_signal_research.py

Uses the first 10 enriched leads.
No API keys required — fetches public website pages only.
"""

import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.workflow_5_8_signal_research.signal_collector import run as collect
from src.workflow_5_8_signal_research.signal_summarizer import run as summarize
from config.settings import RESEARCH_SIGNAL_RAW_FILE, RESEARCH_SIGNALS_FILE

TEST_LIMIT = 10

REQUIRED_FIELDS = [
    "company_name", "website", "place_id",
    "recent_signals", "research_summary", "email_angle",
]


def main():
    print("=" * 60)
    print("Workflow 5.8 Smoke Test — Company Signal Research")
    print("=" * 60)

    # Step 1 — Collect signals
    print(f"\n[1] Collecting signals for up to {TEST_LIMIT} leads...")
    raw = collect(limit=TEST_LIMIT)

    if not raw:
        print("    FAIL: No signals collected. Run Workflow 5.5 first.")
        sys.exit(1)

    total_pages  = sum(len(r["signal_sources"]["website"]) for r in raw)
    total_social = sum(len(r["signal_sources"]["social"])  for r in raw)
    with_signals = sum(
        1 for r in raw
        if r["signal_sources"]["website"] or r["signal_sources"]["social"]
    )

    print(f"\n    Companies processed        : {len(raw)}")
    print(f"    Companies with signals     : {with_signals}")
    print(f"    Total website pages fetched: {total_pages}")
    print(f"    Total social links found   : {total_social}")

    # Step 2 — Summarize
    print(f"\n[2] Running summarizer...")
    summaries = summarize()

    # Step 3 — Sample outputs
    print(f"\n[3] Sample structured outputs (first 3):")
    for r in summaries[:3]:
        print(f"\n    Company : {r['company_name']}")
        print(f"    Angle   : {r['email_angle']}")
        print(f"    Summary : {r['research_summary']}")
        signals_preview = r["recent_signals"][:2]
        print(f"    Signals : {signals_preview}")

    # Step 4 — Field validation
    print(f"\n[4] Field validation:")
    errors = 0
    for r in summaries:
        missing = [f for f in REQUIRED_FIELDS if f not in r]
        if missing:
            print(f"    FAIL: {r.get('company_name', '?')} missing fields: {missing}")
            errors += 1
    if errors == 0:
        print(f"    OK — all {len(summaries)} records have required fields.")

    # Step 5 — Confirm files written
    print(f"\n[5] Verifying output files...")
    for path in [RESEARCH_SIGNAL_RAW_FILE, RESEARCH_SIGNALS_FILE]:
        if not path.exists():
            print(f"    FAIL: {path} not created.")
            sys.exit(1)
        print(f"    OK — {path.name}")

    if errors:
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Workflow 5.8 smoke test completed successfully.")
    print(f"Companies processed        : {len(raw)}")
    print(f"Companies with signals     : {with_signals}")
    print(f"research_signal_raw.json   : written")
    print(f"research_signals.json      : written")
    print("=" * 60)


if __name__ == "__main__":
    main()
