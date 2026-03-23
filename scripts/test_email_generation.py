"""
Smoke test for Workflow 6.0 — Email Generation.

Run from the project root:
    py scripts/test_email_generation.py

Loads 5 leads, generates 5 drafts, validates:
  - No "Hi there"
  - No role confusion ("At {company} we...")
  - Emails under 120 words
  - subject and body both non-empty
"""

import csv
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.workflow_6_email_generation.email_generator import run
from config.settings import GENERATED_EMAILS_FILE, OPENROUTER_API_KEY

TEST_LIMIT = 5

REQUIRED_FIELDS = ["company_name", "kp_name", "kp_email", "subject", "body",
                   "lead_score", "email_angle", "generation_source"]


def _word_count(text: str) -> int:
    return len(text.split())


def main():
    print("=" * 60)
    print("Workflow 6.0 Smoke Test — Email Generation")
    print(f"Provider : {'OpenRouter/' if OPENROUTER_API_KEY else 'rule-based fallback'}"
          + (f"{__import__('config.settings', fromlist=['EMAIL_GEN_MODEL']).EMAIL_GEN_MODEL}"
             if OPENROUTER_API_KEY else ""))
    print("=" * 60)

    errors = 0

    # --- Generate ---
    print(f"\n[1] Generating {TEST_LIMIT} email drafts...")
    results = run(limit=TEST_LIMIT)

    if not results:
        print("FAIL: No emails generated.")
        sys.exit(1)

    print(f"    Generated: {len(results)}")

    # --- Validate each draft ---
    print(f"\n[2] Validating drafts...")
    for r in results:
        company = r.get("company_name", "?")
        body    = r.get("body", "")
        subject = r.get("subject", "")

        # Required fields present
        missing = [f for f in REQUIRED_FIELDS if f not in r]
        if missing:
            print(f"  FAIL [{company}] missing fields: {missing}")
            errors += 1

        # Non-empty subject and body
        if not subject.strip():
            print(f"  FAIL [{company}] empty subject")
            errors += 1
        if not body.strip():
            print(f"  FAIL [{company}] empty body")
            errors += 1
            continue

        # No "Hi there"
        if re.search(r"\bhi there\b", body, re.IGNORECASE):
            print(f"  FAIL [{company}] contains 'Hi there'")
            errors += 1

        # No role confusion: "At <company_name> we"
        if re.search(r"\bAt " + re.escape(company[:15]), body, re.IGNORECASE):
            print(f"  FAIL [{company}] role confusion detected ('At {company[:20]} we...')")
            errors += 1

        # Under 120 words
        wc = _word_count(body)
        if wc > 120:
            print(f"  WARN [{company}] body is {wc} words (limit 120)")

    if errors == 0:
        print(f"  OK — all {len(results)} drafts passed validation.")

    # --- Print all 5 ---
    print(f"\n[3] Email drafts:")
    for i, r in enumerate(results, 1):
        body = r.get("body", "")
        print(f"\n  {'─'*56}")
        print(f"  [{i}] {r.get('company_name', '')}")
        print(f"  Angle   : {r.get('email_angle', '')} | Score: {r.get('lead_score', '')}")
        greeting_line = body.splitlines()[0] if body else ""
        print(f"  Greeting: {greeting_line}")
        print(f"  Subject : {r.get('subject', '')}")
        print(f"  Words   : {_word_count(body)}")
        print(f"  Source  : {r.get('generation_source', '')}")
        print()
        for line in body.splitlines():
            print(f"    {line}")

    # --- Verify CSV ---
    print(f"\n[4] Verifying {GENERATED_EMAILS_FILE.name}...")
    if not GENERATED_EMAILS_FILE.exists():
        print("  FAIL: file not created.")
        errors += 1
    else:
        with open(GENERATED_EMAILS_FILE, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        missing_cols = [c for c in REQUIRED_FIELDS if c not in (rows[0] if rows else {})]
        if missing_cols:
            print(f"  FAIL: missing CSV columns: {missing_cols}")
            errors += 1
        else:
            print(f"  OK — {len(rows)} rows, all required columns present.")

    if errors:
        sys.exit(1)

    print("\n" + "=" * 60)
    print(f"Smoke test passed. {len(results)} drafts generated.")
    print("=" * 60)


if __name__ == "__main__":
    main()
