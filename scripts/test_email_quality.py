# Smoke test for Workflow 6.5 — Email Quality Scoring
import sys
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import (
    SCORED_EMAILS_FILE, SEND_QUEUE_FILE, REJECTED_EMAILS_FILE,
    LLM_PROVIDER, OPENROUTER_API_KEY, ANTHROPIC_API_KEY, OPENAI_API_KEY,
)
from src.workflow_6_5_email_quality.quality_merge import load_generated_emails
from src.workflow_6_5_email_quality.email_quality_scorer import run, OUTPUT_FIELDS

print("=" * 60)
print("Workflow 6.5 Smoke Test — Email Quality Scoring")
print("=" * 60)

# Show scoring mode
if LLM_PROVIDER == "openrouter" and OPENROUTER_API_KEY:
    mode_label = "AI (OpenRouter)"
elif LLM_PROVIDER == "anthropic" and ANTHROPIC_API_KEY:
    mode_label = "AI (Anthropic)"
elif LLM_PROVIDER == "openai" and OPENAI_API_KEY:
    mode_label = "AI (OpenAI)"
elif ANTHROPIC_API_KEY:
    mode_label = "AI (Anthropic — auto)"
elif OPENAI_API_KEY:
    mode_label = "AI (OpenAI — auto)"
else:
    mode_label = "rule-based fallback"
print(f"Scoring mode: {mode_label}\n")

# [1] Load first 10 rows
print("[1] Loading first 10 rows from generated_emails.csv ...")
try:
    rows = load_generated_emails(limit=10)
except Exception as exc:
    print(f"FAIL: Could not load generated_emails.csv: {exc}")
    sys.exit(1)
print(f"    Loaded {len(rows)} records\n")

# [2] Run scoring
print("[2] Running quality scoring (limit=10) ...")
try:
    results = run(limit=10)
except Exception as exc:
    print(f"FAIL: run() raised an exception: {exc}")
    sys.exit(1)

if not results:
    print("FAIL: run() returned no results")
    sys.exit(1)

approved      = [r for r in results if r["approval_status"] == "approved"]
manual_review = [r for r in results if r["approval_status"] == "manual_review"]
rejected      = [r for r in results if r["approval_status"] == "rejected"]
ai_count      = sum(1 for r in results if r["scoring_mode"] == "ai")
rule_count    = sum(1 for r in results if r["scoring_mode"] == "rule")

print(f"\n    Emails processed : {len(results)}")
print(f"    Approved         : {len(approved)}")
print(f"    Manual review    : {len(manual_review)}")
print(f"    Rejected         : {len(rejected)}")
print(f"    AI scored        : {ai_count}")
print(f"    Rule scored      : {rule_count}\n")

# [3] Sample outputs
print("[3] Sample outputs (up to 3):")
for r in results[:3]:
    print(f"    Company      : {r.get('company_name', '')}")
    print(f"    KP email     : {r.get('kp_email', '')}")
    print(f"    Subject      : {r.get('subject', '')[:70]}")
    print(f"    Overall score: {r.get('overall_score', '')}")
    print(f"    Status       : {r.get('approval_status', '')}")
    print(f"    Notes        : {r.get('review_notes', '')}")
    print()

# [4] Field validation
print("[4] Field validation ...")
missing_fields = []
for i, r in enumerate(results):
    for field in OUTPUT_FIELDS:
        if field not in r:
            missing_fields.append(f"Record {i}: missing field '{field}'")

if missing_fields:
    for msg in missing_fields[:10]:
        print(f"    FAIL: {msg}")
    sys.exit(1)
print(f"    All {len(OUTPUT_FIELDS)} OUTPUT_FIELDS present in all {len(results)} records\n")

# [5] Verify files written
print("[5] Verifying output files ...")
files_to_check = [
    ("scored_emails.csv",   SCORED_EMAILS_FILE),
    ("send_queue.csv",      SEND_QUEUE_FILE),
    ("rejected_emails.csv", REJECTED_EMAILS_FILE),
]
for label, path in files_to_check:
    if not path.exists():
        print(f"    FAIL: {label} was not written at {path}")
        sys.exit(1)
    print(f"    OK: {label} exists ({path.stat().st_size} bytes)")

print()
print("Workflow 6.5 smoke test completed successfully.")
sys.exit(0)
