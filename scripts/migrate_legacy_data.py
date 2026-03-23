"""
One-time migration: move legacy data-root files into the run-scoped architecture.

Run this ONCE after upgrading to the run-scoped campaign architecture.
Safe to re-run — skips files that have already been moved.

What it does:
  1. Moves global CRM files → data/crm/
       data/send_logs.csv        → data/crm/send_logs.csv
       data/engagement_logs.csv  → data/crm/engagement_logs.csv
       data/followup_logs.csv    → data/crm/followup_logs.csv
       data/crm_database.csv     → data/crm/crm_database.csv  (if present)

  2. Moves the last campaign's pipeline artifacts → data/runs/<campaign_id>/
       Reads campaign_id from data/campaign_run_state.json.
       Only moves files that exist in data/ and don't already exist in the run dir.

Run:
    py scripts/migrate_legacy_data.py
    py scripts/migrate_legacy_data.py --dry-run   (preview only, no changes)
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

DATA_DIR = PROJECT_ROOT / "data"
RUNS_DIR = DATA_DIR / "runs"
CRM_DIR  = DATA_DIR / "crm"

# CRM files: move to data/crm/
CRM_FILES = [
    "send_logs.csv",
    "engagement_logs.csv",
    "followup_logs.csv",
    "crm_database.csv",
]

# Campaign-scoped files: move to data/runs/<campaign_id>/
CAMPAIGN_FILES = [
    "search_tasks.json",
    "raw_leads.csv",
    "company_pages.json",
    "company_text.json",
    "company_analysis.json",
    "company_content.json",
    "company_profiles.json",
    "qualified_leads.csv",
    "enriched_leads.csv",
    "research_signal_raw.json",
    "research_signals.json",
    "generated_emails.csv",
    "scored_emails.csv",
    "send_queue.csv",
    "rejected_emails.csv",
    "repaired_emails.csv",
    "rescored_emails.csv",
    "final_send_queue.csv",
    "final_rejected_emails.csv",
    "email_templates.json",
    "email_logs.csv",
    "send_batch_summary.json",
    "engagement_summary.csv",
    "company_openings.json",
    "company_signals.json",
    "followup_candidates.csv",
    "followup_queue.csv",
    "followup_blocked.csv",
    "campaign_status.csv",
    "campaign_status_summary.json",
    "campaign_runner_logs.csv",
    "email_repair_errors.csv",
]


def _move(src: Path, dst: Path, dry_run: bool) -> None:
    if not src.exists():
        return
    if dst.exists():
        print(f"  SKIP  {src.name}  (already at destination)")
        return
    print(f"  MOVE  {src}  →  {dst}")
    if not dry_run:
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))


def migrate(dry_run: bool = False) -> None:
    label = "[DRY RUN] " if dry_run else ""
    print(f"{label}=== Legacy data migration ===\n")

    # --- Step 1: CRM files → data/crm/ ---
    print(f"{label}--- Global CRM files → data/crm/ ---")
    if not dry_run:
        CRM_DIR.mkdir(parents=True, exist_ok=True)
    for name in CRM_FILES:
        _move(DATA_DIR / name, CRM_DIR / name, dry_run)

    # --- Step 2: Campaign files → data/runs/<campaign_id>/ ---
    state_file = DATA_DIR / "campaign_run_state.json"
    if not state_file.exists():
        print("\nNo campaign_run_state.json found — skipping campaign artifact migration.")
        return

    with open(state_file, encoding="utf-8") as f:
        state = json.load(f)
    campaign_id = (state.get("campaign_id") or "").strip()

    if not campaign_id:
        print("\ncampaign_run_state.json has no campaign_id — skipping campaign artifact migration.")
        return

    run_dir = RUNS_DIR / campaign_id
    print(f"\n{label}--- Campaign artifacts → data/runs/{campaign_id}/ ---")
    if not dry_run:
        run_dir.mkdir(parents=True, exist_ok=True)

    for name in CAMPAIGN_FILES:
        _move(DATA_DIR / name, run_dir / name, dry_run)

    print(f"\n{label}=== Migration complete. ===")
    if dry_run:
        print("Re-run without --dry-run to apply changes.")


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    migrate(dry_run=dry_run)
