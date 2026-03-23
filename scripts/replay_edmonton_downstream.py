"""
Replay downstream workflows (5 → 6.2 → 6 → 6.5 → 6.7) against the
existing Edmonton run (4ddae6a0) to validate the Round 1/2 fixes.

Does NOT re-scrape, re-crawl, or re-research — reuses:
  company_analysis.json, enriched_leads.csv, research_signals.json

Usage:
    py scripts/replay_edmonton_downstream.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

CAMPAIGN_ID = "4ddae6a0"

import config.run_context as run_context
run_context.set_active_run(CAMPAIGN_ID)

from config.settings import (
    COMPANY_ANALYSIS_FILE,
    ENRICHED_LEADS_FILE,
    RESEARCH_SIGNALS_FILE,
)

# Sanity-check required inputs exist
for label, path in [
    ("company_analysis.json", COMPANY_ANALYSIS_FILE),
    ("enriched_leads.csv",    ENRICHED_LEADS_FILE),
    ("research_signals.json", RESEARCH_SIGNALS_FILE),
]:
    if not Path(str(path)).exists():
        print(f"ERROR: required input missing: {path}")
        sys.exit(1)
    print(f"  OK  {label}")

print()

# --- Workflow 5: Lead Scoring ---
print("=" * 60)
print("WORKFLOW 5 — Lead Scoring (with consulting-penalty fix)")
print("=" * 60)
from src.workflow_5_lead_scoring.lead_scorer import run as run_scoring
run_scoring()

print()

# --- Workflow 6.2: Signal Personalization ---
print("=" * 60)
print("WORKFLOW 6.2 — Signal Personalization (with _normalize_signal fix)")
print("=" * 60)
from src.workflow_6_2_signal_personalization.signal_pipeline import generate_personalized_openings
generate_personalized_openings()

print()

# --- Workflow 6: Email Generation ---
print("=" * 60)
print("WORKFLOW 6 — Email Generation (with best_signal fallback logic)")
print("=" * 60)
from src.workflow_6_email_generation.email_generator import run as run_email_gen
run_email_gen()

print()

# --- Workflow 6.5: Email Quality Scoring ---
print("=" * 60)
print("WORKFLOW 6.5 — Email Quality Scoring")
print("=" * 60)
from src.workflow_6_5_email_quality.email_quality_scorer import run as run_quality
run_quality()

print()

# --- Workflow 6.7: Email Repair ---
print("=" * 60)
print("WORKFLOW 6.7 — Email Repair (with signal context in prompt)")
print("=" * 60)
from src.workflow_6_7_email_repair.repair_pipeline import run as run_repair
run_repair()

print()
print("=" * 60)
print("REPLAY COMPLETE")
print("=" * 60)
