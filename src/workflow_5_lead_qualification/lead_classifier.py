# Workflow 5: Lead Qualification
# Classifies scored profiles and writes qualified leads to CSV.

import csv
import json
from config.settings import COMPANY_PROFILES_FILE, QUALIFIED_LEADS_FILE
from .scoring_engine import score_all_profiles

QUALIFIED_GRADES = {"A", "B"}  # Grades to keep for outreach

QUALIFIED_FIELDS = [
    "company_name", "website", "address", "phone", "rating",
    "business_type", "target_market", "location", "employee_count_estimate",
    "company_summary", "score", "grade", "score_breakdown",
]


def load_company_profiles() -> list[dict]:
    """Load profiles from company_profiles.json."""
    with open(COMPANY_PROFILES_FILE, encoding="utf-8") as f:
        return json.load(f)


def classify_leads(scored_profiles: list[dict]) -> list[dict]:
    """Filter to only A and B grade leads."""
    qualified = [p for p in scored_profiles if p.get("grade") in QUALIFIED_GRADES]
    print(
        f"[Workflow 5] {len(scored_profiles)} profiles → "
        f"{len(qualified)} qualified leads (grades A/B)"
    )
    return qualified


def save_qualified_leads(leads: list[dict]) -> None:
    """Write qualified leads to qualified_leads.csv."""
    if not leads:
        print("[Workflow 5] No qualified leads to save.")
        return
    with open(QUALIFIED_LEADS_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=QUALIFIED_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for lead in leads:
            row = {**lead, "score_breakdown": "; ".join(lead.get("score_breakdown", []))}
            writer.writerow(row)
    print(f"[Workflow 5] Saved {len(leads)} qualified leads → {QUALIFIED_LEADS_FILE}")


def run() -> list[dict]:
    profiles = load_company_profiles()
    scored   = score_all_profiles(profiles)
    leads    = classify_leads(scored)
    save_qualified_leads(leads)
    return leads


if __name__ == "__main__":
    run()
