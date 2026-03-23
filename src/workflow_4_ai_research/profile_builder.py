# Workflow 4: AI Company Research
# Merges AI analysis with raw lead data to build complete company profiles.

import json
import csv
from config.settings import RAW_LEADS_FILE, COMPANY_PROFILES_FILE


def load_raw_leads_map() -> dict[str, dict]:
    """Return raw leads keyed by normalised website URL."""
    with open(RAW_LEADS_FILE, newline="", encoding="utf-8") as f:
        leads = list(csv.DictReader(f))
    return {lead["website"].strip().lower().rstrip("/"): lead for lead in leads}


def build_profiles(ai_analyses: list[dict]) -> list[dict]:
    """
    Merge AI analysis results with raw CRM fields (phone, address, rating)
    to create complete company profile records.
    """
    raw_map = load_raw_leads_map()
    profiles: list[dict] = []

    for analysis in ai_analyses:
        website_key = analysis.get("website", "").strip().lower().rstrip("/")
        raw = raw_map.get(website_key, {})

        profile = {
            # From raw scrape
            "company_name":  analysis.get("company_name") or raw.get("company_name", ""),
            "website":       analysis.get("website", ""),
            "address":       raw.get("address", ""),
            "phone":         raw.get("phone", ""),
            "rating":        raw.get("rating", ""),
            "category":      raw.get("category", ""),
            # From AI analysis
            "company_summary":       analysis.get("company_summary", ""),
            "business_type":         analysis.get("business_type", ""),
            "products":              analysis.get("products", []),
            "target_market":         analysis.get("target_market", ""),
            "location":              analysis.get("location", ""),
            "employee_count_estimate": analysis.get("employee_count_estimate", "unknown"),
        }
        profiles.append(profile)

    return profiles


def save_company_profiles(profiles: list[dict]) -> None:
    """Write company profiles to company_profiles.json."""
    COMPANY_PROFILES_FILE.write_text(json.dumps(profiles, indent=2), encoding="utf-8")
    print(f"[Workflow 4] Saved {len(profiles)} company profiles → {COMPANY_PROFILES_FILE}")


def run(ai_analyses: list[dict]) -> list[dict]:
    profiles = build_profiles(ai_analyses)
    save_company_profiles(profiles)
    return profiles


if __name__ == "__main__":
    from .company_analyzer import run as analyze_run
    run(analyze_run())
