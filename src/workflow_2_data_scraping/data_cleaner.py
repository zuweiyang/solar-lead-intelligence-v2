# Workflow 2: Data Scraping
# Cleans and deduplicates raw lead records before further processing.

import csv
from config.settings import RAW_LEADS_FILE


def load_raw_leads() -> list[dict]:
    """Load all records from raw_leads.csv."""
    with open(RAW_LEADS_FILE, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _normalize_phone(phone: str) -> str:
    """Keep digits only; return empty string if none."""
    digits = "".join(c for c in phone if c.isdigit())
    return digits or ""


def _normalize_url(url: str) -> str:
    """Lowercase and strip trailing slashes for deduplication."""
    url = url.strip().lower()
    return url.rstrip("/")


def clean_leads(leads: list[dict]) -> list[dict]:
    """
    Clean and deduplicate lead records.

    Dedup priority:
    1. place_id  — most reliable (stable Google identifier, handles re-crawls)
    2. website   — fallback for records without a place_id
    Records with neither are dropped.
    """
    seen_place_ids: set[str] = set()
    seen_websites:  set[str] = set()
    cleaned: list[dict] = []

    for lead in leads:
        place_id = lead.get("place_id", "").strip()
        url      = _normalize_url(lead.get("website", ""))

        # Deduplicate by place_id first
        if place_id:
            if place_id in seen_place_ids:
                continue
            seen_place_ids.add(place_id)
        elif url:
            if url in seen_websites:
                continue
            seen_websites.add(url)
        else:
            continue  # no usable identifier — drop

        lead["website"] = url
        lead["phone"]   = _normalize_phone(lead.get("phone", ""))
        cleaned.append(lead)

    print(f"[Workflow 2] Cleaned: {len(leads)} raw → {len(cleaned)} unique leads")
    return cleaned


def run() -> list[dict]:
    leads = load_raw_leads()
    return clean_leads(leads)


if __name__ == "__main__":
    run()
