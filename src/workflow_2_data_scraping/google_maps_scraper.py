# Workflow 2: Data Scraping
# Scrapes company listings from Google Maps using the Google Places API.

import csv
import json
import random
import time
import requests
from config.settings import (
    SEARCH_TASKS_FILE, RAW_LEADS_FILE,
    SCRAPE_DELAY_SECONDS, GOOGLE_MAPS_API_KEY,
)

LEAD_FIELDS = [
    "company_name", "address", "website", "phone",
    "rating", "category", "place_id", "source_keyword", "source_location",
]

PLACES_TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACES_DETAILS_URL     = "https://maps.googleapis.com/maps/api/place/details/json"

# Fields fetched in the Details call (billed per field group)
DETAIL_FIELDS = "name,formatted_address,formatted_phone_number,website,rating,types"


# ---------------------------------------------------------------------------
# Google Places helpers
# ---------------------------------------------------------------------------

# Google's next_page_token activation is server-side and non-deterministic.
# Official guidance: there is a delay of a few seconds before the token is
# valid; requesting too early returns INVALID_REQUEST.
#
# Strategy: short fixed initial wait (happy-path fast), then exponential
# backoff with jitter if INVALID_REQUEST is received, bounded by a hard
# total budget.  Never treat INVALID_REQUEST on a pagetoken as immediately
# fatal.
_PAGETOKEN_INITIAL_DELAY  = 3.0   # seconds to wait before the first pagetoken attempt
_PAGETOKEN_BACKOFF_BASE   = 2.0   # first retry wait (seconds)
_PAGETOKEN_BACKOFF_FACTOR = 2.0   # multiply each successive retry wait
_PAGETOKEN_BACKOFF_CAP    = 8.0   # maximum per-retry wait (seconds)
_PAGETOKEN_TOTAL_BUDGET   = 18.0  # hard total budget from token receipt (seconds)


def _text_search(query: str) -> list[dict]:
    """
    Call Places Text Search and return raw place results (all pages).
    Each result contains: place_id, name, formatted_address, rating, types.

    Pagination note: Google's next_page_token is not immediately valid after
    the previous response. A short initial delay is applied, then INVALID_REQUEST
    responses trigger exponential backoff retries bounded by _PAGETOKEN_TOTAL_BUDGET.
    """
    initial_params = {"query": query, "key": GOOGLE_MAPS_API_KEY}
    params = dict(initial_params)
    places: list[dict] = []
    page_num = 0

    while True:
        resp = requests.get(PLACES_TEXT_SEARCH_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        status = data.get("status", "")

        # ---- handle INVALID_REQUEST on pagination (token not yet active) ----
        # INVALID_REQUEST here means the token exists but is not yet activated
        # server-side.  Retry with exponential backoff + jitter until the hard
        # total budget (_PAGETOKEN_TOTAL_BUDGET) is exhausted.
        if status == "INVALID_REQUEST" and "pagetoken" in params:
            elapsed     = _PAGETOKEN_INITIAL_DELAY   # already consumed above
            next_wait   = _PAGETOKEN_BACKOFF_BASE
            attempt     = 0
            token_ready = False

            while True:
                # Budget check: stop before sleeping if next wait exceeds budget
                if elapsed + next_wait > _PAGETOKEN_TOTAL_BUDGET:
                    print(
                        f"[Workflow 2]   WARN: pagetoken budget exhausted — "
                        f"query='{query}' page={page_num + 1} "
                        f"retries={attempt} elapsed={elapsed:.1f}s "
                        f"budget={_PAGETOKEN_TOTAL_BUDGET:.0f}s — "
                        f"pagination stopped at {len(places)} results"
                    )
                    break

                jitter      = random.uniform(0, next_wait * 0.25)
                actual_wait = next_wait + jitter
                attempt    += 1
                print(
                    f"[Workflow 2]   WARN: pagetoken not yet active — "
                    f"query='{query}' page={page_num + 1} "
                    f"retry={attempt} wait={actual_wait:.1f}s "
                    f"elapsed={elapsed:.1f}s budget={_PAGETOKEN_TOTAL_BUDGET:.0f}s"
                )
                time.sleep(actual_wait)
                elapsed += actual_wait

                resp = requests.get(PLACES_TEXT_SEARCH_URL, params=params, timeout=10)
                resp.raise_for_status()
                data   = resp.json()
                status = data.get("status", "")

                if status != "INVALID_REQUEST":
                    token_ready = True
                    print(
                        f"[Workflow 2]   OK: pagetoken active — "
                        f"query='{query}' page={page_num + 1} "
                        f"retry={attempt} elapsed={elapsed:.1f}s"
                    )
                    break

                # Exponential growth, capped per step
                next_wait = min(next_wait * _PAGETOKEN_BACKOFF_FACTOR,
                                _PAGETOKEN_BACKOFF_CAP)

            if not token_ready:
                break

        # ---- handle other non-OK / ZERO_RESULTS statuses -------------------
        if status not in ("OK", "ZERO_RESULTS"):
            err_msg = data.get("error_message", "")
            context = "page 1 (initial request)" if page_num == 0 else f"page {page_num + 1} (pagetoken)"
            hint = ""
            if status == "REQUEST_DENIED":
                hint = " (check API key, billing, and Places API enable status)"
            elif status == "OVER_QUERY_LIMIT":
                hint = " (daily quota exceeded — try again tomorrow or upgrade billing)"
            print(
                f"[Workflow 2]   ERROR: Places API returned {status} "
                f"for '{query}' at {context}"
                + (f" — {err_msg}" if err_msg else "")
                + hint
            )
            break

        new_results = data.get("results", [])
        places.extend(new_results)
        page_num += 1
        print(
            f"[Workflow 2]   Page {page_num}: +{len(new_results)} results "
            f"(running total: {len(places)}) — '{query}'"
        )

        next_token = data.get("next_page_token")
        if not next_token:
            break

        # Google requires a short delay before the next-page token becomes valid
        time.sleep(_PAGETOKEN_INITIAL_DELAY)
        params = {"pagetoken": next_token, "key": GOOGLE_MAPS_API_KEY}

    return places


def _place_details(place_id: str) -> dict:
    """
    Fetch detailed info for a single place (website, phone, etc.).
    Returns the 'result' dict from the API response.
    """
    params = {
        "place_id": place_id,
        "fields":   DETAIL_FIELDS,
        "key":      GOOGLE_MAPS_API_KEY,
    }
    resp = requests.get(PLACES_DETAILS_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    if data.get("status") != "OK":
        return {}
    return data.get("result", {})


def _primary_category(types: list[str]) -> str:
    """Return the most human-readable type from a place's types list."""
    skip = {"point_of_interest", "establishment", "premise"}
    for t in types:
        if t not in skip:
            return t.replace("_", " ")
    return types[0].replace("_", " ") if types else ""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def scrape_google_maps(query: str, location: str) -> list[dict]:
    """
    Search Google Maps for companies matching query+location and return
    a list of lead dicts with: company_name, address, website, phone, rating, category.
    """
    raw_places = _text_search(query)
    leads: list[dict] = []

    for place in raw_places:
        place_id = place.get("place_id", "")
        website  = place.get("website", "")
        phone    = place.get("formatted_phone_number", "")

        # Only call Place Details when website or phone is missing — saves ~70% API cost
        details: dict = {}
        if place_id and (not website or not phone):
            details = _place_details(place_id)
            time.sleep(0.1)  # stay within per-second rate limit

        leads.append({
            "company_name": place.get("name", ""),
            "address":      place.get("formatted_address", ""),
            "website":      details.get("website", "") or website,
            "phone":        details.get("formatted_phone_number", "") or phone,
            "rating":       str(place.get("rating", "")),
            "category":     _primary_category(place.get("types", [])),
            "place_id":     place_id,
        })

    return leads


def load_pending_tasks() -> list[dict]:
    """Load tasks with status 'pending' from search_tasks.json."""
    with open(SEARCH_TASKS_FILE, encoding="utf-8") as f:
        tasks = json.load(f)
    return [t for t in tasks if t.get("status") == "pending"]


def scrape_all_tasks() -> list[dict]:
    """Run the scraper for every pending search task and collect leads."""
    tasks     = load_pending_tasks()
    all_leads: list[dict] = []

    for task in tasks:
        query = task["query"]
        print(f"[Workflow 2] Scraping: {query}")
        try:
            leads = scrape_google_maps(query, task["location"])
            for lead in leads:
                lead["source_keyword"]  = task["keyword"]
                lead["source_location"] = task["location"]
            all_leads.extend(leads)
            source = "Places API (primary)"
            print(f"[Workflow 2]   → {len(leads)} results [{source}]")
        except Exception as exc:
            print(f"[Workflow 2]   ERROR on '{query}': {exc}")
        time.sleep(SCRAPE_DELAY_SECONDS)

    return all_leads


def save_raw_leads(leads: list[dict]) -> None:
    """Write scraped leads to raw_leads.csv."""
    if not leads:
        print("[Workflow 2] No leads to save.")
        return
    with open(RAW_LEADS_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=LEAD_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(leads)
    print(f"[Workflow 2] Saved {len(leads)} raw leads → {RAW_LEADS_FILE}")


def run() -> list[dict]:
    if not GOOGLE_MAPS_API_KEY:
        raise RuntimeError(
            "[Workflow 2] GOOGLE_MAPS_API_KEY is not set. "
            "Add it to your .env file before running the scrape step."
        )
    leads = scrape_all_tasks()
    save_raw_leads(leads)
    return leads


if __name__ == "__main__":
    run()
