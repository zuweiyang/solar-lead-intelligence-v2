# Workflow 2: Data Scraping
# Scrapes company listings from Google Maps using the Google Places API.

import csv
import json
import random
import time

import requests

from config.settings import (
    SEARCH_TASKS_FILE,
    RAW_LEADS_FILE,
    SCRAPE_DELAY_SECONDS,
    GOOGLE_MAPS_API_KEY,
    PLACES_TEXT_SEARCH_MAX_PAGES,
    PLACES_MAX_UNIQUE_PLACES_PER_RUN,
    PLACES_MAX_DETAILS_CALLS_PER_RUN,
)
from src.utils.text_normalization import normalize_text, normalize_value

LEAD_FIELDS = [
    "company_name", "address", "website", "phone",
    "rating", "category", "place_id", "source_keyword", "source_location",
]

PLACES_TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

DETAIL_FIELDS = "name,formatted_address,formatted_phone_number,website,rating,types"

_PAGETOKEN_INITIAL_DELAY = 3.0
_PAGETOKEN_BACKOFF_BASE = 2.0
_PAGETOKEN_BACKOFF_FACTOR = 2.0
_PAGETOKEN_BACKOFF_CAP = 8.0
_PAGETOKEN_TOTAL_BUDGET = 18.0


def _text_search(query: str, max_pages: int = 1) -> list[dict]:
    initial_params = {"query": query, "key": GOOGLE_MAPS_API_KEY}
    params = dict(initial_params)
    places: list[dict] = []
    page_num = 0

    while True:
        resp = requests.get(PLACES_TEXT_SEARCH_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        status = data.get("status", "")

        if status == "INVALID_REQUEST" and "pagetoken" in params:
            elapsed = _PAGETOKEN_INITIAL_DELAY
            next_wait = _PAGETOKEN_BACKOFF_BASE
            attempt = 0
            token_ready = False

            while True:
                if elapsed + next_wait > _PAGETOKEN_TOTAL_BUDGET:
                    print(
                        f"[Workflow 2]   WARN: pagetoken budget exhausted - "
                        f"query='{query}' page={page_num + 1} retries={attempt} "
                        f"elapsed={elapsed:.1f}s budget={_PAGETOKEN_TOTAL_BUDGET:.0f}s - "
                        f"pagination stopped at {len(places)} results"
                    )
                    break

                jitter = random.uniform(0, next_wait * 0.25)
                actual_wait = next_wait + jitter
                attempt += 1
                print(
                    f"[Workflow 2]   WARN: pagetoken not yet active - "
                    f"query='{query}' page={page_num + 1} retry={attempt} "
                    f"wait={actual_wait:.1f}s elapsed={elapsed:.1f}s "
                    f"budget={_PAGETOKEN_TOTAL_BUDGET:.0f}s"
                )
                time.sleep(actual_wait)
                elapsed += actual_wait

                resp = requests.get(PLACES_TEXT_SEARCH_URL, params=params, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                status = data.get("status", "")

                if status != "INVALID_REQUEST":
                    token_ready = True
                    print(
                        f"[Workflow 2]   OK: pagetoken active - "
                        f"query='{query}' page={page_num + 1} retry={attempt} "
                        f"elapsed={elapsed:.1f}s"
                    )
                    break

                next_wait = min(next_wait * _PAGETOKEN_BACKOFF_FACTOR, _PAGETOKEN_BACKOFF_CAP)

            if not token_ready:
                break

        if status not in ("OK", "ZERO_RESULTS"):
            err_msg = data.get("error_message", "")
            context = "page 1 (initial request)" if page_num == 0 else f"page {page_num + 1} (pagetoken)"
            hint = ""
            if status == "REQUEST_DENIED":
                hint = " (check API key, billing, and Places API enable status)"
            elif status == "OVER_QUERY_LIMIT":
                hint = " (daily quota exceeded - try again tomorrow or upgrade billing)"
            print(
                f"[Workflow 2]   ERROR: Places API returned {status} for '{query}' at {context}"
                + (f" - {err_msg}" if err_msg else "")
                + hint
            )
            break

        new_results = data.get("results", [])
        places.extend(new_results)
        page_num += 1
        print(
            f"[Workflow 2]   Page {page_num}: +{len(new_results)} results "
            f"(running total: {len(places)}) - '{query}'"
        )

        if max_pages > 0 and page_num >= max_pages:
            break

        next_token = data.get("next_page_token")
        if not next_token:
            break

        time.sleep(_PAGETOKEN_INITIAL_DELAY)
        params = {"pagetoken": next_token, "key": GOOGLE_MAPS_API_KEY}

    return places


def _place_details(place_id: str) -> dict:
    params = {
        "place_id": place_id,
        "fields": DETAIL_FIELDS,
        "key": GOOGLE_MAPS_API_KEY,
    }
    resp = requests.get(PLACES_DETAILS_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") != "OK":
        return {}
    return data.get("result", {})


def _primary_category(types: list[str]) -> str:
    skip = {"point_of_interest", "establishment", "premise"}
    for item in types:
        if item not in skip:
            return item.replace("_", " ")
    return types[0].replace("_", " ") if types else ""


def _dedup_key(place: dict) -> str:
    place_id = normalize_text(place.get("place_id", ""))
    if place_id:
        return f"place_id:{place_id}"
    name = normalize_text(place.get("name", "")).lower()
    address = normalize_text(place.get("formatted_address", "")).lower()
    return f"fallback:{name}|{address}"


def _effective_unique_place_budget(limit: int) -> int:
    if limit > 0:
        derived = max(limit * 3, limit)
        if PLACES_MAX_UNIQUE_PLACES_PER_RUN > 0:
            return min(PLACES_MAX_UNIQUE_PLACES_PER_RUN, derived)
        return derived
    return PLACES_MAX_UNIQUE_PLACES_PER_RUN


def _effective_details_budget(limit: int) -> int:
    if limit > 0:
        derived = max(limit * 2, limit)
        if PLACES_MAX_DETAILS_CALLS_PER_RUN > 0:
            return min(PLACES_MAX_DETAILS_CALLS_PER_RUN, derived)
        return derived
    return PLACES_MAX_DETAILS_CALLS_PER_RUN


def scrape_google_maps(query: str, location: str, max_pages: int = 1) -> list[dict]:
    return _text_search(query, max_pages=max_pages)


def load_pending_tasks() -> list[dict]:
    with open(SEARCH_TASKS_FILE, encoding="utf-8") as f:
        tasks = json.load(f)
    return [task for task in tasks if task.get("status") == "pending"]


def scrape_all_tasks(limit: int = 0) -> list[dict]:
    tasks = load_pending_tasks()
    unique_budget = _effective_unique_place_budget(limit)
    details_budget = _effective_details_budget(limit)
    candidate_places: dict[str, dict] = {}
    details_calls = 0

    for task in tasks:
        query = normalize_text(task["query"])
        print(f"[Workflow 2] Scraping: {query}")
        try:
            location = normalize_text(task["location"])
            raw_places = scrape_google_maps(
                query,
                location,
                max_pages=max(1, PLACES_TEXT_SEARCH_MAX_PAGES),
            )
            added = 0
            for place in raw_places:
                key = _dedup_key(place)
                if key in candidate_places:
                    continue
                if unique_budget > 0 and len(candidate_places) >= unique_budget:
                    break
                candidate_places[key] = {
                    **place,
                    "source_keyword": normalize_text(task["keyword"]),
                    "source_location": location,
                }
                added += 1

            print(
                f"[Workflow 2]   -> {len(raw_places)} raw / +{added} unique "
                f"(unique total: {len(candidate_places)}) [Places API]"
            )
        except Exception as exc:
            print(f"[Workflow 2]   ERROR on '{query}': {exc}")

        if unique_budget > 0 and len(candidate_places) >= unique_budget:
            print(
                f"[Workflow 2] Unique-place budget reached "
                f"({len(candidate_places)}/{unique_budget}) -> stopping Places search for this run"
            )
            break

        time.sleep(SCRAPE_DELAY_SECONDS)

    leads: list[dict] = []
    for place in candidate_places.values():
        place_id = place.get("place_id", "")
        website = place.get("website", "")
        phone = place.get("formatted_phone_number", "")

        details: dict = {}
        if place_id and (not website or not phone):
            if details_budget <= 0 or details_calls < details_budget:
                details = _place_details(place_id)
                details_calls += 1
                time.sleep(0.1)
            else:
                print(
                    f"[Workflow 2] Details-call budget reached "
                    f"({details_calls}/{details_budget}) -> skipping extra Place Details"
                )

        leads.append({
            "company_name": normalize_text(place.get("name", "")),
            "address": normalize_text(place.get("formatted_address", "")),
            "website": normalize_text(details.get("website", "") or website),
            "phone": normalize_text(details.get("formatted_phone_number", "") or phone),
            "rating": str(place.get("rating", "")),
            "category": normalize_text(_primary_category(place.get("types", []))),
            "place_id": normalize_text(place_id),
            "source_keyword": normalize_text(place.get("source_keyword", "")),
            "source_location": normalize_text(place.get("source_location", "")),
        })

    return leads


def save_raw_leads(leads: list[dict]) -> None:
    if not leads:
        print("[Workflow 2] No leads to save.")
        return
    with open(RAW_LEADS_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=LEAD_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(normalize_value(leads))
    print(f"[Workflow 2] Saved {len(leads)} raw leads -> {RAW_LEADS_FILE}")


def run(limit: int = 0) -> list[dict]:
    if not GOOGLE_MAPS_API_KEY:
        raise RuntimeError(
            "[Workflow 2] GOOGLE_MAPS_API_KEY is not set. "
            "Add it to your .env file before running the scrape step."
        )
    leads = scrape_all_tasks(limit=limit)
    save_raw_leads(leads)
    return leads


if __name__ == "__main__":
    run()
