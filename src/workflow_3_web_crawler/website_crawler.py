# Workflow 3: Website Crawling
# Crawls company websites from raw_leads.csv and saves raw HTML per page.

import csv
import json
import time
from urllib.parse import urljoin, urlparse

import requests
import tldextract

from config.settings import RAW_LEADS_FILE, COMPANY_PAGES_FILE, CRAWL_DELAY_SECONDS
from src.market_localization import get_crawl_target_paths

MAX_PAGES_PER_SITE = 5
REQUEST_TIMEOUT    = 10
TEST_LIMIT         = 50   # max leads crawled during smoke tests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _root_domain(url: str) -> str:
    """Return 'example.com' from any URL — used for per-domain dedup."""
    ext = tldextract.extract(url)
    return f"{ext.domain}.{ext.suffix}".lower()


def _ensure_https(url: str) -> str:
    if not url.startswith("http"):
        return "https://" + url
    return url


def _fetch(url: str) -> str | None:
    """GET a URL and return HTML text, or None on any error."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT,
                            allow_redirects=True)
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "")
        if "text/html" not in ct:
            return None
        return resp.text
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Core crawl logic
# ---------------------------------------------------------------------------

def crawl_site(base_url: str, country: str = "") -> dict[str, str]:
    """
    Crawl homepage + up to TARGET_PATHS for one site.
    Returns dict of {label: html}, e.g. {"home": "...", "about": "..."}.
    Stops after MAX_PAGES_PER_SITE successful fetches.
    """
    base_url = _ensure_https(base_url)
    pages: dict[str, str] = {}
    target_paths = get_crawl_target_paths(country)

    # Always crawl home first
    html = _fetch(base_url)
    if html:
        pages["home"] = html

    for path in target_paths:
        if len(pages) >= MAX_PAGES_PER_SITE:
            break
        label = path.strip("/").replace("-", "_") or "home"
        url   = urljoin(base_url, path)
        html  = _fetch(url)
        if html:
            pages[label] = html

    return pages


# ---------------------------------------------------------------------------
# Public run()
# ---------------------------------------------------------------------------

def load_leads() -> list[dict]:
    with open(RAW_LEADS_FILE, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# Directory / social domains that are not crawlable company websites.
# Leads whose website resolves to one of these are excluded before crawling.
_SKIP_DOMAINS: frozenset[str] = frozenset({
    "facebook", "instagram", "linkedin", "twitter", "x",
    "yelp", "yellowpages", "tripadvisor", "foursquare",
    "google", "maps.google", "goo.gl",
    "bbb.org", "houzz", "angi", "thumbtack",
    "youtube", "tiktok",
})


def _is_crawlable(domain: str) -> bool:
    """Return False for social-media / directory domains that are not company sites."""
    return bool(domain) and not any(skip in domain for skip in _SKIP_DOMAINS)


def _deduplicate_candidates(leads: list[dict]) -> list[dict]:
    """
    Deduplicate raw leads so crawl_limit applies to distinct companies, not raw CSV rows.

    Raw leads are heavily duplicated: a 766-row CSV for Dubai contains only 143
    distinct place_ids because the same business appears once per keyword that
    matched it.  Without dedup-before-limit, crawl_limit=60 would inspect only
    the first 60 rows (≈11 unique companies) instead of the 60 intended.

    Dedup order: place_id first, then normalised domain.
    Websites that resolve to social/directory domains are also removed here.
    """
    seen_pids:    set[str] = set()
    seen_domains: set[str] = set()
    candidates:   list[dict] = []

    for lead in leads:
        pid    = (lead.get("place_id") or "").strip()
        domain = _root_domain(lead.get("website", ""))

        if pid and pid in seen_pids:
            continue
        if domain and domain in seen_domains:
            continue
        if not _is_crawlable(domain):
            continue

        if pid:
            seen_pids.add(pid)
        if domain:
            seen_domains.add(domain)
        candidates.append(lead)

    return candidates


def run(limit: int = 0) -> list[dict]:
    """
    Crawl websites for leads in raw_leads.csv.

    Args:
        limit: cap on distinct companies to crawl (0 = no limit).
               Applied AFTER deduplication, so limit=60 means "crawl up to
               60 unique companies", not "inspect the first 60 CSV rows".

    Returns:
        List of page records saved to company_pages.json.
    """
    leads = load_leads()
    leads = [l for l in leads if l.get("website")]

    # Dedup and social-filter BEFORE applying limit so the cap is meaningful
    candidates = _deduplicate_candidates(leads)

    raw_count  = len(leads)
    dedup_count = len(candidates)
    print(
        f"[Workflow 3] {raw_count} rows with websites → "
        f"{dedup_count} unique crawlable candidates after dedup+filter"
    )

    if limit:
        candidates = candidates[:limit]
        print(f"[Workflow 3] crawl_limit={limit} → crawling {len(candidates)} candidates")

    results: list[dict] = []

    for lead in candidates:
        place_id = lead.get("place_id", "")
        website  = lead["website"].strip()
        country  = lead.get("country", "")

        print(f"[Workflow 3] Crawling: {website}")
        pages = crawl_site(website, country=country)

        if not pages:
            # Keep a stub entry so the company still reaches the classify step.
            # The classifier will use the company name for solar-relevance detection
            # and fall back to keyword rules — better than silently dropping the lead.
            print(f"[Workflow 3]   → unreachable, keeping stub for classification")
            results.append({
                "place_id": place_id,
                "website":  website,
                "pages":    {},
            })
            time.sleep(CRAWL_DELAY_SECONDS)
            continue

        print(f"[Workflow 3]   → {len(pages)} page(s) fetched")
        results.append({
            "place_id": place_id,
            "website":  website,
            "pages":    pages,
        })

        time.sleep(CRAWL_DELAY_SECONDS)

    COMPANY_PAGES_FILE.write_text(json.dumps(results, indent=2, ensure_ascii=False),
                                  encoding="utf-8")
    print(f"\n[Workflow 3] Saved {len(results)} sites → {COMPANY_PAGES_FILE}")
    return results


if __name__ == "__main__":
    run()
