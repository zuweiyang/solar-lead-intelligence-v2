# Workflow 5.8: Company Signal Research — Signal Collector
# Fetches website and social pages, extracts headline signals per company.

import csv
import json
import time
import urllib.parse
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from config.settings import ENRICHED_LEADS_FILE, RESEARCH_SIGNAL_RAW_FILE
from config.run_paths import RunPaths

REQUEST_TIMEOUT = 10
CRAWL_DELAY     = 1.0
MAX_PAGES       = 5
MAX_SOCIAL      = 3
MAX_ITEMS       = 3

SIGNAL_PATHS = ["/", "/news", "/blog", "/projects", "/case-studies", "/careers"]

SOCIAL_DOMAINS = {
    "linkedin":  "linkedin.com/company",
    "facebook":  "facebook.com",
    "instagram": "instagram.com",
    "youtube":   "youtube.com",
}

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SolarLeadBot/1.0)"}

# URL path fragments that identify social sharing/intent links rather than company pages.
# These appear in share-buttons (e.g. "Share on Facebook") and are not the company's profile.
_SHARE_PATH_FRAGMENTS: frozenset[str] = frozenset({
    "/sharer", "/share", "/intent/tweet", "/intent/retweet",
    "/shareArticle", "/send", "/dialog/feed",
    "/login", "/signup", "/join", "/subscribe",
})

# ---------------------------------------------------------------------------
# URL hygiene helpers
# ---------------------------------------------------------------------------

def _normalize_url(href: str) -> str:
    """
    Normalise a raw href attribute into an absolute URL, or return "" if invalid.

    Handles:
    - scheme-less URLs (//example.com/path  →  https://example.com/path)
    - javascript: URIs     → rejected ("")
    - mailto: / tel: URIs  → rejected ("")
    - relative paths       → rejected ("") — no base URL available here
    """
    href = href.strip()
    if not href:
        return ""
    lower = href.lower()
    # Reject non-HTTP schemes
    if lower.startswith(("javascript:", "mailto:", "tel:", "data:")):
        return ""
    # Normalise scheme-less absolute URLs
    if href.startswith("//"):
        href = "https:" + href
    # Reject relative paths (no domain present)
    if not href.startswith(("http://", "https://")):
        return ""
    return href


def _is_share_link(url: str) -> bool:
    """
    Return True if the URL is a social share/intent link rather than a company page.
    Share links are emitted by share-buttons; they carry the *visitor's* content
    to social media and are not the company's own social profile.
    """
    try:
        parsed = urllib.parse.urlparse(url)
        path_lower = parsed.path.lower()
        query_lower = parsed.query.lower()
    except Exception:
        return False
    for frag in _SHARE_PATH_FRAGMENTS:
        if frag in path_lower:
            return True
    # LinkedIn share URLs carry ?url= or ?mini=true parameters
    if "linkedin.com" in url.lower() and ("shareArticle" in url or "mini=true" in query_lower):
        return True
    return False


def _is_valid_social_url(url: str) -> bool:
    """
    Return True if url is a usable company social-media page (not a share link,
    not a javascript: URI, not scheme-less, not a social login/signup page).
    """
    if not url:
        return False
    if _is_share_link(url):
        return False
    normalized = _normalize_url(url)
    return bool(normalized)


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------

def fetch_html(url: str) -> str | None:
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers=_HEADERS)
        if resp.status_code == 200:
            return resp.text
    except Exception as exc:
        print(f"[Workflow 5.8]   fetch error {url}: {exc}")
    return None


def extract_meta_description(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for attrs in [{"name": "description"}, {"property": "og:description"}]:
        tag = soup.find("meta", attrs=attrs)
        if tag and tag.get("content", "").strip():
            return tag["content"].strip()
    return ""


def extract_headlines(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    seen: set[str] = set()
    results: list[str] = []
    for tag in soup.find_all(["h1", "h2", "h3"]):
        text = tag.get_text(separator=" ", strip=True)
        if len(text) > 10 and text not in seen:
            seen.add(text)
            results.append(text)
        if len(results) >= MAX_ITEMS:
            break
    return results


def extract_social_links(html: str) -> tuple[list[dict], dict[str, int]]:
    """
    Extract social-media profile links from page HTML.

    Returns:
        (links, counts) where counts has keys:
            extracted  — raw hrefs seen that matched a social domain
            normalized — scheme-less hrefs that were fixed
            skipped    — hrefs rejected (share links, javascript:, etc.)
            accepted   — links returned in the result list
    """
    soup = BeautifulSoup(html, "html.parser")
    found: dict[str, str] = {}
    counts = {"extracted": 0, "normalized": 0, "skipped": 0, "accepted": 0}

    for a in soup.find_all("a", href=True):
        raw = (a["href"] or "").strip()
        matched_platform = None
        for platform, domain in SOCIAL_DOMAINS.items():
            if platform not in found and domain in raw.lower():
                matched_platform = platform
                break
        if matched_platform is None:
            continue

        counts["extracted"] += 1
        if raw.startswith("//"):
            counts["normalized"] += 1
        url = _normalize_url(raw)
        if not url or not _is_valid_social_url(url):
            counts["skipped"] += 1
            continue
        found[matched_platform] = url

        if len(found) >= MAX_SOCIAL:
            break

    links = [{"platform": p, "url": u} for p, u in found.items()]
    counts["accepted"] = len(links)
    return links, counts


# ---------------------------------------------------------------------------
# Per-page signal extraction
# ---------------------------------------------------------------------------

def _page_signals(url: str) -> dict | None:
    html = fetch_html(url)
    if not html:
        return None
    headlines = extract_headlines(html)
    meta = extract_meta_description(html)
    if meta and len(meta) > 10 and meta not in headlines:
        headlines = [meta] + headlines
    headlines = headlines[:MAX_ITEMS]
    if not headlines:
        return None
    return {"url": url, "headlines": headlines}


def _social_signals(social_link: dict) -> dict | None:
    html = fetch_html(social_link["url"])
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    snippets: list[str] = []
    meta = extract_meta_description(html)
    if meta and len(meta) > 10:
        snippets.append(meta)
    for tag in soup.find_all(["h1", "h2", "title"]):
        text = tag.get_text(strip=True)
        if len(text) > 10 and text not in snippets:
            snippets.append(text)
        if len(snippets) >= MAX_ITEMS:
            break
    if not snippets:
        return None
    return {
        "platform": social_link["platform"],
        "url":      social_link["url"],
        "snippets": snippets[:MAX_ITEMS],
    }


# ---------------------------------------------------------------------------
# Per-company collection
# ---------------------------------------------------------------------------

def collect_company_signals(record: dict) -> tuple[dict, dict]:
    """
    Collect signals for one company.

    Returns:
        (signals_dict, url_counts) where url_counts has keys:
            extracted, normalized, skipped, accepted, fetched
    """
    website = (record.get("website") or "").strip()
    result: dict = {
        "company_name":   record.get("company_name", ""),
        "website":        website,
        "place_id":       record.get("place_id", ""),
        "signal_sources": {"website": [], "social": []},
    }
    url_counts = {"extracted": 0, "normalized": 0, "skipped": 0, "accepted": 0, "fetched": 0}
    if not website:
        return result, url_counts

    homepage_html = fetch_html(website)
    social_links: list[dict] = []
    if homepage_html:
        social_links, link_counts = extract_social_links(homepage_html)
        for k in ("extracted", "normalized", "skipped", "accepted"):
            url_counts[k] += link_counts[k]

    pages_fetched = 0
    for path in SIGNAL_PATHS:
        if pages_fetched >= MAX_PAGES:
            break
        url = urllib.parse.urljoin(website, path)
        signals = _page_signals(url)
        url_counts["fetched"] += 1
        time.sleep(0.3)
        if signals:
            result["signal_sources"]["website"].append(signals)
            pages_fetched += 1

    for link in social_links[:MAX_SOCIAL]:
        signals = _social_signals(link)
        url_counts["fetched"] += 1
        time.sleep(0.3)
        if signals:
            result["signal_sources"]["social"].append(signals)

    return result, url_counts


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def load_enriched_leads(limit: int = 0, in_path: Path | None = None) -> list[dict]:
    path = in_path or Path(str(ENRICHED_LEADS_FILE))
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return rows[:limit] if limit else rows


def save_raw_signals(signals: list[dict], out_path: Path | None = None) -> None:
    # Use explicit path when provided; fall back to _RunPath snapshot for
    # backward compatibility (standalone / test invocations).
    if out_path is None:
        import os as _os
        out_path = Path(_os.fspath(RESEARCH_SIGNAL_RAW_FILE))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(signals, f, indent=2, ensure_ascii=False)
    print(f"[Workflow 5.8] Saved {len(signals)} raw records → {out_path}")


# ---------------------------------------------------------------------------
# Public run()
# ---------------------------------------------------------------------------

def run(limit: int = 0, paths: RunPaths | None = None) -> list[dict]:
    """
    Collect signals for all enriched leads.

    Args:
        limit: cap on leads to process (0 = all)
        paths: explicit RunPaths from campaign_runner; if None, fetched from
               the active global (standalone / backward-compat invocation).
    """
    if paths is None:
        from config.run_paths import require_active_run_paths
        paths = require_active_run_paths()

    # Choose input: prefer verified enriched leads when available
    in_path = (
        paths.verified_enriched_leads_file
        if paths.verified_enriched_leads_file.exists()
        else paths.enriched_leads_file
    )
    print(f"[Workflow 5.8] Input: {in_path}")

    leads = load_enriched_leads(limit=limit, in_path=in_path)
    if not leads:
        print("[Workflow 5.8] No enriched leads found — writing empty raw signals file.")
        save_raw_signals([], out_path=paths.research_signal_raw_file)
        return []

    print(f"[Workflow 5.8] Collecting signals for {len(leads)} leads...")

    seen_place_ids: set[str] = set()
    results: list[dict] = []

    total_url_counts = {"extracted": 0, "normalized": 0, "skipped": 0, "accepted": 0, "fetched": 0}

    for i, lead in enumerate(leads, 1):
        place_id = lead.get("place_id", "")
        if place_id and place_id in seen_place_ids:
            continue
        if place_id:
            seen_place_ids.add(place_id)

        name = lead.get("company_name") or lead.get("website", f"record {i}")
        print(f"[Workflow 5.8] ({i}/{len(leads)}) {name}")

        signals, url_counts = collect_company_signals(lead)
        results.append(signals)
        for k in total_url_counts:
            total_url_counts[k] += url_counts[k]

        n_web    = len(signals["signal_sources"]["website"])
        n_social = len(signals["signal_sources"]["social"])
        skipped  = url_counts["skipped"]
        skipped_note = f", {skipped} social link(s) skipped" if skipped else ""
        print(f"[Workflow 5.8]   → {n_web} page(s), {n_social} social link(s){skipped_note}")

        time.sleep(CRAWL_DELAY)

    save_raw_signals(results, out_path=paths.research_signal_raw_file)

    c = total_url_counts
    print(
        f"\n[Workflow 5.8] Signal collection complete — {len(results)} companies\n"
        f"  URL hygiene summary:\n"
        f"    Social links extracted : {c['extracted']}\n"
        f"    Scheme-less normalised : {c['normalized']}\n"
        f"    Share/junk links skipped: {c['skipped']}\n"
        f"    Accepted social links  : {c['accepted']}\n"
        f"    Total fetches (pages+social): {c['fetched']}"
    )

    return results


if __name__ == "__main__":
    run()
