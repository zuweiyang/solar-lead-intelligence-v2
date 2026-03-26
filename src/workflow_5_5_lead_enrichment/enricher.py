# Workflow 5.5: Lead Enrichment
# Finds key decision-makers (KPs) and their emails for qualified leads.
#
# Waterfall strategy:
#   1. Apollo.io People Search  → KP + email (requires paid plan)
#   2. Hunter.io Domain Search  → KP + email (fallback)
#   3. Website contact          → real email scraped from company website (Step 3)
#   4. Mock data                → tag: mock  (when both keys absent, for testing)
#   5. Empty                    → tag: none  (live run, all strategies failed)
#
# NOTE: Apollo People Search (/api/v1/mixed_people/search) requires a paid plan.
# Free plan supports /api/v1/organizations/enrich only.

import csv
import json
import re
import time
from pathlib import Path
from urllib.parse import unquote
import tldextract
import requests

from config.settings import (
    QUALIFIED_LEADS_FILE,
    ENRICHED_LEADS_FILE,
    ENRICHED_CONTACTS_FILE,
    COMPANY_TEXT_FILE,
    APOLLO_API_KEY,
    HUNTER_API_KEY,
)
from config.run_paths import RunPaths
from src.market_localization import (
    get_generic_guess_local_parts,
    get_generic_mailbox_local_parts,
)

REQUEST_TIMEOUT  = 10
RATE_LIMIT_DELAY = 1.5   # seconds between Apollo calls
HUNTER_DELAY     = 3.0   # Hunter free plan: stricter rate limit

# ---------------------------------------------------------------------------
# Per-run provider health tracking
# ---------------------------------------------------------------------------
# When a provider returns HTTP 429, it is marked rate-limited for the rest of
# this process invocation so subsequent companies skip it immediately instead
# of burning quota on retries that will also fail.
_PROVIDER_RATE_LIMITED: dict[str, bool] = {}

# Per-run counters — accumulate across all enrich_lead_multi() calls.
# Read via get_enrichment_counters() for end-of-run summaries.
_ENRICHMENT_COUNTERS: dict[str, int] = {
    "apollo_attempts":      0,
    "apollo_ok":            0,
    "apollo_rate_limited":  0,
    "apollo_errors":        0,
    "hunter_attempts":      0,
    "hunter_ok":            0,
    "hunter_rate_limited":  0,
    "hunter_errors":        0,
    "website_ok":           0,
    "guessed_ok":           0,
    "mock_ok":              0,
    "none_ok":              0,
}


def _inc(key: str, n: int = 1) -> None:
    _ENRICHMENT_COUNTERS[key] = _ENRICHMENT_COUNTERS.get(key, 0) + n


def get_enrichment_counters() -> dict[str, int]:
    """Return a snapshot of the current run's enrichment counters."""
    return dict(_ENRICHMENT_COUNTERS)


def _is_rate_limit_error(exc: Exception) -> bool:
    """Return True if the exception indicates an HTTP 429 rate-limit response."""
    import requests as _requests
    if isinstance(exc, _requests.exceptions.HTTPError):
        resp = getattr(exc, "response", None)
        if resp is not None and getattr(resp, "status_code", None) == 429:
            return True
    # Some callers wrap the error in a generic Exception with the status in the message
    return "429" in str(exc)


def _mark_rate_limited(provider: str, domain: str) -> None:
    """Mark provider as rate-limited for this run and log clearly."""
    if not _PROVIDER_RATE_LIMITED.get(provider):
        _PROVIDER_RATE_LIMITED[provider] = True
        print(
            f"[Workflow 5.5] RATE_LIMITED — {provider} returned HTTP 429 for {domain}. "
            f"Skipping {provider} for all remaining leads this run."
        )

TARGET_TITLES = [
    "CEO", "Founder", "Owner", "President",
    "Managing Director", "MD", "Director",
    "Country Manager", "Regional Manager",
    "Head of Operations", "Operations Manager", "Operations Director",
    "Procurement", "Purchasing", "Procurement Manager",
    "Commercial Manager", "Commercial Director",
    "Business Development", "Business Development Manager",
    "Project Manager", "Engineering Manager", "Technical Manager",
    "General Manager", "Partner",
]

# Title substrings that immediately disqualify a contact — clearly non-buyer roles.
# Apollo's person_titles filter is advisory; we re-validate what it returns.
_REJECT_TITLE_FRAGMENTS: frozenset[str] = frozenset({
    # Medical / healthcare
    "nurse", "doctor", "physician", "dentist", "surgeon", "therapist",
    "pharmacist", "radiologist", "paramedic", "midwife", "healthcare",
    # Education
    "teacher", "professor", "lecturer", "instructor", "tutor", "principal",
    "dean", "academic",
    # Finance / admin (non-exec)
    "accountant", "bookkeeper", "auditor", "tax advisor",
    "receptionist", "secretary", "administrative assistant",
    # Design / creative
    "graphic designer", "graphic artist", "illustrator", "animator",
    "photographer", "videographer",
    # Technology (non-solar)
    "software developer", "software engineer", "web developer", "programmer",
    "it support", "it technician", "system administrator", "sysadmin",
    # HR / recruitment
    "human resources", " hr ", "recruiter", "talent acquisition",
    "people operations",
    # Legal
    "lawyer", "attorney", "solicitor", "legal counsel", "paralegal",
    # Real estate
    "real estate agent", "real estate broker", "property manager",
    # Marketing / social (not decision-makers for hardware procurement)
    "marketing coordinator", "social media", "content manager", "brand manager",
    "digital marketing", "marketing specialist", "seo specialist",
    # Junior / support roles
    "intern", "trainee", "junior", "apprentice",
    "customer service", "customer support", "service advisor",
    "quality control", "warehouse", "logistics coordinator",
    "data analyst", "business analyst",
    # Other clearly off-target
    "chef", "cook", "barista", "waitress", "waiter", "bartender",
    "driver", "delivery", "security guard",
})

# Positive buyer-persona signals — titles containing any of these are strongly preferred.
# Contacts whose titles match none of these and have an otherwise unknown role
# will be deprioritised in favour of a clean website contact.
_BUYER_PERSONA_FRAGMENTS: frozenset[str] = frozenset({
    "ceo", "founder", "owner", "president", "managing director", " md",
    "director", "partner", "principal", "head of", "chief",
    "general manager", "country manager", "regional manager",
    "procurement", "purchasing", "commercial", "business development",
    "operations manager", "operations director", "project manager",
    "engineering manager", "technical manager", "technical director",
})


def _is_valid_kp_name(name: str) -> bool:
    """Return True if name is a usable contact name (2+ chars, contains letters)."""
    if not name:
        return False
    stripped = name.strip()
    return len(stripped) >= 2 and any(c.isalpha() for c in stripped)


def _contact_domain_trusted(email: str, website: str, source: str) -> dict:
    """
    For Apollo-sourced contacts, verify the email domain matches the company
    website domain.  A mismatch means Apollo returned a contact from a different
    organisation (false match).

    Returns dict with contact_trust and skip_reason keys.
    Non-Apollo sources are unconditionally trusted — domain can't be verified.
    """
    if source != "apollo" or not email or "@" not in email or not website:
        return {"contact_trust": "trusted", "skip_reason": ""}

    email_domain   = _domain(email.split("@", 1)[1])
    website_domain = _domain(website)

    if not email_domain or not website_domain:
        return {"contact_trust": "trusted", "skip_reason": ""}

    if email_domain != website_domain:
        print(
            f"[Workflow 5.5]   Contact domain mismatch: "
            f"email={email_domain} vs website={website_domain} — marking low_trust"
        )
        return {"contact_trust": "low_trust", "skip_reason": "contact_domain_mismatch"}

    return {"contact_trust": "trusted", "skip_reason": ""}


def _title_is_buyer_persona(title: str) -> bool:
    """Return True if the title contains a strong buyer-persona signal.

    Used to prefer named contacts with decision-making authority over contacts
    that passed _title_is_relevant() but are weak procurement targets
    (e.g. 'Sales Executive', 'Branch Coordinator').
    """
    if not title:
        return False
    lower = title.lower()
    return any(fragment in lower for fragment in _BUYER_PERSONA_FRAGMENTS)

# Local-parts that identify generic/alias mailboxes (not personal contacts).
_GENERIC_LOCAL_PARTS: frozenset[str] = frozenset(get_generic_mailbox_local_parts())
_PLACEHOLDER_EMAIL_DOMAINS: frozenset[str] = frozenset({
    "dominio.com.br",
    "empresa.com",
    "example.com",
    "exemplo.com.br",
    "domain.com",
})
_PLACEHOLDER_EMAIL_LOCALS: frozenset[str] = frozenset({
    "seuemail",
    "email",
    "example",
    "exemplo",
    "test",
})


def _is_generic_mailbox(email: str) -> bool:
    """Return True when the email local-part is a generic/alias address (e.g. info@, sales@)."""
    if not email or "@" not in email:
        return False
    return email.split("@")[0].lower().strip() in _GENERIC_LOCAL_PARTS


def _clean_site_email(email: str) -> str | None:
    """Normalize website-scraped emails and drop placeholders/assets."""
    if not email:
        return None
    addr = unquote(email).strip().lower()
    addr = addr.lstrip("%20").lstrip()
    addr = addr.lstrip("0123456789") if addr[:2].isdigit() else addr
    addr = addr.strip(" <>\"'(),;")
    addr = addr.lstrip(".-_")
    if "@" not in addr:
        return None
    if not re.fullmatch(r"[\w.+\-]+@(?:[\w\-]+\.)+[a-zA-Z]{2,24}", addr, re.ASCII):
        return None
    if addr.endswith((".png", ".jpg", ".jpeg", ".gif", ".css", ".js", ".svg", ".webp")):
        return None
    local, domain = addr.split("@", 1)
    if local in _PLACEHOLDER_EMAIL_LOCALS or domain in _PLACEHOLDER_EMAIL_DOMAINS:
        return None
    return addr


ENRICHED_FIELDS = [
    # original qualified_leads fields
    "company_name", "website", "place_id",
    "company_type", "market_focus", "services_detected",
    "confidence_score", "classification_method", "lead_score", "score_breakdown",
    "target_tier",
    # appended by enrichment
    "kp_name", "kp_title", "kp_email", "enrichment_source",
    # website-scraped contact info (populated when enrichment_source == "website")
    "site_phone",
    "whatsapp_phone",
    # contact channel labels — computed post-enrichment
    "email_sendable", "contact_channel", "alt_outreach_possible",
    "manual_outreach_channel", "manual_outreach_highlight",
    # contact trust / skip signals — computed post-enrichment
    "contact_trust", "skip_reason",
]


# Extended field list for multi-contact output (enriched_contacts.csv).
# Superset of ENRICHED_FIELDS — backward-compat consumers only see ENRICHED_FIELDS.
ENRICHED_CONTACTS_FIELDS = ENRICHED_FIELDS + [
    "contact_rank",        # 1 = primary/best, 2 = backup, 3 = tertiary
    "is_generic_mailbox",  # "true" when email is an alias like info@, sales@
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _domain(url: str) -> str:
    """Extract registrable domain from a URL, e.g. 'example.com'."""
    ext = tldextract.extract(url)
    return f"{ext.domain}.{ext.suffix}".lower() if ext.domain else ""


def _title_matches(title: str) -> bool:
    title_lower = title.lower()
    return any(t.lower() in title_lower for t in TARGET_TITLES)


def _title_is_relevant(title: str) -> bool:
    """Return False when the title is clearly unrelated to solar procurement.

    Apollo's person_titles filter is advisory — it may return approximate matches
    or fall back to any person when exact-title results are scarce.  This check
    prevents obviously wrong roles (e.g. Registered Nurse) from being accepted.

    No title → True: we don't reject unknown roles, just clearly wrong ones.
    """
    if not title:
        return True
    lower = title.lower()
    return not any(fragment in lower for fragment in _REJECT_TITLE_FRAGMENTS)


# ---------------------------------------------------------------------------
# Apollo.io
# ---------------------------------------------------------------------------

def _apollo_org_enrich(domain: str) -> dict:
    """
    Enrich an organisation by domain (available on Apollo free plan).
    Returns org dict with keys: name, linkedin_url, estimated_num_employees, etc.
    Returns {} on any failure.
    """
    headers = {
        "Content-Type":  "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key":     APOLLO_API_KEY,
    }
    resp = requests.post(
        "https://api.apollo.io/api/v1/organizations/enrich",
        json={"domain": domain},
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json().get("organization", {})


def _apollo_reveal_email(person_id: str) -> str:
    """
    Reveal the work email for a person by ID via people/match.
    Returns email string or empty string if not found.
    """
    headers = {
        "Content-Type":  "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key":     APOLLO_API_KEY,
    }
    resp = requests.post(
        "https://api.apollo.io/api/v1/people/match",
        json={"id": person_id, "reveal_personal_emails": False},
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )
    if resp.status_code != 200:
        return ""
    return resp.json().get("person", {}).get("email", "") or ""


def _apollo_people_search(domain: str) -> dict | None:
    """
    Two-step Apollo KP lookup (paid plan):
      1. api_search  → find best-matching person by title, get ID + name
      2. people/match → reveal work email using the person ID

    Docs: https://docs.apollo.io/reference/people-api-search
    """
    headers = {
        "Content-Type":  "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key":     APOLLO_API_KEY,
    }
    resp = requests.post(
        "https://api.apollo.io/api/v1/mixed_people/api_search",
        json={
            "q_organization_domains_list": [domain],
            "person_titles":               TARGET_TITLES,
            "per_page":                    5,
        },
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )
    if resp.status_code == 403:
        return None   # free plan — fall through silently
    resp.raise_for_status()

    people = resp.json().get("people", [])

    # Two-pass: first try buyer-persona contacts, then fall back to any valid contact.
    # This ensures a founder/CEO is returned in preference to a sales executive.
    for require_persona in (True, False):
        for person in people:
            person_id = person.get("id", "")
            if not person_id:
                continue

            title = person.get("title", "") or ""
            if not _title_is_relevant(title):
                print(f"[Workflow 5.5]   Skipping irrelevant title: '{title}'")
                continue

            if require_persona and not _title_is_buyer_persona(title):
                continue   # first pass: skip non-buyer-persona matches

            email = person.get("email", "") or _apollo_reveal_email(person_id)
            if not email or "@" not in email:
                continue

            first = person.get("first_name", "") or ""
            last  = person.get("last_name",  "") or ""
            if require_persona:
                pass  # strong match
            else:
                print(f"[Workflow 5.5]   Accepting non-persona contact (no better match): '{title}'")
            return {
                "kp_name":  f"{first} {last}".strip(),
                "kp_title": title,
                "kp_email": email,
            }
    return None


def _query_apollo(domain: str) -> dict | None:
    """
    Full Apollo waterfall:
    1. org/enrich  → attach LinkedIn URL to lead metadata (always run)
    2. people/search → KP + email (paid plan only; silently skipped on free)
    Returns KP dict if found, None otherwise.
    """
    # Org enrichment runs regardless of plan — data useful for Workflow 6
    try:
        _apollo_org_enrich(domain)   # result stored if needed later
    except Exception:
        pass

    return _apollo_people_search(domain)


def _apollo_people_search_multi(domain: str, max_results: int = 3) -> list[dict]:
    """
    Multi-contact variant of _apollo_people_search.
    Returns up to max_results valid contacts, buyer personas ranked first.
    """
    headers = {
        "Content-Type":  "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key":     APOLLO_API_KEY,
    }
    resp = requests.post(
        "https://api.apollo.io/api/v1/mixed_people/api_search",
        json={
            "q_organization_domains_list": [domain],
            "person_titles":               TARGET_TITLES,
            "per_page":                    5,
        },
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )
    if resp.status_code == 403:
        return []
    resp.raise_for_status()

    people  = resp.json().get("people", [])
    results: list[dict] = []

    for require_persona in (True, False):
        for person in people:
            if len(results) >= max_results:
                break
            person_id = person.get("id", "")
            if not person_id:
                continue
            title = person.get("title", "") or ""
            if not _title_is_relevant(title):
                continue
            if require_persona and not _title_is_buyer_persona(title):
                continue
            email = person.get("email", "") or _apollo_reveal_email(person_id)
            if not email or "@" not in email:
                continue
            if any(r["kp_email"] == email for r in results):
                continue  # dedup within this company
            first = person.get("first_name", "") or ""
            last  = person.get("last_name",  "") or ""
            results.append({
                "kp_name":  f"{first} {last}".strip(),
                "kp_title": title,
                "kp_email": email,
            })
        if len(results) >= max_results:
            break

    return results


# ---------------------------------------------------------------------------
# Hunter.io
# ---------------------------------------------------------------------------

def _query_hunter_multi(domain: str, max_results: int = 3) -> list[dict]:
    """
    Multi-contact variant of _query_hunter.
    Returns up to max_results valid contacts sorted by buyer-persona match then confidence.
    """
    url = "https://api.hunter.io/v2/domain-search"
    params = {
        "domain":  domain,
        "api_key": HUNTER_API_KEY,
        "limit":   10,
        "type":    "personal",
    }
    resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json().get("data", {})

    emails = data.get("emails", [])
    emails.sort(key=lambda e: (
        _title_is_buyer_persona(e.get("position") or ""),
        _title_matches(e.get("position") or ""),
        e.get("confidence", 0),
    ), reverse=True)

    results: list[dict] = []
    for entry in emails:
        if len(results) >= max_results:
            break
        email = entry.get("value", "")
        if not email or "@" not in email:
            continue
        title = entry.get("position") or ""
        if not _title_is_relevant(title):
            continue
        first = entry.get("first_name") or ""
        last  = entry.get("last_name")  or ""
        results.append({
            "kp_name":  f"{first} {last}".strip(),
            "kp_title": title,
            "kp_email": email,
        })
    return results


def _query_hunter(domain: str) -> dict | None:
    """
    Call Hunter.io Domain Search and return the highest-confidence email.
    Returns dict with name/title/email or None on failure / no match.

    Docs: https://hunter.io/api-documentation/v2#domain-search
    """
    url = "https://api.hunter.io/v2/domain-search"
    params = {
        "domain":  domain,
        "api_key": HUNTER_API_KEY,
        "limit":   10,
        "type":    "personal",
    }
    resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json().get("data", {})

    emails = data.get("emails", [])
    # Sort by buyer-persona match first, then confidence descending
    emails.sort(key=lambda e: (
        _title_is_buyer_persona(e.get("position") or ""),
        _title_matches(e.get("position") or ""),
        e.get("confidence", 0),
    ), reverse=True)

    for entry in emails:
        email = entry.get("value", "")
        if not email or "@" not in email:
            continue
        title = entry.get("position") or ""
        if not _title_is_relevant(title):
            print(f"[Workflow 5.5]   Skipping irrelevant title (Hunter): '{title}'")
            continue
        first = entry.get("first_name") or ""
        last  = entry.get("last_name") or ""
        return {
            "kp_name":  f"{first} {last}".strip(),
            "kp_title": title,
            "kp_email": email,
        }
    return None


# ---------------------------------------------------------------------------
# Website contact lookup (uses emails scraped by Workflow 3)
# ---------------------------------------------------------------------------

_site_contact_cache: dict[str, dict] | None = None   # place_id/website → contact
_site_contact_cache_source: str | None = None


def _load_site_contacts() -> dict[str, dict]:
    """
    Load site_emails, site_phones, and whatsapp_phones from company_text.json
    (written by Workflow 3).
    Returns lookup dict keyed by both place_id and normalised website domain.
    """
    global _site_contact_cache, _site_contact_cache_source
    company_text_path = str(Path(str(COMPANY_TEXT_FILE)).resolve())
    if _site_contact_cache is not None and _site_contact_cache_source == company_text_path:
        return _site_contact_cache

    _site_contact_cache = {}
    _site_contact_cache_source = company_text_path
    if not COMPANY_TEXT_FILE.exists():
        return _site_contact_cache

    try:
        with open(COMPANY_TEXT_FILE, encoding="utf-8") as f:
            records = json.load(f)
    except Exception:
        return _site_contact_cache

    for r in records:
        raw_emails = r.get("site_emails") or []
        emails: list[str] = []
        for email in raw_emails:
            cleaned = _clean_site_email(email)
            if cleaned and cleaned not in emails:
                emails.append(cleaned)
        phones = r.get("site_phones") or []
        whatsapp_phones = r.get("whatsapp_phones") or []
        if not emails and not phones and not whatsapp_phones:
            continue
        entry = {
            "site_emails": emails,
            "site_phones": phones,
            "whatsapp_phones": whatsapp_phones,
        }
        if r.get("place_id"):
            _site_contact_cache[r["place_id"]] = entry
        domain = _domain(r.get("website", ""))
        if domain:
            _site_contact_cache[domain] = entry

    return _site_contact_cache


def _query_website_contact(lead: dict) -> dict | None:
    """
    Return contact info scraped directly from the company website.
    Uses the first real email found on the site; phone stored as kp_title note.
    """
    contacts = _load_site_contacts()

    # Try by place_id first, then normalised domain
    place_id = lead.get("place_id", "")
    domain   = _domain(lead.get("website", ""))
    entry    = contacts.get(place_id) or contacts.get(domain)

    if not entry:
        return None

    emails = entry.get("site_emails", [])
    phones = entry.get("site_phones", [])
    whatsapp_phones = entry.get("whatsapp_phones", [])

    if not emails:
        return None

    email = emails[0]
    phone_note = f" | Tel: {phones[0]}" if phones else ""

    return {
        "kp_name":  "",
        "kp_title": f"Contact (website){phone_note}",
        "kp_email": email,
        "site_phones": phones,
        "whatsapp_phones": whatsapp_phones,
    }


def _query_website_contact_multi(lead: dict, max_results: int = 3) -> list[dict]:
    """
    Multi-contact variant of _query_website_contact.
    Returns up to max_results contacts from site_emails (first entry carries site_phones).
    """
    contacts = _load_site_contacts()

    place_id = lead.get("place_id", "")
    domain   = _domain(lead.get("website", ""))
    entry    = contacts.get(place_id) or contacts.get(domain)

    if not entry:
        return []

    emails = entry.get("site_emails", [])
    phones = entry.get("site_phones", [])
    whatsapp_phones = entry.get("whatsapp_phones", [])

    results: list[dict] = []
    for i, email in enumerate(emails[:max_results]):
        phone_note = f" | Tel: {phones[0]}" if phones and i == 0 else ""
        kp: dict = {
            "kp_name":  "",
            "kp_title": f"Contact (website){phone_note}",
            "kp_email": email,
        }
        if i == 0 and phones:
            kp["site_phones"] = phones
        if i == 0 and whatsapp_phones:
            kp["whatsapp_phones"] = whatsapp_phones
        results.append(kp)
    return results


# ---------------------------------------------------------------------------
# Email pattern guesser (no API required, ~30% hit rate for small businesses)
# ---------------------------------------------------------------------------

# Common email patterns ordered by frequency in B2B contacts
_EMAIL_PATTERNS = [
    "{first}@{domain}",
    "{first}.{last}@{domain}",
    "{first}{last}@{domain}",
    "info@{domain}",
    "contact@{domain}",
    "sales@{domain}",
]

_GENERIC_KP_TITLES = ["Owner", "CEO", "Founder"]

# Domains that are NOT company email domains.
# WhatsApp / Telegram redirects and social/link-shortener domains appear as
# "website" values in Google Places listings but produce undeliverable guesses.
_GUESS_DOMAIN_BLOCKLIST: frozenset[str] = frozenset({
    "wa.me",          # WhatsApp redirect — not an email domain
    "whatsapp.com",
    "t.me",           # Telegram
    "telegram.me",
    "bit.ly",         # link shorteners
    "tinyurl.com",
    "goo.gl",
    "linktr.ee",      # Linktree profile
    "linktree.com",
    "facebook.com",
    "fb.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "linkedin.com",
    "youtube.com",
})


# Messaging-redirect domains that signal a non-email primary contact channel.
_MESSAGING_REDIRECT_DOMAINS: frozenset[str] = frozenset({
    "wa.me", "whatsapp.com", "t.me", "telegram.me",
})


def _derive_whatsapp_phone(
    country: str,
    site_phone: str,
    website: str,
    whatsapp_phones: list[str] | None = None,
) -> str:
    """
    Preserve an explicit WhatsApp number when present.
    For any market, if Workflow 3 already detected a WhatsApp signal and carried
    a phone number through, preserve that number as a WhatsApp-capable manual
    contact channel.
    """
    whatsapp_phones = whatsapp_phones or []
    if whatsapp_phones:
        return whatsapp_phones[0]
    web_domain = _domain(website)
    if web_domain in _MESSAGING_REDIRECT_DOMAINS and site_phone:
        return site_phone
    return ""


def _contact_labels(kp_email: str, site_phone: str, whatsapp_phone: str, website: str) -> dict:
    """
    Derive contact-channel metadata after enrichment is complete.

    email_sendable        — True when a valid email address was found.
    contact_channel       — Best available outreach channel for this lead.
    alt_outreach_possible — True when a non-email channel (phone/messaging)
                            is available even though no email was found.
    manual_outreach_channel — Explicit non-email contact option to highlight in
                              tables/exports even when email remains primary.
    manual_outreach_highlight — True when phone/WhatsApp/messaging is available.
    """
    has_email    = bool(kp_email and "@" in kp_email)
    has_phone    = bool(site_phone)
    has_whatsapp = bool(whatsapp_phone)
    web_domain   = _domain(website)
    is_messaging = web_domain in _MESSAGING_REDIRECT_DOMAINS

    if has_email:
        channel = "email"
    elif has_whatsapp:
        channel = "whatsapp"
    elif has_phone:
        channel = "phone"
    elif is_messaging:
        channel = "whatsapp" if "wa" in web_domain else "messaging"
    else:
        channel = "none"

    if has_whatsapp and has_phone:
        manual_channel = "phone+whatsapp"
    elif has_whatsapp:
        manual_channel = "whatsapp"
    elif has_phone:
        manual_channel = "phone"
    elif is_messaging:
        manual_channel = "messaging"
    else:
        manual_channel = "none"

    return {
        "email_sendable":        "true"  if has_email else "false",
        "contact_channel":       channel,
        "alt_outreach_possible": "true"  if (not has_email and (has_phone or has_whatsapp or is_messaging)) else "false",
        "manual_outreach_channel": manual_channel,
        "manual_outreach_highlight": "true" if manual_channel != "none" else "false",
    }


def _guess_email(domain: str, index: int = 0, country: str = "") -> dict:
    """
    Legacy helper that constructs a guessed contact email from domain patterns.
    New runs no longer use guessed emails for enrichment or sending.
    """
    # Role-based addresses are the most reliable guesses
    guess_locals = get_generic_guess_local_parts(country)
    role = guess_locals[index % len(guess_locals)]
    return {
        "kp_name":  "",
        "kp_title": _GENERIC_KP_TITLES[index % len(_GENERIC_KP_TITLES)],
        "kp_email": f"{role}@{domain}",
    }


# ---------------------------------------------------------------------------
# Mock fallback (smoke-test mode — no API keys required)
# ---------------------------------------------------------------------------

_MOCK_TITLES = [
    "Owner", "CEO", "Founder", "President", "Operations Manager",
]


def _mock_kp(company_name: str, domain: str, index: int) -> dict:
    """Generate deterministic fake KP data for smoke testing."""
    first_names = ["James", "Sarah", "Michael", "Lisa", "David"]
    last_names  = ["Smith", "Johnson", "Williams", "Brown", "Jones"]
    i = index % 5
    name  = f"{first_names[i]} {last_names[i]}"
    title = _MOCK_TITLES[i]
    local = name.lower().replace(" ", ".")
    return {
        "kp_name":  name,
        "kp_title": title,
        "kp_email": f"{local}@{domain}",
    }


# ---------------------------------------------------------------------------
# Multi-contact waterfall
# ---------------------------------------------------------------------------

def _make_contact_row(row: dict, rank: int) -> dict:
    """Attach multi-contact metadata fields to a contact row."""
    result = dict(row)
    result["contact_rank"]       = rank
    result["is_generic_mailbox"] = "true" if _is_generic_mailbox(result.get("kp_email", "")) else "false"
    return result


def enrich_lead_multi(lead: dict, index: int = 0, max_contacts: int = 3) -> list[dict]:
    """
    Multi-contact enrichment waterfall: returns up to max_contacts contacts per company.

    Contact list is ordered by preference (rank=1 is most actionable, identical to what
    enrich_lead() returns).  Each contact dict contains all ENRICHED_FIELDS keys plus
    contact_rank and is_generic_mailbox.

    Slot-filling order: Apollo → Hunter → website → mock (no keys).
    """
    domain = _domain(lead.get("website", ""))
    base   = {
        **lead,
        "kp_name": "",
        "kp_title": "",
        "kp_email": "",
        "enrichment_source": "none",
        "site_phone": lead.get("site_phone", ""),
        "whatsapp_phone": lead.get("whatsapp_phone", ""),
    }

    if not domain:
        return [_make_contact_row({**base}, rank=1)]

    contacts: list[tuple[dict, str]] = []  # (kp_fields_dict, source_name)

    # Step 1 — Apollo People Search (paid plan) + org enrich (free plan, side-effect)
    if APOLLO_API_KEY and not _PROVIDER_RATE_LIMITED.get("apollo"):
        _inc("apollo_attempts")
        try:
            try:
                _apollo_org_enrich(domain)
            except Exception:
                pass
            kps = _apollo_people_search_multi(domain, max_results=max_contacts)
            for kp in kps:
                if len(contacts) >= max_contacts:
                    break
                contacts.append((kp, "apollo"))
            if contacts:
                _inc("apollo_ok")
            time.sleep(RATE_LIMIT_DELAY)
        except Exception as exc:
            if _is_rate_limit_error(exc):
                _mark_rate_limited("apollo", domain)
                _inc("apollo_rate_limited")
            else:
                _inc("apollo_errors")
                print(f"[Workflow 5.5]   Apollo error for {domain}: {exc}")
    elif APOLLO_API_KEY and _PROVIDER_RATE_LIMITED.get("apollo"):
        print(f"[Workflow 5.5]   Apollo skipped (rate-limited this run) for {domain}")

    # Step 2 — Hunter (fill remaining slots)
    remaining = max_contacts - len(contacts)
    if HUNTER_API_KEY and remaining > 0 and not _PROVIDER_RATE_LIMITED.get("hunter"):
        _inc("hunter_attempts")
        try:
            kps  = _query_hunter_multi(domain, max_results=remaining)
            seen = {c[0]["kp_email"] for c in contacts}
            added = 0
            for kp in kps:
                if len(contacts) >= max_contacts:
                    break
                if kp["kp_email"] not in seen:
                    contacts.append((kp, "hunter"))
                    seen.add(kp["kp_email"])
                    added += 1
            if added:
                _inc("hunter_ok")
        except Exception as exc:
            if _is_rate_limit_error(exc):
                _mark_rate_limited("hunter", domain)
                _inc("hunter_rate_limited")
            else:
                _inc("hunter_errors")
                print(f"[Workflow 5.5]   Hunter error for {domain}: {exc}")
        finally:
            time.sleep(HUNTER_DELAY)
    elif HUNTER_API_KEY and remaining > 0 and _PROVIDER_RATE_LIMITED.get("hunter"):
        print(f"[Workflow 5.5]   Hunter skipped (rate-limited this run) for {domain}")

    # Step 3 — Website contacts (fill remaining slots)
    remaining = max_contacts - len(contacts)
    if remaining > 0:
        site_kps   = _query_website_contact_multi(lead, max_results=remaining)
        seen       = {c[0]["kp_email"] for c in contacts}
        site_phone = ""
        whatsapp_phone = ""
        added = 0
        for kp in site_kps:
            if len(contacts) >= max_contacts:
                break
            phones = kp.pop("site_phones", [])
            whatsapp_phones = kp.pop("whatsapp_phones", [])
            if phones and not site_phone:
                site_phone = phones[0]
            if whatsapp_phones and not whatsapp_phone:
                whatsapp_phone = whatsapp_phones[0]
            if kp["kp_email"] not in seen:
                contacts.append((kp, "website"))
                seen.add(kp["kp_email"])
                added += 1
        if added:
            _inc("website_ok")
        if site_phone and not base.get("site_phone"):
            base["site_phone"] = site_phone
        if not whatsapp_phone:
            whatsapp_phone = _derive_whatsapp_phone(
                country=lead.get("country", ""),
                site_phone=site_phone,
                website=lead.get("website", ""),
            )
        if whatsapp_phone and not base.get("whatsapp_phone"):
            base["whatsapp_phone"] = whatsapp_phone

    # Step 4 — Mock (no API keys → smoke-test mode)
    if not contacts and not APOLLO_API_KEY and not HUNTER_API_KEY:
        for i in range(min(max_contacts, 3)):
            kp = _mock_kp(lead.get("company_name", ""), domain, index + i)
            contacts.append((kp, "mock"))
        _inc("mock_ok")

    if not contacts:
        _inc("none_ok")
        return [_make_contact_row({**base}, rank=1)]

    rows = []
    for rank, (kp, source) in enumerate(contacts, start=1):
        row = {**base, **kp, "enrichment_source": source}
        rows.append(_make_contact_row(row, rank=rank))
    return rows


# ---------------------------------------------------------------------------
# Single-contact waterfall (backward-compatible)
# ---------------------------------------------------------------------------

def enrich_lead(lead: dict, index: int = 0) -> dict:
    """
    Run the Apollo → Hunter → mock/none waterfall for one lead.
    Returns the lead dict with kp_name, kp_title, kp_email, enrichment_source appended.
    """
    domain = _domain(lead.get("website", ""))
    result = {**lead, "kp_name": "", "kp_title": "", "kp_email": "", "enrichment_source": "none"}

    if not domain:
        return result

    # Step 1 — Apollo People Search (paid plan) + org enrich (free plan)
    if APOLLO_API_KEY:
        try:
            kp = _query_apollo(domain)
            if kp:
                return {**result, **kp, "enrichment_source": "apollo"}
            time.sleep(RATE_LIMIT_DELAY)
        except Exception as exc:
            print(f"[Workflow 5.5]   Apollo error for {domain}: {exc}")

    # Step 2 — Hunter fallback
    if HUNTER_API_KEY:
        try:
            kp = _query_hunter(domain)
            if kp:
                return {**result, **kp, "enrichment_source": "hunter"}
            time.sleep(RATE_LIMIT_DELAY)
        except Exception as exc:
            print(f"[Workflow 5.5]   Hunter error for {domain}: {exc}")
        finally:
            time.sleep(HUNTER_DELAY)

    # Step 3 — Website contact (real email scraped directly from company site)
    kp = _query_website_contact(lead)
    if kp:
        phones = kp.pop("site_phones", [])
        whatsapp_phones = kp.pop("whatsapp_phones", [])
        enriched = {**result, **kp, "enrichment_source": "website"}
        if phones:
            enriched["site_phone"] = phones[0]
        whatsapp_phone = _derive_whatsapp_phone(
            country=lead.get("country", ""),
            site_phone=enriched.get("site_phone", ""),
            website=lead.get("website", ""),
            whatsapp_phones=whatsapp_phones,
        )
        if whatsapp_phone:
            enriched["whatsapp_phone"] = whatsapp_phone
        return enriched

    # Step 4 — Mock (no keys configured → smoke test mode)
    if not APOLLO_API_KEY and not HUNTER_API_KEY:
        kp = _mock_kp(lead.get("company_name", ""), domain, index)
        return {**result, **kp, "enrichment_source": "mock"}

    # No guessed-email fallback. If Apollo / Hunter / website all fail in live mode,
    # we return an empty contact and let downstream steps treat the lead as non-sendable.
    return result


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def load_qualified_leads(limit: int = 0, in_path: Path | None = None) -> list[dict]:
    path = in_path or Path(str(QUALIFIED_LEADS_FILE))
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return rows[:limit] if limit else rows


def save_enriched_leads(leads: list[dict], out_path: Path | None = None) -> None:
    path = out_path or Path(str(ENRICHED_LEADS_FILE))
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ENRICHED_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(leads)
    print(f"[Workflow 5.5] Saved {len(leads)} enriched leads → {path}")


def save_enriched_contacts(
    all_contacts: list[list[dict]],
    out_path: Path | None = None,
) -> None:
    """
    Write enriched_contacts.csv — one row per contact, up to 3 per company.
    all_contacts: list of per-company contact lists (each from enrich_lead_multi()).
    """
    flat = [c for company_contacts in all_contacts for c in company_contacts]
    path = out_path or Path(str(ENRICHED_CONTACTS_FILE))
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ENRICHED_CONTACTS_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(flat)
    print(
        f"[Workflow 5.5] Saved {len(flat)} contact rows "
        f"({len(all_contacts)} companies) → {path}"
    )


# ---------------------------------------------------------------------------
# Public run()
# ---------------------------------------------------------------------------

def run(limit: int = 0, paths: RunPaths | None = None) -> list[dict]:
    """
    Enrich qualified leads with KP contact information.

    Args:
        limit: cap on records to process (0 = all)
        paths: explicit RunPaths from campaign_runner; if None, fetched from
               the active global (standalone / backward-compat invocation).

    Returns:
        List of enriched lead dicts.
    """
    if paths is None:
        from config.run_paths import require_active_run_paths
        paths = require_active_run_paths()

    # Reset per-run state so a second call in the same process (tests, future long-lived
    # runners) starts clean and doesn't inherit rate-limit flags or counter totals.
    _PROVIDER_RATE_LIMITED.clear()
    for k in _ENRICHMENT_COUNTERS:
        _ENRICHMENT_COUNTERS[k] = 0
    global _site_contact_cache, _site_contact_cache_source
    _site_contact_cache = None
    _site_contact_cache_source = None

    leads = load_qualified_leads(limit=limit, in_path=paths.qualified_leads_file)

    if not leads:
        print("[Workflow 5.5] No qualified leads found — writing empty output files.")
        save_enriched_leads([], out_path=paths.enriched_leads_file)
        save_enriched_contacts([], out_path=paths.enriched_contacts_file)
        return []

    mode = "apollo+hunter" if (APOLLO_API_KEY or HUNTER_API_KEY) else "mock (no API keys)"
    print(f"[Workflow 5.5] Enriching {len(leads)} leads — mode: {mode}")

    enriched:     list[dict]       = []   # primary contacts → enriched_leads.csv
    all_contacts: list[list[dict]] = []   # all contacts     → enriched_contacts.csv

    for i, lead in enumerate(leads, 1):
        name = lead.get("company_name") or lead.get("website", f"record {i}")
        print(f"[Workflow 5.5] ({i}/{len(leads)}) {name}")

        contacts = enrich_lead_multi(lead, index=i - 1)

        # Apply post-enrichment processing to every contact in this company's list
        for contact in contacts:
            # Validate contact name — clear single-char garbage (e.g. "A" from Apollo)
            if contact.get("kp_name") and not _is_valid_kp_name(contact["kp_name"]):
                print(f"[Workflow 5.5]   Invalid kp_name '{contact['kp_name']}' — clearing")
                contact["kp_name"] = ""

            # Domain trust check (Apollo-only: verify email domain matches website)
            trust = _contact_domain_trusted(
                email=contact.get("kp_email", ""),
                website=contact.get("website", ""),
                source=contact.get("enrichment_source", ""),
            )
            contact.update(trust)

            # Compute contact-channel labels now that enrichment is finalised
            contact.update(_contact_labels(
                kp_email       = contact.get("kp_email", ""),
                site_phone     = contact.get("site_phone", ""),
                whatsapp_phone = contact.get("whatsapp_phone", ""),
                website        = contact.get("website", ""),
            ))

            # Domain-mismatch contacts are not sendable regardless of email presence
            if contact.get("skip_reason") == "contact_domain_mismatch":
                contact["email_sendable"] = "false"

        primary = contacts[0]
        enriched.append(primary)
        all_contacts.append(contacts)

        # Sanitise before printing: Apollo titles sometimes contain Unicode symbols
        # (e.g. ✔ U+2714) that crash Windows GBK consoles.
        _safe = lambda s: (s or "").encode("ascii", "replace").decode("ascii")
        alt_count = len(contacts) - 1
        print(
            f"[Workflow 5.5]   → {_safe(primary['kp_name']) or '(no contact)'} | "
            f"{_safe(primary['kp_title']) or '-'} | "
            f"{primary['kp_email'] or '-'} [{primary['enrichment_source']}]"
            + (f" (+{alt_count} alt)" if alt_count else "")
        )
        if APOLLO_API_KEY or HUNTER_API_KEY:
            time.sleep(RATE_LIMIT_DELAY)

    save_enriched_leads(enriched, out_path=paths.enriched_leads_file)
    save_enriched_contacts(all_contacts, out_path=paths.enriched_contacts_file)

    c = get_enrichment_counters()
    print(
        f"\n[Workflow 5.5] Enrichment complete — {len(enriched)} leads processed\n"
        f"  Provider summary:\n"
        f"    Apollo  : attempts={c['apollo_attempts']}  ok={c['apollo_ok']}  "
        f"rate_limited={c['apollo_rate_limited']}  errors={c['apollo_errors']}\n"
        f"    Hunter  : attempts={c['hunter_attempts']}  ok={c['hunter_ok']}  "
        f"rate_limited={c['hunter_rate_limited']}  errors={c['hunter_errors']}\n"
        f"    Website : ok={c['website_ok']}\n"
        f"    Guessed : ok={c['guessed_ok']}\n"
        f"    Mock    : ok={c['mock_ok']}\n"
        f"    None    : ok={c['none_ok']}"
    )
    if _PROVIDER_RATE_LIMITED:
        limited = ", ".join(k for k, v in _PROVIDER_RATE_LIMITED.items() if v)
        print(f"  WARNING: Rate-limited providers skipped mid-run: {limited}")

    return enriched


if __name__ == "__main__":
    run()
