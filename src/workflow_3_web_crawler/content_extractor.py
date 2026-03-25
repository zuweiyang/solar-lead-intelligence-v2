# Workflow 3: Content Extraction
# Converts raw HTML pages into clean company text for AI analysis.
# Also extracts real contact info (emails, phones) directly from page HTML.

import json
import re
from bs4 import BeautifulSoup

from config.settings import COMPANY_PAGES_FILE, COMPANY_TEXT_FILE

MAX_CHARS = 5000

# Tags whose entire content (including children) we discard for text extraction
REMOVE_TAGS = [
    "script", "style", "noscript", "iframe",
    "nav", "header", "footer",
    "aside", "form", "button", "input", "select",
    "svg", "img", "figure",
]

# Domain suffixes to reject — any email whose domain ends with one of these is noise
_SKIP_EMAIL_SUFFIXES = (
    "sentry.io", "wixpress.com", "sentry.wixpress.com",
    "googleapis.com", "cloudflare.com", "example.com",
    "w3.org", "schema.org", "facebook.com", "instagram.com",
    "twitter.com", "linkedin.com", "youtube.com", "tiktok.com",
    "amazonaws.com", "sendgrid.net", "mailchimp.com",
)

# Email matcher that supports multi-level public suffixes like .com.br and .co.uk.
# We still require a non-alphanumeric boundary so trailing HTML/CSS noise is not captured.
_EMAIL_RE = re.compile(
    r"[\w.+\-]+@(?:[\w\-]+\.)+[a-zA-Z]{2,24}(?=[^a-zA-Z0-9]|$)",
    re.ASCII,
)

# North American phone — MUST have at least one separator (-, ., space, parens)
# Rejects bare 10-digit number strings embedded in JS/CSS.
_NA_PHONE_RE = re.compile(
    r"""(?:(?:\+?1[\s\-.])\(?\d{3}\)?[\s\-.]\d{3}[\s\-.]\d{4}"""
    r"""|(?:\(\d{3}\)[\s\-.]\d{3}[\s\-.]\d{4})"""
    r"""|\b\d{3}[\-\.]\d{3}[\-\.]\d{4}\b)""",
    re.ASCII,
)

# Brazil phone formats such as:
#   +55 11 97071-3044
#   (11) 97071-3044
#   11 97071-3044
#   (11) 3090-5976
_BRAZIL_PHONE_RE = re.compile(
    r"""(?:(?:\+?55[\s\-.]?)?(?:\(?\d{2}\)?[\s\-.]?)(?:9?\d{4})[\s\-.]?\d{4})""",
    re.ASCII,
)

_WHATSAPP_URL_RE = re.compile(
    r"""(?:wa\.me/|api\.whatsapp\.com/send\?phone=|web\.whatsapp\.com/send\?phone=)(\d{10,15})""",
    re.IGNORECASE,
)

_TEL_URL_RE = re.compile(r"""^tel:(.+)$""", re.IGNORECASE)

# Obvious placeholder numbers to reject
_FAKE_PHONE_PATTERNS = re.compile(r"^(\d)\1{6,}$")   # e.g. 0000000000, 9999999999


def _normalize_phone_match(raw: str, phone_re: re.Pattern[str]) -> str | None:
    """Return a cleaned phone string when the candidate looks like a real phone."""
    digits = re.sub(r"\D", "", raw)

    if phone_re is _NA_PHONE_RE:
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        if len(digits) != 10:
            return None
    else:
        if len(digits) == 13 and digits.startswith("55"):
            digits = digits[2:]
        if len(digits) not in (10, 11):
            return None

    if _FAKE_PHONE_PATTERNS.match(digits):
        return None

    # Reject bare long digit strings that came from hidden data blobs / tracking.
    if raw.strip().isdigit():
        return None

    return raw.strip()


def _visible_page_text(soup: BeautifulSoup) -> str:
    """Return visible text only, excluding high-noise tags."""
    clone = BeautifulSoup(str(soup), "html.parser")
    for tag in clone(REMOVE_TAGS):
        tag.decompose()
    return clone.get_text(separator=" ", strip=True)


def _extract_whatsapp_hint_phones(visible_text: str) -> list[str]:
    """Extract phone candidates that are explicitly tied to WhatsApp wording."""
    hint_phones: list[str] = []
    if "whatsapp" not in visible_text.lower():
        return hint_phones

    segments = re.split(r"(?i)whatsapp", visible_text)
    for segment in segments[1:]:
        local_window = segment[:80]
        for phone_re in (_BRAZIL_PHONE_RE, _NA_PHONE_RE):
            for match in phone_re.finditer(local_window):
                formatted = _normalize_phone_match(match.group(), phone_re)
                if formatted and formatted not in hint_phones:
                    hint_phones.append(formatted)
    return hint_phones


# ---------------------------------------------------------------------------
# HTML → text
# ---------------------------------------------------------------------------

def _extract_text(html: str) -> str:
    """Strip noise from HTML and return readable plain text."""
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(REMOVE_TAGS):
        tag.decompose()

    text = soup.get_text(separator=" ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = "\n".join(line.strip() for line in text.splitlines())
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# ---------------------------------------------------------------------------
# Contact extraction — runs on RAW HTML (before tag removal)
# ---------------------------------------------------------------------------

def _extract_contacts(pages: dict[str, str]) -> tuple[list[str], list[str], list[str]]:
    """
    Scan all pages for real email addresses and phone numbers.

    Returns:
        site_emails — deduplicated list, most likely business emails first
        site_phones — deduplicated list
        whatsapp_phones — deduplicated list parsed from WhatsApp links / hints
    """
    emails_seen: dict[str, int] = {}   # email → occurrence count
    phones_seen: list[str] = []
    whatsapp_seen: list[str] = []

    for html in pages.values():
        # --- emails: check <a href="mailto:"> first (most reliable) ---
        soup = BeautifulSoup(html, "html.parser")
        page_phones: list[str] = []
        visible_text = _visible_page_text(soup)
        hinted_whatsapp_phones = _extract_whatsapp_hint_phones(visible_text)
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().startswith("mailto:"):
                addr = href[7:].split("?")[0].strip().lower()
                if addr and "@" in addr:
                    emails_seen[addr] = emails_seen.get(addr, 0) + 2  # weight higher
            for match in _WHATSAPP_URL_RE.finditer(href):
                digits = match.group(1)
                formatted = f"+{digits}"
                if formatted not in whatsapp_seen:
                    whatsapp_seen.append(formatted)
            tel_match = _TEL_URL_RE.match(href.strip())
            if tel_match:
                tel_value = tel_match.group(1).split("?")[0].strip()
                for phone_re in (_NA_PHONE_RE, _BRAZIL_PHONE_RE):
                    normalized = _normalize_phone_match(tel_value, phone_re)
                    if normalized:
                        if normalized not in phones_seen:
                            phones_seen.append(normalized)
                        if normalized not in page_phones:
                            page_phones.append(normalized)
                        break

        # --- emails: regex scan of full HTML text ---
        for match in _EMAIL_RE.finditer(html):
            addr = match.group().lower()
            domain = addr.split("@")[-1]
            # reject noise domains (suffix match)
            if any(domain == s or domain.endswith("." + s) for s in _SKIP_EMAIL_SUFFIXES):
                continue
            # skip asset file extensions accidentally matched
            if addr.endswith((".png", ".jpg", ".gif", ".css", ".js", ".svg")):
                continue
            emails_seen[addr] = emails_seen.get(addr, 0) + 1

        # --- phones: visible text only to avoid JS/CSS/tracking-number noise ---
        for phone_re in (_NA_PHONE_RE, _BRAZIL_PHONE_RE):
            for match in phone_re.finditer(visible_text):
                formatted = _normalize_phone_match(match.group(), phone_re)
                if not formatted:
                    continue
                if formatted not in phones_seen:
                    phones_seen.append(formatted)
                if formatted not in page_phones:
                    page_phones.append(formatted)

        for hinted_phone in hinted_whatsapp_phones:
            if hinted_phone not in whatsapp_seen:
                whatsapp_seen.append(hinted_phone)

        if "whatsapp" in visible_text.lower() and page_phones and not hinted_whatsapp_phones:
            first_phone = page_phones[0]
            if first_phone not in whatsapp_seen:
                whatsapp_seen.append(first_phone)

    # Sort emails: mailto-weighted first, then by frequency
    sorted_emails = sorted(emails_seen, key=lambda e: -emails_seen[e])
    # Deduplicate phones (keep first 3)
    unique_phones = list(dict.fromkeys(phones_seen))[:3]
    unique_whatsapp = list(dict.fromkeys(whatsapp_seen))[:3]

    return sorted_emails[:5], unique_phones, unique_whatsapp


# ---------------------------------------------------------------------------
# Per-company extraction
# ---------------------------------------------------------------------------

def extract_company_text(record: dict) -> dict:
    """
    Combine text from all crawled pages into one cleaned string per company.
    Also extract real contact emails and phones from the raw HTML.
    Pages are ordered: home first, then the rest.
    """
    pages_raw = record.get("pages", {})
    pages: dict[str, str] = pages_raw if isinstance(pages_raw, dict) else {}

    # Extract contacts from raw HTML BEFORE stripping tags
    site_emails, site_phones, whatsapp_phones = _extract_contacts(pages)

    # home page first, then remaining pages in insertion order
    ordered = ["home"] + [k for k in pages if k != "home"]

    parts: list[str] = []
    total = 0

    for label in ordered:
        html = pages.get(label)
        if not html:
            continue
        text = _extract_text(html)
        if not text:
            continue

        remaining = MAX_CHARS - total
        if remaining <= 0:
            break

        chunk = text[:remaining]
        parts.append(f"[{label}]\n{chunk}")
        total += len(chunk)

    return {
        "place_id":     record.get("place_id", ""),
        "website":      record.get("website", ""),
        "pages_used":   [l for l in ordered if l in pages],
        "company_text": "\n\n".join(parts),
        "site_emails":  site_emails,
        "site_phones":  site_phones,
        "whatsapp_phones": whatsapp_phones,
    }


# ---------------------------------------------------------------------------
# Public run()
# ---------------------------------------------------------------------------

def load_pages() -> list[dict]:
    with open(COMPANY_PAGES_FILE, encoding="utf-8") as f:
        return json.load(f)


def run(page_records: list[dict] | None = None) -> list[dict]:
    """
    Extract text from company_pages.json (or a supplied list).
    Saves results to company_text.json and returns them.
    """
    records = page_records if page_records is not None else load_pages()
    results: list[dict] = []

    crawl_failures = 0
    for record in records:
        extracted = extract_company_text(record)
        if extracted["company_text"]:
            results.append(extracted)
        else:
            # Crawl failed — keep a stub so the classifier can still use the
            # company name for solar-relevance detection and name-based rules.
            results.append(extracted)
            crawl_failures += 1

    COMPANY_TEXT_FILE.write_text(json.dumps(results, indent=2, ensure_ascii=False),
                                 encoding="utf-8")
    print(f"[Workflow 3] Extracted text for {len(results) - crawl_failures} companies, "
          f"{crawl_failures} crawl-failure stubs → {COMPANY_TEXT_FILE}")
    return results


if __name__ == "__main__":
    run()
