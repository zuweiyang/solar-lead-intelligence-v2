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

# Valid TLD: 2-6 alpha chars, followed by non-alphanumeric (prevents partial matches)
_EMAIL_RE = re.compile(r"[\w.+\-]+@[\w\-]+\.[a-zA-Z]{2,6}(?=[^a-zA-Z0-9]|$)", re.ASCII)

# North American phone — MUST have at least one separator (-, ., space, parens)
# Rejects bare 10-digit number strings embedded in JS/CSS
_PHONE_RE = re.compile(
    r"""(?:(?:\+?1[\s\-.])\(?\d{3}\)?[\s\-.]\d{3}[\s\-.]\d{4}"""   # +1 xxx xxx xxxx
    r"""|(?:\(\d{3}\)[\s\-.]\d{3}[\s\-.]\d{4})"""                   # (xxx) xxx-xxxx
    r"""|\b\d{3}[\-\.]\d{3}[\-\.]\d{4}\b)""",                       # xxx-xxx-xxxx
    re.ASCII,
)

# Obvious placeholder numbers to reject
_FAKE_PHONE_PATTERNS = re.compile(r"^(\d)\1{6,}$")   # e.g. 0000000000, 9999999999


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

def _extract_contacts(pages: dict[str, str]) -> tuple[list[str], list[str]]:
    """
    Scan all pages for real email addresses and phone numbers.

    Returns:
        site_emails — deduplicated list, most likely business emails first
        site_phones — deduplicated list
    """
    emails_seen: dict[str, int] = {}   # email → occurrence count
    phones_seen: list[str] = []

    for html in pages.values():
        # --- emails: check <a href="mailto:"> first (most reliable) ---
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().startswith("mailto:"):
                addr = href[7:].split("?")[0].strip().lower()
                if addr and "@" in addr:
                    emails_seen[addr] = emails_seen.get(addr, 0) + 2  # weight higher

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

        # --- phones ---
        for match in _PHONE_RE.finditer(html):
            digits = re.sub(r"\D", "", match.group())
            # North American: 10 digits or 11 starting with 1
            if len(digits) == 11 and digits.startswith("1"):
                digits = digits[1:]
            if len(digits) != 10:
                continue
            if _FAKE_PHONE_PATTERNS.match(digits):
                continue
            formatted = match.group().strip()
            if formatted not in phones_seen:
                phones_seen.append(formatted)

    # Sort emails: mailto-weighted first, then by frequency
    sorted_emails = sorted(emails_seen, key=lambda e: -emails_seen[e])
    # Deduplicate phones (keep first 3)
    unique_phones = list(dict.fromkeys(phones_seen))[:3]

    return sorted_emails[:5], unique_phones


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
    site_emails, site_phones = _extract_contacts(pages)

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
