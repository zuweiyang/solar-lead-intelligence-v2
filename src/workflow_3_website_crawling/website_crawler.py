# Workflow 3: Website Crawling
# Visits company websites and retrieves raw page content.

import csv
import time
from config.settings import RAW_LEADS_FILE, CRAWL_DELAY_SECONDS

TARGET_PATHS = ["/", "/about", "/about-us", "/services", "/projects", "/products"]


def load_leads_with_websites() -> list[dict]:
    """Return leads that have a non-empty website URL."""
    with open(RAW_LEADS_FILE, newline="", encoding="utf-8") as f:
        leads = list(csv.DictReader(f))
    return [lead for lead in leads if lead.get("website")]


def crawl_website(base_url: str) -> dict[str, str]:
    """
    Fetch each target path on the given website and return HTML per path.

    TODO: Implement using one of:
      - requests + BeautifulSoup  (fast, static sites)
      - Playwright                (JavaScript-rendered sites)

    Returns:
        dict mapping path → raw HTML string (empty string if page unreachable)
    """
    raise NotImplementedError("Implement website crawling — see docstring for options.")


def crawl_all_leads(leads: list[dict]) -> list[dict]:
    """Crawl each lead's website and attach page content."""
    results: list[dict] = []

    for lead in leads:
        url = lead["website"]
        if not url.startswith("http"):
            url = "https://" + url

        print(f"[Workflow 3] Crawling: {url}")
        try:
            pages = crawl_website(url)
            results.append({
                "company_name": lead["company_name"],
                "website": url,
                "pages": pages,
            })
        except Exception as exc:
            print(f"[Workflow 3] Error crawling {url}: {exc}")
        time.sleep(CRAWL_DELAY_SECONDS)

    return results


def run() -> list[dict]:
    leads = load_leads_with_websites()
    return crawl_all_leads(leads)


if __name__ == "__main__":
    run()
