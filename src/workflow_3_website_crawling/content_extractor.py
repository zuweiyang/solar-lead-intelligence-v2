# Workflow 3: Website Crawling
# Extracts clean text from crawled HTML and saves structured content records.

import json
from config.settings import COMPANY_CONTENT_FILE

MAX_CONTENT_CHARS = 5000  # Truncate to keep AI token costs manageable


def extract_text_from_html(html: str) -> str:
    """
    Strip HTML tags and return clean readable text.

    TODO: Implement using BeautifulSoup:
      - Remove <script>, <style>, <nav>, <footer> elements
      - Extract text from <main>, <article>, <p>, <h1-h6>
    """
    raise NotImplementedError("Implement HTML text extraction — use BeautifulSoup.")


def extract_company_content(crawl_result: dict) -> dict:
    """Build a structured content record from one crawl result."""
    text_parts: list[str] = []

    for path, html in crawl_result.get("pages", {}).items():
        try:
            text = extract_text_from_html(html)
            if text.strip():
                text_parts.append(f"[{path}]\n{text.strip()}")
        except Exception:
            pass

    combined = "\n\n".join(text_parts)[:MAX_CONTENT_CHARS]

    return {
        "company_name": crawl_result["company_name"],
        "website": crawl_result["website"],
        "company_description": combined,
        "services": "",           # populated by AI in Workflow 4
        "industry_keywords": [],  # populated by AI in Workflow 4
    }


def save_company_content(records: list[dict]) -> None:
    """Write content records to company_content.json."""
    COMPANY_CONTENT_FILE.write_text(json.dumps(records, indent=2), encoding="utf-8")
    print(f"[Workflow 3] Saved {len(records)} content records → {COMPANY_CONTENT_FILE}")


def run(crawl_results: list[dict]) -> list[dict]:
    records = [extract_company_content(r) for r in crawl_results]
    save_company_content(records)
    return records


if __name__ == "__main__":
    from .website_crawler import run as crawl_run
    run(crawl_run())
