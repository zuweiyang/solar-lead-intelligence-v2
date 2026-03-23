# Workflow 4: AI Company Research
# Sends website content to an AI model and returns structured company analysis.

import json
from pathlib import Path
from config.settings import COMPANY_CONTENT_FILE, COMPANY_RESEARCH_PROMPT


def load_company_content() -> list[dict]:
    """Load all content records from company_content.json."""
    with open(COMPANY_CONTENT_FILE, encoding="utf-8") as f:
        return json.load(f)


def _load_prompt_template() -> str:
    return COMPANY_RESEARCH_PROMPT.read_text(encoding="utf-8")


def analyze_company(content_record: dict) -> dict:
    """
    Send company website content to the AI and return a structured profile.

    TODO: Replace the stub below with a real API call:
      - OpenAI:    openai.chat.completions.create(...)
      - Anthropic: anthropic.messages.create(...)

    The prompt template expects: {website_content}
    The model should return valid JSON matching config/prompts/company_research.txt spec.
    """
    prompt_template = _load_prompt_template()
    prompt = prompt_template.format(
        website_content=content_record.get("company_description", "")
    )

    # --- Replace this block with your AI API call ---
    raise NotImplementedError(
        "Implement AI API call in analyze_company() — see docstring for options."
    )
    # ai_response_text = call_ai(prompt)
    # return json.loads(ai_response_text)


def run_analysis(content_records: list[dict]) -> list[dict]:
    """Analyze all companies and attach AI-generated profiles."""
    profiles: list[dict] = []

    for record in content_records:
        print(f"[Workflow 4] Analyzing: {record['company_name']}")
        try:
            profile = analyze_company(record)
            profile["company_name"] = record["company_name"]
            profile["website"] = record["website"]
            profiles.append(profile)
        except Exception as exc:
            print(f"[Workflow 4] Error analyzing {record['company_name']}: {exc}")

    return profiles


def run() -> list[dict]:
    records = load_company_content()
    return run_analysis(records)


if __name__ == "__main__":
    run()
