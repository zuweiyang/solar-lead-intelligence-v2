# Workflow 6: Email Personalization
# Uses AI to generate personalised outreach emails for each qualified lead.

import json
from config.settings import (
    QUALIFIED_LEADS_FILE,
    EMAIL_TEMPLATES_FILE,
    EMAIL_GENERATION_PROMPT,
    SENDER_NAME,
    SENDER_TITLE,
)


def load_qualified_leads() -> list[dict]:
    """Load qualified leads from CSV."""
    import csv
    with open(QUALIFIED_LEADS_FILE, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _load_prompt_template() -> str:
    return EMAIL_GENERATION_PROMPT.read_text(encoding="utf-8")


def generate_email(profile: dict) -> dict:
    """
    Call the AI to produce a personalised email for one lead.

    TODO: Replace the stub below with a real API call:
      - OpenAI:    openai.chat.completions.create(...)
      - Anthropic: anthropic.messages.create(...)

    The prompt template expects: {company_profile}, {sender_name}, {sender_title}
    The model should return JSON matching config/prompts/email_generation.txt spec.
    """
    prompt_template = _load_prompt_template()
    prompt = prompt_template.format(
        company_profile=json.dumps(profile, ensure_ascii=False),
        sender_name=SENDER_NAME,
        sender_title=SENDER_TITLE,
    )

    # --- Replace this block with your AI API call ---
    raise NotImplementedError(
        "Implement AI API call in generate_email() — see docstring for options."
    )
    # ai_response_text = call_ai(prompt)
    # return json.loads(ai_response_text)


def generate_all_emails(leads: list[dict]) -> list[dict]:
    """Generate email content for every qualified lead."""
    templates: list[dict] = []

    for lead in leads:
        print(f"[Workflow 6] Generating email for: {lead['company_name']}")
        try:
            email = generate_email(lead)
            email["company_name"] = lead["company_name"]
            email["website"]      = lead.get("website", "")
            email["grade"]        = lead.get("grade", "")
            templates.append(email)
        except Exception as exc:
            print(f"[Workflow 6] Error generating email for {lead['company_name']}: {exc}")

    return templates


def run() -> list[dict]:
    leads     = load_qualified_leads()
    templates = generate_all_emails(leads)
    EMAIL_TEMPLATES_FILE.write_text(json.dumps(templates, indent=2), encoding="utf-8")
    print(f"[Workflow 6] Saved {len(templates)} email templates → {EMAIL_TEMPLATES_FILE}")
    return templates


if __name__ == "__main__":
    run()
