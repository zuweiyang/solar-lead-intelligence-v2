# Workflow 6: Email Personalization
# Loads, previews, and manages email templates stored in email_templates.json.

import json
from config.settings import EMAIL_TEMPLATES_FILE


def load_templates() -> list[dict]:
    """Load all email templates from JSON."""
    with open(EMAIL_TEMPLATES_FILE, encoding="utf-8") as f:
        return json.load(f)


def get_template_by_company(company_name: str) -> dict | None:
    """Return the email template for a specific company, or None if not found."""
    templates = load_templates()
    for t in templates:
        if t.get("company_name", "").lower() == company_name.lower():
            return t
    return None


def preview_template(template: dict) -> str:
    """Return a human-readable preview of an email template."""
    lines = [
        f"To:       {template.get('company_name', 'Unknown')}",
        f"Website:  {template.get('website', '')}",
        f"Grade:    {template.get('grade', '')}",
        "",
        f"Subject:  {template.get('subject', '')}",
        "",
        template.get("body", ""),
        "",
        "--- Follow-up (Day 3) ---",
        f"Subject:  {template.get('follow_up_subject', '')}",
        "",
        template.get("follow_up_body", ""),
        "",
        "--- Final Follow-up (Day 7) ---",
        f"Subject:  {template.get('final_follow_up_subject', '')}",
        "",
        template.get("final_follow_up_body", ""),
    ]
    return "\n".join(lines)


def filter_templates_by_grade(grade: str) -> list[dict]:
    """Return templates filtered by lead grade (A, B, C, D)."""
    return [t for t in load_templates() if t.get("grade") == grade]


def update_template(company_name: str, updates: dict) -> bool:
    """
    Update fields of an existing template and save back to disk.
    Returns True if the template was found and updated.
    """
    templates = load_templates()
    for i, t in enumerate(templates):
        if t.get("company_name", "").lower() == company_name.lower():
            templates[i] = {**t, **updates}
            EMAIL_TEMPLATES_FILE.write_text(json.dumps(templates, indent=2), encoding="utf-8")
            return True
    return False
