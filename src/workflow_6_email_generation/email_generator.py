# Workflow 6: Email Generation
# Generates cold email drafts via OpenRouter → rule-based fallback.
# Output fields: company_name, kp_name, kp_email, subject, body,
#                lead_score, email_angle, generation_source

import csv
import json
import re

import requests

from config.settings import (
    OPENROUTER_API_KEY,
    EMAIL_GEN_MODEL,
    GENERATED_EMAILS_FILE,
    SENDER_NAME,
    SENDER_TITLE,
)
from src.workflow_6_email_generation.email_merge import merge_leads
from src.workflow_6_email_generation.email_templates import build_rule_based_email
from src.workflow_6_2_signal_personalization.signal_fact_extractor import (
    extract_facts,
    format_facts_for_prompt,
)

OUTPUT_FIELDS = [
    "company_name", "website", "place_id",
    "city", "region", "country", "source_location",
    "kp_name", "kp_title", "kp_email",
    "contact_name", "contact_title", "contact_email",
    "send_target_type", "contact_source",
    "named_contact_available", "generic_contact_available",
    "contact_quality", "generic_only",
    "subject", "body",
    "lead_score", "send_tier", "email_angle", "generation_source",
]

# ---------------------------------------------------------------------------
# Company name display helper
# ---------------------------------------------------------------------------

def _display_name(company_name: str) -> str:
    """Return the email-facing company name: strip bilingual | suffix.

    e.g. "Future Sun Solar | شركة شمس المستقبل" → "Future Sun Solar"
    The raw name is preserved in CSV/CRM storage; this is only applied to
    text that appears in email subjects, greetings, and AI prompts.
    """
    s = (company_name or "").strip()
    if "|" in s:
        s = s.split("|")[0].strip()
    return s


# ---------------------------------------------------------------------------
# Greeting helper
# ---------------------------------------------------------------------------

def _greeting(record: dict) -> str:
    name = (record.get("kp_name") or "").strip()
    if name:
        first = name.split()[0]
        return f"Hi {first},"
    company = _display_name(record.get("company_name") or "")
    short = " ".join(company.split()[:3]) if company else "team"
    return f"Hello {short} team,"


# ---------------------------------------------------------------------------
# Fallback-opening detection
# ---------------------------------------------------------------------------

# These markers identify openings produced by signal_to_opening.py's _fallback()
# function — they are positionally generic and should not be forced verbatim
# into the final email when a better signal is available.
_FALLBACK_MARKERS = (
    "i came across",
    "while looking at solar installers",
    "i came across your company",
    "i noticed your team",
    "i noticed your",
    "i saw your",
)


def _is_fallback_opening(opening_line: str) -> bool:
    """Return True if opening_line is an untailored generic fallback."""
    lower = opening_line.lower()
    return any(marker in lower for marker in _FALLBACK_MARKERS)


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

_SYSTEM = """\
You are writing a short B2B cold email on behalf of a solar hardware and mounting \
systems supplier.
Your goal is to open a conversation with a target company that installs, develops, \
or integrates solar and/or battery storage systems.

Rules:
- Keep the email under 120 words total (including greeting and sign-off).
- Never pretend to be the recipient's company.
- Never say "At {company_name} we..." — you are the sender, not the recipient.
- Do not use generic marketing language.
- Avoid these phrases: "leading supplier", "high quality products", "streamline \
workflows", "Hi there", "I hope this email finds you well".
- The email must feel human and natural — like a message from a real person.
- Do not overstate your knowledge of the recipient. If the input signal is weak, keep the opening broad and factual.
- Avoid phrases like "It looks like..." or "I noticed you're managing..." unless the provided facts explicitly support that phrasing.
- End with a soft CTA such as "Happy to share a few details if useful." or \
"Open to a brief exchange if relevant?"
- Sign off with: Best,\\n[Sender Name][optional title on next line] — use the exact sender details provided in the message.

Adapt the body based on the email_angle field:
- "project_delivery": focus on project execution, materials procurement, or site \
  delivery for EPC / contractor work.
- "installation": focus on installation volume, mounting system efficiency, or \
  residential/commercial mix for installers.
- "storage_integration": focus on battery system deployment, storage integration, \
  or hybrid solar+storage projects.
- "distributor_supply": focus on supply chain support, product range, or helping \
  their installer customer base.
- "project_pipeline": focus on project pipeline support or EPC procurement for developers.
- "general_solar": keep it broad — reference their solar installation or project work.
- "cautious_outreach": write a SHORT generic email under 80 words. Do NOT reference \
  specific projects, locations, or technical details. Just open a conversation.

Send tier guidance (send_tier field):
- A or B1: you may reference the opening line and specific company context naturally.
- B2: keep personalization light — one specific detail maximum; no invented context.
- C: treat as cautious_outreach regardless of other fields.
- Keep the value proposition concrete and modest. Prefer one practical benefit over a list of product claims.

Calculator tool mention (secondary signal — first-touch cold email rules):
- Allowed for project_delivery and installation only. If the email flows naturally and has
  room after the main CTA, you MAY add one soft closing sentence:
  "We also built a simple mounting sizing tool for early-stage project checks — happy to share it if useful."
- For distributor_supply: include only when it fits very naturally. When in doubt, omit.
- For storage_integration or cautious_outreach: do NOT mention the calculator at all.
- Do NOT include any URL in first-touch emails.
- Do NOT use words like "free", "instant", "automated BOM", or any performance claim.
- The calculator mention must remain secondary. If the email already has a clear CTA, omit the calculator sentence.
- Do NOT include unsupported quantified claims: no percentages, no "X% faster/cheaper", no ROI figures, no "reduce X by N", no "save N hours/dollars". Only state what can be verified from the company's public information.

Return ONLY valid JSON, no markdown fences:
{{"subject": "...", "body": "..."}}

Subject line rules:
- Natural and specific, 5-8 words
- Prefer short, plain-English subjects over polished marketing phrasing
- Tailor to email_angle:
  - project_delivery: "Quick question on your EPC projects", "Mounting supply for upcoming projects"
  - installation: "Quick question on your installs", "Mounting supply for your installs"
  - storage_integration: "Quick question on storage projects", "Storage hardware for solar work"
  - distributor_supply: "Quick question for the {company_name} team", "Mounting supply for your customers"
  - cautious_outreach: "Quick question about your solar work", "Solar hardware question"
- Avoid generic subjects like "Solar solutions" or "Energy solutions"
"""

_USER = """\
Company: {company_name}
Company type: {company_type}
Market focus: {market_focus}
Email angle: {email_angle}
Send tier: {send_tier}

Contact name: {kp_name}
Contact email: {kp_email}

Sender name: {sender_name}
Sender title: {sender_title}

Greeting to use: {greeting}

Opening sentence instruction: {opening_instruction}
"""


# Use shared robust JSON parser (handles empty, Extra data, control chars, fences)
from src.workflow_6_email_generation.ai_json_utils import parse_ai_json as _parse_ai_json


# ---------------------------------------------------------------------------
# OpenRouter call
# ---------------------------------------------------------------------------

def _call_openrouter(record: dict) -> dict:
    greeting     = _greeting(record)
    opening_line = (record.get("opening_line") or "").strip()
    best_signal  = (record.get("best_signal")  or "").strip()
    send_tier    = (record.get("send_tier")    or "").strip()

    # Tier C: skip specific personalization entirely — it would be built on weak signals.
    # Write a short, cautious, generic opener instead.
    if send_tier == "C":
        opening_instruction = (
            "Write a short, natural opening sentence describing what this type of company "
            "generally does in solar. Keep it under 12 words. "
            "Do NOT reference specific projects, locations, or figures. "
            'Do NOT use "I came across" or "I was looking into".'
        )
    elif opening_line and not _is_fallback_opening(opening_line):
        # Signal-derived specific opening — instruct the model to use it verbatim.
        opening_instruction = f'Use this exact opening sentence: "{opening_line}"'

    elif best_signal:
        # Opening is generic fallback but a research signal is available.
        # Extract structured facts first, then use them as a whitelist.
        # This prevents the LLM from filling "(none stated)" gaps with world knowledge.
        facts = record.get("signal_facts") or extract_facts(best_signal)
        if facts.get("has_usable_facts"):
            opening_instruction = (
                format_facts_for_prompt(facts) + "\n\n"
                "Write a specific opening sentence using ONLY the facts listed above.\n"
                'Do NOT use "I came across", "I was looking into", or any phrase '
                "that could describe any solar company."
            )
        else:
            # Signal present but no extractable facts — stay generic and safe
            opening_instruction = (
                "Write a natural opening sentence describing what this company does. "
                "Do not mention any specific location, project, or capacity figure. "
                'Do not use "I came across" or "I was looking into".'
            )

    else:
        # No signal at all — derive type hint from company_type.
        ctype = (record.get("company_type") or "").lower()
        if "epc" in ctype or "contractor" in ctype:
            type_hint = "their solar EPC contracting and project delivery work"
        elif "battery" in ctype or "bess" in ctype or "storage" in ctype:
            type_hint = "their battery storage or solar-plus-storage work"
        elif "distributor" in ctype:
            type_hint = "their solar component distribution and supply business"
        elif "developer" in ctype:
            type_hint = "their solar project development pipeline"
        elif "panel installer" in ctype or "installer" in ctype:
            type_hint = "their solar panel installation work"
        else:
            type_hint = "their solar installation and project work"

        # For Tier A leads, enrich the type_hint with scraped services_detected.
        # services_detected is extracted from the company's own website — it is
        # factual and company-specific, not invented.  Naming one or two real
        # services produces a more grounded opener than the type-hint alone.
        if send_tier in ("A", "B1"):
            raw_svcs = record.get("services_detected") or []
            if isinstance(raw_svcs, str):
                raw_svcs = [s.strip() for s in raw_svcs.split(";") if s.strip()]
            # Use the first two specific services; skip generic placeholders
            _SKIP = {"solar", "energy", "services", "solutions", "installation", ""}
            specific = [s for s in raw_svcs if s.lower() not in _SKIP][:2]
            if specific:
                svc_detail = " and ".join(specific)
                type_hint = f"{type_hint} — particularly {svc_detail}"

        opening_instruction = (
            f"Write a natural opening sentence referencing something specific about "
            f"{type_hint}. Avoid generic phrases like 'I came across X while "
            f"looking at solar companies'."
        )

    user_msg = _USER.format(
        company_name         = _display_name(record.get("company_name") or ""),
        company_type         = record.get("company_type", ""),
        market_focus         = record.get("market_focus", ""),
        email_angle          = record.get("email_angle", "general_solar"),
        send_tier            = send_tier or "?",
        kp_name              = record.get("kp_name", "") or "(no name)",
        kp_email             = record.get("kp_email", ""),
        sender_name          = SENDER_NAME or "Your Name",
        sender_title         = SENDER_TITLE or "",
        greeting             = greeting,
        opening_instruction  = opening_instruction,
    )

    resp = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type":  "application/json",
        },
        json={
            "model": EMAIL_GEN_MODEL,
            "messages": [
                {"role": "system", "content": _SYSTEM},
                {"role": "user",   "content": user_msg},
            ],
            "max_tokens": 400,
        },
        timeout=30,
    )
    resp.raise_for_status()
    raw = resp.json()["choices"][0]["message"]["content"].strip()

    company = record.get("company_name", "?")
    parsed = _parse_ai_json(raw, context=company)

    # Validate no "Hi there" slipped through
    body = parsed.get("body", "")
    if re.search(r"\bhi there\b", body, re.IGNORECASE):
        body = re.sub(r"(?i)\bhi there,?\b", greeting, body, count=1)
        parsed["body"] = body

    return parsed


# ---------------------------------------------------------------------------
# Per-record generation
# ---------------------------------------------------------------------------

def generate_email(record: dict) -> tuple[dict, str]:
    """
    Returns (draft_dict, generation_source).
    draft_dict has keys: subject, body
    """
    if OPENROUTER_API_KEY:
        try:
            draft = _call_openrouter(record)
            if draft.get("subject") and draft.get("body"):
                return draft, EMAIL_GEN_MODEL
        except json.JSONDecodeError as exc:
            print(f"[Workflow 6]   AI JSON parse error for {record.get('company_name', '?')}: {exc.msg} — using fallback")
        except requests.exceptions.Timeout:
            print(f"[Workflow 6]   AI timeout for {record.get('company_name', '?')} — using fallback")
        except Exception as exc:
            print(f"[Workflow 6]   AI error for {record.get('company_name', '?')}: {exc} — using fallback")

    # Fallback
    draft = build_rule_based_email(record)
    # Normalise keys: fallback returns email_body, we need body
    if "email_body" in draft and "body" not in draft:
        draft["body"] = draft.pop("email_body")
    return draft, "fallback_template"


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def has_valid_email(record: dict) -> bool:
    email = record.get("kp_email", "")
    return bool(email and "@" in email)


def sort_by_score(records: list[dict]) -> list[dict]:
    return sorted(records, key=lambda r: int(r.get("lead_score") or 0), reverse=True)


def save_generated_emails(emails: list[dict]) -> None:
    with open(GENERATED_EMAILS_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(emails)
    print(f"[Workflow 6] Saved {len(emails)} email drafts → {GENERATED_EMAILS_FILE}")


# ---------------------------------------------------------------------------
# Public run()
# ---------------------------------------------------------------------------

def run(limit: int = 0) -> list[dict]:
    records = merge_leads(limit=limit)
    if not records:
        print("[Workflow 6] No merged records found — writing empty output file.")
        save_generated_emails([])
        return []

    mode = f"OpenRouter/{EMAIL_GEN_MODEL}" if OPENROUTER_API_KEY else "rule-based fallback"
    print(f"[Workflow 6] Generating {len(records)} email drafts — {mode}")

    results: list[dict] = []
    ai_count   = 0
    rule_count = 0

    for i, record in enumerate(records, 1):
        name = record.get("company_name") or record.get("kp_email", f"record {i}")
        print(f"[Workflow 6] ({i}/{len(records)}) {name}")

        draft, source = generate_email(record)

        if not draft.get("subject", "").strip() or not draft.get("body", "").strip():
            print(f"[Workflow 6]   WARN: empty draft for {name}, skipping")
            continue

        if source == "fallback_template":
            rule_count += 1
        else:
            ai_count += 1

        results.append({
            "company_name":      record.get("company_name", ""),
            "website":           record.get("website", ""),
            "place_id":          record.get("place_id", ""),
            "city":              record.get("city", ""),
            "region":            record.get("region", ""),
            "country":           record.get("country", ""),
            "source_location":   record.get("source_location", ""),
            "kp_name":           record.get("kp_name", ""),
            "kp_title":          record.get("kp_title", ""),
            "kp_email":          record.get("kp_email", ""),
            "contact_name":      record.get("contact_name", record.get("kp_name", "")),
            "contact_title":     record.get("contact_title", record.get("kp_title", "")),
            "contact_email":     record.get("contact_email", record.get("kp_email", "")),
            "send_target_type":  record.get("send_target_type", ""),
            "contact_source":    record.get("contact_source", ""),
            "named_contact_available": record.get("named_contact_available", "false"),
            "generic_contact_available": record.get("generic_contact_available", "false"),
            "contact_quality":   record.get("contact_quality", "none"),
            "generic_only":      record.get("generic_only", "false"),
            "subject":           draft["subject"],
            "body":              draft["body"],
            "lead_score":        record.get("lead_score", ""),
            "send_tier":         record.get("send_tier", ""),
            "email_angle":       record.get("email_angle", ""),
            "generation_source": source,
        })
        print(f"[Workflow 6]   [{source}] {draft['subject'][:65]}")

    print(f"[Workflow 6] AI: {ai_count}  |  Fallback: {rule_count}")
    save_generated_emails(results)
    return results


if __name__ == "__main__":
    run()
