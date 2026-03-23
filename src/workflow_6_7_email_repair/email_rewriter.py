# Workflow 6.7: Email Repair Loop — Email Rewriter
# Rewrites weak emails using review_notes to target specific issues.
# Provider waterfall: OpenRouter → Anthropic → OpenAI → rule fallback.

import json
import re
import time

import requests

from config.settings import (
    OPENROUTER_API_KEY, ANTHROPIC_API_KEY, OPENAI_API_KEY,
    LLM_PROVIDER, LLM_MODEL,
    EMAIL_GEN_MODEL,
)
from src.workflow_6_email_generation.email_templates import (
    build_rule_based_email,
    trim_to_limit,
)
from src.workflow_6_2_signal_personalization.signal_fact_extractor import (
    extract_facts,
    format_facts_for_prompt,
)

AI_RATE_DELAY = 0.5

_PROMPT_TMPL = """\
You are revising a cold outreach email draft that scored too low on quality review.

Company: {company_name}
Company type: {company_type}
Market focus: {market_focus}
Contact: {kp_name} ({kp_title})
Email angle: {email_angle}
{signal_section}
CURRENT DRAFT:
Subject: {subject}
Opening: {opening_line}
Body:
{email_body}

REVIEW NOTES (problems to fix):
{review_notes}

Rewrite the email to fix the issues above. Follow all rules:

Tone:
- Calm, professional B2B rep — no hype, no buzzwords, no fake urgency

Opening line:
- Do NOT use "I came across" or "I was looking into"
- Do NOT use "I hope this finds you well" or "I wanted to reach out"
- Do NOT use speculative phrases: "I imagine", "I suspect", "I assume", "presumably", "likely", or any phrase inferring something about the recipient — write factual statements only
- Write an opening specific to THIS company — not a phrase that could describe any solar installer
- If signal facts are listed above, use ONLY those items — items marked "(none stated in source)" do not exist
- Especially: if Locations = "(none stated in source)", write NO geographic reference whatsoever

Body:
- Match tightly to the email angle ({email_angle})
- Battery angle: mention storage integration specifically
- Commercial angle: mention commercial install support and workflow efficiency
- Utility angle: mention scale, project execution, procurement support
- Residential: mention residential operations only
- One specific value proposition — no "companies like yours"

Body:
- Do NOT include unsupported quantified claims: no percentages, no "X% faster/cheaper", no ROI figures, no "reduce X by N", no "save N hours/dollars". Only state what can be verified from the company's public information.

CTA (soft only):
- Use: "Open to a short intro if this is relevant?" or "Happy to share more if useful."
- Do NOT use: "schedule a call", "book time", "let's hop on a call"

Format:
- 80–140 words total, hard max 180 words
- Plain text only, no markdown, no emojis
- Greeting: use first name if known ("{kp_first}"), else "Hi there,"

Return ONLY valid JSON (no markdown fences):
{{"subject": "...", "opening_line": "...", "email_body": "..."}}

subject: 4–7 words, specific to their business, no spam words
opening_line: first sentence after the greeting
email_body: complete email (greeting + opening + body + CTA)\
"""

_PLACEHOLDER_RE = re.compile(r"\{[a-zA-Z_][a-zA-Z0-9_]*\}")


from src.workflow_6_email_generation.ai_json_utils import parse_ai_json as _parse_ai_json


def _fix_json_control_chars(raw: str) -> str:
    """Replace bare newlines/tabs inside JSON string values with escape sequences."""
    out: list[str] = []
    in_str = False
    i = 0
    while i < len(raw):
        c = raw[i]
        if c == "\\" and in_str:
            out.append(c)
            i += 1
            if i < len(raw):
                out.append(raw[i])
        elif c == '"':
            in_str = not in_str
            out.append(c)
        elif in_str and c == "\n":
            out.append("\\n")
        elif in_str and c == "\r":
            out.append("\\r")
        elif in_str and c == "\t":
            out.append("\\t")
        else:
            out.append(c)
        i += 1
    return "".join(out)


def _get_provider() -> tuple[str, str, str] | None:
    if LLM_PROVIDER == "openrouter" and OPENROUTER_API_KEY:
        return ("openrouter", OPENROUTER_API_KEY, LLM_MODEL or EMAIL_GEN_MODEL)
    if LLM_PROVIDER == "anthropic" and ANTHROPIC_API_KEY:
        return ("anthropic", ANTHROPIC_API_KEY, "claude-haiku-4-5-20251001")
    if LLM_PROVIDER == "openai" and OPENAI_API_KEY:
        return ("openai", OPENAI_API_KEY, "gpt-4o-mini")
    if OPENROUTER_API_KEY:
        return ("openrouter", OPENROUTER_API_KEY, LLM_MODEL or EMAIL_GEN_MODEL)
    if ANTHROPIC_API_KEY:
        return ("anthropic", ANTHROPIC_API_KEY, "claude-haiku-4-5-20251001")
    if OPENAI_API_KEY:
        return ("openai", OPENAI_API_KEY, "gpt-4o-mini")
    return None


def _build_prompt(record: dict) -> str:
    kp_name   = (record.get("kp_name") or "").strip()
    kp_first  = kp_name.split()[0] if kp_name else "there"
    notes_raw = record.get("review_notes", "")
    notes_str = "\n".join(
        f"- {n.strip()}" for n in notes_raw.split(";") if n.strip()
    ) or "- Improve overall quality"

    # Prefer pre-extracted signal_facts (structured whitelist) over raw best_signal.
    # If signal_facts is absent (old records), extract on the fly from best_signal.
    signal_facts = record.get("signal_facts") or {}
    best_signal  = (record.get("best_signal") or "").strip()
    if not signal_facts and best_signal:
        signal_facts = extract_facts(best_signal)

    if signal_facts.get("has_usable_facts"):
        signal_section = format_facts_for_prompt(signal_facts)
    elif best_signal:
        # Signal exists but no extractable facts — include raw text with a minimal hint
        signal_section = (
            f"Company context (no specific facts extracted — describe what they do, "
            f"do not invent any location, project, or capacity):\n  {best_signal[:200]}"
        )
    else:
        signal_section = ""

    return _PROMPT_TMPL.format(
        company_name   = record.get("company_name", ""),
        company_type   = record.get("company_type", ""),
        market_focus   = record.get("market_focus", ""),
        kp_name        = kp_name or "unknown",
        kp_title       = record.get("kp_title", "") or "unknown",
        email_angle    = record.get("email_angle", "General solar outreach"),
        subject        = record.get("subject", ""),
        opening_line   = record.get("opening_line", ""),
        email_body     = record.get("email_body", ""),
        review_notes   = notes_str,
        kp_first       = kp_first,
        signal_section = signal_section,
    )


def _parse_ai_response(raw: str, context: str = "") -> dict:
    parsed = _parse_ai_json(raw, context=context)
    parsed["email_body"] = trim_to_limit(parsed.get("email_body", ""))
    return parsed


def _call_openrouter(prompt: str, api_key: str, model: str) -> str:
    resp = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={"model": model, "messages": [{"role": "user", "content": prompt}], "max_tokens": 512},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


def _call_anthropic(prompt: str, api_key: str, model: str) -> str:
    import anthropic
    client  = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model=model, max_tokens=512,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text


def _call_openai(prompt: str, api_key: str, model: str) -> str:
    import openai
    client = openai.OpenAI(api_key=api_key)
    resp   = client.chat.completions.create(
        model=model, max_tokens=512,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.choices[0].message.content


_CALLERS = {
    "openrouter": _call_openrouter,
    "anthropic":  _call_anthropic,
    "openai":     _call_openai,
}


def _rule_repair(record: dict) -> dict:
    """Deterministic fallback: rebuild email from templates with current record data."""
    return build_rule_based_email(record)


def rewrite_email(record: dict) -> tuple[dict, str, str, str]:
    """
    Returns (repaired_draft, repair_mode, repair_source, ai_error).

    ai_error is "" when AI succeeded or no AI was available.
    ai_error is the exception string when AI was attempted but failed.
    The caller is responsible for logging ai_error to the error CSV.
    """
    provider = _get_provider()
    if provider:
        pname, api_key, model = provider
        try:
            raw    = _CALLERS[pname](_build_prompt(record), api_key, model)
            result = _parse_ai_response(raw, context=record.get("company_name", "?"))
            # Validate — no unresolved placeholders
            full = result.get("subject", "") + " " + result.get("email_body", "")
            if _PLACEHOLDER_RE.search(full):
                raise ValueError("Repaired draft still contains placeholders")
            time.sleep(AI_RATE_DELAY)
            return result, "ai", pname, ""
        except Exception as exc:
            identifier = (
                record.get("kp_email")
                or record.get("company_name")
                or "unknown"
            )
            ai_error = str(exc)
            print(
                f"[Workflow 6.7]   Rewrite error ({pname}) for "
                f"{identifier}: {ai_error}"
            )
            result = _rule_repair(record)
            return result, "rule", "repair_fallback", ai_error
    result = _rule_repair(record)
    return result, "rule", "repair_fallback", ""
