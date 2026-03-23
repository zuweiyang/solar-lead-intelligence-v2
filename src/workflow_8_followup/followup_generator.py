# Workflow 8: Follow-up Automation - Follow-up Draft Generator
# Generates follow-up email drafts adapted to stage and engagement behaviour.
# Provider waterfall: OpenRouter -> Anthropic -> OpenAI -> deterministic fallback.

import time

import requests

from config.settings import (
    OPENROUTER_API_KEY,
    ANTHROPIC_API_KEY,
    OPENAI_API_KEY,
    LLM_PROVIDER,
    LLM_MODEL,
    EMAIL_GEN_MODEL,
    SENDER_NAME,
    SENDER_TITLE,
)

AI_RATE_DELAY = 0.5

# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

_PROMPT = """\
You are writing follow-up #{stage_num} of a B2B cold outreach email sequence.

Company: {company_name}
Contact: {kp_name_or_there}
Original email topic: {subject}
Follow-up stage: {followup_stage}
Engagement context: {engagement_hint}
Sender name: {sender_name}
Sender title: {sender_title}

Write a short, professional follow-up email following these rules:

Tone:
- Calm, direct, professional
- No hype, no fake urgency, no pressure
- Do NOT mention tracking, opens, or clicks under any circumstances
- Do NOT say "I saw you opened my email" or reference engagement signals
- Do NOT use "just checking in" or "just bumping this"
- Do NOT say "circling back"
- Do not sound like a sequencer template; keep the language plain and conversational

Stage-specific length and style:
- followup_1: 50-90 words, light reminder, brief value reframe
- followup_2: 60-100 words, add one practical value angle
- followup_3: 50-80 words, final polite touch, low pressure, easy exit

CTA:
- Soft and non-pushy only
- Use: "Happy to share a few details if useful." or "If useful, I can send over a few more details."
- Do NOT use: "schedule a call", "book time", "hop on a call"

Calculator tool mention (follow-up only - email_angle: {email_angle}):
- Only relevant for project_delivery or installation angles. For all other angles, omit entirely.
- followup_1: soft phrasing only, no URL:
  "We also have a simple mounting sizing tool - happy to share it if useful."
  Include this only if it fits naturally after the main point. Do not force it.
- followup_2 with active interest: if calculator_url is provided below, you MAY include it as a
  concrete next step, but keep it one sentence and secondary to the main message.
  calculator_url: {calculator_url}
  If calculator_url is empty, use soft phrasing only (no URL).
- followup_3: do NOT mention the calculator. Keep the final touch minimal.
- Do NOT use words like "free", "instant", "automated", or any performance claim.

Format:
- Plain text only
- No markdown, no emojis
- Include greeting using first name if known, else "Hello [Company] team,"
- End with:
  Best,
  [Sender Name]
  [optional Sender Title]

Return ONLY valid JSON (no markdown fences):
{{"subject": "...", "body": "..."}}

subject: short, 4-7 words, slight variation from original if stage > 1
body: complete email (greeting + content + CTA + sign-off)\
"""

# ---------------------------------------------------------------------------
# Fallback templates (deterministic, no AI)
# ---------------------------------------------------------------------------

_SHORT_NAMES: dict[str, str] = {}


def _short_name(company_name: str) -> str:
    if company_name not in _SHORT_NAMES:
        words = (company_name or "").strip().split()
        _SHORT_NAMES[company_name] = " ".join(words[:2]) if words else "your team"
    return _SHORT_NAMES[company_name]


def _first_name(kp_name: str) -> str:
    parts = (kp_name or "").strip().split()
    return parts[0] if parts else ""


def _signature() -> str:
    raw_name = (SENDER_NAME or "").strip()
    if "|" in raw_name:
        parts = [part.strip() for part in raw_name.split("|") if part.strip()]
        name = parts[0] if parts else "Wayne"
        company = parts[1] if len(parts) > 1 else "OmniSol"
    else:
        name = raw_name or "Wayne"
        company = "OmniSol"
    title = (SENDER_TITLE or "").strip()
    lines = ["Best,", name]
    if title:
        lines.append(title)
    lines.append(company)
    return "\n".join(lines)


_STAGE_SUBJECTS: dict[str, str] = {
    "followup_1": "Re: {original_subject}",
    "followup_2": "A quick follow-up for {short_name}",
    "followup_3": "Closing the loop for {short_name}",
}

_ENGAGEMENT_OPENERS: dict[str, dict[str, str]] = {
    "followup_1": {
        "no_open": "Sharing a quick follow-up in case my earlier note was off-timing.",
        "opened_no_click": "Sending a quick follow-up on my earlier note.",
        "multi_open_no_click": "Adding one brief point to my earlier note.",
        "clicked_no_reply": "Following up with one short note.",
        "unknown": "Sending a quick follow-up on my earlier note.",
    },
    "followup_2": {
        "no_open": "One more brief note in case the timing is better now.",
        "opened_no_click": "Adding one practical point to my earlier message.",
        "multi_open_no_click": "Adding a bit more detail in case useful.",
        "clicked_no_reply": "I thought it might help to add one concrete next step.",
        "unknown": "One more short follow-up in case useful.",
    },
    "followup_3": {
        "no_open": "I'll keep this last note brief.",
        "opened_no_click": "I'll close the loop with one last note.",
        "multi_open_no_click": "One last note from me for now.",
        "clicked_no_reply": "I'll leave this here for now.",
        "unknown": "I'll close the loop with one last note.",
    },
}

_STAGE_VALUE_PROPS: dict[str, str] = {
    "followup_1": (
        "We help solar teams with mounting supply when simpler procurement and smoother delivery matter."
    ),
    "followup_2": (
        "On the practical side, we can support procurement planning, delivery coordination, and spec alignment for upcoming work."
    ),
    "followup_3": "If the fit is not right at the moment, no problem at all.",
}

_CTAS: dict[str, str] = {
    "followup_1": "Happy to share a few details if useful.",
    "followup_2": "If useful, I can send over a few more details.",
    "followup_3": "If timing changes later on, feel free to reach out.",
}


_CALCULATOR_BASE_URL = (
    "https://omnisolglobal.com/calculator"
    "?utm_source=email&utm_medium=followup&utm_campaign=cold_outreach"
)

_CALCULATOR_ANGLES = {"project_delivery", "installation"}


def _calculator_url_for(stage: str, engagement: str, email_angle: str) -> str:
    """Return UTM-tagged URL only for followup_2 + active interest + relevant angle."""
    if (
        stage == "followup_2"
        and engagement == "clicked_no_reply"
        and email_angle in _CALCULATOR_ANGLES
    ):
        return _CALCULATOR_BASE_URL
    return ""


def _build_fallback(candidate: dict) -> dict:
    stage = candidate.get("followup_stage", "followup_1")
    engagement = candidate.get("engagement_status", "unknown")
    company_name = candidate.get("company_name", "")
    kp_name = candidate.get("kp_name", "")
    original_subject = candidate.get("subject", "solar support")

    first = _first_name(kp_name)
    greeting = f"Hi {first}," if first else f"Hello {_short_name(company_name)} team,"

    subj_tpl = _STAGE_SUBJECTS.get(stage, "Re: {original_subject}")
    subject = subj_tpl.format(
        original_subject=original_subject,
        short_name=_short_name(company_name),
    )

    opener = _ENGAGEMENT_OPENERS.get(stage, {}).get(engagement, "Sending a quick follow-up on my earlier note.")
    value_prop = _STAGE_VALUE_PROPS.get(stage, "")
    cta = _CTAS.get(stage, "Happy to share a few details if useful.")

    parts = [f"{greeting}\n\n{opener}"]
    if value_prop:
        parts.append(value_prop)

    email_angle = (candidate.get("email_angle") or "").strip()
    calculator_url = _calculator_url_for(stage, engagement, email_angle)
    if calculator_url:
        parts.append(
            f"We also have a simple mounting sizing tool that may be useful at the early project stage: {calculator_url}"
        )
    elif stage == "followup_1" and email_angle in _CALCULATOR_ANGLES:
        parts.append(
            "We also have a simple mounting sizing tool - happy to share it if useful."
        )

    parts.append(cta)
    parts.append(_signature())

    body = "\n\n".join(parts)
    return {"subject": subject, "body": body}


# ---------------------------------------------------------------------------
# Provider waterfall
# ---------------------------------------------------------------------------

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


def _build_prompt(candidate: dict) -> str:
    stage = candidate.get("followup_stage", "followup_1")
    stage_num = stage.split("_")[-1]
    kp_name = (candidate.get("kp_name") or "").strip()
    engagement = candidate.get("engagement_status", "unknown")
    email_angle = (candidate.get("email_angle") or "").strip()

    _HINTS = {
        "no_open": "no engagement recorded yet",
        "opened_no_click": "some engagement - clarify the value proposition",
        "multi_open_no_click": "recurring engagement - offer more detail",
        "clicked_no_reply": "active interest - offer a concrete next step",
        "unknown": "unknown engagement",
    }
    hint = _HINTS.get(engagement, "unknown engagement")

    calculator_url = _calculator_url_for(stage, engagement, email_angle)

    return _PROMPT.format(
        stage_num=stage_num,
        company_name=candidate.get("company_name", ""),
        kp_name_or_there=kp_name or "the team",
        subject=candidate.get("subject", "solar equipment support"),
        followup_stage=stage,
        engagement_hint=hint,
        email_angle=email_angle or "general_solar",
        calculator_url=calculator_url,
        sender_name=(SENDER_NAME or "").strip() or "Wayne",
        sender_title=(SENDER_TITLE or "").strip(),
    )


from src.workflow_6_email_generation.ai_json_utils import parse_ai_json as _parse_ai_json


def _parse_ai_response(raw: str, context: str = "") -> dict:
    return _parse_ai_json(raw, context=context)


def _call_openrouter(prompt: str, key: str, model: str) -> str:
    resp = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json={"model": model, "messages": [{"role": "user", "content": prompt}], "max_tokens": 400},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


def _call_anthropic(prompt: str, key: str, model: str) -> str:
    import anthropic

    client = anthropic.Anthropic(api_key=key)
    msg = client.messages.create(
        model=model,
        max_tokens=400,
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text


def _call_openai(prompt: str, key: str, model: str) -> str:
    import openai

    client = openai.OpenAI(api_key=key)
    resp = client.chat.completions.create(
        model=model,
        max_tokens=400,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.choices[0].message.content


_CALLERS = {
    "openrouter": _call_openrouter,
    "anthropic": _call_anthropic,
    "openai": _call_openai,
}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def generate_followup(candidate: dict) -> tuple[dict, str, str]:
    """
    Generate a follow-up draft for one candidate.

    Returns (draft_dict, generation_mode, generation_source) where:
        draft_dict = {"subject": "...", "body": "..."}
        generation_mode = "ai" | "rule"
        generation_source = provider name | "fallback_template"
    """
    provider = _get_provider()
    if provider:
        pname, key, model = provider
        try:
            raw = _CALLERS[pname](_build_prompt(candidate), key, model)
            draft = _parse_ai_response(raw, context=candidate.get("company_name", "?"))
            if draft.get("subject") and draft.get("body"):
                time.sleep(AI_RATE_DELAY)
                return draft, "ai", pname
        except Exception as exc:
            print(
                f"[Workflow 8]   AI error ({pname}) for "
                f"{candidate.get('company_name', '?')}: {exc}"
            )
    draft = _build_fallback(candidate)
    return draft, "rule", "fallback_template"
