# Workflow 6.5: Email Quality Scoring — Main Scorer
# Scores emails via AI (OpenRouter / Anthropic / OpenAI) with rule-based fallback.

import csv
import time
from pathlib import Path

import requests

from config.settings import (
    ANTHROPIC_API_KEY, OPENAI_API_KEY, OPENROUTER_API_KEY,
    LLM_PROVIDER, LLM_MODEL,
    SCORED_EMAILS_FILE, SEND_QUEUE_FILE, REJECTED_EMAILS_FILE,
)
from src.workflow_6_5_email_quality.quality_merge import load_generated_emails
from src.workflow_6_5_email_quality.quality_rules import (
    score_personalization, score_relevance, score_spam_risk,
    compute_overall_score, determine_approval_status, rule_score_email,
)

AI_MODEL      = "claude-haiku-4-5-20251001"
AI_RATE_DELAY = 0.5

OUTPUT_FIELDS = [
    "company_name", "website", "place_id",
    "city", "region", "country", "source_location",
    "kp_name", "kp_title", "kp_email",
    "contact_name", "contact_title", "contact_email",
    "send_target_type", "contact_source",
    "named_contact_available", "generic_contact_available",
    "contact_quality", "generic_only",
    "company_type", "market_focus", "lead_score",
    "subject", "opening_line", "email_body",
    "email_angle", "generation_mode", "generation_source",
    "personalization_score", "relevance_score", "spam_risk_score",
    "overall_score", "approval_status", "review_notes",
    "scoring_mode", "scoring_source",
]

_PROMPT_TMPL = """\
You are a B2B cold outreach reviewer.
Evaluate this email draft for a {company_type} company named {company_name}.

Subject: {subject}
Opening line: {opening_line}
Email body:
{email_body}

Email angle: {email_angle}

Return ONLY valid JSON (no markdown, no explanation):
{{"personalization_score": 0-100, "relevance_score": 0-100, "spam_risk_score": 0-100, "review_notes": ["..."]}}

Rules: no invented information; strict JSON only.\
"""


def _get_provider() -> tuple[str, str] | None:
    if LLM_PROVIDER == "openrouter" and OPENROUTER_API_KEY:
        return ("openrouter", OPENROUTER_API_KEY)
    if LLM_PROVIDER == "anthropic" and ANTHROPIC_API_KEY:
        return ("anthropic", ANTHROPIC_API_KEY)
    if LLM_PROVIDER == "openai" and OPENAI_API_KEY:
        return ("openai", OPENAI_API_KEY)
    if ANTHROPIC_API_KEY:
        return ("anthropic", ANTHROPIC_API_KEY)
    if OPENAI_API_KEY:
        return ("openai", OPENAI_API_KEY)
    return None


def _build_prompt(record: dict) -> str:
    return _PROMPT_TMPL.format(
        company_type=record.get("company_type", ""),
        company_name=record.get("company_name", ""),
        subject=record.get("subject", ""),
        opening_line=record.get("opening_line", ""),
        email_body=record.get("email_body", ""),
        email_angle=record.get("email_angle", ""),
    )


def _parse_ai_response(raw: str, context: str = "") -> dict:
    from src.workflow_6_email_generation.ai_json_utils import (
        parse_ai_json, validate_required_keys,
    )
    parsed = parse_ai_json(raw, context=context)
    validate_required_keys(
        parsed,
        ["personalization_score", "relevance_score", "spam_risk_score"],
        context=context,
    )
    return parsed


def _call_openrouter(prompt: str, key: str) -> str:
    model = LLM_MODEL or "anthropic/claude-3-haiku"
    resp = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json={"model": model, "messages": [{"role": "user", "content": prompt}]},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


def _call_anthropic(prompt: str, key: str) -> str:
    import anthropic
    client = anthropic.Anthropic(api_key=key)
    message = client.messages.create(
        model=AI_MODEL,
        max_tokens=256,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text


def _call_openai(prompt: str, key: str) -> str:
    import openai
    client = openai.OpenAI(api_key=key)
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        max_tokens=256,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.choices[0].message.content


def _score_with_ai(record: dict, provider: str, key: str) -> dict:
    context = record.get("company_name", "?")
    prompt  = _build_prompt(record)
    if provider == "openrouter":
        raw = _call_openrouter(prompt, key)
    elif provider == "anthropic":
        raw = _call_anthropic(prompt, key)
    else:
        raw = _call_openai(prompt, key)

    parsed  = _parse_ai_response(raw, context=context)
    pers    = int(parsed.get("personalization_score", 60))
    rel     = int(parsed.get("relevance_score", 60))
    spam    = int(parsed.get("spam_risk_score", 20))
    overall = compute_overall_score(pers, rel, spam)

    ai_notes = parsed.get("review_notes", [])
    aug = dict(record)
    aug["_personalization_score"] = pers
    aug["_relevance_score"]       = rel
    status, rule_notes = determine_approval_status(overall, spam, aug)
    all_notes = list(ai_notes) + [n for n in rule_notes if n not in ai_notes]

    return {
        "personalization_score": pers,
        "relevance_score":       rel,
        "spam_risk_score":       spam,
        "overall_score":         overall,
        "approval_status":       status,
        "review_notes":          ";".join(all_notes),
    }


def score_email(record: dict, counters: dict | None = None) -> dict:
    """
    Score one email.  Optional `counters` dict is updated in-place:
      ai_ok, ai_parse_fail, ai_timeout, ai_http_error, fallback_used
    """
    from src.workflow_6_email_generation.ai_json_utils import classify_parse_failure
    import requests as _requests

    if counters is None:
        counters = {}

    provider_info = _get_provider()
    scores: dict = {}
    mode   = "rule"
    source = "rule"

    if provider_info:
        provider, key = provider_info
        try:
            scores = _score_with_ai(record, provider, key)
            mode   = "ai"
            source = provider
            counters["ai_ok"] = counters.get("ai_ok", 0) + 1
            time.sleep(AI_RATE_DELAY)
        except _requests.exceptions.Timeout as exc:
            counters["ai_timeout"] = counters.get("ai_timeout", 0) + 1
            print(
                f"[Workflow 6.5]   timeout ({provider}) for "
                f"{record.get('company_name', '?')} — using rule fallback"
            )
        except Exception as exc:
            fc = classify_parse_failure(exc)
            counter_key = f"ai_{fc}" if fc != "unknown" else "ai_parse_fail"
            counters[counter_key] = counters.get(counter_key, 0) + 1
            print(
                f"[Workflow 6.5]   AI error [{fc}] ({provider}) for "
                f"{record.get('company_name', '?')}: {str(exc)[:120]}"
            )

    if not scores:
        scores = rule_score_email(record)
        counters["fallback_used"] = counters.get("fallback_used", 0) + 1

    result = {k: record.get(k, "") for k in OUTPUT_FIELDS}
    result.update(scores)
    result["scoring_mode"]   = mode
    result["scoring_source"] = source
    return result


def save_csv(records: list[dict], path: Path, fields: list[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)


def run(limit: int = 0) -> list[dict]:
    records = load_generated_emails(limit=limit)
    if not records:
        print("[Workflow 6.5] No emails to score — writing empty output files.")
        save_csv([], SCORED_EMAILS_FILE,   OUTPUT_FIELDS)
        save_csv([], SEND_QUEUE_FILE,      OUTPUT_FIELDS)
        save_csv([], REJECTED_EMAILS_FILE, OUTPUT_FIELDS)
        return []

    provider_info = _get_provider()
    if provider_info:
        print(f"[Workflow 6.5] Scoring {len(records)} emails — AI mode ({provider_info[0]})")
    else:
        print(f"[Workflow 6.5] Scoring {len(records)} emails — rule-based fallback")

    scored: list[dict] = []
    counters: dict = {}

    for i, record in enumerate(records, 1):
        name = record.get("company_name") or record.get("website", f"record {i}")
        print(f"[Workflow 6.5] ({i}/{len(records)}) {name}")
        result = score_email(record, counters=counters)
        scored.append(result)

    approved      = [r for r in scored if r["approval_status"] == "approved"]
    manual_review = [r for r in scored if r["approval_status"] == "manual_review"]
    rejected      = [r for r in scored if r["approval_status"] == "rejected"]

    save_csv(scored,   SCORED_EMAILS_FILE,   OUTPUT_FIELDS)
    save_csv(approved, SEND_QUEUE_FILE,      OUTPUT_FIELDS)
    save_csv(rejected, REJECTED_EMAILS_FILE, OUTPUT_FIELDS)

    ai_ok       = counters.get("ai_ok",        0)
    fallback    = counters.get("fallback_used", 0)
    timeouts    = counters.get("ai_timeout",    0)
    parse_fails = sum(v for k, v in counters.items()
                      if k.startswith("ai_") and k not in ("ai_ok", "ai_timeout"))

    print(
        f"\n[Workflow 6.5] Complete:\n"
        f"  Input          : {len(records)}\n"
        f"  Approved       : {len(approved)}\n"
        f"  Manual review  : {len(manual_review)}\n"
        f"  Rejected       : {len(rejected)}\n"
        f"  AI scored      : {ai_ok}\n"
        f"  AI parse fails : {parse_fails}\n"
        f"  AI timeouts    : {timeouts}\n"
        f"  Rule fallback  : {fallback}"
    )
    print(f"[Workflow 6.5] → {SCORED_EMAILS_FILE}")
    print(f"[Workflow 6.5] → {SEND_QUEUE_FILE} ({len(approved)} approved)")
    print(f"[Workflow 6.5] → {REJECTED_EMAILS_FILE} ({len(rejected)} rejected)")
    return scored


if __name__ == "__main__":
    run()
