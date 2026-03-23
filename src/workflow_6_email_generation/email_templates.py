# Workflow 6: Email Generation - Rule-Based Templates
# Deterministic subject, opening, body, CTA, and signature generation.

import re

from config.settings import SENDER_NAME, SENDER_TITLE

# ---------------------------------------------------------------------------
# Subject line
# ---------------------------------------------------------------------------

_SUBJECT_MAP: dict[str, str] = {
    "storage": "Quick question on storage projects",
    "installation": "Quick question on your installs",
    "supply": "Quick question for {short_name}",
    # legacy angle values (kept for backward compatibility)
    "Mention battery storage support": "Storage support for solar installers",
    "Mention commercial installation scalability": "Commercial solar install support",
    "Mention support for larger-scale project execution": "Larger-scale solar project support",
    "Mention support for growing installation teams": "Support for growing install teams",
    "Mention support for residential solar operations": "Residential solar support",
}
_SUBJECT_FALLBACK = "Quick question about your solar work"

_SPAM = re.compile(r"\b(free|guarantee|cheapest|urgent|act now)\b", re.IGNORECASE)


def _short_name(company_name: str) -> str:
    """First two words of the company name, stripping bilingual | suffix."""
    s = (company_name or "").strip()
    if "|" in s:
        s = s.split("|")[0].strip()
    words = s.split()
    return " ".join(words[:2]) if words else "your team"


def build_subject(record: dict) -> str:
    angle = record.get("email_angle", "")
    template = _SUBJECT_MAP.get(angle, _SUBJECT_FALLBACK)
    subject = template.format(short_name=_short_name(record.get("company_name", "")))
    return _SPAM.sub("", subject).strip()


# ---------------------------------------------------------------------------
# Opening line
# ---------------------------------------------------------------------------

_ANGLE_OPENERS: dict[str, str] = {
    "storage": "I saw {company} is active in both solar and storage work.",
    "installation": "I saw {company} is active in {market}solar installation work.",
    "supply": "I saw {company} is active in solar projects.",
    # legacy
    "Mention battery storage support": (
        "I saw your team is active in both solar and battery storage work."
    ),
    "Mention commercial installation scalability": (
        "I saw {company} is active in commercial solar installation."
    ),
    "Mention support for larger-scale project execution": (
        "I saw your team appears to handle larger-scale solar project work."
    ),
    "Mention support for growing installation teams": (
        "I saw {company} is building out its solar installation operations."
    ),
    "Mention support for residential solar operations": (
        "I saw your team is focused on residential solar installation."
    ),
}
_OPENER_FALLBACK = "I saw {company} is active in solar installation."


def _market_label(market_focus: str) -> str:
    m = (market_focus or "").strip().lower()
    if m in ("mixed", ""):
        return ""
    if m == "utility-scale":
        return "utility-scale "
    return m + " "


def build_opening_line(record: dict) -> str:
    angle = record.get("email_angle", "")
    company = _short_name(record.get("company_name") or "") or "your team"
    market = _market_label(record.get("market_focus", ""))
    opener = _ANGLE_OPENERS.get(angle, _OPENER_FALLBACK)
    return opener.format(company=company, market=market)


# ---------------------------------------------------------------------------
# Body by angle
# ---------------------------------------------------------------------------

_RELEVANCE_MAP: dict[str, str] = {
    "storage": (
        "We work with solar teams on mounting and hardware support where battery integration is part of the project mix."
    ),
    "installation": (
        "We support installation teams with mounting supply when predictable delivery and straightforward site execution matter."
    ),
    "supply": (
        "We work with solar companies on mounting supply and project support when they need a more reliable sourcing option."
    ),
    # legacy
    "Mention battery storage support": (
        "We work with solar installers on battery storage integration where dependable supply and practical project support matter."
    ),
    "Mention commercial installation scalability": (
        "We support commercial solar installers with mounting supply and practical project support."
    ),
    "Mention support for larger-scale project execution": (
        "We work with EPCs and developers on procurement support for larger-scale solar projects."
    ),
    "Mention support for growing installation teams": (
        "We help growing installation teams with more dependable mounting supply and simpler procurement."
    ),
    "Mention support for residential solar operations": (
        "We support residential solar installers with equipment sourcing and reliable logistics."
    ),
}
_RELEVANCE_FALLBACK = (
    "We work with solar installation companies on mounting supply and project support."
)

_VALUE_PROP_MAP: dict[str, str] = {
    "storage": "The focus is making procurement a bit easier when project requirements vary.",
    "installation": "The focus is helping teams keep installs moving without overcomplicating procurement.",
    "supply": "The focus is fewer sourcing delays and a simpler handoff into execution.",
}
_VALUE_PROP_FALLBACK = "The focus is fewer sourcing delays and a smoother delivery process."

_CTAS = [
    "Happy to share a few details if useful.",
    "Open to a brief exchange if relevant?",
    "If useful, I can send over a few details.",
]


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


def build_email_body(record: dict, opening_line: str) -> str:
    angle = record.get("email_angle", "")
    relevance = _RELEVANCE_MAP.get(angle, _RELEVANCE_FALLBACK)
    value_prop = _VALUE_PROP_MAP.get(angle, _VALUE_PROP_FALLBACK)
    cta = _CTAS[int(record.get("lead_score") or 0) % len(_CTAS)]
    kp_name = (record.get("kp_name") or "").strip()
    raw_company = (record.get("company_name") or "").strip()
    company = raw_company.split("|")[0].strip() if "|" in raw_company else raw_company

    if kp_name:
        greeting = f"Hi {kp_name.split()[0]},"
    else:
        short = " ".join(company.split()[:3]) if company else "team"
        greeting = f"Hello {short} team,"

    return (
        f"{greeting}\n\n"
        f"{opening_line}\n\n"
        f"{relevance} {value_prop}\n\n"
        f"{cta}\n\n"
        f"{_signature()}"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def word_count(text: str) -> int:
    return len(text.split())


def trim_to_limit(text: str, max_words: int = 180) -> str:
    words = text.split()
    if len(words) <= max_words:
        return text
    trimmed = " ".join(words[:max_words])
    last_period = trimmed.rfind(".")
    return trimmed[: last_period + 1] if last_period > 0 else trimmed


# ---------------------------------------------------------------------------
# Full rule-based builder
# ---------------------------------------------------------------------------

def build_rule_based_email(record: dict) -> dict:
    subject = build_subject(record)
    opening_line = build_opening_line(record)
    body = trim_to_limit(build_email_body(record, opening_line))
    return {
        "subject": subject,
        "opening_line": opening_line,
        "email_body": body,
        "body": body,
    }
