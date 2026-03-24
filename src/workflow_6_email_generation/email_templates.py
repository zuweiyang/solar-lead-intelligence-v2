# Workflow 6: Email Generation - Rule-Based Templates
# Deterministic subject, opening, body, CTA, and signature generation.

import re

from config.settings import SENDER_NAME, SENDER_TITLE
from src.market_localization import get_email_language

# ---------------------------------------------------------------------------
# Subject line
# ---------------------------------------------------------------------------

_EN_SUBJECT_MAP: dict[str, str] = {
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
_EN_SUBJECT_FALLBACK = "Quick question about your solar work"

_PT_SUBJECT_MAP: dict[str, str] = {
    "storage": "Pergunta rápida sobre projetos com armazenamento",
    "installation": "Pergunta rápida sobre suas instalações",
    "supply": "Pergunta rápida para a equipe da {short_name}",
    "Mention battery storage support": "Suporte para integradores solares",
    "Mention commercial installation scalability": "Suporte para instalações solares comerciais",
    "Mention support for larger-scale project execution": "Suporte para projetos solares de maior porte",
    "Mention support for growing installation teams": "Suporte para equipes de instalação em expansão",
    "Mention support for residential solar operations": "Suporte para operações solares residenciais",
}
_PT_SUBJECT_FALLBACK = "Pergunta rápida sobre seu trabalho em energia solar"

_SPAM = re.compile(r"\b(free|guarantee|cheapest|urgent|act now|gratis|grátis|urgente)\b", re.IGNORECASE)


def _short_name(company_name: str) -> str:
    """First two words of the company name, stripping bilingual | suffix."""
    s = (company_name or "").strip()
    if "|" in s:
        s = s.split("|")[0].strip()
    words = s.split()
    return " ".join(words[:2]) if words else "your team"


def _is_portuguese(record: dict) -> bool:
    return get_email_language(record.get("country", "")) == "pt-BR"


def build_subject(record: dict) -> str:
    angle = record.get("email_angle", "")
    if _is_portuguese(record):
        template = _PT_SUBJECT_MAP.get(angle, _PT_SUBJECT_FALLBACK)
    else:
        template = _EN_SUBJECT_MAP.get(angle, _EN_SUBJECT_FALLBACK)
    subject = template.format(short_name=_short_name(record.get("company_name", "")))
    return _SPAM.sub("", subject).strip()


# ---------------------------------------------------------------------------
# Opening line
# ---------------------------------------------------------------------------

_EN_ANGLE_OPENERS: dict[str, str] = {
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
_EN_OPENER_FALLBACK = "I saw {company} is active in solar installation."

_PT_ANGLE_OPENERS: dict[str, str] = {
    "storage": "Vi que {company} atua em projetos solares com armazenamento.",
    "installation": "Vi que {company} atua com instalações solares {market}.",
    "supply": "Vi que {company} atua em projetos de energia solar.",
    "Mention battery storage support": (
        "Vi que sua equipe atua com projetos solares e armazenamento."
    ),
    "Mention commercial installation scalability": (
        "Vi que {company} atua com instalação solar comercial."
    ),
    "Mention support for larger-scale project execution": (
        "Vi que sua equipe participa de projetos solares de maior porte."
    ),
    "Mention support for growing installation teams": (
        "Vi que {company} está ampliando sua operação de instalação solar."
    ),
    "Mention support for residential solar operations": (
        "Vi que sua equipe está focada em instalação solar residencial."
    ),
}
_PT_OPENER_FALLBACK = "Vi que {company} atua com instalação solar."


def _market_label(market_focus: str) -> str:
    m = (market_focus or "").strip().lower()
    if m in ("mixed", ""):
        return ""
    if m == "utility-scale":
        return "utility-scale "
    return m + " "


def _market_label_pt(market_focus: str) -> str:
    m = (market_focus or "").strip().lower()
    mapping = {
        "residential": "residenciais",
        "commercial": "comerciais",
        "utility-scale": "de grande porte",
        "industrial": "industriais",
    }
    if m in ("mixed", ""):
        return ""
    return mapping.get(m, m)


def build_opening_line(record: dict) -> str:
    angle = record.get("email_angle", "")
    company = _short_name(record.get("company_name") or "") or "your team"
    if _is_portuguese(record):
        market = _market_label_pt(record.get("market_focus", ""))
        opener = _PT_ANGLE_OPENERS.get(angle, _PT_OPENER_FALLBACK)
    else:
        market = _market_label(record.get("market_focus", ""))
        opener = _EN_ANGLE_OPENERS.get(angle, _EN_OPENER_FALLBACK)
    return opener.format(company=company, market=market)


# ---------------------------------------------------------------------------
# Body by angle
# ---------------------------------------------------------------------------

_EN_RELEVANCE_MAP: dict[str, str] = {
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
_EN_RELEVANCE_FALLBACK = (
    "We work with solar installation companies on mounting supply and project support."
)

_PT_RELEVANCE_MAP: dict[str, str] = {
    "storage": (
        "Trabalhamos com equipes solares no fornecimento de estruturas e hardware quando o projeto inclui integração com armazenamento."
    ),
    "installation": (
        "Apoiamos equipes de instalação com fornecimento de estruturas quando previsibilidade de entrega e execução em campo fazem diferença."
    ),
    "supply": (
        "Trabalhamos com empresas solares no fornecimento de estruturas e apoio ao projeto quando elas precisam de uma opção de sourcing mais confiável."
    ),
    "Mention battery storage support": (
        "Trabalhamos com integradores solares em projetos com armazenamento quando fornecimento estável e suporte prático fazem diferença."
    ),
    "Mention commercial installation scalability": (
        "Apoiamos instaladores solares comerciais com fornecimento de estruturas e suporte prático ao projeto."
    ),
    "Mention support for larger-scale project execution": (
        "Trabalhamos com EPCs e desenvolvedores no suporte de compras para projetos solares de maior porte."
    ),
    "Mention support for growing installation teams": (
        "Apoiamos equipes de instalação em crescimento com fornecimento mais previsível e compras mais simples."
    ),
    "Mention support for residential solar operations": (
        "Apoiamos instaladores solares residenciais com sourcing de equipamentos e logística mais confiável."
    ),
}
_PT_RELEVANCE_FALLBACK = (
    "Trabalhamos com empresas de instalação solar no fornecimento de estruturas e apoio ao projeto."
)

_EN_VALUE_PROP_MAP: dict[str, str] = {
    "storage": "The focus is making procurement a bit easier when project requirements vary.",
    "installation": "The focus is helping teams keep installs moving without overcomplicating procurement.",
    "supply": "The focus is fewer sourcing delays and a simpler handoff into execution.",
}
_EN_VALUE_PROP_FALLBACK = "The focus is fewer sourcing delays and a smoother delivery process."

_PT_VALUE_PROP_MAP: dict[str, str] = {
    "storage": "A ideia é simplificar um pouco as compras quando os requisitos do projeto variam.",
    "installation": "A ideia é ajudar as equipes a manter as instalações em andamento sem complicar as compras.",
    "supply": "A ideia é reduzir atrasos de sourcing e facilitar a passagem para a execução.",
}
_PT_VALUE_PROP_FALLBACK = "A ideia é reduzir atrasos de sourcing e tornar a entrega mais previsível."

_EN_CTAS = [
    "Happy to share a few details if useful.",
    "Open to a brief exchange if relevant?",
    "If useful, I can send over a few details.",
]
_PT_CTAS = [
    "Se fizer sentido, posso compartilhar alguns detalhes.",
    "Aberto a uma breve troca se for relevante?",
    "Se for útil, posso enviar algumas informações.",
]


def _signature(record: dict) -> str:
    raw_name = (SENDER_NAME or "").strip()
    if "|" in raw_name:
        parts = [part.strip() for part in raw_name.split("|") if part.strip()]
        name = parts[0] if parts else "Wayne"
        company = parts[1] if len(parts) > 1 else "OmniSol"
    else:
        name = raw_name or "Wayne"
        company = "OmniSol"
    title = (SENDER_TITLE or "").strip()
    lines = ["Atenciosamente," if _is_portuguese(record) else "Best,", name]
    if title:
        lines.append(title)
    lines.append(company)
    return "\n".join(lines)


def build_email_body(record: dict, opening_line: str) -> str:
    angle = record.get("email_angle", "")
    if _is_portuguese(record):
        relevance = _PT_RELEVANCE_MAP.get(angle, _PT_RELEVANCE_FALLBACK)
        value_prop = _PT_VALUE_PROP_MAP.get(angle, _PT_VALUE_PROP_FALLBACK)
        ctas = _PT_CTAS
    else:
        relevance = _EN_RELEVANCE_MAP.get(angle, _EN_RELEVANCE_FALLBACK)
        value_prop = _EN_VALUE_PROP_MAP.get(angle, _EN_VALUE_PROP_FALLBACK)
        ctas = _EN_CTAS
    cta = ctas[int(record.get("lead_score") or 0) % len(ctas)]
    kp_name = (record.get("kp_name") or "").strip()
    raw_company = (record.get("company_name") or "").strip()
    company = raw_company.split("|")[0].strip() if "|" in raw_company else raw_company

    if kp_name:
        greeting = f"Olá {kp_name.split()[0]}," if _is_portuguese(record) else f"Hi {kp_name.split()[0]},"
    else:
        short = " ".join(company.split()[:3]) if company else "team"
        greeting = f"Olá equipe da {short}," if _is_portuguese(record) else f"Hello {short} team,"

    return (
        f"{greeting}\n\n"
        f"{opening_line}\n\n"
        f"{relevance} {value_prop}\n\n"
        f"{cta}\n\n"
        f"{_signature(record)}"
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
