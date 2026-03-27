from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketProfile:
    country: str
    search_keywords: tuple[str, ...]
    crawl_target_paths: tuple[str, ...]
    crawl_home_hints: tuple[str, ...]
    generic_guess_local_parts: tuple[str, ...]
    generic_mailbox_local_parts: tuple[str, ...]
    email_language: str
    email_language_name: str
    crawl_accept_language: str


def _country_key(country: str) -> str:
    return (country or "").strip().lower()


def _merge_unique(*groups: tuple[str, ...]) -> tuple[str, ...]:
    merged: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for item in group:
            key = item.lower()
            if key in seen:
                continue
            seen.add(key)
            merged.append(item)
    return tuple(merged)


DEFAULT_SEARCH_KEYWORDS: tuple[str, ...] = (
    "solar installer",
    "solar contractor",
    "commercial solar installer",
    "solar developer",
    "solar energy company",
    "solar EPC",
    "energy storage integrator",
    "BESS integrator",
)

DEFAULT_CRAWL_TARGET_PATHS: tuple[str, ...] = (
    "/contact",
    "/contact-us",
    "/about",
    "/about-us",
    "/services",
    "/projects",
    "/products",
)

DEFAULT_CRAWL_HOME_HINTS: tuple[str, ...] = ()

DEFAULT_GENERIC_GUESS_LOCAL_PARTS: tuple[str, ...] = (
    "info",
    "sales",
    "contact",
)

DEFAULT_GENERIC_MAILBOX_LOCAL_PARTS: tuple[str, ...] = (
    "info",
    "contact",
    "contacts",
    "hello",
    "hi",
    "enquiries",
    "enquiry",
    "enquire",
    "inquiry",
    "inquiries",
    "sales",
    "admin",
    "administration",
    "office",
    "general",
    "mail",
    "email",
    "webmaster",
    "support",
    "help",
    "service",
    "services",
    "team",
    "company",
    "business",
    "noreply",
    "no-reply",
    "accounts",
    "billing",
    "reception",
    "marketing",
    "news",
    "newsletter",
    "media",
    "hr",
    "jobs",
    "careers",
    "recruitment",
)

DEFAULT_EMAIL_LANGUAGE = "en"
DEFAULT_EMAIL_LANGUAGE_NAME = "English"
DEFAULT_CRAWL_ACCEPT_LANGUAGE = "en-US,en;q=0.9"

DEFAULT_MARKET_PROFILE = MarketProfile(
    country="default",
    search_keywords=DEFAULT_SEARCH_KEYWORDS,
    crawl_target_paths=DEFAULT_CRAWL_TARGET_PATHS,
    crawl_home_hints=DEFAULT_CRAWL_HOME_HINTS,
    generic_guess_local_parts=DEFAULT_GENERIC_GUESS_LOCAL_PARTS,
    generic_mailbox_local_parts=DEFAULT_GENERIC_MAILBOX_LOCAL_PARTS,
    email_language=DEFAULT_EMAIL_LANGUAGE,
    email_language_name=DEFAULT_EMAIL_LANGUAGE_NAME,
    crawl_accept_language=DEFAULT_CRAWL_ACCEPT_LANGUAGE,
)


BRAZIL_SEARCH_KEYWORDS: tuple[str, ...] = (
    "integrador solar",
    "empresa EPC solar",
    "empresa de energia solar comercial",
    "distribuidor fotovoltaico",
    "estrutura fotovoltaica",
    "fabricante de estrutura fotovoltaica",
)

BRAZIL_CRAWL_HOME_HINTS: tuple[str, ...] = (
    "/pt-br",
    "/pt",
    "/br",
)

BRAZIL_CRAWL_TARGET_PATHS: tuple[str, ...] = (
    "/contato",
    "/fale-conosco",
    "/orcamento",
    "/solicite-orcamento",
    "/sobre",
    "/servicos",
    "/projetos",
)

BRAZIL_GENERIC_GUESS_LOCAL_PARTS: tuple[str, ...] = (
    "contato",
    "comercial",
    "vendas",
    "atendimento",
    "info",
)

BRAZIL_GENERIC_MAILBOX_LOCAL_PARTS: tuple[str, ...] = (
    "contato",
    "comercial",
    "vendas",
    "atendimento",
    "orcamento",
    "suporte",
    "financeiro",
)

BRAZIL_EMAIL_LANGUAGE = "pt-BR"
BRAZIL_EMAIL_LANGUAGE_NAME = "Brazilian Portuguese"
BRAZIL_CRAWL_ACCEPT_LANGUAGE = "pt-BR,pt;q=0.9,en;q=0.6"

BRAZIL_MARKET_PROFILE = MarketProfile(
    country="Brazil",
    search_keywords=BRAZIL_SEARCH_KEYWORDS,
    crawl_target_paths=_merge_unique(BRAZIL_CRAWL_TARGET_PATHS, DEFAULT_CRAWL_TARGET_PATHS),
    crawl_home_hints=BRAZIL_CRAWL_HOME_HINTS,
    generic_guess_local_parts=BRAZIL_GENERIC_GUESS_LOCAL_PARTS,
    generic_mailbox_local_parts=_merge_unique(
        DEFAULT_GENERIC_MAILBOX_LOCAL_PARTS,
        BRAZIL_GENERIC_MAILBOX_LOCAL_PARTS,
    ),
    email_language=BRAZIL_EMAIL_LANGUAGE,
    email_language_name=BRAZIL_EMAIL_LANGUAGE_NAME,
    crawl_accept_language=BRAZIL_CRAWL_ACCEPT_LANGUAGE,
)

MARKET_PROFILES: dict[str, MarketProfile] = {
    "brazil": BRAZIL_MARKET_PROFILE,
}


def get_market_profile(country: str = "") -> MarketProfile:
    return MARKET_PROFILES.get(_country_key(country), DEFAULT_MARKET_PROFILE)


def get_search_keywords(country: str) -> list[str]:
    return list(get_market_profile(country).search_keywords)


def get_crawl_target_paths(country: str) -> list[str]:
    return list(get_market_profile(country).crawl_target_paths)


def get_crawl_home_hints(country: str) -> list[str]:
    return list(get_market_profile(country).crawl_home_hints)


def get_generic_guess_local_parts(country: str) -> tuple[str, ...]:
    return get_market_profile(country).generic_guess_local_parts


def get_generic_mailbox_local_parts(country: str = "") -> tuple[str, ...]:
    return get_market_profile(country).generic_mailbox_local_parts


def get_email_language(country: str = "") -> str:
    return get_market_profile(country).email_language


def get_email_language_name(country: str = "") -> str:
    return get_market_profile(country).email_language_name


def get_crawl_accept_language(country: str = "") -> str:
    return get_market_profile(country).crawl_accept_language
