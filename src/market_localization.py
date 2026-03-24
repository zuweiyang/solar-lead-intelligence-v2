from __future__ import annotations

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

BRAZIL_SEARCH_KEYWORDS: tuple[str, ...] = (
    "energia solar",
    "empresa de energia solar",
    "instalador solar",
    "energia fotovoltaica",
    "empresa de energia fotovoltaica",
    "integrador solar",
    "EPC solar",
    "integrador de armazenamento de energia",
    "integrador BESS",
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

BRAZIL_CRAWL_TARGET_PATHS: tuple[str, ...] = (
    "/contato",
    "/fale-conosco",
    "/orcamento",
    "/solicite-orcamento",
    "/sobre",
    "/servicos",
    "/projetos",
    "/contact",
    "/about",
)

DEFAULT_GENERIC_GUESS_LOCAL_PARTS: tuple[str, ...] = (
    "info",
    "sales",
    "contact",
)

BRAZIL_GENERIC_GUESS_LOCAL_PARTS: tuple[str, ...] = (
    "contato",
    "comercial",
    "vendas",
    "atendimento",
    "info",
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

BRAZIL_GENERIC_MAILBOX_LOCAL_PARTS: tuple[str, ...] = (
    "contato",
    "comercial",
    "vendas",
    "atendimento",
    "orcamento",
    "suporte",
    "financeiro",
)

DEFAULT_EMAIL_LANGUAGE = "en"
BRAZIL_EMAIL_LANGUAGE = "pt-BR"


def _country_key(country: str) -> str:
    return (country or "").strip().lower()


def _merge_unique(*groups: tuple[str, ...]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for item in group:
            key = item.lower()
            if key in seen:
                continue
            seen.add(key)
            merged.append(item)
    return merged


def get_search_keywords(country: str) -> list[str]:
    if _country_key(country) == "brazil":
        return list(BRAZIL_SEARCH_KEYWORDS)
    return list(DEFAULT_SEARCH_KEYWORDS)


def get_crawl_target_paths(country: str) -> list[str]:
    if _country_key(country) == "brazil":
        return _merge_unique(BRAZIL_CRAWL_TARGET_PATHS, DEFAULT_CRAWL_TARGET_PATHS)
    return list(DEFAULT_CRAWL_TARGET_PATHS)


def get_generic_guess_local_parts(country: str) -> tuple[str, ...]:
    if _country_key(country) == "brazil":
        return BRAZIL_GENERIC_GUESS_LOCAL_PARTS
    return DEFAULT_GENERIC_GUESS_LOCAL_PARTS


def get_generic_mailbox_local_parts(country: str = "") -> tuple[str, ...]:
    if _country_key(country) == "brazil":
        return tuple(_merge_unique(DEFAULT_GENERIC_MAILBOX_LOCAL_PARTS, BRAZIL_GENERIC_MAILBOX_LOCAL_PARTS))
    return DEFAULT_GENERIC_MAILBOX_LOCAL_PARTS


def get_email_language(country: str = "") -> str:
    if _country_key(country) == "brazil":
        return BRAZIL_EMAIL_LANGUAGE
    return DEFAULT_EMAIL_LANGUAGE


def get_email_language_name(country: str = "") -> str:
    if get_email_language(country) == BRAZIL_EMAIL_LANGUAGE:
        return "Brazilian Portuguese"
    return "English"
